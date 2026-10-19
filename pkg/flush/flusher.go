// pkg/flush/flusher.go —— Write-Behind 分片落库
package flush

import (
	"context"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"game_study/pkg/log"
	"game_study/pkg/metrics"
)

// PersistFunc 实际写库函数（返回 nil 表示成功）。
type PersistFunc func(ctx context.Context, playerID int64) error

// Flusher 10 分片分时落库 worker。
type Flusher struct {
	shards   []*shard
	logger   *log.Logger
	interval time.Duration
	stagger  time.Duration
	wg       sync.WaitGroup
	stopCh   chan struct{}
	stopOnce sync.Once
}

// DeleteFunc 批量物理删除（12.4.5 墓碑）。ids 已是同一张表的主键集合。
type DeleteFunc func(ctx context.Context, table string, ids []int64) error

// Config 配置。
type Config struct {
	ShardCount int
	Interval   time.Duration
	Stagger    time.Duration
	BatchRows  int
	// DeleteFn 墓碑执行器；为空时删除标记会被忽略（仅告警），不影响正常落库
	DeleteFn DeleteFunc
}

// New 创建 Flusher。
func New(cfg Config, fn PersistFunc, logger *log.Logger) *Flusher {
	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 10
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.Stagger <= 0 {
		cfg.Stagger = cfg.Interval / time.Duration(cfg.ShardCount)
	}
	// stagger 必须小于 interval，否则同一轮里多个分片会挤在同一时刻，
	// 失去「分散轮询」的意义；配置写错时回退到均分。
	if cfg.Stagger >= cfg.Interval {
		cfg.Stagger = cfg.Interval / time.Duration(cfg.ShardCount)
	}
	f := &Flusher{
		shards:   make([]*shard, cfg.ShardCount),
		logger:   logger,
		interval: cfg.Interval,
		stagger:  cfg.Stagger,
		stopCh:   make(chan struct{}),
	}
	for i := range f.shards {
		f.shards[i] = newShard(i, fn, cfg.DeleteFn, cfg.BatchRows)
	}
	return f
}

// Start 启动所有分片 worker。
//
// 偏移使用配置里的 stagger（而不是 interval/分片数硬算）：改配置就能改错峰，
// 否则配置项永远不生效。
func (f *Flusher) Start(ctx context.Context) {
	for i, sh := range f.shards {
		f.wg.Add(1)
		go f.runShard(ctx, sh, time.Duration(i)*f.stagger)
	}
}

// runShard 分片落库循环（12.4.3 分散轮询）。
//
// 唤醒时刻对齐到「每分钟的第 i×stagger 秒」而不是「启动后每 interval 一次」：
// 后者在滚动发布时所有 Pod 的相位会被打乱，形成随机尖峰；对齐分钟边界后，
// 无论何时重启，10 个分片始终均匀铺满一分钟。
func (f *Flusher) runShard(ctx context.Context, sh *shard, offset time.Duration) {
	defer f.wg.Done()

	lastSuccess := time.Now()
	for {
		next := nextTick(f.interval, offset)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-f.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		start := time.Now()
		err := sh.flush(ctx)
		elapsed := time.Since(start)
		if metrics.Enabled() {
			metrics.FlushObserve(elapsed)
			if err != nil {
				metrics.FlushFail(sh.index)
			} else {
				lastSuccess = time.Now()
				metrics.FlushRows(true, sh.dirtyCount())
			}
			// 落库延迟：距上次成功落库的秒数（20.3 告警项：超 2 个周期告警）
			metrics.FlushLag(sh.index, time.Since(lastSuccess).Seconds())
			metrics.FlushDirty(sh.index, sh.dirtyCount())
		}
		if err != nil {
			f.logger.Warn("flush shard failed", "shard", sh.index, "err", err)
		}
	}
}

// nextTick 下一个「interval 边界 + 分片偏移」。
//
// 生产 interval=60s 时这就是分钟边界（第 0 片 :00，第 1 片 :06 … 第 9 片 :54）。
// **必须按 interval 而不是硬编码 1 分钟**：测试/压测环境把 interval 调成 2s，
// 写死一分钟会让落库等 60s，直接导致「改了数据但库里迟迟不变」。
func nextTick(interval, offset time.Duration) time.Time {
	if interval <= 0 {
		interval = time.Minute
	}
	now := time.Now()
	next := now.Truncate(interval).Add(interval).Add(offset)
	if !next.After(now) {
		next = next.Add(interval)
	}
	return next
}

// MarkDirty 标记玩家需要落库。
func (f *Flusher) MarkDirty(playerID int64) {
	idx := ShardIndex(playerID, len(f.shards))
	f.shards[idx].mark(playerID)
}

// ShardIndex 根据 playerID 计算分片。
func ShardIndex(playerID int64, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	return int(crc32.ChecksumIEEE([]byte(idBytes(playerID))) % uint32(shardCount))
}

func idBytes(id int64) []byte {
	return []byte{
		byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24),
		byte(id >> 32), byte(id >> 40), byte(id >> 48), byte(id >> 56),
	}
}

// FlushAll 立即排空全部分片（优雅关闭用）。
//
// 单次 flush 只处理一个批次：脏数据超过 batch_rows 时必须继续下一批，
// 否则关服只落「前 500 个玩家」，其余静默丢失。循环同时带两道保险：
//   - ctx 到期即停止（停服有总超时，不允许无限等待）；
//   - 一轮之后脏数据没有减少（持久化持续失败）即停止并返回错误，
//     避免「失败→重回 dirty→再失败」的死循环。
func (f *Flusher) FlushAll(ctx context.Context) error {
	var firstErr error
	for _, sh := range f.shards {
		for {
			if err := ctx.Err(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				break
			}
			before := sh.dirtyCount() + sh.deleteCount()
			if before == 0 {
				break
			}
			if err := sh.flush(ctx); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				// 失败项已重回 dirty/墓碑队列，留待下次启动补偿；
				// 本次关服继续重试只会拖长停机时间。
				break
			}
			if sh.dirtyCount()+sh.deleteCount() >= before {
				if firstErr == nil {
					firstErr = fmt.Errorf("flush shard %d made no progress (%d pending)", sh.index, before)
				}
				break
			}
		}
	}
	return firstErr
}

// DirtyCount 当前全部待落库玩家数（关闭前确认排空、测试断言用）。
func (f *Flusher) DirtyCount() int {
	total := 0
	for _, sh := range f.shards {
		total += sh.dirtyCount()
	}
	return total
}

// Stop 停止 worker。
func (f *Flusher) Stop() {
	f.stopOnce.Do(func() { close(f.stopCh) })
	f.wg.Wait()
}

type shard struct {
	index  int
	fn     PersistFunc
	delete DeleteFunc
	batch  int

	// flushMu 串行化「定时 worker」与「关闭时 FlushAll」的并发 flush，
	// 避免同一玩家在同一时刻被两个 goroutine 重复写库。
	flushMu sync.Mutex

	mu    sync.Mutex
	dirty map[int64]struct{}
	// pendingDel 待物理删除的墓碑（table -> 主键集合，12.4.5）
	pendingDel map[string][]int64
}

func newShard(index int, fn PersistFunc, del DeleteFunc, batch int) *shard {
	if batch <= 0 {
		batch = 100
	}
	return &shard{
		index:      index,
		fn:         fn,
		delete:     del,
		batch:      batch,
		dirty:      make(map[int64]struct{}),
		pendingDel: make(map[string][]int64),
	}
}

// markDelete 登记墓碑。
func (s *shard) markDelete(table string, id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingDel[table] = append(s.pendingDel[table], id)
}

// takeDeletes 取走全部待删记录（处理期新标记留到下轮）。
func (s *shard) takeDeletes() map[string][]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pendingDel) == 0 {
		return nil
	}
	out := s.pendingDel
	s.pendingDel = make(map[string][]int64)
	return out
}

// putBackDeletes 删除失败时回填（下轮重试）。
func (s *shard) putBackDeletes(failed map[string][]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for table, ids := range failed {
		s.pendingDel[table] = append(s.pendingDel[table], ids...)
	}
}

// deleteCount 待删记录总数（监控用）。
func (s *shard) deleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, ids := range s.pendingDel {
		n += len(ids)
	}
	return n
}

func (s *shard) mark(playerID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty[playerID] = struct{}{}
}

// dirtyCount 当前脏实体数。
func (s *shard) dirtyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dirty)
}

func (s *shard) flush(ctx context.Context) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	// 墓碑优先：先删再写，避免「删了又写回」的中间态
	if err := s.flushDeletes(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	if len(s.dirty) == 0 {
		s.mu.Unlock()
		return nil
	}
	// 取一批
	batch := make([]int64, 0, s.batch)
	for id := range s.dirty {
		batch = append(batch, id)
		if len(batch) >= s.batch {
			break
		}
	}
	for _, id := range batch {
		delete(s.dirty, id)
	}
	s.mu.Unlock()

	var firstErr error
	for _, id := range batch {
		if err := s.fn(ctx, id); err != nil {
			// 失败重回 dirty，下轮重试
			s.mark(id)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// flushDeletes 执行墓碑删除（合批：同一张表的 id 一次删完）。
func (s *shard) flushDeletes(ctx context.Context) error {
	pending := s.takeDeletes()
	if len(pending) == 0 {
		return nil
	}
	if s.delete == nil {
		// 未注入删除器：丢弃并告警（不能无限堆积）
		total := 0
		for _, ids := range pending {
			total += len(ids)
		}
		if total > 0 {
			if metrics.Enabled() {
				metrics.FlushTombstone(s.index, "dropped", total)
			}
		}
		return nil
	}

	var firstErr error
	failed := make(map[string][]int64, len(pending))
	for table, ids := range pending {
		for _, batch := range chunkIDs(ids, s.batch) {
			if err := s.delete(ctx, table, batch); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				failed[table] = append(failed[table], batch...)
				if metrics.Enabled() {
					metrics.FlushTombstone(s.index, "fail", len(batch))
				}
				continue
			}
			if metrics.Enabled() {
				metrics.FlushTombstone(s.index, "ok", len(batch))
			}
		}
	}
	if len(failed) > 0 {
		s.putBackDeletes(failed)
	}
	return firstErr
}

// MarkDelete 登记一条待物理删除的记录（12.4.5）。
//
// 语义：业务侧从内存/Redis 已删除，这里只负责把 MySQL 行删掉。
// 与 upsert 走同一分片（按 id 哈希），保证「先删后写」的顺序。
func (f *Flusher) MarkDelete(table string, id int64) {
	idx := ShardIndex(id, len(f.shards))
	f.shards[idx].markDelete(table, id)
}

// chunkIDs 按大小切分 id 列表。
func chunkIDs(ids []int64, size int) [][]int64 {
	if size <= 0 {
		size = DefaultBatchRows
	}
	var out [][]int64
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[start:end])
	}
	return out
}

// Pending 返回待落库数量（监控用）。
func (f *Flusher) Pending() int64 {
	var total int64
	for _, sh := range f.shards {
		sh.mu.Lock()
		total += int64(len(sh.dirty))
		sh.mu.Unlock()
	}
	return total
}

// PendingDeletes 返回待物理删除的记录数（监控用）。
func (f *Flusher) PendingDeletes() int {
	total := 0
	for _, sh := range f.shards {
		total += sh.deleteCount()
	}
	return total
}

// Shards 返回分片数（指标聚合用）。
func (f *Flusher) Shards() int { return len(f.shards) }
