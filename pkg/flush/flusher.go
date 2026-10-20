// pkg/flush/flusher.go —— Write-Behind 分片落库
//
// 职责：接收 MarkDirty/MarkDelete 登记，由 N 个分片 worker 在错峰时刻
// 把脏玩家批量写库、把墓碑批量删除。这是「非缓存玩家」的落库通道；
// 缓存玩家（Redis 权威）走 pkg/writebehind 的脏队列，两条通道互补。
//
// 设计取舍（详见设计文档 12.4）：
//   - 攒批窗口：不即时写，等一个落库周期（默认 60s）把期间所有修改合并，
//     用「进程崩溃丢一个窗口的数据」换「语句数下降两个量级」；
//   - 分片 + 错峰：playerID 哈希到 10 个分片，各分片在一分钟内的不同
//     偏移秒触发，把数据库写压力摊平（否则整服同时 flush 形成尖峰）；
//   - 墓碑：删除登记后不立即执行，与 upsert 同周期合批，削掉删除风暴。
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
//
// 由 New 的调用方注入（通常是 writebehind 的落库逻辑），Flusher 只负责
// 调度与重试，不认识玩家数据结构——依赖倒置：落库框架与落库内容解耦。
// 契约：对单个 playerID 做全量幂等写（失败可安全重试），函数内应自建
// 事务或复用既有能力；ctx 携带本轮 flush 的超时。
type PersistFunc func(ctx context.Context, playerID int64) error

// Flusher 10 分片分时落库 worker。
//
// 字段说明：
//   - shards：分片数组，MarkDirty 按 playerID 哈希定位，保证同一玩家
//     的 dirty 与墓碑永远落在同一分片、同一 goroutine，无需跨分片协调；
//   - interval/stagger：落库周期与分片间错峰偏移；
//   - wg：等待所有分片 goroutine 退出（Stop 的同步点）；
//   - stopCh + stopOnce：关闭信号；Once 保证 close 只执行一次
//     （重复 close channel 会 panic）。
//
// 并发：MarkDirty/MarkDelete 可被任意业务 goroutine 并发调用；
// Start 与 Stop 成对，由进程主控 goroutine 调用。
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
//
// 与 PersistFunc 同构的策略注入；Config.DeleteFn 为空时墓碑只告警不执行
// （见 flushDeletes），保证「没接删除器」不等于「进程起不来」。
type DeleteFunc func(ctx context.Context, table string, ids []int64) error

// Config 配置。
//
// 零值全部有默认值（见 New）：ShardCount=10、Interval=60s、
// Stagger=Interval/ShardCount、BatchRows=100。
type Config struct {
	// ShardCount 分片数；只影响本进程内的错峰粒度（与 writebehind 的
	// Redis 队列分区是两套独立分桶，不必相等）。
	ShardCount int
	// Interval 落库周期；生产 60s，测试可调到秒级（nextTick 按 interval
	// 对齐而非硬编码 1 分钟，见 runShard 注释）。
	Interval time.Duration
	// Stagger 分片间偏移；必须 < Interval，否则失去错峰意义（New 内兜底）。
	Stagger time.Duration
	// BatchRows 单轮 flush 处理的玩家数上限；限制单轮时长，
	// 超出部分留到下一轮（保证 flush 不长期占住数据库连接）。
	BatchRows int
	// DeleteFn 墓碑执行器；为空时删除标记会被忽略（仅告警），不影响正常落库
	DeleteFn DeleteFunc
}

// New 创建 Flusher。
//
// 默认值与不变量：
//   - ShardCount/Interval 零值回落（10 / 60s）；
//   - stagger 默认按分片均分，且**必须小于 interval**：否则同一轮里多个
//     分片会挤在同一时刻，失去「分散轮询」的意义；配置写错时回退到均分。
//
// stopCh 在此创建（make channel），Start 前 Flusher 已可接收标记。
//
// Go 细节：cfg 按值传入，函数内修改的是副本，不影响调用方——
// 默认值修正因此不会产生意外副作用。
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
//
// Go 细节 —— WaitGroup 的 Add 时机：必须在 go 语句**之前** Add。
// 若先启动 goroutine 再 Add，worker 可能秒级完成并调用 Done，
// Wait 若此时已进入等待会看到计数为 0 而放行（竞态），
// 或在 Add 前返回；「先 Add 后 go」保证计数在任何 Done 之前已生效。
// 每个分片一个独立 goroutine，互不阻塞（各自 flush 的串行见 shard.flushMu）。
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
//
// Go 细节：
//   - defer f.wg.Done() 与 Start 的 Add 配对；goroutine 内 panic 会杀死
//     整个进程，Done 是防御性习惯；
//   - time.NewTimer + Stop：timer 是堆上对象，触发前不 Stop 会一直驻留
//     直到超时（timer 泄漏）；三个退出分支都先 Stop 再 return。Stop 返回
//     false 不代表 channel 没数据（触发与 Stop 竞争时 channel 里可能已有
//     值），但本函数随即 return，不构成泄漏；
//   - select 三路监听：ctx.Done（进程停）/ stopCh（Stop()）/ timer.C（到点），
//     Go 的 select 在多个分支同时就绪时随机选一个，不会饿死任何一路；
//   - lastSuccess 只在成功时刷新，用于 metrics.FlushLag 计算落库延迟
//     （20.3 告警项：超过 2 个周期未成功即告警）。
func (f *Flusher) runShard(ctx context.Context, sh *shard, offset time.Duration) {
	defer f.wg.Done()

	lastSuccess := time.Now()
	for {
		next := nextTick(f.interval, offset)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			// 上级取消/停机：先 Stop 再退出，防止 timer 滞留堆上
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
//
// Go 细节：time.Truncate 按自 unix 零点起的绝对间隔截断（对 60s 即对齐
// 分钟边界），与进程运行了多久无关；末尾 !next.After(now) 兜底处理
// 「截断+offset 后仍不晚于当前时刻」的边界（offset 恰跨过 now），
// 顺延一个周期，保证返回值严格大于 now，否则 timer 会立即触发空转。
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
//
// 幂等：map 集合语义，同一玩家一轮内重复标记只算一次（修改 N 次也只
// 触发一次全量写——写的是最新快照，中间态无落库价值）。
// 线程安全：分片内部有锁（见 shard.mark），可被任意 goroutine 调用。
func (f *Flusher) MarkDirty(playerID int64) {
	idx := ShardIndex(playerID, len(f.shards))
	f.shards[idx].mark(playerID)
}

// ShardIndex 根据 playerID 计算分片。
//
// 用 crc32 而不是 playerID % shardCount：ID 由发号器分配时往往带连续性/
// 低位规律，取模会把相邻 ID 挤到同片；crc32 打散后分布均匀。
// shardCount <=0 兜底返回 0（防御配置错误，宁可集中也不 panic）。
func ShardIndex(playerID int64, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	return int(crc32.ChecksumIEEE([]byte(idBytes(playerID))) % uint32(shardCount))
}

// idBytes 把 int64 转成小端 8 字节（crc32 的输入）。
//
// Go 细节：byte(id>>56) 逐字节位移取值，负数同样有确定的小端表示（补码），
// 哈希结果确定。用 strconv 转字符串也可以，但多一次 itoa 分配——
// MarkDirty 是热路径（每次写操作都可能触发），8 次位移远比一次
// 字符串转换便宜。
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
//
// 失败语义：返回第一个错误（firstErr 只记不覆盖）；未完成的分片留待
// 下次启动补偿（重启后数据仍在内存则重新标记，缓存玩家本就走
// writebehind 的 Redis 脏队列，不依赖本路径）。
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
			// 无进展检测：flush 成功但 pending 没降（理论上不该发生），
			// 继续循环只会空转烧 CPU，按失败处理
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
//
// 各分片单独加锁统计后汇总；返回值是「时刻快照」，并发 MarkDirty 下
// 瞬时小误差可接受（用于观测而非精确控制）。
func (f *Flusher) DirtyCount() int {
	total := 0
	for _, sh := range f.shards {
		total += sh.dirtyCount()
	}
	return total
}

// Stop 停止 worker。
//
// 先 close(stopCh) 唤醒所有分片（runShard 的 select 收到后退出），
// 再 wg.Wait() 等它们真正结束——两步缺一不可：只 close 不等，
// 调用方无法知道 flush 循环已停；只 Wait 不 close，goroutine 会
// 一直睡到下一轮周期。
//
// Go 细节：close(stopCh) 是广播式唤醒（所有 receiver 同时就绪），
// 比逐分片发信号简单；sync.Once 保证重复调用不会二次 close
// （close 已关闭的 channel 直接 panic）。幂等 Stop 让停机路径
// 可以在多处防御性调用。
func (f *Flusher) Stop() {
	f.stopOnce.Do(func() { close(f.stopCh) })
	f.wg.Wait()
}

// shard 单个落库分片：一个 dirty 集合 + 一张墓碑表 + 串行 flush。
//
// 两把锁分工（核心原则：持锁不做 IO）：
//   - mu：保护 dirty/pendingDel 两个 map，只在 mark/take/count 时
//     短暂持有，绝不跨数据库调用；
//   - flushMu：串行化整个 flush 流程（定时 worker 与关闭时 FlushAll
//     可能并发进入），保证同一时刻只有一个 goroutine 在写这个分片，
//     同一玩家不会被两个 flush 重复处理。
//
// Go 细节：dirty 用 map[int64]struct{} 实现集合——struct{} 是零大小
// 类型，不占值空间；「存在即脏」不需要计数，重复 mark 自然去重。
type shard struct {
	// index 分片序号（指标标签与日志用）。
	index int
	// fn 落库函数（注入的 PersistFunc）。
	fn PersistFunc
	// delete 墓碑执行器（可为 nil，见 flushDeletes）。
	delete DeleteFunc
	// batch 单轮处理的玩家数上限。
	batch int

	// flushMu 串行化「定时 worker」与「关闭时 FlushAll」的并发 flush，
	// 避免同一玩家在同一时刻被两个 goroutine 重复写库。
	flushMu sync.Mutex

	// mu 保护下列两个 map；持有期间禁止调用 fn/delete（IO）。
	mu    sync.Mutex
	dirty map[int64]struct{}
	// pendingDel 待物理删除的墓碑（table -> 主键集合，12.4.5）
	pendingDel map[string][]int64
}

// newShard 创建分片。
//
// batch <=0 兜底 100（比分片级 DefaultBatchRows 更保守的单轮量：
// 优先保证 flush 时长短）；两个 map 必须 make，nil map 写入会 panic。
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
//
// 只追加不执行：执行在 flushDeletes。与 dirty 不同，这里允许重复
// （同一 id 删两次由数据库幂等消化），不做去重以换取 O(1) 登记。
func (s *shard) markDelete(table string, id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingDel[table] = append(s.pendingDel[table], id)
}

// takeDeletes 取走全部待删记录（处理期新标记留到下轮）。
//
// take-and-swap 模式：锁内把整个 map 交给调用方、立刻换上新 map，
// 而不是锁内逐条拷贝——处理期（可能跨秒级 IO）到来的新墓碑落在
// 新 map 上，既不丢也不阻塞登记方。
//
// Go 细节：out := s.pendingDel 是 map 引用赋值（非拷贝），换新 map 后
// 旧 map 只被返回值和 putBackDeletes 触达，无数据竞争。
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
//
// 只回填失败批次，成功的不再入队——at-least-once 语义：失败项可能
// 实际已删，MySQL 的 DELETE 幂等，重复执行无副作用。
func (s *shard) putBackDeletes(failed map[string][]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for table, ids := range failed {
		s.pendingDel[table] = append(s.pendingDel[table], ids...)
	}
}

// deleteCount 待删记录总数（监控用）。
//
// 遍历 map 汇总 len；与 dirtyCount 一样是时刻快照。
func (s *shard) deleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, ids := range s.pendingDel {
		n += len(ids)
	}
	return n
}

// mark 标记脏玩家（幂等集合写入）。
//
// MarkDirty 的热路径终点：一次加锁 + 一次 map 赋值，持锁时间 O(1)。
func (s *shard) mark(playerID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty[playerID] = struct{}{}
}

// dirtyCount 当前脏实体数。
//
// Go 细节：len(map) 本身是 O(1)，但并发写时 len 不是原子操作——
// map 并发读写会触发 Go 运行时的 fatal error（不可 recover），
// 读侧也必须持锁，本文件的 mu 同时保护读写两侧。
func (s *shard) dirtyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dirty)
}

// flush 执行一轮落库：先墓碑、后一批脏玩家。
//
// 顺序理由：墓碑优先，避免「删了又写回」的中间态（先写后删会把
// 本轮刚更新的数据删掉）。取批用「遍历 map 取前 batch 个再删除」：
// map 遍历顺序随机，但每玩家独立全量写，顺序无所谓；删除后
// 失败项由 mark 重回 dirty（at-least-once + 幂等写兜底）。
//
// Go 细节：先 Unlock 再逐玩家执行 fn——**绝不持锁做 IO**，
// 否则 MarkDirty 会被一次慢 flush 堵住（锁粒度设计见 shard 注释）。
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
	// 取一批：容量按 batch 预分配，遍历到 batch 即止
	batch := make([]int64, 0, s.batch)
	for id := range s.dirty {
		batch = append(batch, id)
		if len(batch) >= s.batch {
			break
		}
	}
	// 取走即从 dirty 移除：本轮负责到底；失败项在下面 mark 重回
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
//
// 流程：takeDeletes 取走 → 无删除器则计数告警后丢弃（不能无限堆积，
// 否则内存泄漏且运维无感知）→ 按表分批发 DeleteFunc → 失败批次
// putBackDeletes 回填。
// 返回首个错误但**继续处理其余表**：一张表的失败不连带其它表。
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
// 与 upsert 走同一分片（按 id 哈希），保证「先删后写」的顺序——
// 若删除走另一条通道，可能出现「旧数据又被写回」的复活 bug。
func (f *Flusher) MarkDelete(table string, id int64) {
	idx := ShardIndex(id, len(f.shards))
	f.shards[idx].markDelete(table, id)
}

// chunkIDs 按大小切分 id 列表。
//
// 返回的子切片共享 ids 底层数组（只切不拷），调用方只读安全；
// size<=0 兜底 DefaultBatchRows（500）。
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
//
// 直接遍历分片并逐个加锁读 len（不复用 dirtyCount 是为了让监控
// 与落库路径解耦，语义等价）；返回 int64 与指标类型对齐。
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
//
// 与 DirtyCount/Pending 的区别是口径：这里数的是墓碑条数而非玩家数，
// 用于观察删除积压（墓碑长期不降说明 DeleteFunc 持续失败）。
func (f *Flusher) PendingDeletes() int {
	total := 0
	for _, sh := range f.shards {
		total += sh.deleteCount()
	}
	return total
}

// Shards 返回分片数（指标聚合用）。
func (f *Flusher) Shards() int { return len(f.shards) }
