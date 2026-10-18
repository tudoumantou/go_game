// pkg/flush/flusher.go —— Write-Behind 分片落库
package flush

import (
	"context"
	"hash/crc32"
	"sync"
	"time"

	"game_study/pkg/log"
)

// PersistFunc 实际写库函数（返回 nil 表示成功）。
type PersistFunc func(ctx context.Context, playerID int64) error

// Flusher 10 分片分时落库 worker。
type Flusher struct {
	shards   []*shard
	logger   *log.Logger
	interval time.Duration
	wg       sync.WaitGroup
	stopCh   chan struct{}
	stopOnce sync.Once
}

// Config 配置。
type Config struct {
	ShardCount int
	Interval   time.Duration
	Stagger    time.Duration
	BatchRows  int
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
	f := &Flusher{
		shards:   make([]*shard, cfg.ShardCount),
		logger:   logger,
		interval: cfg.Interval,
		stopCh:   make(chan struct{}),
	}
	for i := range f.shards {
		f.shards[i] = newShard(i, fn, cfg.BatchRows)
	}
	return f
}

// Start 启动所有分片 worker。
func (f *Flusher) Start(ctx context.Context) {
	for i, sh := range f.shards {
		f.wg.Add(1)
		go f.runShard(ctx, sh, time.Duration(i)*f.interval/time.Duration(len(f.shards)))
	}
}

func (f *Flusher) runShard(ctx context.Context, sh *shard, offset time.Duration) {
	defer f.wg.Done()
	// 错峰启动，避免同时打库
	select {
	case <-ctx.Done():
		return
	case <-f.stopCh:
		return
	case <-time.After(offset):
	}

	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-f.stopCh:
			return
		case <-ticker.C:
			if err := sh.flush(ctx); err != nil {
				f.logger.Warn("flush shard failed", "shard", sh.index, "err", err)
			}
		}
	}
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

// FlushAll 立即 flush 全部分片（优雅关闭用）。
func (f *Flusher) FlushAll(ctx context.Context) error {
	var firstErr error
	for _, sh := range f.shards {
		if err := sh.flush(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Stop 停止 worker。
func (f *Flusher) Stop() {
	f.stopOnce.Do(func() { close(f.stopCh) })
	f.wg.Wait()
}

type shard struct {
	index int
	fn    PersistFunc
	batch int

	mu    sync.Mutex
	dirty map[int64]struct{}
}

func newShard(index int, fn PersistFunc, batch int) *shard {
	if batch <= 0 {
		batch = 100
	}
	return &shard{
		index: index,
		fn:    fn,
		batch: batch,
		dirty: make(map[int64]struct{}),
	}
}

func (s *shard) mark(playerID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty[playerID] = struct{}{}
}

func (s *shard) flush(ctx context.Context) error {
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
