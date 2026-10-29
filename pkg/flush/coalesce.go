// pkg/flush/coalesce.go —— 按键合并落库（write-behind 的通用原语）
//
// flusher.go 解决的是「几千个玩家按 60 秒周期分片错峰落库」：玩家数量大、
// 周期长、要摊平数据库尖峰。但还有一类场景长得很不一样：
//
//	抢位置    10 个房间，每秒变几十次，只需要落最终态
//	全服结算  一两个键，结算完写一次就够
//
// 这类场景用 Flusher 是错的（分片、错峰、60 秒攒批全都不需要），于是
// 每个玩法自己写一遍「map 收集脏键 → ticker 换出 → 写库 → 失败放回」。
// seat 就写了 188 行这样的代码，其中 150 行是通用逻辑。
//
// Coalescer 把那 150 行收进这里：同一个键在一个窗口内的多次变更只落最后
// 一次，写失败的键放回集合等下一轮。它不管分片、不管错峰、不做墓碑——
// 那些是 Flusher 的职责，两者是同包内的两个粒度：
//
//	Flusher[玩家级, 分钟级]   大量键 + 长周期 + 分片错峰
//	Coalescer[任意键, 秒级]   少量键 + 短周期 + 只需合并
package flush

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"game_study/pkg/log"
)

// CoalesceConfig 合并窗口配置。
type CoalesceConfig struct {
	// Interval 合并窗口；零值回落 1s。
	//
	// 窗口就是崩溃时的数据丢失上界：1s 意味着进程被 kill -9 最多丢
	// 一秒内的内存改动（重启后以库里的快照为准）。
	Interval time.Duration
	// Timeout 单轮落库超时；零值回落 3s。
	//
	// 与 Interval 独立：写库慢不能拖住下一轮标记，也不能让慢查询一直
	// 占着连接。
	Timeout time.Duration
}

// 默认值。取这两个数而不是让调用方必须填：绝大多数场景就是这两个量级，
// 想调的人自然会去读注释。
const (
	defaultCoalesceInterval = time.Second
	defaultCoalesceTimeout  = 3 * time.Second
)

// CoalesceFunc 落库单个键。
//
// 返回 error 表示这次没写成功，键会被放回集合等下一轮重试——所以实现
// 必须保证「失败时不产生副作用」，否则重试会写两遍。
type CoalesceFunc[K comparable, V any] func(ctx context.Context, key K, val V) error

// Coalescer 把同一个键在一个窗口内的多次变更合并成一次落库。
//
// 零值不可用，必须经 NewCoalescer 构造。
type Coalescer[K comparable, V any] struct {
	fn       CoalesceFunc[K, V]
	logger   *log.Logger
	interval time.Duration
	timeout  time.Duration

	mu      sync.Mutex
	pending map[K]V

	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	started  bool
}

// NewCoalescer 创建合并落库器。
func NewCoalescer[K comparable, V any](cfg CoalesceConfig, fn CoalesceFunc[K, V], logger *log.Logger) *Coalescer[K, V] {
	if cfg.Interval <= 0 {
		cfg.Interval = defaultCoalesceInterval
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultCoalesceTimeout
	}
	if logger == nil {
		logger = log.New("coalesce")
	}
	return &Coalescer[K, V]{
		fn:       fn,
		logger:   logger,
		interval: cfg.Interval,
		timeout:  cfg.Timeout,
		pending:  make(map[K]V, 16),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Mark 登记一个键的变更（后写覆盖先写）。
//
// 空值场景（只关心「这个键脏了」）传 V 的零值即可，比如
// Mark(roomID, struct{}{})。不给 Mark 再开一个无值版本：多一个同名方法
// 换不到任何东西，调用方看得见的那次转换反而更清楚。
func (c *Coalescer[K, V]) Mark(key K, val V) {
	c.mu.Lock()
	c.pending[key] = val // 无条件覆盖：后来的意图更新
	c.mu.Unlock()
}

// Start 拉起后台 worker。
//
// 重复调用是空操作：装配层可能在不同路径上都会调一次，重复起 goroutine
// 会让同一个键被两个 worker 同时写。
func (c *Coalescer[K, V]) Start() {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.mu.Unlock()
	go c.loop()
}

// Stop 停 worker 并做最后一次落库（关服路径）。
//
// 调用前应当不再有新的 Mark：最后一次 Flush 之后再来的标记不会被写。
// 返回最后一次落库的错误（多个键失败时用 errors.Join 合并）。
func (c *Coalescer[K, V]) Stop() error {
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()
	if started {
		<-c.done
	}
	return c.Flush(context.Background())
}

// Flush 立即把当前待写集合写库，并把失败的键放回。
//
// 先换出新 map 再解锁写库：写库可能耗时，不能持锁做 IO——否则 Mark
// 会被一次慢查询堵住，而 Mark 是在 Actor 里调的。
func (c *Coalescer[K, V]) Flush(ctx context.Context) error {
	c.mu.Lock()
	batch := c.pending
	c.pending = make(map[K]V, len(batch))
	c.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var errs []error
	for key, val := range batch {
		if err := c.fn(ctx, key, val); err != nil {
			// key 是泛型参数，不能直接交给 zap（它只认字符串键）：
			// 先转字符串，日志字段名固定为 key。
			c.logger.Warn("coalesce flush failed, will retry", "key", fmt.Sprint(key), "err", err)
			errs = append(errs, err)
			c.retry(key, val)
		}
	}
	return errors.Join(errs...)
}

// Len 当前待写键数（指标/测试用）。
func (c *Coalescer[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

func (c *Coalescer[K, V]) loop() {
	defer close(c.done)
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			// 失败已在 Flush 内部逐键记过日志，这里不再重复打整批错误
			// （同一件事打两遍会让告警计数翻倍）。
			_ = c.Flush(context.Background())
		case <-c.stopCh:
			return
		}
	}
}

// retry 把写失败的键放回待写集合。
//
// 只在集合里没有该键时放回：本轮写库期间可能已经有更新的变更，直接覆盖
// 会让「新状态被旧状态顶掉」——这是合并落库最容易写错的一处。
func (c *Coalescer[K, V]) retry(key K, val V) {
	c.mu.Lock()
	if _, ok := c.pending[key]; !ok {
		c.pending[key] = val
	}
	c.mu.Unlock()
}
