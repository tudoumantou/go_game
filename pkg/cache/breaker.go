// pkg/cache/breaker.go —— Redis 熔断器（连续失败后快速失败）
//
// 解决什么问题：Redis 故障有两种形态——进程死了（连接立即被拒，毫秒级）
// 和网络黑洞（丢包，每条命令等满超时）。后者才是真正的伤害：一次登录要
// 串行发十几条 Redis 命令，每条等 5s，登录 P99 直接破表；而所有降级路径
// （读回落 MySQL、写只告警）本来都能工作，却被超时拖住。
//
// 做法：在 go-redis 的 Hook 层统计连续失败，达到阈值后打开熔断，
// ProcessHook 里直接返回 ErrBreakerOpen——命令根本不发出去，降级路径
// 立刻生效。冷却期后进入半开，只放行少量探测：成功关闭、失败重新打开。
//
// 为什么挂在 Hook 而不是包一层 Client：UniversalClient 有几十个方法，
// 逐个包一遍既容易漏，又会把 Pipeline/事务这类组合命令的语义搞乱。
// Hook 是库提供的唯一「所有命令都必经过」的切面。
//
// 为什么 redis.Nil 不算失败：key 不存在是正常业务结果（EXISTS/HGET），
// 把它算作故障会让正常业务把熔断器打穿。
package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"game_study/pkg/log"
	"game_study/pkg/metrics"

	"github.com/redis/go-redis/v9"
)

// ErrBreakerOpen 熔断打开期间的快速失败错误。
//
// 调用方不需要特判它：既有的降级路径（回落 MySQL / 只告警）对 Redis
// 错误一视同仁。单独定义的意义是日志与指标能区分「真故障」和「被熔断
// 挡下的请求」。
var ErrBreakerOpen = errors.New("cache: redis circuit breaker open")

// BreakerState 熔断状态。
type BreakerState int32

const (
	// BreakerClosed 正常放行。
	BreakerClosed BreakerState = iota
	// BreakerOpen 快速失败，命令不发出去。
	BreakerOpen
	// BreakerHalfOpen 冷却到期，只放行探测请求。
	BreakerHalfOpen
)

// String 状态名（日志/指标用）。
func (s BreakerState) String() string {
	switch s {
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

// 默认参数（配置 0 值时采用）。
const (
	// DefaultBreakerThreshold 连续失败多少次打开熔断。
	DefaultBreakerThreshold = 5
	// DefaultBreakerCooldown 打开后多久进入半开。
	DefaultBreakerCooldown = 5 * time.Second
	// DefaultBreakerProbes 半开状态放行的探测请求数。
	DefaultBreakerProbes = 1
)

// BreakerOptions 熔断参数。
type BreakerOptions struct {
	// Threshold 连续失败阈值；0 = 默认 5，负数 = 关闭熔断。
	Threshold int
	// Cooldown 打开后进入半开的等待时间；0 = 默认 5s。
	Cooldown time.Duration
	// Probes 半开放行的探测请求数；0 = 默认 1。
	Probes int
}

// disabled 熔断是否被配置关闭。
func (o BreakerOptions) disabled() bool { return o.Threshold < 0 }

// normalize 补默认值。
func (o BreakerOptions) normalize() BreakerOptions {
	if o.Threshold == 0 {
		o.Threshold = DefaultBreakerThreshold
	}
	if o.Cooldown <= 0 {
		o.Cooldown = DefaultBreakerCooldown
	}
	if o.Probes <= 0 {
		o.Probes = DefaultBreakerProbes
	}
	return o
}

// Breaker 熔断器（实现 redis.Hook）。
//
// 并发模型：状态与计数全部是 atomic，热路径（closed 状态）只有一次
// atomic.Load——Redis 每秒数万次调用，一把互斥锁会成为新的瓶颈。
// 状态迁移用 CAS，多个 goroutine 同时失败时只有一个负责发通知。
type Breaker struct {
	threshold  int32
	cooldown   int64 // 纳秒
	probeLimit int32

	state    atomic.Int32
	failures atomic.Int32
	openedAt atomic.Int64 // 打开时刻（UnixNano）
	probes   atomic.Int32 // 半开状态已放行的探测数

	logger *log.Logger
}

// NewBreaker 创建熔断器。
func NewBreaker(opts BreakerOptions) *Breaker {
	o := opts.normalize()
	return &Breaker{
		threshold:  int32(o.Threshold),
		cooldown:   int64(o.Cooldown),
		probeLimit: int32(o.Probes),
		logger:     log.New("redis"),
	}
}

// State 当前状态。
func (b *Breaker) State() BreakerState { return BreakerState(b.state.Load()) }

// Failures 当前连续失败计数（观测用）。
func (b *Breaker) Failures() int { return int(b.failures.Load()) }

// Allow 判断是否可以放行一条命令。
//
// 三态语义：
//   - closed：放行；
//   - open：冷却未到 → 拒绝；冷却已到 → 尝试切半开，成功者放行探测；
//   - half_open：探测名额未用完 → 放行，否则拒绝。
func (b *Breaker) Allow() bool {
	switch BreakerState(b.state.Load()) {
	case BreakerClosed:
		return true
	case BreakerOpen:
		if time.Since(time.Unix(0, b.openedAt.Load())) < time.Duration(b.cooldown) {
			return false
		}
		// 冷却到期：只有一个 goroutine 能把状态推进到半开，
		// 其余人继续按 open 拒绝——避免「刚到期就被一波流量打穿」。
		b.state.CompareAndSwap(int32(BreakerOpen), int32(BreakerHalfOpen))
		b.probes.Store(0)
		return b.allowProbe()
	default: // half_open
		return b.allowProbe()
	}
}

// allowProbe 半开状态下发放探测名额。
func (b *Breaker) allowProbe() bool {
	for {
		n := b.probes.Load()
		if n >= b.probeLimit {
			return false
		}
		if b.probes.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// Success 记录一次成功：清空失败计数，半开 → 关闭。
//
// open 状态下的迟到成功不改变状态：那是熔断打开前发出的在途请求，
// 让它关闭会导致「刚打开就关」的抖动。等冷却期到，由探测请求决定。
func (b *Breaker) Success() {
	b.failures.Store(0)
	if b.state.CompareAndSwap(int32(BreakerHalfOpen), int32(BreakerClosed)) {
		b.probes.Store(0)
		b.notify(BreakerClosed)
	}
}

// Failure 记录一次失败：closed 累计到阈值 → 打开；half_open → 重新打开。
func (b *Breaker) Failure() {
	switch BreakerState(b.state.Load()) {
	case BreakerHalfOpen:
		// 探测失败：立刻回到 open，冷却重新计时。
		b.trip()
	case BreakerClosed:
		if b.failures.Add(1) >= b.threshold {
			b.trip()
		}
	}
	// open：已熔断，忽略（多半是打开前的在途失败）
}

// trip 打开熔断并通知（并发失败只有一个 goroutine 触发通知）。
func (b *Breaker) trip() {
	b.openedAt.Store(time.Now().UnixNano())
	b.probes.Store(0)
	if b.state.Swap(int32(BreakerOpen)) != int32(BreakerOpen) {
		b.notify(BreakerOpen)
	}
}

// notify 状态变化：日志 + 指标。
func (b *Breaker) notify(to BreakerState) {
	if metrics.Enabled() {
		metrics.RedisBreakerState(int32(to))
		if to == BreakerOpen {
			metrics.RedisBreakerOpened()
		}
	}
	if b.logger != nil {
		if to == BreakerOpen {
			b.logger.Warn("redis circuit breaker opened; fail fast until cooldown",
				"threshold", b.threshold, "cooldown", time.Duration(b.cooldown).String())
			return
		}
		b.logger.Info("redis circuit breaker closed; traffic resumed")
	}
}

// Observe 按命令结果更新状态。
func (b *Breaker) Observe(err error) {
	switch {
	case err == nil:
		b.Success()
	case errors.Is(err, redis.Nil):
		// key 不存在是正常结果，不是故障
		b.Success()
	case errors.Is(err, ErrBreakerOpen):
		// 被自己拒绝的请求不参与统计（否则拒绝会自我放大）
	default:
		b.Failure()
	}
}

// ---- redis.Hook 实现 ----

// DialHook 建连钩子：建连失败由 ProcessHook 观察到，这里不重复计数。
func (b *Breaker) DialHook(next redis.DialHook) redis.DialHook { return next }

// ProcessHook 单命令钩子：熔断打开时直接返回 ErrBreakerOpen，命令不发出去。
func (b *Breaker) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !b.Allow() {
			if metrics.Enabled() {
				metrics.RedisBreakerRejected()
			}
			return ErrBreakerOpen
		}
		err := next(ctx, cmd)
		b.Observe(err)
		return err
	}
}

// ProcessPipelineHook 批量命令钩子：整批算一次判定。
//
// 不按单条命令计数：pipeline 是一次网络往返，成败是整体的；
// 逐条统计会让一次批量失败直接把计数冲过阈值。
func (b *Breaker) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !b.Allow() {
			if metrics.Enabled() {
				metrics.RedisBreakerRejected()
			}
			return ErrBreakerOpen
		}
		err := next(ctx, cmds)
		b.Observe(err)
		return err
	}
}

// 编译期断言：Breaker 必须满足 redis.Hook。
var _ redis.Hook = (*Breaker)(nil)
