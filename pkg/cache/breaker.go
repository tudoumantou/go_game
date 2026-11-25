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
//
// 状态机本身已上提到 pkg/breaker（通用 client 级熔断器唯一来源）；
// 本文件只保留 Redis 特有的语义：redis.Nil 豁免、Hook 接入、专用指标。
package cache

import (
	"context"
	"errors"
	"time"

	"game_study/pkg/breaker"
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

// BreakerState 熔断状态。别名指向通用状态机，调用方代码零改动。
type BreakerState = breaker.State

const (
	BreakerClosed   = breaker.Closed
	BreakerOpen     = breaker.Open
	BreakerHalfOpen = breaker.HalfOpen
)

// 默认参数（配置 0 值时采用）。
const (
	DefaultBreakerThreshold = breaker.DefaultThreshold
	DefaultBreakerCooldown  = breaker.DefaultCooldown
	DefaultBreakerProbes    = breaker.DefaultProbes
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

// Breaker Redis 专用熔断器：内嵌通用状态机，只叠加 Redis 语义与指标。
//
// 并发模型由 pkg/breaker 保证（atomic + CAS，closed 热路径一次 Load）。
type Breaker struct {
	*breaker.Breaker

	// 归一化后的参数，仅用于日志/测试观测；真实判定在内嵌状态机。
	threshold int32
	cooldown  time.Duration
	logger    *log.Logger
}

// NewBreaker 创建熔断器。
func NewBreaker(opts BreakerOptions) *Breaker {
	o := opts.normalize()
	b := &Breaker{
		threshold: int32(o.Threshold),
		cooldown:  o.Cooldown,
		logger:    log.New("redis"),
	}
	b.Breaker = breaker.New(breaker.Options{
		Threshold: opts.Threshold,
		Cooldown:  opts.Cooldown,
		Probes:    opts.Probes,
		Name:      "redis",
		OnChange:  func(s breaker.State) { b.notify(BreakerState(s)) },
	})
	return b
}

// notify 状态变化：日志 + 指标（由通用状态机在 open/closed 迁移时回调）。
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
				"threshold", b.threshold, "cooldown", b.cooldown.String())
			return
		}
		b.logger.Info("redis circuit breaker closed; traffic resumed")
	}
}

// Observe 按 Redis 命令结果更新状态。
func (b *Breaker) Observe(err error) {
	switch {
	case err == nil:
		b.Success()
	case errors.Is(err, redis.Nil):
		b.Success() // key 不存在是正常结果，不是故障
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
