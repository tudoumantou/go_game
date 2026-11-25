// Package breaker —— 通用三态熔断器（closed / open / half-open）。
//
// 与 pkg/cache/breaker.go 的关系：那份是 Redis Hook 专用适配器（处理
// redis.Nil、挂 redis.Hook、用 gs_redis_breaker_* 指标），本包是被它和
// RPC 客户端共同依赖的**状态机唯一来源**。两处的差异化语义（什么算失败、
// 状态变化要发什么指标）留给调用方，状态迁移本身只在这里实现一份。
//
// 并发模型沿用 Redis 那份被 -race 验证过的做法：计数与状态全部 atomic，
// 热路径（closed）只有一次 Load；迁移用 CAS，并发失败只有一个 goroutine
// 负责通知。RPC 客户端在玩家链路上，锁会把熔断本身变成新的瓶颈。
package breaker

import (
	"errors"
	"sync/atomic"
	"time"
)

// ErrOpen 熔断打开时的快速失败哨兵（拦截器把它转成 gRPC Unavailable）。
var ErrOpen = errors.New("breaker: circuit open")

// State 熔断状态。
type State int32

const (
	// Closed 正常放行。
	Closed State = iota
	// Open 快速失败，请求不发出去。
	Open
	// HalfOpen 冷却到期，只放行少量探测。
	HalfOpen
)

// String 状态名（日志/指标用）。
func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

const (
	// DefaultThreshold 连续失败多少次打开熔断。
	DefaultThreshold = 5
	// DefaultCooldown 打开后多久进入半开。
	DefaultCooldown = 5 * time.Second
	// DefaultProbes 半开状态放行的探测请求数。
	DefaultProbes = 1
)

// Options 熔断参数。
type Options struct {
	// Threshold 连续失败阈值；0 = 默认 5，负数 = 关闭熔断（Failure 不累计）。
	Threshold int
	// Cooldown 打开后进入半开的等待时间；0 = 默认 5s。
	Cooldown time.Duration
	// Probes 半开放行的探测请求数；0 = 默认 1。
	Probes int
	// OnChange 状态迁移通知（open / closed，不含 open→half_open）。
	// 调用方在这里记日志/指标；为 nil 时无通知。
	OnChange func(State)
	// Name 熔断器名称，仅用于错误文案定位。
	Name string
}

// Disabled 配置是否关闭熔断。
func (o Options) Disabled() bool { return o.Threshold < 0 }

func (o Options) normalize() Options {
	if o.Threshold == 0 {
		o.Threshold = DefaultThreshold
	}
	if o.Cooldown <= 0 {
		o.Cooldown = DefaultCooldown
	}
	if o.Probes <= 0 {
		o.Probes = DefaultProbes
	}
	return o
}

// Breaker 三态熔断器。可被多个 goroutine 并发使用。
type Breaker struct {
	threshold  int32
	cooldown   int64 // 纳秒
	probeLimit int32
	name       string

	state    atomic.Int32
	failures atomic.Int32
	openedAt atomic.Int64
	probes   atomic.Int32
	onChange func(State)
}

// New 创建熔断器。
func New(opts Options) *Breaker {
	o := opts.normalize()
	return &Breaker{
		threshold:  int32(o.Threshold),
		cooldown:   int64(o.Cooldown),
		probeLimit: int32(o.Probes),
		name:       o.Name,
		onChange:   o.OnChange,
	}
}

// Name 熔断器名称。
func (b *Breaker) Name() string { return b.name }

// State 当前状态。
func (b *Breaker) State() State { return State(b.state.Load()) }

// Failures 当前连续失败计数（观测用）。
func (b *Breaker) Failures() int { return int(b.failures.Load()) }

// Allow 判断是否可以放行一次调用。
//
//   - closed：放行；
//   - open：冷却未到 → 拒绝；冷却已到 → CAS 推进到 half-open，只有一个
//     goroutine 能成功并拿到探测名额，其余继续按 open 拒绝（防止刚到期
//     就被一波流量打穿）；
//   - half-open：探测名额未用完 → 放行，否则拒绝。
func (b *Breaker) Allow() bool {
	switch State(b.state.Load()) {
	case Closed:
		return true
	case Open:
		if time.Since(time.Unix(0, b.openedAt.Load())) < time.Duration(b.cooldown) {
			return false
		}
		b.state.CompareAndSwap(int32(Open), int32(HalfOpen))
		b.probes.Store(0)
		return b.allowProbe()
	default: // HalfOpen
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

// Success 记录一次成功：清空失败计数，half-open → closed。
//
// open 状态下的迟到成功不改变状态：那是熔断打开前发出的在途请求，
// 让它关闭会导致「刚打开就关」的抖动。等冷却期到由探测请求决定。
func (b *Breaker) Success() {
	b.failures.Store(0)
	if b.state.CompareAndSwap(int32(HalfOpen), int32(Closed)) {
		b.probes.Store(0)
		b.notify(Closed)
	}
}

// Failure 记录一次失败：closed 累计到阈值 → open；half-open → 重新 open。
func (b *Breaker) Failure() {
	if b.threshold < 0 { // 配置关闭
		return
	}
	switch State(b.state.Load()) {
	case HalfOpen:
		b.trip()
	case Closed:
		if b.failures.Add(1) >= b.threshold {
			b.trip()
		}
	}
	// open：已熔断，忽略打开前的在途失败（否则自我放大）
}

// Observe 按错误更新状态：nil 成功、非 nil 失败。
// 需要更细语义（如 redis.Nil 不算失败）的调用方自己分类后调 Success/Failure。
func (b *Breaker) Observe(err error) {
	if err == nil {
		b.Success()
	} else {
		b.Failure()
	}
}

// Reset 手动复位（测试 / 运维干预）。
func (b *Breaker) Reset() {
	b.failures.Store(0)
	b.probes.Store(0)
	b.state.Store(int32(Closed))
}

// trip 打开熔断并通知（并发失败只有一个 goroutine 触发通知）。
func (b *Breaker) trip() {
	b.openedAt.Store(time.Now().UnixNano())
	b.probes.Store(0)
	if b.state.Swap(int32(Open)) != int32(Open) {
		b.notify(Open)
	}
}

func (b *Breaker) notify(to State) {
	if b.onChange != nil {
		b.onChange(to)
	}
}
