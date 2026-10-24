// Package ratelimit 提供两种轻量限流器：单桶 Bucket 与按 key 分桶 Keyed。
//
// 为什么不用 golang.org/x/time/rate：只需要「令牌桶 + 按 key 兜一张表」这
// 一点能力，引入一个依赖不划算；而且这里需要注入时钟（测试要推进时间，
// 不能真睡），自己写反而更直接。
//
// 时钟一律通过 now 注入：默认 time.Now，测试可精确控制补充速率与清理时机。
package ratelimit

import (
	"sync"
	"time"
)

// Bucket 令牌桶：容量 burst，每秒补充 rate 个令牌。
//
// 初始是满桶：允许一次突发（比如登录后客户端立刻连发几个请求），
// 之后按速率回落。并发安全。
type Bucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64
	burst  float64
	now    func() time.Time
}

// NewBucket 创建令牌桶；rate<=0 表示不限制（Allow 恒 true）。
func NewBucket(rate, burst float64) *Bucket {
	if burst < 1 {
		burst = 1
	}
	return &Bucket{
		tokens: burst,
		last:   time.Now(),
		rate:   rate,
		burst:  burst,
		now:    time.Now,
	}
}

// SetClock 注入时钟（测试用；须在首次 Allow 之前调用）。
func (b *Bucket) SetClock(now func() time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.now = now
	b.last = now()
}

// Allow 尝试取一个令牌。
func (b *Bucket) Allow() bool {
	if b.rate <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	b.refill(now)
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// refill 按经过的时间补充令牌（调用方持锁）。
func (b *Bucket) refill(now time.Time) {
	if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}
	b.last = now
}

// keyBucket 单个 key 的令牌状态。
type keyBucket struct {
	tokens float64
	last   time.Time
}

// Keyed 按 key 分桶的限流器，适合「每连接 / 每玩家 / 每 IP」。
//
// 内存：key 数量随在线规模增长，因此带空闲清理——超过 idle 没有访问的桶
// 会被删掉。清理是惰性的（每 idle/2 顺带扫一次），不需要后台 goroutine。
type Keyed struct {
	mu      sync.Mutex
	buckets map[string]*keyBucket
	rate    float64
	burst   float64
	idle    time.Duration
	lastGC  time.Time
	now     func() time.Time
}

// NewKeyed 创建分桶限流器；idle<=0 时用 10 分钟。
//
// rate<=0 表示不限制（Allow 恒 true）——用配置关掉限流时不必改调用点。
func NewKeyed(rate, burst float64, idle time.Duration) *Keyed {
	if burst < 1 {
		burst = 1
	}
	if idle <= 0 {
		idle = 10 * time.Minute
	}
	return &Keyed{
		buckets: make(map[string]*keyBucket),
		rate:    rate,
		burst:   burst,
		idle:    idle,
		now:     time.Now,
	}
}

// SetClock 注入时钟（测试用；须在首次 Allow 之前调用）。
func (k *Keyed) SetClock(now func() time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.now = now
	k.lastGC = now()
}

// Allow 尝试为 key 取一个令牌。
func (k *Keyed) Allow(key string) bool {
	if k.rate <= 0 {
		return true
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	now := k.now()
	k.maybeSweep(now)

	b, ok := k.buckets[key]
	if !ok {
		b = &keyBucket{tokens: k.burst, last: now}
		k.buckets[key] = b
	} else if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * k.rate
		if b.tokens > k.burst {
			b.tokens = k.burst
		}
		b.last = now
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// maybeSweep 每 idle/2 顺手清一次空闲桶（调用方持锁）。
//
// 惰性清理的理由：限流器在热路径上，起后台 goroutine 要么多一个生命周期
// 要管，要么在停机时漏；而清扫本身很便宜（一趟 map 遍历）。
func (k *Keyed) maybeSweep(now time.Time) {
	interval := k.idle / 2
	if !k.lastGC.IsZero() && now.Sub(k.lastGC) < interval {
		return
	}
	k.lastGC = now
	cutoff := now.Add(-k.idle)
	for key, b := range k.buckets {
		if b.last.Before(cutoff) {
			delete(k.buckets, key)
		}
	}
}

// Len 当前跟踪的 key 数量（运维/测试观测用）。
func (k *Keyed) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.buckets)
}
