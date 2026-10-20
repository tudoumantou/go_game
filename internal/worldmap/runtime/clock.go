package runtime

import (
	"sync"
	"time"
)

// Clock 是场景循环唯一允许的时间来源（文档 18.2）。
//
// 为什么必须抽成接口：场景里的所有时间推导（移动位置、采集结算、
// 战斗超时、刷新周期）都是「按时间算结果」。如果代码直接调 time.Now()，
// 测试就必须真实等待——验证一次 8 小时采集要跑 8 小时，不可能覆盖。
// 假时钟让「时间推进」变成一行 Set/Advance，从而能表驱动覆盖所有到期路径。
//
// 生产实现必须是单调时钟语义：time.Now() 本身包含单调读数，
// 但序列化成 UnixMilli 后会丢失单调性，所以落库全部用毫秒，
// 比较"是否到期"时用毫秒差而不是 time.Time.Sub。
type Clock interface {
	Now() time.Time
}

// RealClock 是生产时钟。
type RealClock struct{}

// Now 返回当前时间。
func (RealClock) Now() time.Time { return time.Now() }

// FakeClock 是测试时钟，可手动推进。
//
// 并发安全：SceneLoop 单 goroutine 读，测试主 goroutine 写，
// 因此必须加锁——这不是"过度设计"，而是 -race 会直接报错的真实竞争。
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock 创建停在指定时刻的假时钟。建议用 UTC，
// 避免本地时区在 CI 与开发机不同导致跨天断言漂移。
func NewFakeClock(at time.Time) *FakeClock {
	return &FakeClock{now: at.UTC()}
}

// Now 返回当前假时间。
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance 前进 d。负数会被忽略（时间不能倒流——
// 真实世界可能因 NTP 回拨而"倒流"，但测试里制造倒流只会让断言难写；
// 需要验证时钟跳变的代码应走 Set 并显式测试）。
func (f *FakeClock) Advance(d time.Duration) {
	if d < 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Set 直接设置时刻（用于构造时钟跳变场景）。
func (f *FakeClock) Set(at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = at.UTC()
}

// NowMS 是 Clock 的便捷扩展：绝大多数规划输入要的是 Unix 毫秒。
// 定义成包级函数而不是接口方法，避免每个实现都要写一遍。
func NowMS(c Clock) int64 { return c.Now().UnixMilli() }
