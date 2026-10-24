package ratelimit

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// clock 可推进的测试时钟。
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1700000000, 0)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestBucketBurstThenRefill 初始满桶允许突发，之后按速率补充。
//
// 这条是「不误伤正常玩家」的核心：登录后客户端常连发几个请求，
// 只要总量不超过 burst 就必须全放行。
func TestBucketBurstThenRefill(t *testing.T) {
	c := newClock()
	b := NewBucket(2, 3) // 每秒 2 个，容量 3
	b.SetClock(c.Now)

	for i := 0; i < 3; i++ {
		require.True(t, b.Allow(), "第 %d 个令牌应放行（容量 3）", i+1)
	}
	require.False(t, b.Allow(), "桶空后必须拒绝")

	// 过 500ms：补充 1 个
	c.Advance(500 * time.Millisecond)
	require.True(t, b.Allow())
	require.False(t, b.Allow())

	// 过 10 秒：补满到 burst=3，不会超过容量
	c.Advance(10 * time.Second)
	for i := 0; i < 3; i++ {
		require.True(t, b.Allow(), "补满后应有 %d 个令牌", 3-i)
	}
	require.False(t, b.Allow(), "容量是上限，不能无限攒")
}

// TestBucketDisabled rate<=0 时不限流（配置关掉限流的路径）。
func TestBucketDisabled(t *testing.T) {
	b := NewBucket(0, 1)
	for i := 0; i < 100; i++ {
		require.True(t, b.Allow())
	}
}

// TestKeyedIsolatesKeys 每个 key 独立计数，互不挤占。
func TestKeyedIsolatesKeys(t *testing.T) {
	c := newClock()
	k := NewKeyed(1, 2, time.Minute)
	k.SetClock(c.Now)

	require.True(t, k.Allow("a"))
	require.True(t, k.Allow("a"))
	require.False(t, k.Allow("a"), "a 用完自己的容量")

	require.True(t, k.Allow("b"), "b 不受 a 影响")
	require.True(t, k.Allow("b"))
	require.False(t, k.Allow("b"))
}

// TestKeyedSweepsIdleKeys 空闲 key 会被清掉，活跃 key 保留。
//
// 不清的话，map 会随「曾经连过的连接数」无限增长——网关跑一周能攒出
// 几十万个条目。
func TestKeyedSweepsIdleKeys(t *testing.T) {
	c := newClock()
	k := NewKeyed(100, 100, time.Minute)
	k.SetClock(c.Now)

	require.True(t, k.Allow("old"))
	require.Equal(t, 1, k.Len())

	// 推进 61 秒（超过 idle）：下次 Allow 触发清扫，old 被移除
	c.Advance(61 * time.Second)
	require.True(t, k.Allow("new"))

	require.Equal(t, 1, k.Len(), "空闲的 old 应被清掉，只剩 new")
}

// TestKeyedKeepsActiveKey 活跃 key 不能因为清扫而被重置额度。
//
// 如果清扫把「刚刚还在用」的 key 删掉，限流会周期性失效——刷子只要
// 卡在清扫间隙就能绕过。
func TestKeyedKeepsActiveKey(t *testing.T) {
	c := newClock()
	k := NewKeyed(1, 2, time.Minute)
	k.SetClock(c.Now)

	// 用完 a 的额度，然后持续访问让 last 保持新鲜
	require.True(t, k.Allow("a"))
	require.True(t, k.Allow("a"))
	for i := 0; i < 5; i++ {
		c.Advance(30 * time.Second)
		k.Allow("a") // 被拒，但 last 已刷新
	}

	// 此时 b 从未出现过，触发清扫；a 仍在
	require.True(t, k.Allow("b"))
	require.Contains(t, k.buckets, "a", "活跃 key 不能被清扫掉")
}

// TestKeyedConcurrent 并发调用不 panic、不超额。
//
// 限流器在每条消息的热路径上，必须扛住多 goroutine 并发。
func TestKeyedConcurrent(t *testing.T) {
	c := newClock()
	k := NewKeyed(1, 10, time.Minute)
	k.SetClock(c.Now)

	var (
		mu    sync.Mutex
		allow int
		wg    sync.WaitGroup
	)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if k.Allow("same") {
				mu.Lock()
				allow++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	require.EqualValues(t, 10, allow, "容量 10，必须恰好放行 10 个")
}
