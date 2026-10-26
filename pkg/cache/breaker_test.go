// breaker_test.go —— Redis 熔断器：状态机 + 真实 Hook 行为
//
// 关注两件事：
//  1. 状态机的每个迁移路径（closed→open→half_open→closed / open）；
//  2. 熔断打开后命令确实不再发出去（不是"发出去再失败"）。
package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newQuietBreaker 构造不打日志的熔断器（单测不刷屏）。
func newQuietBreaker(t *testing.T, opts BreakerOptions) *Breaker {
	t.Helper()
	b := NewBreaker(opts)
	b.logger = nil
	return b
}

// TestBreakerOpensAfterThreshold 连续失败到阈值 → 打开；成功一次清零。
func TestBreakerOpensAfterThreshold(t *testing.T) {
	b := newQuietBreaker(t, BreakerOptions{Threshold: 3, Cooldown: time.Minute, Probes: 1})
	ctx := context.Background()

	require.Equal(t, BreakerClosed, b.State())
	require.True(t, b.Allow(), "closed 状态应放行")

	// 失败两次还没到阈值——中途成功一次会清零
	b.Observe(errors.New("dial tcp: connection refused"))
	b.Observe(errors.New("dial tcp: connection refused"))
	require.Equal(t, BreakerClosed, b.State(), "未到阈值不应打开")

	b.Observe(nil)
	require.Equal(t, 0, b.Failures(), "成功清零计数")

	b.Observe(errors.New("boom"))
	b.Observe(errors.New("boom"))
	require.Equal(t, BreakerClosed, b.State())
	b.Observe(errors.New("boom"))
	require.Equal(t, BreakerOpen, b.State(), "第三次连续失败应打开")
	require.False(t, b.Allow(), "open 状态应拒绝")

	_ = ctx
}

// TestBreakerNilIsNotFailure key 不存在不是故障，不能把熔断器打穿。
func TestBreakerNilIsNotFailure(t *testing.T) {
	b := newQuietBreaker(t, BreakerOptions{Threshold: 2, Cooldown: time.Minute})

	for i := 0; i < 10; i++ {
		b.Observe(redis.Nil)
	}
	require.Equal(t, BreakerClosed, b.State(), "redis.Nil 不应触发熔断")
	require.True(t, b.Allow())
}

// TestBreakerHalfOpenRecovers 冷却到期 → 半开 → 探测成功 → 关闭。
func TestBreakerHalfOpenRecovers(t *testing.T) {
	b := newQuietBreaker(t, BreakerOptions{Threshold: 1, Cooldown: 20 * time.Millisecond, Probes: 1})

	b.Observe(errors.New("boom"))
	require.Equal(t, BreakerOpen, b.State())

	// 冷却未到：继续拒绝
	require.False(t, b.Allow())

	time.Sleep(30 * time.Millisecond)
	require.True(t, b.Allow(), "冷却到期应放行探测")
	require.Equal(t, BreakerHalfOpen, b.State())

	b.Observe(nil)
	require.Equal(t, BreakerClosed, b.State(), "探测成功应关闭")
	require.True(t, b.Allow())
}

// TestBreakerHalfOpenFailureReopens 半开探测失败 → 重新打开，冷却重新计时。
func TestBreakerHalfOpenFailureReopens(t *testing.T) {
	b := newQuietBreaker(t, BreakerOptions{Threshold: 1, Cooldown: 20 * time.Millisecond})

	b.Observe(errors.New("boom"))
	time.Sleep(30 * time.Millisecond)
	require.True(t, b.Allow())
	require.Equal(t, BreakerHalfOpen, b.State())

	b.Observe(errors.New("still down"))
	require.Equal(t, BreakerOpen, b.State(), "探测失败应重新打开")
	require.False(t, b.Allow(), "冷却应重新计时")
}

// TestBreakerProbeLimit 半开只放行配置数量的探测，其余继续拒绝。
func TestBreakerProbeLimit(t *testing.T) {
	b := newQuietBreaker(t, BreakerOptions{Threshold: 1, Cooldown: 10 * time.Millisecond, Probes: 2})

	b.Observe(errors.New("boom"))
	time.Sleep(15 * time.Millisecond)

	require.True(t, b.Allow(), "第 1 个探测")
	require.True(t, b.Allow(), "第 2 个探测")
	require.False(t, b.Allow(), "超出探测名额应拒绝")
}

// TestBreakerLateSuccessKeepsOpen 打开后才回来的在途成功不关闭熔断。
//
// 否则「刚打开就被一条迟到的成功关掉」，会来回抖动。
func TestBreakerLateSuccessKeepsOpen(t *testing.T) {
	b := newQuietBreaker(t, BreakerOptions{Threshold: 1, Cooldown: time.Minute})

	b.Observe(errors.New("boom"))
	require.Equal(t, BreakerOpen, b.State())

	b.Observe(nil)
	require.Equal(t, BreakerOpen, b.State(), "open 状态的迟到成功不应关闭熔断")
}

// TestBreakerHookFailsFast 真实客户端：熔断打开后命令不再发往 Redis。
func TestBreakerHookFailsFast(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{
		Addr:       mr.Addr(),
		MaxRetries: 0, // 不重试：让失败立即计数，测试不等待
	})
	t.Cleanup(func() { _ = rdb.Close() })

	b := newQuietBreaker(t, BreakerOptions{Threshold: 3, Cooldown: time.Minute})
	rdb.AddHook(b)
	ctx := context.Background()

	// 正常时能读到
	require.NoError(t, rdb.Set(ctx, "k", "v", 0).Err())

	// Redis 宕机 → 连续失败
	mr.Close()
	for i := 0; i < 3; i++ {
		require.Error(t, rdb.Get(ctx, "k").Err())
	}
	require.Equal(t, BreakerOpen, b.State())

	// 熔断后：立即返回 ErrBreakerOpen，且不再发起连接
	start := time.Now()
	err := rdb.Get(ctx, "k").Err()
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrBreakerOpen, "熔断后应返回哨兵错误")
	require.Less(t, elapsed, 20*time.Millisecond, "熔断后必须快速失败，不能等超时")
}

// TestBreakerPipelineAlsoGuarded pipeline 与单命令共用同一个判定。
func TestBreakerPipelineAlsoGuarded(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: 0})
	t.Cleanup(func() { _ = rdb.Close() })

	b := newQuietBreaker(t, BreakerOptions{Threshold: 2, Cooldown: time.Minute})
	rdb.AddHook(b)
	ctx := context.Background()

	mr.Close()
	for i := 0; i < 2; i++ {
		pipe := rdb.Pipeline()
		pipe.Get(ctx, "k")
		_, execErr := pipe.Exec(ctx)
		require.Error(t, execErr)
	}
	require.Equal(t, BreakerOpen, b.State())

	pipe := rdb.Pipeline()
	pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	require.ErrorIs(t, err, ErrBreakerOpen, "pipeline 也应被熔断挡下")
}

// TestBreakerDisabled 配置负数阈值 = 关闭熔断（不装 Hook）。
func TestBreakerDisabled(t *testing.T) {
	require.True(t, BreakerOptions{Threshold: -1}.disabled())
	require.False(t, BreakerOptions{Threshold: 0}.disabled(), "0 是默认开启而非关闭")
	require.False(t, NewBreaker(BreakerOptions{Threshold: 0}).threshold == 0, "0 应被归一为默认阈值")
}

// TestBreakerConcurrent -race 下并发失败/成功不出现状态撕裂。
func TestBreakerConcurrent(t *testing.T) {
	b := newQuietBreaker(t, BreakerOptions{Threshold: 50, Cooldown: time.Millisecond})

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if b.Allow() {
					if i%2 == 0 {
						b.Observe(errors.New("boom"))
					} else {
						b.Observe(nil)
					}
				}
			}
		}(i)
	}
	wg.Wait()

	// 状态必须是三个合法值之一，且计数非负
	st := b.State()
	require.True(t, st == BreakerClosed || st == BreakerOpen || st == BreakerHalfOpen)
	require.GreaterOrEqual(t, b.Failures(), 0)
}
