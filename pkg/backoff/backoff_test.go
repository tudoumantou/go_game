package backoff

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestNextDoublesAndCaps 首次等于 base，之后翻倍封顶。
//
// 这条同时锁住两个曾经各写一遍的实现差异：cross/center 是「翻倍后封顶」，
// leader 是「翻倍前判上限」——后者从 20s 翻到 40s 就再也不判了，越过
// 自己注释里写的 30s。
func TestNextDoublesAndCaps(t *testing.T) {
	b := New(time.Second, 30*time.Second)
	want := []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for i, w := range want {
		if got := b.Next(); got != w {
			t.Fatalf("第 %d 次 Next 应为 %v，实际 %v", i+1, w, got)
		}
	}
}

// TestNextDoesNotOvershootMax 从 max 的中途翻倍也不能越过上限。
func TestNextDoesNotOvershootMax(t *testing.T) {
	b := New(20*time.Second, 30*time.Second)
	if got := b.Next(); got != 20*time.Second {
		t.Fatalf("首次应为 20s，实际 %v", got)
	}
	if got := b.Next(); got != 30*time.Second {
		t.Fatalf("第二次应封顶 30s，实际 %v", got)
	}
	if got := b.Current(); got != 30*time.Second {
		t.Fatalf("Current 应等于下一次等待值 30s，实际 %v", got)
	}
}

// TestNewFixesBadArgs base<=0 取默认值，max<base 退化为固定间隔。
func TestNewFixesBadArgs(t *testing.T) {
	if got := New(0, time.Second).Next(); got != defaultBase {
		t.Fatalf("base=0 应取默认 %v，实际 %v", defaultBase, got)
	}
	b := New(5*time.Second, time.Second) // max < base
	for i := 0; i < 3; i++ {
		if got := b.Next(); got != 5*time.Second {
			t.Fatalf("max<base 时应固定 5s，第 %d 次得到 %v", i+1, got)
		}
	}
}

// TestReset 重置后回到 base。
func TestReset(t *testing.T) {
	b := New(time.Second, time.Minute)
	b.Next()
	b.Next()
	b.Reset()
	if got := b.Next(); got != time.Second {
		t.Fatalf("Reset 后应回到 base，实际 %v", got)
	}
}

// TestLoopReturnsCtxErr ctx 取消是「进程退出」，必须以错误返回让上层区分。
func TestLoopReturnsCtxErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := New(time.Millisecond, time.Millisecond)

	var n atomic.Int32
	err := Loop(ctx, nil, b, func(context.Context) error {
		if n.Add(1) == 3 {
			cancel()
		}
		return errors.New("boom") // 错误不参与控制流
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应返回 context.Canceled，实际 %v", err)
	}
	if n.Load() < 3 {
		t.Fatalf("fn 应至少跑 3 次，实际 %d", n.Load())
	}
}

// TestLoopStopsOnStop stop 关闭是「主动停机」，返回 nil。
func TestLoopStopsOnStop(t *testing.T) {
	stop := make(chan struct{})
	b := New(time.Millisecond, time.Millisecond)

	err := Loop(context.Background(), stop, b, func(context.Context) error {
		close(stop)
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("stop 触发应返回 nil，实际 %v", err)
	}
}

// TestLoopCallsOnRetryOnlyOnError onRetry 只在出错时回调，成功返回不刷日志。
func TestLoopCallsOnRetryOnlyOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := New(time.Millisecond, time.Millisecond)

	var retries atomic.Int32
	var calls atomic.Int32
	err := Loop(ctx, nil, b, func(context.Context) error {
		if calls.Add(1) >= 3 {
			cancel()
			return nil // 最后一轮不带错误
		}
		return errors.New("boom")
	}, func(err error, wait time.Duration) {
		retries.Add(1)
		if wait <= 0 {
			t.Errorf("onRetry 的 wait 应为正，实际 %v", wait)
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应返回 context.Canceled，实际 %v", err)
	}
	if retries.Load() != 2 {
		t.Fatalf("应回调 2 次（两次出错），实际 %d", retries.Load())
	}
}

// TestLoopBacksOff 退避真的在等：两轮至少花掉一个 base。
func TestLoopBacksOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	base := 30 * time.Millisecond
	b := New(base, base)

	start := time.Now()
	var n atomic.Int32
	err := Loop(ctx, nil, b, func(context.Context) error {
		if n.Add(1) == 2 {
			cancel()
		}
		return nil
	}, nil)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应返回 context.Canceled，实际 %v", err)
	}
	if elapsed < base {
		t.Fatalf("两轮之间应等待至少 %v，实际只花了 %v", base, elapsed)
	}
}
