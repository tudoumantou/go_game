package periodic

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recorder 记录 Warn 调用，用于断言「该记的记了、不该记的没记」。
type recorder struct {
	mu   sync.Mutex
	msgs []string
}

func (r *recorder) Warn(msg string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, msg)
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *recorder) first() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.msgs) == 0 {
		return ""
	}
	return r.msgs[0]
}

// TestRunsOnInterval 验证「按周期执行，且第一拍在 interval 之后」。
//
// 后半句是关键：有人会误以为 Run 会先跑一次。这条用例把它钉住——
// 若实现改成「先执行再等 ticker」，第二拍的计数会多 1。
func TestRunsOnInterval(t *testing.T) {
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:     "t",
			Interval: 5 * time.Millisecond,
			Fn: func(context.Context) error {
				n.Add(1)
				return nil
			},
		})
	}()

	require.Eventually(t, func() bool { return n.Load() >= 3 }, time.Second, 2*time.Millisecond)
	cancel()
	<-done
	require.GreaterOrEqual(t, n.Load(), int32(3))
}

// TestStopsOnContextCancel 取消后必须在一个周期内退出，不能多等一拍。
//
// 用一个很长的 interval：若实现是「先 sleep 再判 ctx」，这条用例会
// 卡到测试超时；正确实现应当在 cancel 后立刻返回。
func TestStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:     "t",
			Interval: time.Hour,
			Fn:       func(context.Context) error { return nil },
		})
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("cancel 之后没退出：说明循环在 sleep 而不是 select ctx.Done")
	}
}

// TestGateSkipsExecution gate 为 false 时 Fn 一次都不该被调，也不能算失败。
func TestGateSkipsExecution(t *testing.T) {
	var n atomic.Int32
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:     "t",
			Interval: 2 * time.Millisecond,
			Gate:     func() bool { return false },
			Logger:   rec,
			Fn: func(context.Context) error {
				n.Add(1)
				return nil
			},
		})
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done

	require.Zero(t, n.Load(), "gate 为 false 时 Fn 不该执行")
	require.Zero(t, rec.count(), "跳过不是失败，不该记日志")
}

// TestGateAllowsExecution 补上「gate 为 true 时确实跑了」，否则上一条用例
// 在「Fn 永远不执行」的实现下也是绿的。
func TestGateAllowsExecution(t *testing.T) {
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:     "t",
			Interval: 2 * time.Millisecond,
			Gate:     func() bool { return true },
			Fn: func(context.Context) error {
				n.Add(1)
				return nil
			},
		})
	}()

	require.Eventually(t, func() bool { return n.Load() >= 2 }, time.Second, 2*time.Millisecond)
	cancel()
	<-done
}

// TestErrorKeepsLoopRunning 默认策略：报错只记 Warn，下一拍照跑。
func TestErrorKeepsLoopRunning(t *testing.T) {
	var n atomic.Int32
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:     "reconcile",
			Interval: 2 * time.Millisecond,
			Logger:   rec,
			Fn: func(context.Context) error {
				n.Add(1)
				return errors.New("boom")
			},
		})
	}()

	require.Eventually(t, func() bool { return n.Load() >= 3 }, time.Second, 2*time.Millisecond)
	cancel()
	<-done

	require.GreaterOrEqual(t, rec.count(), 2, "每次失败都该记一条 Warn")
	require.Equal(t, "reconcile tick failed", rec.first())
}

// TestStopOnError 心跳类任务：失败即退出，不能每秒再失败一次。
func TestStopOnError(t *testing.T) {
	var n atomic.Int32
	rec := &recorder{}
	done := make(chan struct{})

	go func() {
		defer close(done)
		Run(context.Background(), Options{
			Name:        "heartbeat",
			Interval:    time.Millisecond,
			StopOnError: true,
			Logger:      rec,
			Fn: func(context.Context) error {
				n.Add(1)
				return errors.New("stream closed")
			},
		})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StopOnError 时应当退出循环")
	}
	require.Equal(t, int32(1), n.Load(), "只该执行一次就退出")
	require.Equal(t, "heartbeat stopped on error", rec.first())
}

// TestCanceledContextIsNotTaskFailure 关服时 Fn 通常返回 context.Canceled，
// 这不该被记成任务失败，也不该触发 StopOnError 的「stopped on error」。
//
// 缺失这条会让每次正常停机都留一条假告警，久了整个 Warn 字段就没人看了。
func TestCanceledContextIsNotTaskFailure(t *testing.T) {
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:        "t",
			Interval:    time.Millisecond,
			StopOnError: true,
			Logger:      rec,
			Fn: func(ctx context.Context) error {
				cancel() // 模拟：取下一条消息时发现 ctx 已取消
				return ctx.Err()
			},
		})
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ctx 取消后应当退出")
	}
	require.Zero(t, rec.count(), "取消不是任务失败，不该记 Warn")
}

// TestDoneChannel 额外退出通道：连接断开关 Done，不必 cancel 整个 ctx。
func TestDoneChannel(t *testing.T) {
	var n atomic.Int32
	stop := make(chan struct{})
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:     "t",
			Interval: 2 * time.Millisecond,
			Done:     stop,
			Fn: func(context.Context) error {
				n.Add(1)
				return nil
			},
		})
	}()

	require.Eventually(t, func() bool { return n.Load() >= 2 }, time.Second, 2*time.Millisecond)
	close(stop)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Done 关闭后应当退出")
	}
}

// TestNonPositiveIntervalDoesNotPanic 周期来自配置时配成 0 是常见事故。
//
// time.NewTicker(0) 会 panic；本包必须把它变成「记一条 Warn、任务不启动」，
// 因为 panic 发生在启动期，会把「有一个后台任务没跑」伪装成「进程起不来」。
func TestNonPositiveIntervalDoesNotPanic(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
	}{
		{"零", 0},
		{"负数", -time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := &recorder{}
			var n atomic.Int32
			// 同步调用：实现正确时会立刻返回；若它 panic 或阻塞，
			// require.NotPanics 与超时都会失败。
			require.NotPanics(t, func() {
				done := make(chan struct{})
				go func() {
					defer close(done)
					Run(context.Background(), Options{
						Name:     "t",
						Interval: c.interval,
						Logger:   rec,
						Fn: func(context.Context) error {
							n.Add(1)
							return nil
						},
					})
				}()
				select {
				case <-done:
				case <-time.After(200 * time.Millisecond):
					t.Fatal("非正周期应当立即返回而不是进入循环")
				}
			})
			require.Equal(t, "periodic task interval is not positive, not started", rec.first(),
				"必须留下痕迹，否则「任务没跑」会静默发生")
			require.Zero(t, n.Load())
		})
	}
}

// TestEmptyNameFallsBack Name 为空时日志里用 "periodic" 而不是空字符串
// ——无主错误比缺一条日志更难查。
func TestEmptyNameFallsBack(t *testing.T) {
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Interval: 2 * time.Millisecond,
			Logger:   rec,
			Fn:       func(context.Context) error { return errors.New("boom") },
		})
	}()

	require.Eventually(t, func() bool { return rec.count() >= 1 }, time.Second, 2*time.Millisecond)
	cancel()
	<-done
	require.Equal(t, "periodic tick failed", rec.first())
}

// TestNoBodyDoesNotSpin Fn 为空时不能进入「每拍空转」的循环。
func TestNoBodyDoesNotSpin(t *testing.T) {
	rec := &recorder{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(context.Background(), Options{Name: "t", Interval: time.Millisecond, Logger: rec})
	}()
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("没有 Fn 时应当直接返回")
	}
	require.Equal(t, "periodic task has no body, not started", rec.first())
}

// TestGuard 缺省即禁止：nil 开关不能退化成「无 gate 全放行」。
//
// 这三条覆盖了 leader 类任务的全部语义：没接选举→不跑、选上了→跑、
// 失去了→不跑。少任何一条，把 Guard 写成「nil 直接放行」都会是绿的。
func TestGuard(t *testing.T) {
	require.False(t, Guard(nil)(), "nil 开关必须解释为「永不执行」")

	yes := Guard(func() bool { return true })
	require.True(t, yes())

	no := Guard(func() bool { return false })
	require.False(t, no())
}

// TestGuardNilGateNeverRuns 端到端：Guard(nil) 接进 Run 之后 Fn 一次都不跑。
func TestGuardNilGateNeverRuns(t *testing.T) {
	var n atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Options{
			Name:     "leader-task",
			Interval: time.Millisecond,
			Gate:     Guard(nil),
			Fn: func(context.Context) error {
				n.Add(1)
				return nil
			},
		})
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	require.Zero(t, n.Load(), "没接选举时跑有主任务会造成多实例重复执行")
}
