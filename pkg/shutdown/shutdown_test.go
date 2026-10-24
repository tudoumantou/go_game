// pkg/shutdown/shutdown_test.go —— 关闭编排顺序与失败传播
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

// recorder 按执行顺序记录步骤名，可注入失败与阻塞。
type recorder struct {
	mu    sync.Mutex
	order []string
	fail  map[string]error
	block map[string]time.Duration
}

func newRecorder() *recorder {
	return &recorder{fail: map[string]error{}, block: map[string]time.Duration{}}
}

func (r *recorder) step(name string) func(context.Context) error {
	return func(ctx context.Context) error {
		r.mu.Lock()
		r.order = append(r.order, name)
		fail := r.fail[name]
		block := r.block[name]
		r.mu.Unlock()
		if block > 0 {
			// 响应 ctx：真实步骤会检查 stepCtx，测试也必须如此，否则测不出超时。
			select {
			case <-time.After(block):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return fail
	}
}

func (r *recorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// TestRunOrdersByPhase 阶段优先、阶段内保持注册顺序：乱序注册也必须按
// DRAIN → DRAINING → FLUSH → STOP 执行，否则「先刷盘后停在途」会重新出现。
func TestRunOrdersByPhase(t *testing.T) {
	rec := newRecorder()
	m := NewManager(nil)
	m.Register("stop-actor", PhaseStop, rec.step("stop-actor"))
	m.Register("flush", PhaseFlush, rec.step("flush"))
	m.Register("drain-inflight", PhaseDraining, rec.step("drain-inflight"))
	m.Register("unregister", PhaseDrain, rec.step("unregister"))
	m.Register("stop-rpc", PhaseDraining, rec.step("stop-rpc"))

	err := m.Run(context.Background())
	require.NoError(t, err)
	want := []string{"unregister", "drain-inflight", "stop-rpc", "flush", "stop-actor"}
	got := rec.got()
	require.EqualValues(t, len(want), len(got))
	for i := range want {
		require.EqualValues(t, want[i], got[i])
	}
}

// TestRunAggregatesErrors 单步失败不中断后续清理，但必须出现在返回值里。
func TestRunAggregatesErrors(t *testing.T) {
	rec := newRecorder()
	rec.fail["flush"] = errors.New("mysql down")
	rec.fail["stop-rpc"] = errors.New("graceful stop timeout")

	m := NewManager(nil)
	m.Register("stop-rpc", PhaseDraining, rec.step("stop-rpc"))
	m.Register("flush", PhaseFlush, rec.step("flush"))
	m.Register("stop-actor", PhaseStop, rec.step("stop-actor"))

	err := m.Run(context.Background())
	require.Error(t, err, "expected aggregated error")
	msg := err.Error()
	require.Contains(t, msg, "flush", fmt.Sprintf("flush error missing: %v", err))
	require.Contains(t, msg, "mysql down")
	require.Contains(t, msg, "stop-rpc", fmt.Sprintf("rpc error missing: %v", err))
	// 后续步骤仍执行
	got := rec.got()
	require.Len(t, got, 3, fmt.Sprintf("stop step skipped: %v", got))
	require.EqualValues(t, "stop-actor", got[2])
}

// TestRunStopsWhenBudgetExhausted 总预算耗尽后不再启动新步骤，并报告从哪一步中断。
//
// 用手动 cancel + 显式放行代替「20ms 超时 vs 80ms 阻塞」：
// 原写法在全量并行跑时偶发失败——负载高时 context 的定时器会延迟触发，
// 两个 timer 可能同时到期，select 随机选中 time.After 分支，步骤就"正常完成"了，
// 于是 Run 返回 nil。时间竞争测不出确定的行为。
func TestRunStopsWhenBudgetExhausted(t *testing.T) {
	rec := newRecorder()
	started := make(chan struct{})
	release := make(chan struct{})

	m := NewManager(nil)
	m.Register("drain-inflight", PhaseDraining, func(context.Context) error {
		close(started)
		<-release
		return nil // 故意不响应 ctx：真实步骤可能已经跑完，而预算恰好用尽
	})
	m.Register("flush", PhaseFlush, rec.step("flush"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()

	<-started
	cancel() // 预算耗尽（生产里是 WithTimeout 到期）
	close(release)

	select {
	case err := <-done:
		require.Error(t, err, "预算耗尽必须报错，不能静默跳过剩余步骤")
		require.Contains(t, err.Error(), "budget", fmt.Sprintf("unexpected error: %v", err))
		require.Contains(t, err.Error(), "flush", "要告诉运维是从哪一步开始被放弃")
		require.Empty(t, rec.got(), "预算耗尽后不能再执行后续步骤")
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未在预期时间内返回")
	}
}

// TestRunStepTimeout 单步超过 stepTimeout 时按失败返回，不拖死整个关闭流程。
//
// 步骤只等 ctx.Done()、不等 time.After：原写法（100ms 阻塞 vs 20ms 超时）
// 在高负载下两个 timer 会同时到期，select 随机选中「睡够」分支返回 nil，
// 用例偶发失败。只等待一个信号才能确定地测到超时。
func TestRunStepTimeout(t *testing.T) {
	rec := newRecorder()

	m := NewManager(nil).WithStepTimeout(20 * time.Millisecond)
	m.Register("stop-rpc", PhaseDraining, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	m.Register("flush", PhaseFlush, rec.step("flush"))

	err := m.Run(context.Background())
	require.Error(t, err, fmt.Sprintf("expected step timeout error, got %v", err))
	require.Contains(t, err.Error(), "stop-rpc")
	// 超时的步骤不影响后续步骤：flush 仍然要跑（停机不能因为一步慢就全放弃）
	require.Equal(t, []string{"flush"}, rec.got(), "后续步骤必须继续执行")
}
