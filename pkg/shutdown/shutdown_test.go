// pkg/shutdown/shutdown_test.go —— 关闭编排顺序与失败传播
package shutdown

import (
	"context"
	"errors"
	"strings"
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

	if err := m.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"unregister", "drain-inflight", "stop-rpc", "flush", "stop-actor"}
	got := rec.got()
	if len(got) != len(want) {
		t.Fatalf("order: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order: got %v want %v", got, want)
		}
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
	if err == nil {
		t.Fatal("expected aggregated error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "flush") || !strings.Contains(msg, "mysql down") {
		t.Fatalf("flush error missing: %v", err)
	}
	if !strings.Contains(msg, "stop-rpc") {
		t.Fatalf("rpc error missing: %v", err)
	}
	// 后续步骤仍执行
	if got := rec.got(); len(got) != 3 || got[2] != "stop-actor" {
		t.Fatalf("stop step skipped: %v", got)
	}
}

// TestRunStopsWhenBudgetExhausted 总预算耗尽后不再启动新步骤，并报告从哪一步中断。
func TestRunStopsWhenBudgetExhausted(t *testing.T) {
	rec := newRecorder()
	rec.block["drain-inflight"] = 80 * time.Millisecond

	m := NewManager(nil)
	m.Register("drain-inflight", PhaseDraining, rec.step("drain-inflight"))
	m.Register("flush", PhaseFlush, rec.step("flush"))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := m.Run(ctx)
	if err == nil {
		t.Fatal("expected budget error")
	}
	if !strings.Contains(err.Error(), "budget") {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, name := range rec.got() {
		if name == "flush" {
			t.Fatal("flush must not run after budget exhaustion")
		}
	}
}

// TestRunStepTimeout 单步超过 stepTimeout 时按失败返回，不拖死整个关闭流程。
func TestRunStepTimeout(t *testing.T) {
	rec := newRecorder()
	rec.block["stop-rpc"] = 100 * time.Millisecond

	m := NewManager(nil).WithStepTimeout(20 * time.Millisecond)
	m.Register("stop-rpc", PhaseDraining, rec.step("stop-rpc"))
	m.Register("flush", PhaseFlush, rec.step("flush"))

	err := m.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "stop-rpc") {
		t.Fatalf("expected step timeout error, got %v", err)
	}
	if got := rec.got(); len(got) != 2 || got[1] != "flush" {
		t.Fatalf("flush skipped after step timeout: %v", got)
	}
}
