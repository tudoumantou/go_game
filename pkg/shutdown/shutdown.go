// pkg/shutdown/shutdown.go —— 优雅关闭编排（18.1：DRAIN→DRAINING→FLUSH→STOP）
package shutdown

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"game_study/pkg/log"
)

// Phase 关闭阶段。
type Phase int

const (
	PhaseDrain    Phase = iota // 摘注册，不再接受新流量
	PhaseDraining              // 等存量请求完成（停入口 + 排空在途）
	PhaseFlush                 // 刷盘，失败必须上报
	PhaseStop                  // 停止组件
)

func (p Phase) String() string {
	switch p {
	case PhaseDrain:
		return "DRAIN"
	case PhaseDraining:
		return "DRAINING"
	case PhaseFlush:
		return "FLUSH"
	case PhaseStop:
		return "STOP"
	default:
		return "UNKNOWN"
	}
}

// Step 关闭步骤。
type Step struct {
	Name  string
	Phase Phase
	Fn    func(ctx context.Context) error
}

// Manager 关闭编排器。
type Manager struct {
	mu          sync.Mutex
	steps       []Step
	logger      *log.Logger
	stepTimeout time.Duration
}

// NewManager 创建 Manager。
func NewManager(logger *log.Logger) *Manager {
	return &Manager{logger: logger, stepTimeout: 20 * time.Second}
}

// WithStepTimeout 覆盖单步超时（测试与短关闭预算场景使用）。
func (m *Manager) WithStepTimeout(d time.Duration) *Manager {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d > 0 {
		m.stepTimeout = d
	}
	return m
}

// Register 注册关闭步骤（同一阶段内按注册顺序执行）。
func (m *Manager) Register(name string, phase Phase, fn func(ctx context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps = append(m.steps, Step{Name: name, Phase: phase, Fn: fn})
}

// Run 按阶段执行全部关闭步骤，返回所有失败步骤的聚合错误。
//
// 单步失败不中断后续步骤：跳过清理会让进程更难收场；但失败必须出现在返回值里，
// 由 main 以非零码退出。这样「保存失败」不会被日志里的一行 Warn 掩盖。
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	steps := make([]Step, len(m.steps))
	copy(steps, m.steps)
	timeout := m.stepTimeout
	m.mu.Unlock()

	var errs []error
	for _, phase := range []Phase{PhaseDrain, PhaseDraining, PhaseFlush, PhaseStop} {
		for _, st := range steps {
			if st.Phase != phase {
				continue
			}
			// 总预算已耗尽：剩余步骤不再执行，如实报告从哪一步开始被放弃。
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("shutdown budget exhausted before %s: %w", st.Name, err))
				return errors.Join(errs...)
			}
			if m.logger != nil {
				m.logger.Info("shutdown step", "phase", phase.String(), "step", st.Name)
			}
			stepCtx, cancel := context.WithTimeout(ctx, timeout)
			err := st.Fn(stepCtx)
			cancel()
			if err != nil {
				if m.logger != nil {
					m.logger.Warn("shutdown step failed", "step", st.Name, "err", err)
				}
				errs = append(errs, fmt.Errorf("%s (%s): %w", st.Name, phase.String(), err))
			}
		}
	}
	return errors.Join(errs...)
}

// Printf 调试用。
func (p Phase) Printf() string { return fmt.Sprintf("phase=%s", p.String()) }
