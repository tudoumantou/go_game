// pkg/shutdown/shutdown.go —— 优雅关闭编排（18.1：DRAIN→DRAINING→FLUSH→STOP）
package shutdown

import (
	"context"
	"fmt"
	"sync"
	"time"

	"game_study/pkg/log"
)

// Phase 关闭阶段。
type Phase int

const (
	PhaseDrain    Phase = iota // 摘注册，不再接受新流量
	PhaseDraining              // 等待存量请求完成
	PhaseFlush                 // 刷盘
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
	mu     sync.Mutex
	steps  []Step
	logger *log.Logger
}

// NewManager 创建 Manager。
func NewManager(logger *log.Logger) *Manager {
	return &Manager{logger: logger}
}

// Register 注册关闭步骤（按注册顺序执行）。
func (m *Manager) Register(name string, phase Phase, fn func(ctx context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps = append(m.steps, Step{Name: name, Phase: phase, Fn: fn})
}

// Run 执行全部关闭步骤。
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	steps := make([]Step, len(m.steps))
	copy(steps, m.steps)
	m.mu.Unlock()

	// 按阶段顺序执行
	for _, phase := range []Phase{PhaseDrain, PhaseDraining, PhaseFlush, PhaseStop} {
		for _, st := range steps {
			if st.Phase != phase {
				continue
			}
			m.logger.Info("shutdown step", "phase", phase.String(), "step", st.Name)
			stepCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			err := st.Fn(stepCtx)
			cancel()
			if err != nil {
				m.logger.Warn("shutdown step failed", "step", st.Name, "err", err)
			}
		}
	}
	return nil
}

// Printf 调试用。
func (p Phase) Printf() string { return fmt.Sprintf("phase=%s", p.String()) }
