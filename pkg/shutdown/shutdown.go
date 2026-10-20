// pkg/shutdown/shutdown.go —— 优雅关闭编排（18.1：DRAIN→DRAINING→FLUSH→STOP）
//
// 架构职责：Manager 按 Phase 顺序执行注册的 Step，每步带独立超时，
// 聚合全部失败由 main 转成非零退出码。
// 协作关系：组件侧 Register（关 listener、摘注册、刷盘、停组件），
// main 侧 Run(ctx) 驱动；ctx 是进程级总预算（如 K8s 给的 30s）。
// 设计取舍（两阶段顺序）：DRAIN 摘流量（拒新）→ DRAINING 排空在途 →
// FLUSH 刷盘 → STOP 停组件。先拒新后排空，否则在途永远收敛不到零；
// 先刷盘后停组件，否则排空期/刷盘期的写入直接失败。顺序即正确性，
// 所以 Run 用显式的 Phase 序列遍历，而不是按 iota 值排序——新增 Phase
// 时忘记加进序列会直接漏执行，显式列表让每次新增都改一处、可 review。
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
// iota 从 0 递增：数值顺序即依赖顺序（先 DRAIN 后 STOP），但 Run 并不
// 按数值排序遍历，而是用显式序列（见 Run），数值仅用于日志与可读性。
type Phase int

const (
	PhaseDrain    Phase = iota // 摘注册，不再接受新流量
	PhaseDraining              // 等存量请求完成（停入口 + 排空在途）
	PhaseFlush                 // 刷盘，失败必须上报
	PhaseStop                  // 停止组件
)

// String 实现 fmt.Stringer：日志里 phase 打 "DRAIN" 而不是 "0"。
// default 分支兜住未来新增的 Phase 值（老二进制混入新 Step 时）——
// 宁可打 UNKNOWN 也不打错值误导排障。
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
// Name 用于日志与错误信息（人类可读）；Phase 决定执行批次；
// Fn 是实际动作，ctx 已被 Manager 包上单步超时（WithTimeout），
// Fn 应把 ctx 传给所有阻塞调用（DB/Kafka/网络），否则超时形同虚设。
type Step struct {
	Name  string
	Phase Phase
	Fn    func(ctx context.Context) error
}

// Manager 关闭编排器。
//
// 线程契约：Register 在启动期并发调用安全；Run 执行期间会拷贝 steps
// 快照后释放锁——执行期新 Register 的 Step 不进入本次 Run（关服路径
// 不接受新组件，这是特性：正在关的进程不该还有人在注册）。
type Manager struct {
	mu          sync.Mutex
	steps       []Step
	logger      *log.Logger
	stepTimeout time.Duration
}

// NewManager 创建 Manager。
// 默认单步 20s：覆盖一次 Kafka/DB flush 批次的富余量，又不让单步卡死
// 吞掉总预算（总预算由 ctx 控制，单步超时是二级防线）。
func NewManager(logger *log.Logger) *Manager {
	return &Manager{logger: logger, stepTimeout: 20 * time.Second}
}

// WithStepTimeout 覆盖单步超时（测试与短关闭预算场景使用）。
// 链式返回 *Manager：builder 风格，NewManager(...).WithStepTimeout(1s)。
// d<=0 忽略而非设 0：0 会让所有步骤立刻超时，几乎一定是调用方误传。
func (m *Manager) WithStepTimeout(d time.Duration) *Manager {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d > 0 {
		m.stepTimeout = d
	}
	return m
}

// Register 注册关闭步骤（同一阶段内按注册顺序执行）。
// 阶段间顺序由 Phase 决定，阶段内顺序即注册顺序——有依赖的步骤
// （如「先停 producer 再关 DB」）应放同阶段并按序注册，跨阶段依赖
// 用 Phase 表达，不要指望注册顺序穿越阶段。
func (m *Manager) Register(name string, phase Phase, fn func(ctx context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.steps = append(m.steps, Step{Name: name, Phase: phase, Fn: fn})
}

// Run 按阶段执行全部关闭步骤，返回所有失败步骤的聚合错误。
//
// 单步失败不中断后续步骤：跳过清理会让进程更难收场；但失败必须出现在返回值里，
// 由 main 以非零码退出。这样「保存失败」不会被日志里的一行 Warn 掩盖。
// 返回值语义：nil = 全部成功；errors.Join 聚合 = 有步骤失败（含预算耗尽
// 被放弃的步骤）；ctx 预算耗尽时提前返回，未执行的步骤记为一条错误。
func (m *Manager) Run(ctx context.Context) error {
	// 快照拷贝 + 读超时后立刻解锁：Fn 可能执行很久，持锁会阻塞 Register
	// 与其它 Manager 方法；拷贝还防御执行期 append 与底层数组共享的竞态
	// （直接 range m.steps 可能读到并发 append 的新元素或踩到扩容）。
	m.mu.Lock()
	steps := make([]Step, len(m.steps))
	copy(steps, m.steps)
	timeout := m.stepTimeout
	m.mu.Unlock()

	var errs []error
	// 显式 Phase 序列（不依赖 iota 数值排序）：新增 Phase 必须加进这里，
	// 漏加 = 该阶段步骤永不执行，编译期不报错——用显式列表让疏漏可见。
	for _, phase := range []Phase{PhaseDrain, PhaseDraining, PhaseFlush, PhaseStop} {
		for _, st := range steps {
			if st.Phase != phase {
				continue
			}
			// 总预算已耗尽：剩余步骤不再执行，如实报告从哪一步开始被放弃。
			// 不静默跳过：K8s 只给有限关服时间，被放弃的步骤必须能在
			// 退出日志里看到，运维才知道是谁没跑完。
			if err := ctx.Err(); err != nil {
				errs = append(errs, fmt.Errorf("shutdown budget exhausted before %s: %w", st.Name, err))
				return errors.Join(errs...)
			}
			if m.logger != nil {
				m.logger.Info("shutdown step", "phase", phase.String(), "step", st.Name)
			}
			// 单步超时从总预算里切分：stepCtx 的 Deadline = min(总预算, now+timeout)，
			// 总预算先到期时子 ctx 一并取消（context 取消沿树传播）。
			stepCtx, cancel := context.WithTimeout(ctx, timeout)
			err := st.Fn(stepCtx)
			// 立即 cancel 而非 defer：循环里 defer 会累积到 Run 返回，
			// 每个 stepCtx 背后的 timer 要等到函数结束才释放；立即调用
			// 让每步的资源当步回收（go vet lostcancel 也要求这个模式）。
			cancel()
			if err != nil {
				if m.logger != nil {
					m.logger.Warn("shutdown step failed", "step", st.Name, "err", err)
				}
				errs = append(errs, fmt.Errorf("%s (%s): %w", st.Name, phase.String(), err))
			}
		}
	}
	// errors.Join：全部失败一次带回（Go 1.20），main 打一行 Warn + 非零退出
	return errors.Join(errs...)
}

// Printf 调试用。
// 独立于 String：调试输出带 "phase=" 前缀，直接可粘进日志过滤器。
func (p Phase) Printf() string { return fmt.Sprintf("phase=%s", p.String()) }
