// Package mgr 实现 RPG 场景的权威 Actor（MGR：Map Global Runtime）。
//
// # 它在架构中的位置（见 scenemap/doc.go 的架构总则）
//
//	一张可玩的图 = 1 个 AOI Actor + 1 个 MGR Actor + N 个玩家 Actor
//
// 本包是其中的 MGR：
//   - **唯一权威**：站位、格子占用、怪物 AI 状态、关图计时都在这里；
//   - **单写者**：所有状态只在 run 这个 goroutine 里读写，不加锁；
//   - **命令化**：外部（玩家 Actor / RoomManager / 定时器）只能通过
//     mailbox 投递 Command，串行执行；
//   - **只发通知、不关自己**：关图走 OnClose 交给 RoomManager 收口，
//     避免"Actor 在自己的 goroutine 里等自己退出"的死锁。
//
// # 为什么必须串行
//
// 一只怪从"选中目标"到"扣血"要读三个状态（自身 AI、目标位置、仇恨表），
// 中间只要被另一个 goroutine 插入修改，就会出现"打已经死掉的目标"
// "仇恨表被并发写"这类问题。单 Actor 用串行换确定性：所有结算按命令
// 顺序发生，线上可以按日志逐条回放。
//
// # 四个定时器（规格 7.1）
//
//	loop      1s    吃倍速   刷怪推进 / 玩法 Loop / 收割关图
//	buffCheck 500ms 吃倍速   怪物 buff / 召唤超时（按 UUID 升序结算）
//	oneSec    1s    不吃     场景触发器（无触发器自动停表）
//	halfSec   500ms 不吃     默认关闭
//
// 用 time.Timer 自续期而不是 time.Ticker：Ticker 的间隔不可改，倍速一换
// 就得 Stop 再 New，而 New 出来的时刻与旧 Tick 之间会丢/多一拍。
// Timer 每次触发后 Reset(interval/speed) 可以精确表达"下一拍就是新的间隔"。
package mgr

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
)

// commandBudget 是单次唤醒最多处理的命令数。
//
// 为什么不一次把 mailbox 抽干：如果上游持续高速投递（压测、被刷），
// 一次抽干会让 MGR 长时间不回到 select，定时器全部延后、停服信号也
// 收不到。限制预算保证"每 64 条命令就让出一次调度点"，
// 让定时器/ctx.Done 有机会被处理。这是典型的"批处理 + 公平性"取舍。
const commandBudget = 64

// Logger 是 MGR 的日志依赖。只依赖三个方法，方便测试注入假实现。
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NopLogger 丢弃全部日志。
type NopLogger struct{}

// Info 实现 Logger。
func (NopLogger) Info(string, ...any) {}

// Warn 实现 Logger。
func (NopLogger) Warn(string, ...any) {}

// Error 实现 Logger。
func (NopLogger) Error(string, ...any) {}

// WalkableFunc 是静态可走查询。
//
// 由房间生成器（地图矩阵）注入。用函数而不是 *grid.Blocker：
// MGR 只需要"这格能不能走"，不需要改静态矩阵；机关开门这类动态改图
// 由拥有 Blocker 的一方负责，MGR 通过同一个函数立刻感知。
//
// nil 表示"不做静态校验"（只校验边界与动态占位）。测试里常用 nil。
type WalkableFunc func(p scenemap.GridPos) bool

// Config 是 MGR 的构造参数。
type Config struct {
	// Key 地图唯一标识。
	Key scenemap.MapKey
	// Route 分线号。
	Route scenemap.Route
	// MapID 地图配置 ID；0 时取 Key.MapID，二者不一致视为配置错误。
	MapID int32
	// Geo 像素↔格换算参数。零值时取 grid.DefaultGeometry()。
	Geo grid.Geometry
	// Size 地图格子尺寸。
	Size grid.Size
	// MailboxSize 邮箱容量。
	MailboxSize int
	// TickInterval loop 定时器的基础间隔（默认 1s）。
	TickInterval time.Duration
	// EnterProtect 空房保护时长：开图/离图后至少保留这么久（默认 60s）。
	EnterProtect time.Duration
	// OfflineGrace 掉线后保留时长（默认 120s）。
	OfflineGrace time.Duration
	// QuitClose 主动退图时是否立即关图（否则按 EnterProtect 保留空房）。
	QuitClose bool
	// SpeedPermille 倍速（千分比）：1000=1x，2000=2x。默认 1000。
	SpeedPermille int32

	// ---- 以下是落地时补充的参数（原规格 3.3 未列）----

	// MaxStep 单次位置同步允许的最大切比雪夫步长（默认 3）。
	// 3 而不是 1：客户端会跳包/追帧，1 会把正常玩家全部判成异常。
	MaxStep int32
	// HalfSecEnabled 是否启用 500ms 定时器（默认关闭）。
	HalfSecEnabled bool
	// LenientNoEffect 是否对"命令未改变状态"只记日志而不报错。
	// 默认 false（严格）：开发期立刻炸出来；线上热修可以开成宽松。
	LenientNoEffect bool
	// Self 地图自身编号（由 RoomManager 分配，可留 0）。
	Self scenemap.UUID
	// InitArgs 透传给 Gameplay.MgrInit 的参数（副本难度、队伍信息等）。
	InitArgs []any
	// CallTimeout 同步调用的默认超时（默认 3s）。
	CallTimeout time.Duration
}

// normalize 校验并补齐默认值。
//
// 配置错误不在 New 里 panic：Actor 可能是在 RoomManager 的 goroutine 里
// 构造的，panic 会带走整个房间管理器。这里把错误存起来，Start 时走
// "开图失败 → OnClose" 的正常流程，让问题以业务错误的形式暴露。
func (c *Config) normalize() error {
	if err := c.Key.Validate(); err != nil {
		return err
	}
	if c.MapID == 0 {
		c.MapID = c.Key.MapID
	}
	if c.MapID != c.Key.MapID {
		return fmt.Errorf("%w: MapID=%d 与 Key.MapID=%d 不一致",
			scenemap.ErrInvalidArgument, c.MapID, c.Key.MapID)
	}
	if c.Geo.CellW == 0 && c.Geo.CellH == 0 && c.Geo.AoiGrid == 0 {
		c.Geo = grid.DefaultGeometry()
	}
	if err := c.Geo.Validate(); err != nil {
		return err
	}
	if !c.Size.Valid() {
		return fmt.Errorf("%w: 地图尺寸非法 %dx%d", scenemap.ErrInvalidArgument, c.Size.Width, c.Size.Height)
	}
	if c.MailboxSize <= 0 {
		c.MailboxSize = 1024
	}
	if c.TickInterval <= 0 {
		c.TickInterval = time.Second
	}
	if c.EnterProtect <= 0 {
		c.EnterProtect = 60 * time.Second
	}
	if c.OfflineGrace <= 0 {
		c.OfflineGrace = 120 * time.Second
	}
	if c.SpeedPermille <= 0 {
		c.SpeedPermille = 1000
	}
	if c.MaxStep <= 0 {
		c.MaxStep = 3
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = 3 * time.Second
	}
	return nil
}

// Deps 是 MGR 的外部依赖。全部可空：空时退化为"无 AOI / 无玩法 / 无日志"，
// 这让"只想测索引与关图计时"的测试不必构造一堆桩。
type Deps struct {
	// AOI 视野与广播。nil 时用 NoopAOI。
	AOI AOIHandle
	// Gameplay 玩法插件。nil 时用 Unimplemented（只跑框架本身）。
	Gameplay Gameplay
	// Logger 日志。nil 时丢弃。
	Logger Logger
	// Clock 时钟（unix 毫秒）。nil 时用 time.Now。
	Clock func() int64
	// OnClose 关图通知。MGR 只发通知，真正关图由 RoomManager 收口。
	OnClose func(key scenemap.MapKey, reason string)
	// Walkable 静态可走查询。nil 时只校验边界与动态占位。
	Walkable WalkableFunc
	// AITick 怪物 AI 的每轮推进（500ms，吃倍速）。
	//
	// 用函数钩子而不是让 mgr import entity：entity 是 mgr 的扩展
	// （它要读写 State.NPES），反向 import 会成环。entity 包提供
	// `entity.TickAll(s, now)`，装配时塞进来即可。
	// nil 表示不跑 AI（纯框架/测试）。
	AITick func(s *State, nowMS int64)
	// NpcDead 怪物死亡回调（由 entity 在血量归零时调用，
	// 这里允许玩法覆盖结算顺序）。
	NpcDead func(s *State, uuid scenemap.UUID, npcID int32)
}

// Mgr 是场景权威 Actor 的句柄。
//
// 句柄本身并发安全（唯一可变的是原子量、mailbox、channel）。
// 句柄**不暴露任何场景状态**：想读状态只能 Call，把读变成命令。
type Mgr struct {
	cfg      Config
	deps     Deps
	gameplay Gameplay
	cfgErr   error

	mb *scenemap.Mailbox[Call]

	// done 在 run 退出时关闭，供外部等待。
	done     chan struct{}
	doneOnce sync.Once
	// startOnce/stopOnce 保证幂等。
	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	// speed 用原子量：SetSpeed 可能从任意 goroutine 调用。
	// 定时器间隔的重建发生在 run goroutine（见 reconfig）。
	speed atomic.Int32
	// reconfig 是"配置变了，重建定时器"的唤醒信号，容量 1 且非阻塞投递：
	// 连续多次 SetSpeed 只需要重建一次，多出来的信号没有意义。
	reconfig chan struct{}

	// closeOnce 保证 OnClose 只发一次（超时关图与命令关图可能同时发生）。
	closeOnce sync.Once

	// hooksMu 保护三个"可在运行期替换"的回调。
	// 它们不是热路径（一般只有启动时设置一次），用读写锁比原子指针清晰。
	hooksMu     sync.RWMutex
	trigger     TriggerFunc
	condTrigger TriggerCondFunc
	refresh     RefreshFunc

	// ---- 以下字段只在 run goroutine 内访问（单写者）----
	st       *State
	loopT    *repeatingTimer
	buffT    *repeatingTimer
	oneSecT  *repeatingTimer
	halfSecT *repeatingTimer
	oneSecC  <-chan time.Time
	halfSecC <-chan time.Time
	// oneSecArmed/halfSecArmed 记录开关状态，避免每次 rearm 都重置相位。
	oneSecArmed bool
	halfArmed   bool
	// loopTicks 已执行的 loop 次数（监控/测试）。
	loopTicks uint64
}

// New 构造 Mgr（**不启动** goroutine，需要再调 Start）。
//
// 配置非法时不会 panic：错误被保存下来，Start 时以"开图失败"处理，
// 触发 OnClose。这样 RoomManager 能统一回收资源。
func New(cfg Config, deps Deps) *Mgr {
	if deps.AOI == nil {
		deps.AOI = NoopAOI{}
	}
	if deps.Logger == nil {
		deps.Logger = NopLogger{}
	}
	if deps.Clock == nil {
		deps.Clock = func() int64 { return time.Now().UnixMilli() }
	}
	gp := deps.Gameplay
	if gp == nil {
		gp = &Unimplemented{}
	}

	m := &Mgr{
		deps:     deps,
		gameplay: gp,
		done:     make(chan struct{}),
		reconfig: make(chan struct{}, 1),
	}
	m.cfg = cfg
	if err := m.cfg.normalize(); err != nil {
		m.cfgErr = err
	}
	m.speed.Store(m.cfg.SpeedPermille)
	m.mb = scenemap.NewMailbox[Call](m.cfg.MailboxSize)
	return m
}

// Key 返回地图标识（只读）。
func (m *Mgr) Key() scenemap.MapKey { return m.cfg.Key }

// Speed 返回当前倍速（千分比）。
func (m *Mgr) Speed() int32 { return m.speed.Load() }

// Done 返回一个在 MGR 退出时关闭的 channel。
func (m *Mgr) Done() <-chan struct{} { return m.done }

// SetSpeed 修改倍速（千分比，1000=1x）。
//
// 只做两件事：写原子量 + 唤醒 run 重建定时器。**不在这里 Reset 定时器**——
// time.Timer 不是并发安全的，只能由 run goroutine 操作。
func (m *Mgr) SetSpeed(permille int32) error {
	if permille <= 0 {
		return fmt.Errorf("%w: 倍速必须为正, got %d", scenemap.ErrInvalidArgument, permille)
	}
	m.speed.Store(permille)
	select {
	case m.reconfig <- struct{}{}:
	default:
	}
	return nil
}

// SetTrigger 设置场景触发器（每秒一次，不吃倍速）。传 nil 表示停表。
func (m *Mgr) SetTrigger(fn TriggerFunc) {
	m.hooksMu.Lock()
	m.trigger = fn
	m.hooksMu.Unlock()
	m.wakeReconfig()
}

// SetCondTrigger 设置格子变化触发器（COND_2）。传 nil 表示不触发。
func (m *Mgr) SetCondTrigger(fn TriggerCondFunc) {
	m.hooksMu.Lock()
	m.condTrigger = fn
	m.hooksMu.Unlock()
}

// SetRefresh 设置刷怪器推进函数（遗留接口）。
func (m *Mgr) SetRefresh(fn RefreshFunc) {
	m.hooksMu.Lock()
	m.refresh = fn
	m.hooksMu.Unlock()
}

// wakeReconfig 非阻塞通知 run 重建定时器。
func (m *Mgr) wakeReconfig() {
	select {
	case m.reconfig <- struct{}{}:
	default:
	}
}

// Post 异步投递命令。
//
// 返回 ErrOverloaded 表示"没接受"，语义上**没有副作用**，调用方可重试；
// 返回 ErrClosed 表示图已关，不要重试。这两种区分是 mailbox 有界的价值。
func (m *Mgr) Post(cmd Command) error {
	if cmd == nil {
		return fmt.Errorf("%w: 命令为 nil", scenemap.ErrInvalidArgument)
	}
	return m.mb.TrySend(Call{cmd: cmd})
}

// PostCtx 带上下文阻塞投递（"必须送达"的控制命令用）。
func (m *Mgr) PostCtx(ctx context.Context, cmd Command) error {
	if cmd == nil {
		return fmt.Errorf("%w: 命令为 nil", scenemap.ErrInvalidArgument)
	}
	return m.mb.Send(ctx, Call{cmd: cmd})
}

// Call 同步执行命令并等待回执（内部用 scenemap.Call 做超时与 panic 隔离）。
//
// 超时返回 ErrTimeout 时**结果未知**：命令可能已经执行。调用方不能把它
// 当失败重试（重试必须幂等，例如"设置坐标"而不是"增加经验"）。
func (m *Mgr) Call(ctx context.Context, cmd Command) (any, error) {
	if cmd == nil {
		return nil, fmt.Errorf("%w: 命令为 nil", scenemap.ErrInvalidArgument)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// reply 必须带缓冲：超时后 MGR 仍会写一次，无缓冲会让 run goroutine
	// 永久阻塞在这一条命令上（整张图卡死）。scenemap.Call 会校验这一点。
	reply := make(chan any, 1)
	v, err := scenemap.Call[any](ctx, m.cfg.CallTimeout, reply, func() error {
		return m.mb.Send(ctx, Call{cmd: cmd, reply: reply})
	})
	if err != nil {
		return nil, err
	}
	if r, ok := v.(callReply); ok {
		return r.Value, r.Err
	}
	return v, nil
}

// Start 启动 MGR goroutine。
//
// 用 scenemap.SafeRun 承载：任何一个玩法/命令的 panic 都不会带走进程，
// 而是记录日志 + 发关图通知，由 RoomManager 收拾残局。
//
// recover 的边界就是这里：run 内部不再逐命令 recover——单个命令 panic
// 说明状态可能已经被改坏，继续跑等于把错误数据扩散到整张图。
func (m *Mgr) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	m.startOnce.Do(func() {
		m.started.Store(true)
		child, cancel := context.WithCancel(ctx)
		m.ctx, m.cancel = child, cancel
		scenemap.SafeRun("scenemap/mgr:"+m.cfg.Key.Key(), m.onPanic, func() {
			m.run(child)
		})
	})
}

// Stop 请求退出并等待 goroutine 结束。ctx 控制等待时长。
func (m *Mgr) Stop(ctx context.Context) error {
	m.stopOnce.Do(func() {
		if m.started.Load() {
			if m.cancel != nil {
				m.cancel()
			}
			return
		}
		// 从未启动：直接收尾，避免 Stop 永远等一个不会关闭的 done。
		m.mb.Close()
		m.closeDone()
	})
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// closeDone 幂等关闭 done。
func (m *Mgr) closeDone() { m.doneOnce.Do(func() { close(m.done) }) }

// onPanic 是 SafeRun 的 panic 回调：记录 + 通知关图。
func (m *Mgr) onPanic(err error) {
	m.deps.Logger.Error("scenemap/mgr: 场景 goroutine panic，强制关图",
		"key", m.cfg.Key.Key(), "err", err)
	m.notifyClose(nil, "panic: "+err.Error())
}

// run 是 MGR 的主循环（单写者线程）。
func (m *Mgr) run(ctx context.Context) {
	defer func() {
		m.stopTimers()
		m.mb.Close()
		m.closeDone()
	}()

	if m.cfgErr != nil {
		m.deps.Logger.Error("scenemap/mgr: 配置非法，开图失败", "err", m.cfgErr)
		m.notifyClose(nil, "invalid_config: "+m.cfgErr.Error())
		return
	}

	st := newState(m)
	m.st = st

	// 玩法初始化。MgrInit 不走 applyOne：它没有"命令必须改状态"的语义，
	// 很多玩法初始化就是纯构建（读配置、分配 map），revision 不变很正常。
	if err := m.gameplay.MgrInit(st, m.cfg.InitArgs...); err != nil {
		m.deps.Logger.Error("scenemap/mgr: MgrInit 失败，开图失败",
			"key", m.cfg.Key.Key(), "err", err)
		m.terminate(st, "init_failed")
		return
	}

	// 空房保护：开图后先给一个保底时间，防止"开图 → 还没进人就关"。
	if m.gameplay.CanCheckStopMap(st) {
		st.CloseTime = m.now() + m.cfg.EnterProtect.Milliseconds()
	} else {
		st.CloseTime = 0
	}

	m.startTimers()

	for {
		select {
		case <-ctx.Done():
			m.terminate(st, "shutdown")
			return

		case <-m.reconfig:
			m.rearmTimers()

		case call, ok := <-m.mb.C():
			// 注意：这里必须用 `call := <-ch` 接住消息。写成 `case <-m.mb.C():`
			// 会把消息**取出并丢弃**（Go 的 channel 接收是"取走"语义），
			// 表现为"命令永远没执行、调用方全部超时"，而定时器一切正常。
			if !ok {
				m.terminate(st, "mailbox_closed")
				return
			}
			m.dispatch(call)
			m.drainInbox()

		case <-m.loopT.C():
			m.onLoopTick(st)

		case <-m.buffT.C():
			m.onBuffTick(st)

		case <-m.oneSecC:
			m.onOneSecTick(st)

		case <-m.halfSecC:
			m.onHalfSecTick(st)
		}
	}
}

// drainInbox 在预算内批量执行命令。
func (m *Mgr) drainInbox() bool {
	for i := 0; i < commandBudget; i++ {
		call, ok := m.mb.TryRecv()
		if !ok {
			// 空或已关闭；关闭由 select 的 ok 分支处理，这里直接返回。
			return true
		}
		m.dispatch(call)
	}
	return true
}

// dispatch 执行一条命令并回写结果。
func (m *Mgr) dispatch(c Call) {
	reply, err := m.applyOne(m.st, c.cmd)
	if c.reply == nil {
		if err != nil {
			m.deps.Logger.Warn("scenemap/mgr: 异步命令失败",
				"key", m.cfg.Key.Key(), "cmd", c.cmd.Name(), "err", err)
		}
		return
	}
	// 非阻塞写：调用方可能已经超时离开，绝不能让 run goroutine 卡在写 reply 上。
	select {
	case c.reply <- callReply{Value: reply, Err: err}:
	default:
	}
}

// applyOne 是命令执行的统一入口：执行 + revision 副作用检测。
//
// 规格 11.2 的原问题：参考实现 的 回调函数 如果忘了返回新 state，框架只打
// warning，于是"玩家点了没反应"成为线上玄学。Go 版把"有没有改状态"变成
// 可比较的 revision，并在开发期直接返回 ErrNoEffect。
//
// 例外：命令声明 MayNoop()=true（移动微调、广播、tick）时允许无效果。
func (m *Mgr) applyOne(s *State, cmd Command) (any, error) {
	before := s.Revision
	reply, err := cmd.Apply(s)
	if err != nil {
		return nil, err
	}
	if s.Revision == before && !commandMayNoop(cmd) {
		e := fmt.Errorf("%w: %s", scenemap.ErrNoEffect, cmd.Name())
		if m.cfg.LenientNoEffect {
			// 线上热修模式：只告警，不打断业务。
			m.deps.Logger.Warn("scenemap/mgr: 命令未改变状态",
				"key", m.cfg.Key.Key(), "cmd", cmd.Name())
			return reply, nil
		}
		return nil, e
	}
	return reply, nil
}

// now 返回当前时间（unix 毫秒）。
func (m *Mgr) now() int64 { return m.deps.Clock() }

// broadcast 转发一条广播给 AOI。
func (m *Mgr) broadcast(msg any, scope BroadcastScope) (any, error) {
	if m.deps.AOI == nil {
		return nil, nil
	}
	if err := m.deps.AOI.Broadcast(msg, scope); err != nil {
		return nil, err
	}
	return nil, nil
}

// aoiCtx 返回 AOI 调用用的上下文。Stop 之后上下文取消，AOI 调用自然失败，
// 这是期望行为（图已经关了，不该再往 AOI 塞东西）。
func (m *Mgr) aoiCtx() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

// aoiEnter / aoiSync / aoiLeave 是对 AOI 调用的薄封装。
//
// 统一在这里吞掉并记录 AOI 错误：视野是表现层，失败可以靠下一次 Sync 补齐，
// 不能因为 AOI 忙就把权威状态回滚掉（见 AOIHandle 的注释）。
func (m *Mgr) aoiEnter(e AOIEntity) {
	if err := m.deps.AOI.Enter(m.aoiCtx(), e); err != nil {
		m.deps.Logger.Warn("scenemap/mgr: AOI Enter 失败",
			"key", m.cfg.Key.Key(), "uuid", e.UUID, "err", err)
	}
}

func (m *Mgr) aoiSync(uuid scenemap.UUID, px, py int32, dir scenemap.Dir, state int32) {
	if err := m.deps.AOI.Sync(m.aoiCtx(), uuid, px, py, dir, state); err != nil {
		m.deps.Logger.Warn("scenemap/mgr: AOI Sync 失败",
			"key", m.cfg.Key.Key(), "uuid", uuid, "err", err)
	}
}

func (m *Mgr) aoiLeave(uuid scenemap.UUID) {
	if err := m.deps.AOI.Leave(m.aoiCtx(), uuid); err != nil {
		m.deps.Logger.Warn("scenemap/mgr: AOI Leave 失败",
			"key", m.cfg.Key.Key(), "uuid", uuid, "err", err)
	}
}
