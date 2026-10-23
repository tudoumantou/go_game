package mgr

import (
	"fmt"

	"game_study/internal/scenemap"
)

// Command 是一条能在 MGR goroutine 上就地修改状态的操作。
//
// # 为什么是 "Apply(s)" 而不是 "Apply(s) (newState, err)"
//
// 参考实现 版靠函数返回新 state 来"提交"变更；Go 的 map 是指针语义，就地
// 修改更自然，但也带来"改了却没人知道"的风险。所以用 State.Revision 做
// 副作用检测（见 Mgr.applyOne）：Apply 返回后比对 revision，没变就说明
// 这条命令什么都没做——不是错误就是 bug，必须显式声明。
//
// # 契约
//
//   - Apply 只在 MGR goroutine 上被调用，可以放心读写 *State；
//   - Apply 里不要起 goroutine、不要阻塞（会卡住整张图）；
//   - 先校验、后修改：一旦开始改，就不要再返回 error（框架不回滚）；
//   - 任何可观察的状态变更都必须调用 s.Touch()。
type Command interface {
	// Name 用于日志与错误信息（"命令未生效: move"）。
	Name() string
	// Apply 就地修改状态并返回业务回执。
	Apply(s *State) (reply any, err error)
}

// noopCommand 是可选的"允许无效果"声明。
//
// 用可选接口而不是在 Command 里加一个方法：绝大多数命令（进图/离图/关图）
// 必须改状态，只有少数（移动微调、广播、tick）天然可能什么都不改。
// 可选接口让"默认严格"成为零成本的选择。
type noopCommand interface {
	MayNoop() bool
}

// commandMayNoop 查询命令是否声明"允许无效果"。
func commandMayNoop(c Command) bool {
	if n, ok := c.(noopCommand); ok {
		return n.MayNoop()
	}
	return false
}

// callReply 是同步调用 Call 的回复载荷。
//
// 放进 channel 的是 (值, 错误) 两个字段而不是直接传 error：调用方既可能
// 需要回执（如进图落点），也可能只需要知道成功与否。用一个结构体承载，
// 避免"error 混在 any 里"的类型断言歧义。
type callReply struct {
	Value any
	Err   error
}

// Call 结构是一条待执行的命令 + 同步回复通道。
//
// reply 为 nil 表示异步投递（Post），框架不关心结果也不回写；
// 非 nil 时必须带缓冲（scenemap.Call 会校验），否则超时后写方永久阻塞。
type Call struct {
	cmd   Command
	reply chan any
}

// FuncCommand 把普通函数适配成 Command（玩法/测试用）。
//
// 用法：
//
//	m.Call(ctx, mgr.NewFunc("snapshot", func(s *mgr.State) (any, error) {
//	    return len(s.Users), nil
//	}))
//
// 注意：函数里改状态同样必须 s.Touch()，否则会被判为 ErrNoEffect。
type FuncCommand struct {
	// CmdName 命令名（日志用）。
	CmdName string
	// AllowNoop 是否允许不改变状态。
	AllowNoop bool
	// Fn 实际逻辑。
	Fn func(s *State) (any, error)
}

// NewFunc 构造一个函数命令。
func NewFunc(name string, fn func(*State) (any, error)) *FuncCommand {
	return &FuncCommand{CmdName: name, Fn: fn}
}

// Name 实现 Command。
func (f *FuncCommand) Name() string {
	if f == nil || f.CmdName == "" {
		return "func"
	}
	return f.CmdName
}

// Apply 实现 Command。
func (f *FuncCommand) Apply(s *State) (any, error) {
	if f == nil || f.Fn == nil {
		return nil, fmt.Errorf("%w: FuncCommand 未设置 Fn", scenemap.ErrInvalidArgument)
	}
	return f.Fn(s)
}

// MayNoop 实现 noopCommand。
func (f *FuncCommand) MayNoop() bool { return f != nil && f.AllowNoop }

// CommandEnterPlayer 是"实体进图"的统一入口（规格 5.5 按 UUID 类型三分）。
//
// 名字保留 EnterPlayer 是因为 99% 的调用是玩家；宠物/召唤/怪物走 Kind 分支。
// 用一个命令覆盖全部类型，是为了让"进图"只有一条串行路径——分成多条命令
// 后，玩家与宠物进图的顺序就取决于调用方，容易出现"宠物先于主人进图"。
type CommandEnterPlayer struct {
	UUID    scenemap.UUID
	GridPos scenemap.GridPos
	Pixel   scenemap.PixelPos
	Dir     scenemap.Dir
	// Kind 实体类型；0 视为 Player。
	Kind scenemap.EntityKind
	// MasterUUID 宠物/召唤的主人。
	MasterUUID scenemap.UUID
	// CharName 角色名（仅日志）。
	CharName string

	// 以下为怪物参数（Kind=NPE 时使用）。
	TemplateID     int32
	Radius         int32
	HP             int64
	MaxHP          int64
	SummonExpireAt int64
}

// Name 实现 Command。
func (c CommandEnterPlayer) Name() string { return "enter" }

// Apply 实现 Command。
func (c CommandEnterPlayer) Apply(s *State) (any, error) {
	return s.owner.entityEnter(s, c)
}

// CommandLeave 是"实体离图"命令。
type CommandLeave struct {
	UUID   scenemap.UUID
	Reason scenemap.LeaveReason
}

// Name 实现 Command。
func (c CommandLeave) Name() string { return "leave" }

// Apply 实现 Command。
func (c CommandLeave) Apply(s *State) (any, error) {
	return s.owner.entityLeave(s, c.UUID, c.Reason)
}

// CommandMove 是客户端位置同步（规格 6.3 SyncMap）。
type CommandMove struct {
	UUID  scenemap.UUID
	Pixel scenemap.PixelPos
	Dir   scenemap.Dir
	// State 客户端动画状态位，原样存下并广播。
	State int32
}

// Name 实现 Command。
func (c CommandMove) Name() string { return "move" }

// Apply 实现 Command。
func (c CommandMove) Apply(s *State) (any, error) {
	return s.owner.syncMap(s, c)
}

// MayNoop 实现 noopCommand：同格同朝向同像素的重复包确实什么都不改。
func (c CommandMove) MayNoop() bool { return true }

// CommandBcast 是"按范围广播"命令。
//
// 广播不改场景状态，所以 MayNoop=true；但它必须走 MGR 的 mailbox 而不是
// 让调用方直接调 AOI——否则广播顺序与状态变更顺序会错乱（客户端先收到
// "怪死了"再收到"怪打你"，表现就是死亡动画前多挨一下）。
type CommandBcast struct {
	Msg   any
	Scope BroadcastScope
}

// Name 实现 Command。
func (c CommandBcast) Name() string { return "bcast" }

// Apply 实现 Command。
func (c CommandBcast) Apply(s *State) (any, error) {
	return s.owner.broadcast(c.Msg, c.Scope)
}

// MayNoop 实现 noopCommand：广播不改变状态。
func (c CommandBcast) MayNoop() bool { return true }

// CommandClose 是"关图"命令。
//
// 它只负责"停掉玩法 + 发通知"，真正的房间登记表清理由 RoomManager 收到
// OnClose 后做（规格 4.4：MGR 不关自己，避免自引用死锁）。
type CommandClose struct {
	Reason string
}

// Name 实现 Command。
func (c CommandClose) Name() string { return "close" }

// Apply 实现 Command。
func (c CommandClose) Apply(s *State) (any, error) {
	reason := c.Reason
	if reason == "" {
		reason = "command_close"
	}
	s.owner.terminate(s, reason)
	s.closed = true
	s.CloseTime = 0
	s.Touch()
	return nil, nil
}

// CommandTick 手动推进一次 loop 逻辑。
//
// 用途：测试里不想等真实定时器；GM 工具强制结算一次刷怪/关图。
// 它不改变倍速与定时器状态，只是"立刻做一次 loop 该做的事"。
type CommandTick struct{}

// Name 实现 Command。
func (c CommandTick) Name() string { return "tick" }

// Apply 实现 Command。
func (c CommandTick) Apply(s *State) (any, error) {
	s.owner.handleLoop(s, s.owner.now())
	return nil, nil
}

// MayNoop 实现 noopCommand：空图 tick 本来就可能什么都不做。
func (c CommandTick) MayNoop() bool { return true }
