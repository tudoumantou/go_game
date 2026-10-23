package mgr

import "game_study/internal/scenemap"

// Gameplay 是"玩法插件"接口（规格 12 的 MapGameplay）。
//
// # 为什么是 8 个方法而不是一个事件总线
//
// 玩法需要精确知道"玩家进/出、换格、刷怪、关图"这些时机，用强类型接口
// 让漏实现变成编译错误；事件总线（map[string]func(any)）在多人协作时
// 一定会出现"字符串拼错、静默不触发"。
//
// # 依赖倒置
//
// MGR 只认这个接口，不认具体玩法（主城/副本/战场）。主进程构造 Mgr 时
// 注入实现；测试注入 stubGameplay。这样 MGR 的测试不需要拉起任何玩法。
//
// # 错误语义
//
// 所有方法返回 error，MGR 的处理：
//   - MgrInit 失败 → 不开图（关图并通知 OnClose）；
//   - PlayerEnter 失败 → 回滚进图（这是唯一能回滚的时机，见 enter.go）；
//   - 其余方法出错 → 记录日志，**不回滚**（状态已提交，回滚代价更高）；
//   - 玩法方法内 panic → 由 Mgr 的 SafeRun 兜住，整张图走关图流程。
//
// 因此玩法里不要写"失败可以重试"的逻辑，把错误当"这条业务没做成"记录即可。
type Gameplay interface {
	// MgrInit 开图时调用一次，args 来自 Config.InitArgs（副本参数/难度等）。
	MgrInit(s *State, args ...any) error
	// Loop 每 1 秒（吃倍速）调用一次。
	Loop(s *State) error
	// PlayerEnter 玩家进图成功后调用（此时索引与 Users 已更新）。
	PlayerEnter(uuid scenemap.UUID, s *State) error
	// PlayerLeave 玩家已从索引/Users 移除后调用。
	PlayerLeave(uuid scenemap.UUID, reason scenemap.LeaveReason, s *State) error
	// EntitySync 实体格子变化后调用（同格微调不会触发）。
	EntitySync(uuid scenemap.UUID, oldPos, newPos scenemap.GridPos, s *State) error
	// HandleNpcDead 怪物死亡结算（掉落/任务/计数）。
	HandleNpcDead(npcID int32, uuid scenemap.UUID, x, y int32, s *State) error
	// CanCheckStopMap 是否允许关图计时。
	// 返回 false 表示"这张图现在不能关"（例如战斗进行中、Boss 未被击杀），
	// MGR 会把 CloseTime 归零并保留空房。
	CanCheckStopMap(s *State) bool
	// OnTerminate 关图前调用一次（存档/结算/发奖）。
	OnTerminate(s *State) error
}

// Unimplemented 提供全部方法的空实现。
//
// 用法：玩法结构体嵌入它，只覆盖关心的方法：
//
//	type MyMap struct{ mgr.Unimplemented }
//	func (m *MyMap) Loop(s *mgr.State) error { ... }
//
// 八个空方法全靠手写既啰嗦又容易漏（漏了就是编译错误，但错误信息指向
// 嵌入点，排查费劲）。Go 没有"默认方法"，嵌入空实现是标准解法。
//
// 注意用指针接收者：值类型的 Unimplemented 被嵌入后，只有 *T 才拥有
// 全部方法集（与 Gameplay 的指针语义一致）。
type Unimplemented struct{}

// MgrInit 默认什么都不做。
func (*Unimplemented) MgrInit(*State, ...any) error { return nil }

// Loop 默认什么都不做。
func (*Unimplemented) Loop(*State) error { return nil }

// PlayerEnter 默认什么都不做。
func (*Unimplemented) PlayerEnter(scenemap.UUID, *State) error { return nil }

// PlayerLeave 默认什么都不做。
func (*Unimplemented) PlayerLeave(scenemap.UUID, scenemap.LeaveReason, *State) error { return nil }

// EntitySync 默认什么都不做。
func (*Unimplemented) EntitySync(scenemap.UUID, scenemap.GridPos, scenemap.GridPos, *State) error {
	return nil
}

// HandleNpcDead 默认什么都不做。
func (*Unimplemented) HandleNpcDead(int32, scenemap.UUID, int32, int32, *State) error { return nil }

// CanCheckStopMap 默认允许关图。
//
// 默认 true 而不是 false：忘记实现的玩法如果默认"永不关图"，线上表现是
// 内存被空房间慢慢吃满（很难发现）；默认"可以关"最多是该关的时候早关
// 了一点，会在测试期暴露。宁可早关也不要泄漏。
func (*Unimplemented) CanCheckStopMap(*State) bool { return true }

// OnTerminate 默认什么都不做。
func (*Unimplemented) OnTerminate(*State) error { return nil }
