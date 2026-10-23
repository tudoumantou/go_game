package aoi

import "game_study/internal/scenemap"

// AOI 对外投递的消息类型。
//
// 这些是"框架内部消息"，不是客户端协议：AOI 只负责把"谁该收到什么"
// 交给 Sink，Sink 的实现（玩家投递器）再翻译成 4000 段协议。
// MsgType 的值只用于 Behavior 过滤（"这类消息哪些实体不该收"），
// 与真正的协议号解耦；这里对齐 4000 段只是为了让日志好认。
const (
	// MsgTypeEntityEnter 进图全量视野包。
	MsgTypeEntityEnter int32 = 4010
	// MsgTypeEntityAppear 单个实体进入视野。
	MsgTypeEntityAppear int32 = 4011
	// MsgTypeEntityMove 实体移动。
	MsgTypeEntityMove int32 = 4012
	// MsgTypeEntityLeave 单个实体离开视野。
	MsgTypeEntityLeave int32 = 4013
)

// MessageTyper 是可标注"消息类型"的消息。业务广播（技能特效、聊天、
// Buff 刷新…）实现它之后，AOI 才能拿 msgType 去问 Behavior：
// "这个实体该不该收这类消息"。
//
// 不实现也能广播，此时 msgType 视为 0（系统消息，怪物也收）。
type MessageTyper interface {
	MsgType() int32
}

// MessageTypeOf 提取消息类型，非 MessageTyper 返回 0。
func MessageTypeOf(msg any) int32 {
	if t, ok := msg.(MessageTyper); ok {
		return t.MsgType()
	}
	return 0
}

// EnterViewMsg 是"进图全量视野包"，投递给刚进图的实体自己。
//
// 为什么要有全量包而不是让客户端等 9 条 Appear：进图/重连时客户端
// 手里是空的，全量包把"自己 + 一屏实体"一次交出去，客户端只做一次
// 场景构建；用 Appear 逐条下发会让客户端在几帧内反复重排。
type EnterViewMsg struct {
	// Self 自己的外观快照（客户端据此摆自己）。
	Self Brief
	// Entities 视野内已有实体（按 UUID 升序，不含 Self）。
	Entities []Brief
	// Revision 快照版本，客户端可用于判断后续包是否连续。
	Revision uint64
}

// MsgType 实现 MessageTyper。
func (EnterViewMsg) MsgType() int32 { return MsgTypeEntityEnter }

// AppearMsg 是"单个实体进入你的视野"。
type AppearMsg struct {
	Entity Brief
}

// MsgType 实现 MessageTyper。
func (AppearMsg) MsgType() int32 { return MsgTypeEntityAppear }

// MoveMsg 是实体位置更新。只发给"新旧九宫格并集"内的实体（不含自己）。
type MoveMsg struct {
	UUID  scenemap.UUID
	X     int32
	Y     int32
	Dir   scenemap.Dir
	State int32
	// Cell 是新的视野格，客户端可据此校正自己的九宫格缓存。
	Cell CellKey
	// MasterUUID 便于客户端把宠物/召唤挂到主人节点上。
	MasterUUID scenemap.UUID
}

// MsgType 实现 MessageTyper。
func (MoveMsg) MsgType() int32 { return MsgTypeEntityMove }

// LeaveMsg 是"实体离开你的视野"。
//
// 只带 UUID + Kind：客户端按 UUID 从场景里摘掉节点即可，
// 不需要再序列化一份外观（而且此时实体可能已经不存在了）。
type LeaveMsg struct {
	UUID scenemap.UUID
	Kind scenemap.EntityKind
}

// MsgType 实现 MessageTyper。
func (LeaveMsg) MsgType() int32 { return MsgTypeEntityLeave }
