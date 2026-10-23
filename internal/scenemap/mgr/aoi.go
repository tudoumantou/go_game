package mgr

import (
	"context"

	"game_study/internal/scenemap"
)

// AOIEntity 是进 AOI 的实体快照。
//
// 值类型 + 只读字段：AOI Actor 会持有它，MGR 之后对 State 的修改**不会**
// 反向影响 AOI 已经收下的副本。"传值而不是传指针"是跨 Actor 传递的基本
// 纪律——传指针等于把 MGR 的内部状态借给另一个 goroutine。
type AOIEntity struct {
	UUID    scenemap.UUID
	Kind    scenemap.EntityKind
	GridPos scenemap.GridPos
	Pixel   scenemap.PixelPos
	Dir     scenemap.Dir
	// State 客户端动画状态位（站立/跑动/施法）。
	State int32
	// MasterUUID 宠物/召唤的主人；0 表示无。
	MasterUUID scenemap.UUID
}

// AOIHandle 是 MGR 对 AOI Actor 的全部依赖。
//
// # 为什么是接口而不是具体类型
//
// 依赖倒置：MGR 绝不 import aoi 包。原因是 aoi 也需要知道"场景里有谁"，
// 两边直接互相 import 会成环；更重要的是，MGR 的单元测试不需要启动
// 一个真正的 AOI Actor（stubAOI 记录调用即可）。
//
// # 调用语义
//
// 全部方法都被 MGR 在自己的 goroutine 里同步调用。实现方必须保证：
//   - 不阻塞超过一个 mailbox 投递的时间（内部用 TrySend，满了就丢弃或
//     合并——AOI 丢一帧视野可以靠下一次同步补上，但卡住 MGR 会拖垮结算）；
//   - 不回调 MGR（会死锁：MGR 正在等它返回）。
//
// # 失败处理
//
// MGR 对 AOI 错误的态度是"记录 + 继续"：视野是表现层，丢了可以靠下一次
// Sync 补齐；而站位/结算是权威数据，不能因为 AOI 忙就回滚。所以这里返回
// error 只是为了让实现方与监控看到问题，MGR 不会因此中断命令。
type AOIHandle interface {
	// Enter 实体进入视野系统。
	Enter(ctx context.Context, e AOIEntity) error
	// Sync 更新位置/朝向/状态。同格微调也会调用（AOI 需要插值）。
	Sync(ctx context.Context, uuid scenemap.UUID, px, py int32, dir scenemap.Dir, state int32) error
	// Leave 实体离开视野系统。
	Leave(ctx context.Context, uuid scenemap.UUID) error
	// Broadcast 按范围广播一条消息。
	Broadcast(msg any, scope BroadcastScope) error
}

// ScopeKind 是广播范围类型。枚举值会进协议，只能追加。
type ScopeKind int32

const (
	// ScopeAll 全图广播（关图、公告）。
	ScopeAll ScopeKind = 0
	// ScopeAround 以某格为中心、半径 r 的圆内广播（技能特效）。
	ScopeAround ScopeKind = 1
	// ScopeUUIDs 指定收件人（私聊式同步、单个玩家的纠偏包）。
	ScopeUUIDs ScopeKind = 2
)

// BroadcastScope 描述一条广播的范围。
//
// 用结构体而不是"多个可选参数"：三种范围互斥，用 Kind 区分可以让 AOI
// 实现一眼看出该走哪条路径；零值即 ScopeAll，符合"最常用的是全图"。
type BroadcastScope struct {
	Kind ScopeKind
	// Center/Radius 仅 ScopeAround 使用（格坐标 + 半径）。
	Center scenemap.GridPos
	Radius int32
	// UUIDs 仅 ScopeUUIDs 使用。
	UUIDs []scenemap.UUID
	// Exclude 需要排除的 UUID（通常是施法者自己）。
	Exclude scenemap.UUID
}

// BroadcastAll 构造全图广播范围。
func BroadcastAll() BroadcastScope { return BroadcastScope{Kind: ScopeAll} }

// BroadcastAround 构造圆形范围广播。
func BroadcastAround(center scenemap.GridPos, radius int32) BroadcastScope {
	return BroadcastScope{Kind: ScopeAround, Center: center, Radius: radius}
}

// BroadcastTo 构造指定收件人广播。
func BroadcastTo(uuids ...scenemap.UUID) BroadcastScope {
	return BroadcastScope{Kind: ScopeUUIDs, UUIDs: uuids}
}

// NoopAOI 丢弃全部 AOI 调用（无视野的单机/测试场景）。
type NoopAOI struct{}

// Enter 实现 AOIHandle。
func (NoopAOI) Enter(context.Context, AOIEntity) error { return nil }

// Sync 实现 AOIHandle。
func (NoopAOI) Sync(context.Context, scenemap.UUID, int32, int32, scenemap.Dir, int32) error {
	return nil
}

// Leave 实现 AOIHandle。
func (NoopAOI) Leave(context.Context, scenemap.UUID) error { return nil }

// Broadcast 实现 AOIHandle。
func (NoopAOI) Broadcast(any, BroadcastScope) error { return nil }
