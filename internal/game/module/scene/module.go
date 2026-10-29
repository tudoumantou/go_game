// Package scenemod 玩家侧场景模块：协议 → 场景 Actor，场景回执 → 客户端。
//
// 模块本身不保存位置、不判断视野——这些属于场景 Actor（见 internal/game/scene）。
// 它只做三件事：转发请求、在下线时通知离场、把错误码翻译成协议响应。
//
// 架构职责：协议翻译层。玩家的场景意图（进/移/离/同步）经本模块转成
// 跨 Actor 消息（scn.EnterMsg 等）投给场景 Actor；场景的广播与回执
// 由场景侧直接推回玩家 Actor，不经本模块。
//
// Ext 槽位：无。唯一的场景相关持久字段是 Profile.SceneID（玩家层
// profile Ext 的字段），本模块只读写它，不另开槽位。
//
// 事件订阅：无。
//
// 配置表关系：无（场景清单与实例管理在 internal/game/scene）。
//
// 懒初始化：不适用（无本地行概念；场景 Actor 由 Manager 按需拉起）。
//
// 关键语义：ctx.Actor.Send 是「投递不等结果」的异步消息——
// 返回 error 只表示投递失败（信箱满/ Actor 已停），不表示场景处理成败；
// 场景的处理结果由场景 Actor 另路推送，本模块的同步回包只表达
// 「已受理」或「投递失败」。
//
// 包名说明：目录叫 scene，包名叫 scenemod——避免与 internal/game/scene
// 同名，导入方不必为了区分而改别名。
package scenemod

import (
	"game_study/internal/game/module"
	scn "game_study/internal/game/scene"
	"game_study/pkg/log"
	"game_study/pkg/pb/game/v1"
)

// Module 场景模块。
//
// 转发型模块：无自有可变状态（mgr 是进程级共享的场景管理器，只读引用），
// OnMessage 在玩家 Actor 内串行执行，不需要锁。
type Module struct {
	// 基座是 Define[NoState]：位置与视野由场景 Actor 独占，本模块不持有
	// 玩家快照内的状态，只做协议翻译与投递。生命周期里只有 OnLogout 有
	// 实际逻辑（下线时通知场景移除实体），其余无操作。
	*module.Bound[module.NoState]

	// mgr 场景 Actor 管理器：按 scene_id 解析场景 Actor 的 PID
	mgr    *scn.Manager
	logger *log.Logger
}

// New 创建场景模块。
func New(mgr *scn.Manager, logger *log.Logger) *Module {
	m := &Module{mgr: mgr, logger: logger}
	m.Bound = module.Define[module.NoState]("scene").
		WithLogger(logger).
		WithLogout(m.onLogout)
	// 进入/移动成功时不回包：请求转给场景 Actor，结果由场景广播异步送达
	// （MsgSceneEntityNotify / MsgSceneEnterResp）；失败路径处理器自己 Reply。
	module.Route(m.Bound, &gamev1.SceneEnterReq{}, m.onEnter)
	module.Route(m.Bound, &gamev1.SceneMoveReq{}, m.onMove)
	m.Bound.HandleRoute(&gamev1.SceneLeaveReq{}, m.onLeave)
	module.Route(m.Bound, &gamev1.SceneSyncReq{}, m.onSync)
	return m
}
