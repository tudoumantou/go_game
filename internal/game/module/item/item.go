// internal/game/module/item/item.go —— 道具模块
//
// 架构职责：道具配置的「归属层」。P6 实际使用道具的入口在 bag 模块
// （协议 3001），本模块只保留模块位：登记道具维度的扩展点
// （配置校验、新道具类型接入），当前无协议无状态。
//
// Ext 槽位：无。道具持有量是玩家级资产，统一由 PlayerState.Items()
// 管理并随玩家快照提交，不单独占槽位。
// 事件订阅：无。
// 配置表：item.xml（gamedata.GetItemCfg）。
// 懒初始化：无私有状态，不存在建行问题；保留空模块是为了让
// 「道具域」有明确落点，新增道具玩法（礼包码、道具合成）时不必挪目录。
//
// 模块形态：嵌入 module.Thin 薄模块基座。不注册任何协议（MsgIDs 为 nil，
// Registry 不会建路由项），只挂登录/下线两个观测钩子。
package item

import (
	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// Module 道具模块。
type Module struct {
	*module.Thin
}

// New 创建道具模块。
func New(logger *log.Logger) *Module {
	m := &Module{}
	m.Thin = module.NewThin("item",
		module.ThinLogger(logger),
		module.ThinLogin(func(ctx *module.Context) error {
			if logger != nil {
				logger.Debug("item login", "player_id", ctx.State.PlayerID())
			}
			return nil
		}),
		// 下线标脏只是提示 flusher 关注，权威提交仍是快照 CAS。
		module.ThinLogout(func(ctx *module.Context) error {
			ctx.State.MarkDirty("item")
			return nil
		}),
	)
	return m
}

// 保证 Module 满足 module.Module（Thin 已预实现全部方法）。
var _ module.Module = (*Module)(nil)
