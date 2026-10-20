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
package item

import (
	"context"

	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// Module 道具模块。
type Module struct {
	logger *log.Logger
}

// New 创建道具模块。
func New(logger *log.Logger) *Module {
	return &Module{logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "item" }

// MsgIDs 实现 module.Module（道具使用走 bag 模块，见 17.1 #20）。
//
// nil 路由是刻意的：两个模块都不注册 3001，Registry 才不会有歧义；
// 若哪天拆出独立道具协议，在这里加协议号即可，不影响 bag。
func (m *Module) MsgIDs() []uint32 { return nil }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	// 保留参数置空：签名必须满足 module.Module 接口，
	// 置空是「当前不消费任何入参」的自文档写法。
	_ = ctx
	_ = msgID
	_ = body
	return nil
}

// OnSave 实现 module.Module。
//
// 道具资产不在本模块落库（随玩家快照），无事可做。
func (m *Module) OnSave(ctx context.Context) error {
	_ = ctx
	return nil
}

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error {
	m.logger.Debug("item login", "player_id", ctx.State.PlayerID())
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}
