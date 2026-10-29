// internal/game/module/profile/profile.go —— 玩家信息模块（2001/2002）
//
// 架构职责：把玩家层聚合的「基础信息 + 五维战斗属性」打包下发的协议面。
// 自己不持有任何状态：Profile 基础字段来自 PlayerState（随主快照提交），
// 属性最终值来自 GetAttr（基础值 + 各模块 bonus 之和）。
//
// Ext 槽位：无。这是典型的「纯读模块」——只做组装与序列化。
// 事件订阅：无。
// 配置表：attr_formula.xml（P7）；P6 五维 AttrId 1-5 为协议常量。
// 懒初始化：无状态可初始化。
//
// 模块形态：module.Define 声明式基座（D = module.NoState）。本模块零副作用
// （不碰任何内存），注册进基座即承诺可回滚——写缓存失败可直接丢弃重放。
package profile

import (
	"game_study/internal/game/module"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
)

// 属性 ID（attr_formula.xml 约定，P6 内置）。
//
// 这是协议层常量而非状态：五个位置固定，客户端按位渲染面板，
// 与 equip 模块 attrPairs 共用同一套编号。
const (
	attrHP  int32 = 1
	attrMP  int32 = 2
	attrATK int32 = 3
	attrDEF int32 = 4
	attrSPD int32 = 5
)

// Module 玩家信息模块。
type Module struct {
	*module.Bound[module.NoState]
}

// New 创建模块。
func New(logger *log.Logger) *Module {
	m := &Module{}
	m.Bound = module.Define[module.NoState]("profile").
		WithLogger(logger).
		// 下线标脏只是提示 flusher 关注，权威提交仍是快照 CAS。
		WithLogout(func(c *module.Ctx[module.NoState]) error {
			c.State.MarkDirty("profile")
			return nil
		})
	module.Route(m.Bound, &gamev1.PlayerInfoReq{}, m.onPlayerInfo)
	return m
}

// onPlayerInfo 组装玩家信息：Profile 基础字段 + 五维战斗属性。
//
// 属性走 State.GetAttr 而不是直接读 Profile：装备/伙伴的加成由各模块
// 维护在 bonus 上，最终值 = 基础值 + 加成，这里只取最终值下发。
// 这样新增加成来源（坐骑、称号）不需要改本模块——只要那个模块
// 在自己 OnLogin 里 AddBonus。
func (m *Module) onPlayerInfo(c *module.Ctx[module.NoState], _ *gamev1.PlayerInfoReq) error {
	p := c.State.Profile()
	resp := &gamev1.PlayerInfoResp{
		Code:   0,
		RoleId: c.State.PlayerID(),
		Name:   p.Name,
		Level:  p.Level,
		Exp:    p.Exp,
		ExpMax: c.State.ExpMax(),
		Coin:   p.Coin,
		Attrs: []*gamev1.AttrPair{
			{AttrId: attrHP, Value: c.State.GetAttr("hp")},
			{AttrId: attrMP, Value: c.State.GetAttr("mp")},
			{AttrId: attrATK, Value: c.State.GetAttr("atk")},
			{AttrId: attrDEF, Value: c.State.GetAttr("def")},
			{AttrId: attrSPD, Value: c.State.GetAttr("spd")},
		},
	}
	return c.Reply(resp)
}

// 保证 Module 满足 module.Module（Thin 已预实现全部方法）。
var _ module.Module = (*Module)(nil)
