// internal/game/module/profile/profile.go —— 玩家信息模块（2001/2002）
//
// 架构职责：把玩家层聚合的「基础信息 + 五维战斗属性」打包下发的协议面。
// 自己不持有任何状态：Profile 基础字段来自 PlayerState（随主快照提交），
// 属性最终值来自 GetAttr（基础值 + 各模块 bonus 之和）。
//
// Ext 槽位：无。这是典型的「纯读模块」——只做组装与序列化。
// 事件订阅：无。
// 配置表：attr_formula.xml（P7）；P6 五维 AttrId 1-5 为协议常量。
// 懒初始化：无状态可初始化；Rollbackable 恒为 true 是因为
// 整个请求零副作用（不碰任何内存），缓存写失败可直接丢弃重放。
package profile

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：2001 拉取 / 2002 响应）。
const (
	msgIDPlayerInfoReq  = codec.MsgPlayerInfoReq
	msgIDPlayerInfoResp = codec.MsgPlayerInfoResp
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
	logger *log.Logger
}

// New 创建模块。
func New(logger *log.Logger) *Module {
	return &Module{logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "profile" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDPlayerInfoReq} }

// Rollbackable 玩家信息查询只读，写缓存失败时可直接回滚内存。
//
// 恒 true 的底气：本模块任何路径都不改 State，重放零副作用。
func (*Module) Rollbackable(uint32) bool { return true }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDPlayerInfoReq:
		return m.onPlayerInfo(ctx)
	default:
		return fmt.Errorf("profile: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

// onPlayerInfo 组装玩家信息：Profile 基础字段 + 五维战斗属性。
//
// 属性走 State.GetAttr 而不是直接读 Profile：装备/伙伴的加成由各模块
// 维护在 bonus 上，最终值 = 基础值 + 加成，这里只取最终值下发。
// 这样新增加成来源（坐骑、称号）不需要改本模块——只要那个模块
// 在自己 OnLogin 里 AddBonus。
func (m *Module) onPlayerInfo(ctx *module.Context) error {
	p := ctx.State.Profile()
	resp := &gamev1.PlayerInfoResp{
		Code:   0,
		RoleId: ctx.State.PlayerID(),
		Name:   p.Name,
		Level:  p.Level,
		Exp:    p.Exp,
		ExpMax: ctx.State.ExpMax(),
		Coin:   p.Coin,
		Attrs: []*gamev1.AttrPair{
			{AttrId: attrHP, Value: ctx.State.GetAttr("hp")},
			{AttrId: attrMP, Value: ctx.State.GetAttr("mp")},
			{AttrId: attrATK, Value: ctx.State.GetAttr("atk")},
			{AttrId: attrDEF, Value: ctx.State.GetAttr("def")},
			{AttrId: attrSPD, Value: ctx.State.GetAttr("spd")},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return ctx.Reply(msgIDPlayerInfoResp, body)
}
