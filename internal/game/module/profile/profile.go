// internal/game/module/profile/profile.go —— 玩家信息模块（2001/2002）
package profile

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：2001 拉取 / 2002 响应）。
const (
	msgIDPlayerInfoReq  uint32 = 2001
	msgIDPlayerInfoResp uint32 = 2002
)

// 属性 ID（attr_formula.xml 约定，P6 内置）。
const (
	attrHP     int32 = 1
	attrMP     int32 = 2
	attrATK    int32 = 3
	attrDEF    int32 = 4
	attrSPD    int32 = 5
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

func (m *Module) onPlayerInfo(ctx *module.Context) error {
	p := ctx.State.Profile()
	resp := &gamev1.PlayerInfoResp{
		Code:   0,
		RoleId: ctx.State.PlayerID(),
		Name:   p.Name,
		Level:  p.Level,
		Exp:    p.Exp,
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
