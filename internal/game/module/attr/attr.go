// internal/game/module/attr/attr.go —— 属性模块
package attr

import (
	"context"

	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// Module 属性模块。
type Module struct {
	logger *log.Logger
}

// New 创建属性模块。
func New(logger *log.Logger) *Module {
	return &Module{logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "attr" }

// MsgIDs 实现 module.Module（属性查询随 PlayerInfo 下发，暂无独立协议）。
func (m *Module) MsgIDs() []uint32 { return nil }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	// TODO(P6): 属性查询/加点协议
	_ = msgID
	_ = body
	return nil
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error {
	_ = ctx
	return nil
}

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error {
	m.logger.Debug("attr login", "player_id", ctx.State.PlayerID())
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}
