// internal/game/module/bag/bag.go —— 背包模块
package bag

import (
	"context"

	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// Module 背包模块。
type Module struct {
	logger *log.Logger
}

// New 创建背包模块。
func New(logger *log.Logger) *Module {
	return &Module{logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "bag" }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	_ = ctx
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
	m.logger.Debug("bag login", "player_id", ctx.State.PlayerID())
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}
