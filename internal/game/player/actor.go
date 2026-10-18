// internal/game/player/actor.go —— 玩家 Actor
package player

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/actor"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 下行推送协议号（4.9：2011 等级变化）。
const msgIDLevelInfoNotify uint32 = 2011

// Message 类型常量。
const (
	MsgBind     = "player.bind"
	MsgUnbind   = "player.unbind"
	MsgPacket   = "player.packet"
	MsgSave     = "player.save"
	MsgKick     = "player.kick"
)

// BindMsg 绑定消息。
type BindMsg struct {
	SessionID uint64
	PlayerID  int64
}

// UnbindMsg 解绑消息。
type UnbindMsg struct {
	SessionID uint64
}

// PacketMsg 客户端消息。
type PacketMsg struct {
	MsgID uint32
	Seq   uint32
	Body  []byte
}

// GrantItem 奖励道具。
type GrantItem struct {
	ItemID int32
	Count  int64
}

// PushMsg 向该玩家客户端推送一条消息（跨模块统一出口）。
type PushMsg struct {
	MsgID uint32
	Body  []byte
}

// GrantItemsMsg 发放奖励（充值发货/邮件领取等）。
type GrantItemsMsg struct {
	Source string // 来源标记（recharge/mail/...），用于日志与幂等排查
	Items  []GrantItem
	Done   chan error // 可选：同步等待结果
}

// SendFunc 下行发送（sessionID + 协议号 + 包体），由组装层注入。
type SendFunc func(sessionID uint64, msgID uint32, body []byte) error

// Actor 玩家 Actor。
type Actor struct {
	playerID int64
	state    *State
	registry *module.Registry
	logger   *log.Logger
	repo     *Repository
	onDirty  func(playerID int64)
	sendFunc SendFunc

	sessionID uint64
	online    bool
	loaded    bool
}

// NewActor 创建玩家 Actor。
func NewActor(playerID int64, registry *module.Registry, logger *log.Logger, repo *Repository, onDirty func(int64), sendFunc SendFunc) *Actor {
	return &Actor{
		playerID: playerID,
		state:    NewState(playerID),
		registry: registry,
		logger:   logger.With("player_id", playerID),
		repo:     repo,
		onDirty:  onDirty,
		sendFunc: sendFunc,
	}
}

// State 返回玩家状态。
func (a *Actor) State() *State { return a.state }

// Receive 实现 actor.Actor。
func (a *Actor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case *BindMsg:
		a.onBind(ctx, msg)
	case *UnbindMsg:
		a.onUnbind(ctx, msg)
	case *PacketMsg:
		a.onPacket(ctx, msg)
	case *SaveMsg:
		a.onSave(ctx)
	case *GrantItemsMsg:
		a.onGrantItems(ctx, msg)
	case *PushMsg:
		_ = a.send(msg.MsgID, msg.Body)
	default:
		a.logger.Debug("unknown message", "type", fmt.Sprintf("%T", msg))
	}
}

// SaveMsg 触发保存。
type SaveMsg struct{}

func (a *Actor) onBind(ctx *actor.Context, msg *BindMsg) {
	a.sessionID = msg.SessionID
	a.online = true

	// 首次上线：从 MySQL 预加载
	if !a.loaded && a.repo != nil {
		state, err := a.repo.Load(context.Background(), a.playerID)
		if err != nil {
			a.logger.Error("load player failed", "err", err)
			_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
			return
		}
		a.state = state
		a.loaded = true
	}

	mctx := a.moduleCtx(ctx)
	if err := a.registry.LoginAll(mctx); err != nil {
		a.logger.Error("module login failed", "err", err)
		_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
		return
	}
	a.logger.Info("player online", "session_id", msg.SessionID)
	_ = ctx.Respond(&BindResult{Code: 0})
}

func (a *Actor) onUnbind(ctx *actor.Context, msg *UnbindMsg) {
	if !a.online {
		return
	}
	mctx := a.moduleCtx(ctx)
	if err := a.registry.LogoutAll(mctx); err != nil {
		a.logger.Warn("module logout failed", "err", err)
	}
	a.online = false
	a.sessionID = 0
	// 标记 dirty，由 flusher 统一落库
	if a.onDirty != nil {
		a.onDirty(a.playerID)
	}
	a.logger.Info("player offline", "session_id", msg.SessionID)
}

func (a *Actor) onPacket(ctx *actor.Context, msg *PacketMsg) {
	mctx := a.moduleCtx(ctx)
	if err := a.registry.Dispatch(mctx, msg.MsgID, msg.Body); err != nil {
		a.logger.Warn("packet handle failed", "msg_id", msg.MsgID, "err", err)
	}
}

// onGrantItems 发放奖励（Actor 内单线程执行，无需加锁）。
func (a *Actor) onGrantItems(ctx *actor.Context, msg *GrantItemsMsg) {
	for _, it := range msg.Items {
		if it.Count <= 0 {
			continue
		}
		a.state.AddItem(it.ItemID, it.Count)
	}
	a.state.MarkDirty("bag")
	if a.onDirty != nil {
		a.onDirty(a.playerID)
	}
	a.logger.Info("grant items", "source", msg.Source, "count", len(msg.Items))
	if msg.Done != nil {
		msg.Done <- nil
	}
}

func (a *Actor) onSave(ctx *actor.Context) {
	mctx := a.moduleCtx(ctx)
	for _, m := range a.registry.All() {
		if err := m.OnSave(mctx.Actor.Context()); err != nil {
			a.logger.Warn("module save failed", "module", m.Name(), "err", err)
		}
	}
	if a.repo != nil {
		if err := a.repo.Save(context.Background(), a.state); err != nil {
			a.logger.Error("save player failed", "err", err)
		}
	}
}

func (a *Actor) moduleCtx(actx *actor.Context) *module.Context {
	mctx := &module.Context{
		Actor: actx,
		State: a.state,
		Send:  a.send,
	}
	mctx.AddExp = a.addExp
	mctx.Emit = func(evt module.Event) {
		a.registry.DispatchEvent(mctx, evt)
	}
	return mctx
}

// addExp 增加经验并推送等级变化（任何模块发经验都走这里，保证通知一致）。
func (a *Actor) addExp(n int64) (bool, int32) {
	up, level := a.state.AddExp(n)
	if n > 0 {
		a.state.MarkDirty("profile")
		if a.onDirty != nil {
			a.onDirty(a.playerID)
		}
	}
	if a.sendFunc != nil && a.sessionID != 0 {
		p := a.state.Profile()
		notify := &gamev1.LevelInfoNotify{
			Level:  p.Level,
			Exp:    p.Exp,
			ExpMax: a.state.ExpMax(),
		}
		if raw, err := proto.Marshal(notify); err == nil {
			_ = a.send(msgIDLevelInfoNotify, raw)
		}
	}
	if up {
		a.logger.Info("level up", "player_id", a.playerID, "level", level)
	}
	return up, level
}

// send 包装为模块可用的下行发送（携带当前 sessionID）。
func (a *Actor) send(msgID uint32, body []byte) error {
	if a.sendFunc == nil || a.sessionID == 0 {
		return nil
	}
	return a.sendFunc(a.sessionID, msgID, body)
}

// BindResult 绑定结果。
type BindResult struct {
	Code int32
	Err  string
}
