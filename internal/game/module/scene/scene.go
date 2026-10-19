// Package scene 玩家侧场景模块：协议 → 场景 Actor，场景回执 → 客户端。
//
// 模块本身不保存位置、不判断视野——这些属于场景 Actor（见 internal/game/scene）。
// 它只做三件事：转发请求、在下线时通知离场、把错误码翻译成协议响应。
package scenemod

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	scn "game_study/internal/game/scene"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	"game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const (
	codeOK       int32 = 0
	codeNoScene  int32 = 4001 // 场景不存在
	codeBusy     int32 = 4003 // 场景繁忙（Actor 信箱满）
	codeNotIn    int32 = 4005 // 不在场景中
	codeNotReady int32 = 4006 // 场景未就绪
)

// Module 场景模块。
type Module struct {
	mgr    *scn.Manager
	logger *log.Logger
}

// New 创建场景模块。
func New(mgr *scn.Manager, logger *log.Logger) *Module {
	return &Module{mgr: mgr, logger: logger}
}

// Name 模块名。
func (*Module) Name() string { return "scene" }

// MsgIDs 本模块处理的协议号。
func (*Module) MsgIDs() []uint32 {
	return []uint32{
		codec.MsgSceneEnterReq,
		codec.MsgSceneMoveReq,
		codec.MsgSceneLeaveReq,
		codec.MsgSceneSyncReq,
	}
}

// Init 初始化（无模块级状态）。
func (*Module) Init(*module.Context) error { return nil }

// OnSave 场景状态由场景 Actor 持有；落库只涉及 Profile.SceneID（玩家层已处理）。
func (*Module) OnSave(context.Context) error { return nil }

// OnLogin 登录不做任何事：进场景由客户端显式请求（SceneEnterReq）。
//
// 不在登录时自动进场景，是因为「上次在哪个场景」属于客户端表现决策；
// 服务端只记住 Profile.SceneID 供查询，不代替客户端选场景。
func (*Module) OnLogin(*module.Context) error { return nil }

// OnLogout 下线时通知场景移除实体。
//
// 这是三条清理路径中的第一条（另两条：Actor 终止、心跳超时）。
// 它可能失败——信箱满或场景 Actor 已停——所以场景侧必须有心跳兜底，
// 不能只依赖这条消息。
func (m *Module) OnLogout(ctx *module.Context) error {
	sceneID := ctx.State.Profile().SceneID
	if sceneID <= 0 {
		return nil
	}
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		return nil // 场景未启动（配置变更）：没有实体需要清理
	}
	if err := ctx.Actor.Send(pid, &scn.LeaveMsg{PlayerID: ctx.State.PlayerID()}); err != nil {
		m.logger.Warn("scene leave notify rejected",
			"player_id", ctx.State.PlayerID(), "scene_id", sceneID, "err", err)
	}
	return nil
}

// OnMessage 协议分发。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case codec.MsgSceneEnterReq:
		return m.onEnter(ctx, body)
	case codec.MsgSceneMoveReq:
		return m.onMove(ctx, body)
	case codec.MsgSceneLeaveReq:
		return m.onLeave(ctx)
	case codec.MsgSceneSyncReq:
		return m.onSync(ctx, body)
	default:
		return fmt.Errorf("scene: unsupported msg %d", msgID)
	}
}

func (m *Module) onEnter(ctx *module.Context, body []byte) error {
	var req gamev1.SceneEnterReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SceneEnterReq: %w", err)
	}

	pid := m.mgr.PID(req.SceneId)
	if pid == nil {
		return m.replyEnter(ctx, codeNoScene, req.SceneId)
	}

	// 切场景：先让旧场景移除实体，再进新场景。
	//
	// 两条消息发往不同 Actor，没有全局顺序——旧场景可能仍在推送。
	// 玩家 Actor 按「当前 SceneID」过滤推送，因此这个窗口不会串场。
	if old := ctx.State.Profile().SceneID; old > 0 && old != req.SceneId {
		if oldPID := m.mgr.PID(old); oldPID != nil {
			_ = ctx.Actor.Send(oldPID, &scn.LeaveMsg{PlayerID: ctx.State.PlayerID()})
		}
	}

	p := ctx.State.Profile()
	msg := &scn.EnterMsg{
		PlayerID: ctx.State.PlayerID(),
		PID:      ctx.Actor.PID(),
		Name:     p.Name,
		Level:    p.Level,
		X:        req.X,
		Y:        req.Y,
	}
	if err := ctx.Actor.Send(pid, msg); err != nil {
		// 信箱满：场景正在被大量请求打，回「繁忙」而不是静默丢弃。
		m.logger.Warn("scene enter rejected", "scene_id", req.SceneId, "err", err)
		return m.replyEnter(ctx, codeBusy, req.SceneId)
	}
	return nil
}

func (m *Module) onMove(ctx *module.Context, body []byte) error {
	var req gamev1.SceneMoveReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SceneMoveReq: %w", err)
	}

	sceneID := ctx.State.Profile().SceneID
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		return m.replyMove(ctx, codeNotIn, 0, 0, req.Seq) // 不在场景中
	}
	if err := ctx.Actor.Send(pid, &scn.MoveMsg{
		PlayerID: ctx.State.PlayerID(),
		X:        req.X,
		Y:        req.Y,
		Seq:      req.Seq,
	}); err != nil {
		return m.replyMove(ctx, codeNotIn, 0, 0, req.Seq)
	}
	return nil
}

func (m *Module) onLeave(ctx *module.Context) error {
	if err := m.leaveScene(ctx); err != nil {
		return err
	}
	return m.reply(ctx, codec.MsgSceneLeaveResp, &gamev1.SceneLeaveResp{Code: codeOK})
}

// leaveScene 通知当前场景移除实体，并清空场景归属。
func (m *Module) leaveScene(ctx *module.Context) error {
	sceneID := ctx.State.Profile().SceneID
	if sceneID <= 0 {
		return nil
	}
	if pid := m.mgr.PID(sceneID); pid != nil {
		_ = ctx.Actor.Send(pid, &scn.LeaveMsg{PlayerID: ctx.State.PlayerID()})
	}
	ctx.State.SetSceneID(0)
	ctx.State.MarkDirty("profile")
	return nil
}

func (m *Module) onSync(ctx *module.Context, body []byte) error {
	var req gamev1.SceneSyncReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SceneSyncReq: %w", err)
	}
	sceneID := ctx.State.Profile().SceneID
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		return m.reply(ctx, codec.MsgSceneSyncResp, &gamev1.SceneSyncResp{Code: codeNotIn, SceneId: sceneID})
	}
	if err := ctx.Actor.Send(pid, &scn.SyncMsg{PlayerID: ctx.State.PlayerID(), PID: ctx.Actor.PID()}); err != nil {
		return m.reply(ctx, codec.MsgSceneSyncResp, &gamev1.SceneSyncResp{Code: codeNotReady, SceneId: sceneID})
	}
	return nil
}

func (m *Module) replyEnter(ctx *module.Context, code, sceneID int32) error {
	return m.reply(ctx, codec.MsgSceneEnterResp, &gamev1.SceneEnterResp{Code: code, SceneId: sceneID})
}

func (m *Module) replyMove(ctx *module.Context, code, x, y, seq int32) error {
	return m.reply(ctx, codec.MsgSceneMoveResp, &gamev1.SceneMoveResp{Code: code, X: x, Y: y, Seq: seq})
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
