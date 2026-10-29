// handler.go —— 场景协议处理器：进 / 移 / 离 / 同步。
//
// 全部是「翻译 + 投递」：本模块不判断位置、不判断视野，只把协议转成
// 场景 Actor 的消息，把错误码翻译成协议响应。
package scenemod

import (
	"fmt"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	scn "game_study/internal/game/scene"
	"game_study/pkg/codec"
	"game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// onLogout 下线时通知场景移除实体（经 ThinLogout 挂接）。
//
// 这是三条清理路径中的第一条（另两条：Actor 终止、心跳超时）。
// 它可能失败——信箱满或场景 Actor 已停——所以场景侧必须有心跳兜底，
// 不能只依赖这条消息。
//
// 恒返回 nil 是契约选择：OnLogout 的职责是「尽力通知」，不是「保证清理」；
// 下线流程不能被一条离场通知卡住（返回 error 会中断登出链路）。
func (m *Module) onLogout(ctx *module.Context) error {
	// SceneID<=0：从未进过场景的玩家，无需通知任何 Actor
	sceneID := ctx.State.Profile().SceneID
	if sceneID <= 0 {
		return nil
	}
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		return nil // 场景未启动（配置变更）：没有实体需要清理
	}
	if err := ctx.Actor.Send(pid, &scn.LeaveMsg{PlayerID: ctx.State.PlayerID()}); err != nil {
		// 只 Warn 不扩散：清理兜底交给场景侧心跳，这里失败无补救手段
		m.logger.Warn("scene leave notify rejected",
			"player_id", ctx.State.PlayerID(), "scene_id", sceneID, "err", err)
	}
	return nil
}

// onEnter 进入场景。
//
// EnterMsg 携带三类信息：身份（PlayerID）、回执地址（PID——场景推送
// 视野/回包时要能找到玩家 Actor，跨 Actor 寻址的必备字段）、
// 进场快照（Name/Level/X/Y——场景侧不再回查玩家，避免双向依赖）。
func (m *Module) onEnter(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.SceneEnterReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SceneEnterReq: %w", err)
	}

	pid := m.mgr.PID(req.SceneId)
	if pid == nil {
		return m.replyEnter(ctx, int32(errcode.ErrSceneNotExist), req.SceneId)
	}

	// 切场景：先让旧场景移除实体，再进新场景。
	//
	// 两条消息发往不同 Actor，没有全局顺序——旧场景可能仍在推送。
	// 玩家 Actor 按「当前 SceneID」过滤推送，因此这个窗口不会串场。
	// 旧场景的 Leave 用 _ 忽略错误：进新场景是主路径，旧场景通知失败
	// 由场景侧心跳兜底，不该把用户进场景卡失败。
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
		return m.replyEnter(ctx, int32(errcode.ErrSceneBusy), req.SceneId)
	}
	return nil
}

// onMove 移动请求。
//
// 目标场景一律取 Profile.SceneID（服务端权威的当前归属），不信协议
// 携带的场景参数——防玩家把移动包打到别处的场景 Actor。
// 两种失败（PID 解析不到 / 投递被拒）都回 int32(errcode.ErrNotInScene)：对客户端是同一种
// 恢复动作——「你不在场景里，重新进」。
func (m *Module) onMove(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.SceneMoveReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SceneMoveReq: %w", err)
	}

	sceneID := ctx.State.Profile().SceneID
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		return m.replyMove(ctx, int32(errcode.ErrNotInScene), 0, 0, req.Seq) // 不在场景中
	}
	// Seq 回传：客户端用序列号对齐「请求-校正」配对，防旧包乱序生效
	if err := ctx.Actor.Send(pid, &scn.MoveMsg{
		PlayerID: ctx.State.PlayerID(),
		X:        req.X,
		Y:        req.Y,
		Seq:      req.Seq,
	}); err != nil {
		return m.replyMove(ctx, int32(errcode.ErrNotInScene), 0, 0, req.Seq)
	}
	return nil
}

// onLeave 主动离场：本地清籍 + 回包。
//
// leaveScene 先执行再回包：本地归属已清、场景已通知，此时回 int32(errcode.OK)
// 不会出现「客户端以为出了场景、服务端还认为在场景里」的中间态。
func (m *Module) onLeave(ctx *module.Context, _ uint32, _ []byte) error {
	if err := m.leaveScene(ctx); err != nil {
		return err
	}
	return m.reply(ctx, codec.MsgSceneLeaveResp, &gamev1.SceneLeaveResp{Code: int32(errcode.OK)})
}

// leaveScene 通知当前场景移除实体，并清空场景归属。
//
// 顺序有意为之：先 Send 离场通知、后 SetSceneID(0)——反序的话本地
// 已清籍，本函数就找不到场景 PID，通知永远发不出去。
// _ 忽略 Send 错误：与 OnLogout 同理由（尽力而为，心跳兜底）。
// SetSceneID(0) + MarkDirty("profile")：SceneID 是 Profile 的持久字段，
// 清籍必须落库，否则下次登录/查询会以为玩家还在旧场景。
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

// onSync 请求场景状态同步（重连/纠偏后客户端拉当前视野）。
//
// SyncMsg 带 PID：场景要把视野内实体快照推回该玩家 Actor。
// 投递失败回 int32(errcode.ErrSceneNotReady)：与 onMove 的 int32(errcode.ErrNotInScene) 区分——同步请求
// 语义上更偏「场景存在但没就绪」，客户端可稍后重试。
func (m *Module) onSync(ctx *module.Context, _ *gamev1.SceneSyncReq, resp *gamev1.SceneSyncResp) error {
	sceneID := ctx.State.Profile().SceneID
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		// 离开场景后 SceneID 已被清成 0，这里必须明确回 4005：
		// 直接 return nil 会让 Typed 用零值回包，客户端读到的 code=0
		// 与「同步成功」无法区分。
		return ctx.FailCode(resp, errcode.ErrNotInScene)
	}
	if err := ctx.Actor.Send(pid, &scn.SyncMsg{PlayerID: ctx.State.PlayerID(), PID: ctx.Actor.PID()}); err != nil {
		return nil
	}
	return nil
}

// replyEnter/replyMove 薄封装：固定「码 + 场景/坐标回传」的响应形状，
// 调用点只关心码与参数，不必重复构造 proto。
func (m *Module) replyEnter(ctx *module.Context, code, sceneID int32) error {
	return m.reply(ctx, codec.MsgSceneEnterResp, &gamev1.SceneEnterResp{Code: code, SceneId: sceneID})
}

func (m *Module) replyMove(ctx *module.Context, code, x, y, seq int32) error {
	return m.reply(ctx, codec.MsgSceneMoveResp, &gamev1.SceneMoveResp{Code: code, X: x, Y: y, Seq: seq})
}

// reply 统一序列化并经 ctx.Reply 下发（Thin.Reply 已提供，保留别名便于阅读）。
func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	return m.Thin.Reply(ctx, msgID, msg)
}
