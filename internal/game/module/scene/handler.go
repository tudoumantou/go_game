// handler.go —— 场景协议处理器：进 / 移 / 离 / 同步。
//
// 全部是「翻译 + 投递」：本模块不判断位置、不判断视野，只把协议转成
// 场景 Actor 的消息，把错误码翻译成协议响应。
//
// 四个处理器都是 module.Route / HandleRoute 形态（解包由基座做），
// 回包由处理器自己决定：进入/移动成功时不回包（结果由场景侧异步推送）。
package scenemod

import (
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	scn "game_study/internal/game/scene"
	"game_study/pkg/pb/game/v1"
)

// onLogout 下线时通知场景移除实体（经 WithLogout 挂接）。
//
// 这是三条清理路径中的第一条（另两条：Actor 终止、心跳超时）。
// 它可能失败——信箱满或场景 Actor 已停——所以场景侧必须有心跳兜底，
// 不能只依赖这条消息。
//
// 恒返回 nil 是契约选择：OnLogout 的职责是「尽力通知」，不是「保证清理」；
// 下线流程不能被一条离场通知卡住（返回 error 会中断登出链路）。
func (m *Module) onLogout(ctx *module.Ctx[module.NoState]) error {
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
func (m *Module) onEnter(c *module.Ctx[module.NoState], req *gamev1.SceneEnterReq) error {
	pid := m.mgr.PID(req.SceneId)
	if pid == nil {
		return m.replyEnter(c, int32(errcode.ErrSceneNotExist), req.SceneId)
	}

	// 切场景：先让旧场景移除实体，再进新场景。
	//
	// 两条消息发往不同 Actor，没有全局顺序——旧场景可能仍在推送。
	// 玩家 Actor 按「当前 SceneID」过滤推送，因此这个窗口不会串场。
	// 旧场景的 Leave 用 _ 忽略错误：进新场景是主路径，旧场景通知失败
	// 由场景侧心跳兜底，不该把用户进场景卡失败。
	if old := c.State.Profile().SceneID; old > 0 && old != req.SceneId {
		if oldPID := m.mgr.PID(old); oldPID != nil {
			_ = c.Actor.Send(oldPID, &scn.LeaveMsg{PlayerID: c.State.PlayerID()})
		}
	}

	p := c.State.Profile()
	msg := &scn.EnterMsg{
		PlayerID: c.State.PlayerID(),
		PID:      c.Actor.PID(),
		Name:     p.Name,
		Level:    p.Level,
		X:        req.X,
		Y:        req.Y,
	}
	if err := c.Actor.Send(pid, msg); err != nil {
		// 信箱满：场景正在被大量请求打，回「繁忙」而不是静默丢弃。
		m.logger.Warn("scene enter rejected", "scene_id", req.SceneId, "err", err)
		return m.replyEnter(c, int32(errcode.ErrSceneBusy), req.SceneId)
	}
	// 成功不回包：进场结果由场景 Actor 推送 SceneEnterResp（带完整视野）
	return nil
}

// onMove 移动请求。
//
// 目标场景一律取 Profile.SceneID（服务端权威的当前归属），不信协议
// 携带的场景参数——防玩家把移动包打到别处的场景 Actor。
// 两种失败（PID 解析不到 / 投递被拒）都回 int32(errcode.ErrNotInScene)：对客户端是同一种
// 恢复动作——「你不在场景里，重新进」。
func (m *Module) onMove(c *module.Ctx[module.NoState], req *gamev1.SceneMoveReq) error {
	sceneID := c.State.Profile().SceneID
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		return m.replyMove(c, int32(errcode.ErrNotInScene), 0, 0, req.Seq) // 不在场景中
	}
	// Seq 回传：客户端用序列号对齐「请求-校正」配对，防旧包乱序生效
	if err := c.Actor.Send(pid, &scn.MoveMsg{
		PlayerID: c.State.PlayerID(),
		X:        req.X,
		Y:        req.Y,
		Seq:      req.Seq,
	}); err != nil {
		return m.replyMove(c, int32(errcode.ErrNotInScene), 0, 0, req.Seq)
	}
	// 成功不回包：位置由场景 Actor 广播/校正
	return nil
}

// onLeave 主动离场：本地清籍 + 回包。
//
// leaveScene 先执行再回包：本地归属已清、场景已通知，此时回 int32(errcode.OK)
// 不会出现「客户端以为出了场景、服务端还认为在场景里」的中间态。
func (m *Module) onLeave(c *module.Ctx[module.NoState]) error {
	if err := m.leaveScene(c); err != nil {
		return err
	}
	return c.Reply(&gamev1.SceneLeaveResp{Code: int32(errcode.OK)})
}

// leaveScene 通知当前场景移除实体，并清空场景归属。
//
// 顺序有意为之：先 Send 离场通知、后 SetSceneID(0)——反序的话本地
// 已清籍，本函数就找不到场景 PID，通知永远发不出去。
// _ 忽略 Send 错误：与 OnLogout 同理由（尽力而为，心跳兜底）。
// SetSceneID(0) + MarkDirty("profile")：SceneID 是 Profile 的持久字段，
// 清籍必须落库，否则下次登录/查询会以为玩家还在旧场景。
func (m *Module) leaveScene(c *module.Ctx[module.NoState]) error {
	sceneID := c.State.Profile().SceneID
	if sceneID <= 0 {
		return nil
	}
	if pid := m.mgr.PID(sceneID); pid != nil {
		_ = c.Actor.Send(pid, &scn.LeaveMsg{PlayerID: c.State.PlayerID()})
	}
	c.State.SetSceneID(0)
	c.State.MarkDirty("profile")
	return nil
}

// onSync 请求场景状态同步（重连/纠偏后客户端拉当前视野）。
//
// SyncMsg 带 PID：场景要把视野内实体快照推回该玩家 Actor。
// pid==nil 必须明确回 ErrNotInScene：回零值包（code=0）会与「同步成功」
// 无法区分，客户端会以为视野已刷新。
//
// 投递失败这个分支现在回的是零值包（code=0），与函数注释里「回
// ErrSceneNotReady」的旧描述不一致——这是迁移前就有的行为，本次只统一
// 签名、不改语义，留给单独一轮判断该修还是该改文档。
func (m *Module) onSync(c *module.Ctx[module.NoState], _ *gamev1.SceneSyncReq) error {
	sceneID := c.State.Profile().SceneID
	pid := m.mgr.PID(sceneID)
	if pid == nil {
		return c.FailCode(&gamev1.SceneSyncResp{}, errcode.ErrNotInScene)
	}
	// 投递失败也回成功包——迁移前就是这样（见上面的函数注释），
	// 这里只把「忽略错误」写成显式的 _，不再伪装成两个不同分支。
	_ = c.Actor.Send(pid, &scn.SyncMsg{PlayerID: c.State.PlayerID(), PID: c.Actor.PID()})
	return c.Reply(&gamev1.SceneSyncResp{})
}

// replyEnter/replyMove 薄封装：固定「码 + 场景/坐标回传」的响应形状，
// 调用点只关心码与参数，不必重复构造 proto。
func (m *Module) replyEnter(c *module.Ctx[module.NoState], code, sceneID int32) error {
	return c.Reply(&gamev1.SceneEnterResp{Code: code, SceneId: sceneID})
}

func (m *Module) replyMove(c *module.Ctx[module.NoState], code, x, y, seq int32) error {
	return c.Reply(&gamev1.SceneMoveResp{Code: code, X: x, Y: y, Seq: seq})
}
