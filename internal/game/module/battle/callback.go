// callback.go —— 来自中心服与跨服战斗服的回调。
//
// 这些函数由 gRPC 流驱动、在各自的 goroutine 里执行（不是玩家 Actor），
// 所以每个都要自己加锁——与 handler.go 的运行上下文完全不同。
package battle

import (
	"context"
	"fmt"
	"game_study/internal/common/errcode"
	"game_study/pkg/codec"
	"google.golang.org/protobuf/proto"
	"time"

	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
)

// OnMatchResult 处理中心服下发的匹配结果（P8 主路径）。
//
// 关键点：同一房间的多个参战游戏服会各自收到自己的 MatchResult 并各自请求建房，
// CrossServer 侧按 room_id 幂等处理（首个真建，其余直接返回成功）。
// 准入令牌一律采用 MatchResult 里的值（中心服签发），忽略建房响应里的 tokens。
func (m *Module) OnMatchResult(res *internalv2.MatchResult) {
	if res == nil || res.RoomId == 0 || res.BattleToken == "" {
		return
	}
	playerID := res.PlayerId
	if playerID == 0 {
		m.logger.Warn("match result without player id", "room_id", res.RoomId)
		return
	}

	bg := context.Background()
	// 建房（幂等）+ 送自己的快照
	// 返回值里的 tokens 故意忽略：令牌权威在中心服，一律采用 MatchResult 的 BattleToken
	if _, _, err := m.cross.CreateRoom(bg, CreateRoomReq{
		RoomID:       res.RoomId,
		Mode:         res.Mode,
		PlayerIDs:    []int64{playerID},
		PresetTokens: map[int64]string{playerID: res.BattleToken},
		Timeout:      m.battleTimeout(),
	}); err != nil {
		m.logger.Error("create room from match result failed", "room_id", res.RoomId, "err", err)
		return
	}

	snap := m.buildSnapshot(playerID)
	if err := m.cross.SendSnapshot(res.RoomId, playerID, res.BattleToken, snap); err != nil {
		m.logger.Warn("send snapshot failed", "room_id", res.RoomId, "err", err)
	}

	m.mu.Lock()
	m.sessions[playerID] = &Session{
		RoomID:  res.RoomId,
		Token:   res.BattleToken,
		PeerID:  res.PeerId,
		Started: time.Now(),
		Peer:    res.PeerSnapshot,
	}
	m.mu.Unlock()

	notify := &gamev1.BattleStartNotify{
		RoomId:      res.RoomId,
		BattleToken: res.BattleToken,
		Opponent:    res.PeerSnapshot,
		TimeoutSec:  int64(m.battleTimeout().Seconds()),
	}
	if raw, err := proto.Marshal(notify); err == nil {
		m.pusher.Push(playerID, codec.MsgBattleStartNotify, raw)
	}
	m.logger.Info("battle started via center", "room_id", res.RoomId,
		"player_id", playerID, "peer", res.PeerId, "cross", res.CrossAddr)
}

// OnRoomReady 实现 cross.Handler：房间就绪（当前无需额外动作，快照已送）。
func (m *Module) OnRoomReady(roomID uint64) {
	m.logger.Debug("cross room ready", "room_id", roomID)
}

// OnBattleSync 实现 cross.Handler：收到对手消息 → 推给本地玩家。
func (m *Module) OnBattleSync(msg *internalv2.BattleMsg) {
	if msg == nil {
		return
	}
	notify := &gamev1.BattleSyncNotify{
		RoomId:   msg.RoomId,
		PlayerId: msg.PlayerId,
		MsgId:    msg.MsgId,
		Body:     msg.Body,
	}
	raw, err := proto.Marshal(notify)
	if err != nil {
		return
	}
	// 推给房间内除发起者外的所有本地玩家
	// 先在锁内收集 targets、再解锁推送：Push 内部要跨 Actor 投递，可能慢，
	// 持锁执行会把所有玩家的 OnMessage 全堵住
	m.mu.Lock()
	targets := make([]int64, 0, 2)
	for pid, s := range m.sessions {
		if s.RoomID == msg.RoomId && pid != msg.PlayerId {
			targets = append(targets, pid)
		}
	}
	m.mu.Unlock()

	for _, pid := range targets {
		m.pusher.Push(pid, codec.MsgBattleSyncNotify, raw)
	}
}

// OnBattleResult 实现 cross.Handler：兜底结算（超时平局等）。
func (m *Module) OnBattleResult(res *internalv2.BattleResult) {
	if res == nil {
		return
	}
	m.mu.Lock()
	s := m.sessions[res.PlayerId]
	// 房间号再校验：防旧房间的迟到结算把玩家新会话误标为已结算
	if s != nil && s.RoomID == res.RoomId {
		// 置 Settled：客户端随后上报的 onResultReport 将走 6003 幂等分支
		s.Settled = true
	}
	m.mu.Unlock()

	notify := &gamev1.BattleSettleNotify{
		RoomId: res.RoomId,
		Code:   int32(errcode.OK),
	}
	if raw, err := proto.Marshal(notify); err == nil {
		m.pusher.Push(res.PlayerId, codec.MsgBattleSettleNotify, raw)
	}

	m.logger.Info("battle settled by cross", "room_id", res.RoomId,
		"player_id", res.PlayerId, "result", res.Result)

	// 全服竞技场榜（P14）：胜 +30，负 -10（不低于 0 由中心服 ZSet 语义天然处理）
	if m.ranker != nil {
		// res.Result 编码：0=胜 1=负 2=平；平局不加分（delta=0 时下面跳过上报，
		// 避免为零变动白发一次中心服请求）
		delta := int64(-10)
		if res.Result == 0 {
			delta = 30
		} else if res.Result == 2 {
			delta = 0
		}
		if delta != 0 {
			// 房间 + 玩家唯一：同一次结算无论重投几次都只加一次分。
			bizID := fmt.Sprintf("battle:%d:%d", res.RoomId, res.PlayerId)
			if err := m.ranker.ReportArenaScore(res.PlayerId, delta, bizID); err != nil {
				// 上报失败只记 Debug：积分是旁路数据，不影响战斗主链路
				m.logger.Debug("report arena score failed", "player_id", res.PlayerId, "err", err)
			}
		}
	}

	// 结算后清理会话（延迟一点，避免和正在处理的上报竞争）
	// 不立即删的原因：onResultReport 可能正持锁读 sessions；延迟 2s 让在途请求走完。
	// 删前再校验 RoomID：若玩家已开下一场（新 RoomID），不能误删新会话。
	go func() {
		time.Sleep(2 * time.Second)
		m.mu.Lock()
		if cur, ok := m.sessions[res.PlayerId]; ok && cur.RoomID == res.RoomId {
			delete(m.sessions, res.PlayerId)
		}
		m.mu.Unlock()
	}()
}
