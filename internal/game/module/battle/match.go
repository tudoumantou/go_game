// match.go —— 匹配与建房：凑人、申请房间、下发开战通知。
//
// 与 handler.go 分开是因为它的调用方有两个：客户端发起的 onMatch，
// 以及中心服下发的 OnMatchResult。两条路径最终都汇到 startMatch。
package battle

import (
	"context"

	"game_study/pkg/codec"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
	"time"
)

// startMatch 建房并通知双方开战。
func (m *Module) startMatch(mode string, players []int64) {
	// 建房链路自带超时（CreateRoomReq.Timeout + RPC 层超时），
	// 不复用请求 ctx——onMatch 返回后请求 ctx 可能已被取消，会误杀建房
	bg := context.Background()
	roomID, tokens, err := m.cross.CreateRoom(bg, CreateRoomReq{
		Mode:      mode,
		PlayerIDs: players,
		Timeout:   defaultBattleTimeout,
	})
	if err != nil {
		m.logger.Error("create cross room failed", "mode", mode, "err", err)
		// 回队，让玩家重新匹配（简化处理）
		// 放回队尾而非原地：避免故障节点反复排在最前面无限失败
		m.mu.Lock()
		for _, p := range players {
			m.queue[mode] = append(m.queue[mode], p)
			m.queued[p] = true
		}
		m.mu.Unlock()
		return
	}

	// 为每个玩家建会话，并推送 BattleStartNotify（含对手快照）
	snaps := make(map[int64][]byte, len(players))
	for _, p := range players {
		snaps[p] = m.buildSnapshot(p)

		// demo 期固定 1v1：对手就是从 players 里排除自己的那一个
		var peers []int64
		for _, q := range players {
			if q != p {
				peers = append(peers, q)
			}
		}
		var peerID int64
		if len(peers) > 0 {
			peerID = peers[0]
		}

		m.mu.Lock()
		m.sessions[p] = &Session{
			RoomID:  roomID,
			Token:   tokens[p],
			PeerID:  peerID,
			Started: time.Now(),
			Peer:    snaps[peerID],
		}
		m.mu.Unlock()
	}

	// 快照入房（先送快照，房间满员后才 ROOM_READY，保证有序）
	for _, p := range players {
		if err := m.cross.SendSnapshot(roomID, p, tokens[p], snaps[p]); err != nil {
			m.logger.Warn("send snapshot failed", "room_id", roomID, "player_id", p, "err", err)
		}
	}

	// 通知客户端开战
	for _, p := range players {
		m.mu.Lock()
		s := m.sessions[p]
		m.mu.Unlock()
		if s == nil {
			continue
		}
		notify := &gamev1.BattleStartNotify{
			RoomId:      roomID,
			BattleToken: s.Token,
			Opponent:    s.Peer,
			TimeoutSec:  int64(defaultBattleTimeout.Seconds()),
		}
		if raw, err := proto.Marshal(notify); err == nil {
			m.pusher.Push(p, codec.MsgBattleStartNotify, raw)
		}
	}

	m.logger.Info("battle started", "room_id", roomID, "mode", mode, "players", players)
}

// buildSnapshot 构造玩家战斗快照（demo 期只带基础信息，不含完整状态）。
func (m *Module) buildSnapshot(playerID int64) []byte {
	// 快照是给「对手」看的，不含敏感数据
	snap := &gamev1.PlayerInfoResp{RoleId: playerID}
	if raw, err := proto.Marshal(snap); err == nil {
		return raw
	}
	// marshal 失败降级返回 nil：proto 空消息可容忍，战斗链路不因此中断
	return nil
}
