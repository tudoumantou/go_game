// internal/game/module/battle/battle.go —— 战斗模块（5001-5021，设计文档 4.9.3 / 10.3）
//
// Demo 期职责边界（服务端只做三件事）：
//  1. 通知发起：匹配凑人 → 向 CrossServer 申请建房 → 下发 BattleStartNotify；
//  2. 消息透传：BattleOpReport 原样转发到房间（不解析 body）；
//  3. 接收结果：BattleResultReport → 令牌校验 + 幂等 → 回 BattleSettleNotify。
//
// 战斗演算（技能/伤害/胜负）完全在客户端；服务端不做防作弊校验（后置到战斗 2.0）。
package battle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：5001 开始通知，5010/5011 结算，5020/5021 透传，5002/5003 匹配）。
const (
	msgIDStartNotify  uint32 = 5001
	msgIDMatchReq     uint32 = 5002
	msgIDMatchResp    uint32 = 5003
	msgIDResultReport uint32 = 5010
	msgIDSettleNotify uint32 = 5011
	msgIDSyncNotify   uint32 = 5020
	msgIDOpReport     uint32 = 5021
)

// 业务码。
const (
	codeOK           int32 = 0
	codeAlreadyIn    int32 = 6001 // 已在队列/战斗中
	codeTokenInvalid int32 = 6002 // 令牌无效
	codeAlreadyDone  int32 = 6003 // 已上报过
	codeModeClosed   int32 = 6004 // 玩法未开放
)

// 战斗时限（超时服务端平局兜底，见 4.8 第 5 条）。
const defaultBattleTimeout = 180 * time.Second

// matchPlayers 一个房间的参战人数（demo 期 1v1）。
const matchPlayers = 2

// CrossCaller 跨服调用能力（由游戏服组装层实现）。
type CrossCaller interface {
	// Available 是否有可用的跨服节点。
	Available() bool
	// CreateRoom 申请建房，返回 roomID 与各玩家令牌。
	CreateRoom(ctx context.Context, mode string, playerIDs []int64, timeout time.Duration) (uint64, map[int64]string, error)
	// SendSnapshot 送玩家快照入房。
	SendSnapshot(roomID uint64, playerID int64, token string, snapshot []byte) error
	// SendOp 转发投操作。
	SendOp(roomID uint64, playerID int64, msgID uint32, body []byte) error
	// SendResult 转发结算。
	SendResult(roomID uint64, playerID int64, token string, result int32, rewards []byte) error
}

// Pusher 向在线玩家推送（由组装层实现，跨 Actor 安全）。
type Pusher interface {
	Push(playerID int64, msgID uint32, body []byte) bool
}

// Session 单场战斗的会话状态（玩家侧）。
type Session struct {
	RoomID  uint64
	Token   string
	PeerID  int64
	Started time.Time
	Peer    []byte // 对手快照
	Settled bool
}

// Module 战斗模块。
type Module struct {
	cross  CrossCaller
	pusher Pusher
	logger *log.Logger

	mu sync.Mutex
	// sessions player_id → 当前战斗（玩家战斗中）
	sessions map[int64]*Session
	// queue 匹配队列（mode → 玩家列表）
	queue map[string][]int64
	// queued 玩家是否已在队列（防重复入队）
	queued map[int64]bool
}

// New 创建战斗模块。
func New(cross CrossCaller, pusher Pusher, logger *log.Logger) *Module {
	return &Module{
		cross:    cross,
		pusher:   pusher,
		logger:   logger,
		sessions: make(map[int64]*Session),
		queue:    make(map[string][]int64),
		queued:   make(map[int64]bool),
	}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "battle" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 {
	return []uint32{msgIDMatchReq, msgIDResultReport, msgIDOpReport}
}

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDMatchReq:
		return m.onMatch(ctx, body)
	case msgIDOpReport:
		return m.onOpReport(ctx, body)
	case msgIDResultReport:
		return m.onResultReport(ctx, body)
	default:
		return fmt.Errorf("battle: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module：掉线不清战斗——战斗靠超时兜底结算（4.8 第 5 条）。
func (m *Module) OnLogout(ctx *module.Context) error { return nil }

// onMatch 发起匹配：入队 → 凑够人建房 → 双方下发 BattleStartNotify。
func (m *Module) onMatch(ctx *module.Context, body []byte) error {
	var req gamev1.MatchReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode MatchReq: %w", err)
	}
	mode := req.Mode
	if mode == "" {
		mode = "arena"
	}
	playerID := ctx.State.PlayerID()

	resp := &gamev1.MatchResp{Mode: mode}

	if !m.cross.Available() {
		resp.Code = codeModeClosed
		return m.reply(ctx, msgIDMatchResp, resp)
	}

	m.mu.Lock()
	if m.queued[playerID] {
		m.mu.Unlock()
		resp.Code = codeAlreadyIn
		return m.reply(ctx, msgIDMatchResp, resp)
	}
	if _, inBattle := m.sessions[playerID]; inBattle {
		m.mu.Unlock()
		resp.Code = codeAlreadyIn
		return m.reply(ctx, msgIDMatchResp, resp)
	}
	m.queue[mode] = append(m.queue[mode], playerID)
	m.queued[playerID] = true
	queued := len(m.queue[mode])

	// 凑够人数 → 出队建房
	var pair []int64
	if queued >= matchPlayers {
		pair = append(pair, m.queue[mode][:matchPlayers]...)
		m.queue[mode] = m.queue[mode][matchPlayers:]
		for _, p := range pair {
			delete(m.queued, p)
		}
	}
	m.mu.Unlock()

	resp.Code = codeOK
	resp.Queued = int32(queued - len(pair))
	if err := m.reply(ctx, msgIDMatchResp, resp); err != nil {
		return err
	}

	if len(pair) == matchPlayers {
		m.startMatch(mode, pair)
	}
	return nil
}

// startMatch 建房并通知双方开战。
func (m *Module) startMatch(mode string, players []int64) {
	bg := context.Background()
	roomID, tokens, err := m.cross.CreateRoom(bg, mode, players, defaultBattleTimeout)
	if err != nil {
		m.logger.Error("create cross room failed", "mode", mode, "err", err)
		// 回队，让玩家重新匹配（简化处理）
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
			m.pusher.Push(p, msgIDStartNotify, raw)
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
	return nil
}

// onOpReport 客户端操作上报 → 原样透传到房间（不解析 body）。
func (m *Module) onOpReport(ctx *module.Context, body []byte) error {
	var req gamev1.BattleOpReport
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode BattleOpReport: %w", err)
	}
	playerID := ctx.State.PlayerID()

	m.mu.Lock()
	s := m.sessions[playerID]
	m.mu.Unlock()
	if s == nil || s.RoomID != req.RoomId || s.Settled {
		return nil // 非当前战斗的消息，静默丢弃（客户端重发/延迟）
	}

	// 透传：body 原样，不解析
	if err := m.cross.SendOp(req.RoomId, playerID, req.MsgId, req.Body); err != nil {
		m.logger.Debug("relay op failed", "room_id", req.RoomId, "err", err)
	}
	return nil
}

// onResultReport 客户端上报结论：令牌校验 + 幂等 → 转发跨服 → 回执。
func (m *Module) onResultReport(ctx *module.Context, body []byte) error {
	var req gamev1.BattleResultReport
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode BattleResultReport: %w", err)
	}
	playerID := ctx.State.PlayerID()

	resp := &gamev1.BattleSettleNotify{RoomId: req.RoomId}

	m.mu.Lock()
	s := m.sessions[playerID]
	m.mu.Unlock()

	if s == nil || s.RoomID != req.RoomId {
		resp.Code = codeTokenInvalid // 房间不匹配视同令牌无效
		return m.reply(ctx, msgIDSettleNotify, resp)
	}
	if req.BattleToken != s.Token {
		resp.Code = codeTokenInvalid
		return m.reply(ctx, msgIDSettleNotify, resp)
	}
	if s.Settled {
		resp.Code = codeAlreadyDone
		return m.reply(ctx, msgIDSettleNotify, resp)
	}

	// 幂等标记（先在内存置位，避免并发重复上报）
	m.mu.Lock()
	s.Settled = true
	m.mu.Unlock()

	// 转发给 CrossServer（CrossServer 收齐双方结果后统一结算回推）
	if err := m.cross.SendResult(req.RoomId, playerID, s.Token, req.Result, req.Rewards); err != nil {
		m.logger.Warn("send result to cross failed", "room_id", req.RoomId, "err", err)
	}

	// 即时回执（权威奖励由结算推送 5011 再下发一次）
	resp.Code = codeOK
	resp.Rewards = req.Rewards
	return m.reply(ctx, msgIDSettleNotify, resp)
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
	m.mu.Lock()
	targets := make([]int64, 0, 2)
	for pid, s := range m.sessions {
		if s.RoomID == msg.RoomId && pid != msg.PlayerId {
			targets = append(targets, pid)
		}
	}
	m.mu.Unlock()

	for _, pid := range targets {
		m.pusher.Push(pid, msgIDSyncNotify, raw)
	}
}

// OnBattleResult 实现 cross.Handler：兜底结算（超时平局等）。
func (m *Module) OnBattleResult(res *internalv2.BattleResult) {
	if res == nil {
		return
	}
	m.mu.Lock()
	s := m.sessions[res.PlayerId]
	if s != nil && s.RoomID == res.RoomId {
		s.Settled = true
	}
	m.mu.Unlock()

	notify := &gamev1.BattleSettleNotify{
		RoomId: res.RoomId,
		Code:   codeOK,
	}
	if raw, err := proto.Marshal(notify); err == nil {
		m.pusher.Push(res.PlayerId, msgIDSettleNotify, raw)
	}

	m.logger.Info("battle settled by cross", "room_id", res.RoomId,
		"player_id", res.PlayerId, "result", res.Result)

	// 结算后清理会话（延迟一点，避免和正在处理的上报竞争）
	go func() {
		time.Sleep(2 * time.Second)
		m.mu.Lock()
		if cur, ok := m.sessions[res.PlayerId]; ok && cur.RoomID == res.RoomId {
			delete(m.sessions, res.PlayerId)
		}
		m.mu.Unlock()
	}()
}

// InBattle 玩家是否在战斗中（供其他模块判断是否冻结）。
func (m *Module) InBattle(playerID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[playerID]
	return ok && !s.Settled
}

// QueueLen 当前队列人数（测试/指标用）。
func (m *Module) QueueLen(mode string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queue[mode])
}

// SessionOf 返回玩家当前战斗会话（测试用）。
func (m *Module) SessionOf(playerID int64) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[playerID]; ok {
		cp := *s
		return &cp
	}
	return nil
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
