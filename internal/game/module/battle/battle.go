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
	"game_study/pkg/codec"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：5001 开始通知，5010/5011 结算，5020/5021 透传，5002/5003 匹配）。
const (
	msgIDStartNotify  = codec.MsgBattleStartNotify
	msgIDMatchReq     = codec.MsgMatchReq
	msgIDMatchResp    = codec.MsgMatchResp
	msgIDResultReport = codec.MsgBattleResultReport
	msgIDSettleNotify = codec.MsgBattleSettleNotify
	msgIDSyncNotify   = codec.MsgBattleSyncNotify
	msgIDOpReport     = codec.MsgBattleOpReport
)

// 业务码（沿用全项目约定：0 恒为成功，非 0 为可预期失败，客户端按码直接弹提示，
// 不占 error 通道——error 只留给「真故障」如解码失败）。
const (
	codeOK           int32 = 0    // 成功
	codeAlreadyIn    int32 = 6001 // 已在队列/战斗中
	codeTokenInvalid int32 = 6002 // 令牌无效
	codeAlreadyDone  int32 = 6003 // 已上报过
	codeModeClosed   int32 = 6004 // 玩法未开放（跨服不可用）
)

// 战斗时限（超时服务端平局兜底，见 4.8 第 5 条）。
const defaultBattleTimeout = 180 * time.Second

// matchPlayers 一个房间的参战人数（demo 期 1v1）。
const matchPlayers = 2

// CreateRoomReq 建房请求。
//
// RoomID 非 0 时使用中心服分配的房间号（P8 路径，多参战方各自请求同一号，幂等）；
// PresetTokens 携带中心服已签发的准入令牌（令牌权威在中心服）。
type CreateRoomReq struct {
	// RoomID 中心服预分配房间号；0 表示由 CrossServer 自行分配
	RoomID uint64
	// Mode 玩法模式（arena 等），CrossServer 按模式路由房间实现
	Mode string
	// PlayerIDs 全部参战玩家 id
	PlayerIDs []int64
	// PresetTokens 中心服签发的 playerID → 准入令牌
	PresetTokens map[int64]string
	// Timeout 房间时限，超时由 CrossServer 按平局兜底结算
	Timeout time.Duration
	// ExpectPlayers 房间满员人数；0 表示按 len(PlayerIDs) 推断
	ExpectPlayers int32
}

// CrossCaller 跨服调用能力（由游戏服组装层实现）。
//
// 接口注入而非直接依赖 cross 包：一是模块可单测（测试传 fake），
// 二是战斗会话被多个玩家 Actor 并发访问，实现方必须自行保证并发安全。
type CrossCaller interface {
	// Available 是否有可用的跨服节点。
	Available() bool
	// CreateRoom 申请建房，返回 roomID 与各玩家令牌。
	CreateRoom(ctx context.Context, req CreateRoomReq) (uint64, map[int64]string, error)
	// SendSnapshot 送玩家快照入房。
	SendSnapshot(roomID uint64, playerID int64, token string, snapshot []byte) error
	// SendOp 转发投操作。
	// 契约：body 原样透传不解析；失败只记日志不回客户端——操作丢失由客户端重发/超时兜底。
	SendOp(roomID uint64, playerID int64, msgID uint32, body []byte) error
	// SendResult 转发结算。
	// 注意：CrossServer 收齐房间内双方结果才统一结算回推，单方上报不触发结算。
	SendResult(roomID uint64, playerID int64, token string, result int32, rewards []byte) error
}

// CenterMatcher 中心服匹配能力（P8）。为空表示走本地匹配降级路径。
//
// Match 是「上报即返回」的异步语义：结果经 OnMatchResult 回调回来，
// 调用方不阻塞等待凑人——匹配等待时长不可预期，绝不能占住玩家 Actor。
type CenterMatcher interface {
	// Available 中心服 Report 流是否就绪。
	Available() bool
	// Match 上报匹配请求（结果异步经 OnMatchResult 回来）。
	Match(playerID int64, mode string, elo int32, snapshot []byte) error
}

// RankReporter 排行分上报（P14：战斗结算后写全服榜）。
type RankReporter interface {
	// ReportArenaScore 上报竞技场积分（delta 为增减量，中心服负责累加）。
	// bizID 是本次结算的唯一键：中心服按它去重，同一次结算重投不会重复加分。
	ReportArenaScore(playerID int64, score int64, bizID string) error
}

// Pusher 向在线玩家推送（由组装层实现，跨 Actor 安全）。
//
// 返回值语义：false 表示玩家不在线（已掉线），调用方按「丢弃即可」处理——
// 战斗结算不因一方掉线而阻塞另一方。
type Pusher interface {
	Push(playerID int64, msgID uint32, body []byte) bool
}

// Session 单场战斗的会话状态（玩家侧）。
type Session struct {
	// RoomID 所在房间（0 表示无会话）
	RoomID uint64
	// Token 本玩家的准入令牌，结算上报时校验
	Token string
	// PeerID 对手 id（多人玩法取首个；demo 期固定 1v1）
	PeerID int64
	// Started 开战时间，用于统计/超时判断
	Started time.Time
	// Peer 对手快照（proto 序列化后的 PlayerInfoResp）
	Peer []byte
	// Settled 是否已结算；true 之后会话进入「待回收」状态
	Settled bool
}

// Module 战斗模块。
//
// 与玩家内模块（背包/任务等）的本质区别：那些模块的状态挂在玩家快照上，
// 由 Actor 单线程串行访问、天然免锁；而战斗是全模块级共享状态——
// 会被多个玩家的 Actor 并发读写（各自的 OnMessage），
// 还会被 CrossServer/中心服回调线程直接触达（OnBattleSync 等），
// 因此必须用 sync.Mutex 显式保护。
type Module struct {
	*module.Thin

	cross  CrossCaller
	center CenterMatcher
	ranker RankReporter
	pusher Pusher
	logger *log.Logger

	mu sync.Mutex
	// sessions player_id → 当前战斗（玩家战斗中）
	sessions map[int64]*Session
	// queue 匹配队列（mode → 玩家列表）
	queue map[string][]int64
	// queued 玩家是否已在队列（防重复入队）
	queued map[int64]bool
	// timeout 单场战斗时限
	timeout time.Duration
}

// New 创建战斗模块。
//
// 注意 cross 不可为空（onMatch 直接调用其 Available）；ranker 后置注入（SetRanker），
// 因为排行上报是 P14 增量能力，demo 期允许缺失。
func New(cross CrossCaller, center CenterMatcher, pusher Pusher, logger *log.Logger) *Module {
	m := &Module{
		cross:    cross,
		center:   center,
		pusher:   pusher,
		logger:   logger,
		sessions: make(map[int64]*Session),
		queue:    make(map[string][]int64),
		queued:   make(map[int64]bool),
	}
	m.Thin = module.NewThin("battle", module.ThinLogger(logger))
	m.HandleR(msgIDMatchReq, msgIDMatchResp, &gamev1.MatchResp{}, module.Typed(msgIDMatchResp, m.onMatch))
	// 战报是单向上报：客户端不等回包，服务端也没有响应协议可回。
	// 用 Handle 而不是 HandleR（后者要求声明响应）。
	m.Handle(msgIDOpReport, m.onOpReport)
	m.HandleR(msgIDResultReport, msgIDSettleNotify, &gamev1.BattleSettleNotify{}, module.Typed(msgIDSettleNotify, m.onResultReport))
	return m
}

// onMatch 发起匹配：入队 → 凑够人建房 → 双方下发 BattleStartNotify。
func (m *Module) onMatch(ctx *module.Context, req *gamev1.MatchReq, resp *gamev1.MatchResp) error {
	// mode 空串兜底为 arena：避免客户端不传模式时把队列打散到 "" 键下永远凑不到人
	mode := req.Mode
	if mode == "" {
		mode = "arena"
	}
	// playerID 一律取 Actor 内的会话身份，不信协议体携带的 id（防伪造他人匹配）
	playerID := ctx.State.PlayerID()

	*resp = gamev1.MatchResp{Mode: mode}

	// 跨服节点全挂时直接拒绝：不给玩家「进了队列却永远匹配不到」的希望
	if !m.cross.Available() {
		resp.Code = codeModeClosed
		return nil
	}

	m.mu.Lock()
	if m.queued[playerID] {
		m.mu.Unlock()
		resp.Code = codeAlreadyIn
		return nil
	}
	if s, inBattle := m.sessions[playerID]; inBattle {
		if !s.Settled {
			m.mu.Unlock()
			resp.Code = codeAlreadyIn
			return nil
		}
		// 已结算但会话还未回收（结算到清理之间有短暂窗口）：
		// 立即清掉残留，允许重新匹配 —— 否则玩家打完一场要等 2s 才能再排。
		delete(m.sessions, playerID)
	}

	// 中心服可用 → 走中心服匹配（P8 主路径）；否则降级为本服匹配（P13 直连）
	if m.center != nil && m.center.Available() {
		// 先解锁再调中心服：Match 内部走 RPC，可能慢，绝不能持锁等待
		m.mu.Unlock()
		// demo 期无独立 Elo 存储，用等级折算近似分（等级 ×100）
		elo := ctx.State.Profile().Level * 100
		if err := m.center.Match(playerID, mode, elo, m.buildSnapshot(playerID)); err != nil {
			m.logger.Warn("center match failed", "player_id", playerID, "err", err)
			resp.Code = codeModeClosed
			return nil
		}
		resp.Code = codeOK
		resp.Queued = 1 // 中心服匹配池不回传精确人数，客户端按「已入队」处理
		return nil
	}

	m.queue[mode] = append(m.queue[mode], playerID)
	m.queued[playerID] = true
	// queued 在锁内取快照：解锁后的回包计算不能再摸共享 map（数据竞争）
	queued := len(m.queue[mode])

	// 凑够人数 → 出队建房
	var pair []int64
	if queued >= matchPlayers {
		pair = append(pair, m.queue[mode][:matchPlayers]...)
		m.queue[mode] = m.queue[mode][matchPlayers:]
		for _, p := range pair {
			delete(m.queued, p)
		}
		// queue 与 queued 两个结构必须同步变更，否则「防重复入队」防护失效
	}
	m.mu.Unlock()

	resp.Code = codeOK
	// 凑满时两人出队后剩 0；未凑满则把当前排队人数透给客户端做「匹配中 n 人」
	resp.Queued = int32(queued - len(pair))
	// 回包由 module.Typed 统一完成（这里再回一次会多发一条响应，
	// 多出来的那条会被下一次请求当成自己的响应读走）。

	// 建房放在回包之后：先让客户端渲染「匹配中」，再同步建房（本地路径量小，
	// 阻塞当前 Actor 可接受；建房内部还要 RPC CrossServer，可能秒级返回）
	if len(pair) == matchPlayers {
		m.startMatch(mode, pair)
	}
	return nil
}

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
	// marshal 失败降级返回 nil：proto 空消息可容忍，战斗链路不因此中断
	return nil
}

// onOpReport 客户端操作上报 → 原样透传到房间（不解析 body）。
func (m *Module) onOpReport(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.BattleOpReport
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode BattleOpReport: %w", err)
	}
	playerID := ctx.State.PlayerID()

	m.mu.Lock()
	s := m.sessions[playerID]
	m.mu.Unlock()
	// Settled 之后到达的操作同样丢弃：房间即将销毁，透传已无意义
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
func (m *Module) onResultReport(ctx *module.Context, req *gamev1.BattleResultReport, resp *gamev1.BattleSettleNotify) error {
	playerID := ctx.State.PlayerID()

	*resp = gamev1.BattleSettleNotify{RoomId: req.RoomId}

	m.mu.Lock()
	s := m.sessions[playerID]
	m.mu.Unlock()

	// 房间不匹配视同令牌无效：会话里的 room 是建房时绑定的，
	// 客户端改报别的房间，等价于拿着别房的信物来冒领
	if s == nil || s.RoomID != req.RoomId {
		resp.Code = codeTokenInvalid // 房间不匹配视同令牌无效
		return nil
	}
	// 令牌逐个比对：防玩家 A 用自己房间的令牌给 B 的房间冒领结算
	if req.BattleToken != s.Token {
		resp.Code = codeTokenInvalid
		return nil
	}
	// 已结算 → 6003：客户端重发/双端重复上报的幂等回执
	if s.Settled {
		resp.Code = codeAlreadyDone
		return nil
	}

	// 幂等标记（先在内存置位，避免并发重复上报）
	// 先占坑再转发：即使下面转发失败也不接受第二次上报——
	// 简化取舍：宁可丢一次转发（CrossServer 侧还有兜底结算），也不能重复计分
	m.mu.Lock()
	s.Settled = true
	m.mu.Unlock()

	// 转发给 CrossServer（CrossServer 收齐双方结果后统一结算回推）
	if err := m.cross.SendResult(req.RoomId, playerID, s.Token, req.Result, req.Rewards); err != nil {
		// 转发失败只告警、不回错误码：客户端侧结果已显示，
		// 权威奖励由 CrossServer 收齐双方后回推 5011，最终一致
		m.logger.Warn("send result to cross failed", "room_id", req.RoomId, "err", err)
	}

	// 即时回执（权威奖励由结算推送 5011 再下发一次）
	resp.Code = codeOK
	resp.Rewards = req.Rewards
	return nil
}

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
		m.pusher.Push(playerID, msgIDStartNotify, raw)
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
	// 房间号再校验：防旧房间的迟到结算把玩家新会话误标为已结算
	if s != nil && s.RoomID == res.RoomId {
		// 置 Settled：客户端随后上报的 onResultReport 将走 6003 幂等分支
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
		// 返回浅拷贝而非原指针：调用方拿到的是快照，
		// 不会与内部状态的并发修改产生数据竞争
		cp := *s
		return &cp
	}
	return nil
}

// battleTimeout 单场战斗时限（超时由 CrossServer 按平局兜底结算）。
func (m *Module) battleTimeout() time.Duration {
	if m.timeout > 0 {
		return m.timeout
	}
	return defaultBattleTimeout
}

// SetRanker 注入排行上报能力（P14）。
func (m *Module) SetRanker(r RankReporter) { m.ranker = r }

// SetBattleTimeout 覆盖默认战斗时限（组装层按配置注入）。
func (m *Module) SetBattleTimeout(d time.Duration) {
	if d > 0 {
		m.timeout = d
	}
}

// reply 统一序列化并回包。
//
// 约定：可预期失败一律以业务码（msg.Code）表达；走到 error 返回的只有
// proto.Marshal 失败或 ctx.Reply 失败这类真故障，由上层记日志。
func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
