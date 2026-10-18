// internal/cross/battle/room.go —— 战斗房间 Actor（设计文档 10.5）
//
// 职责（demo 期战斗演算在客户端，服务端只做三件事）：
//  1. 房间生命周期：建房 → 玩家入房 → 战斗中 → 结算 → 销毁；
//  2. 战斗消息透传：收到某玩家的 BattleMsg，原样转发给房间内其他玩家（不解析 body）；
//  3. 结算：客户端上报结论 或 超时平局兜底 → 生成 BattleResult 推送各参战游戏服。
//
// 并发模型：房间状态由单个 Actor 串行处理，无锁；跨 Actor 只通过消息。
package battle

import (
	"fmt"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// 房间状态。
const (
	StateWaiting int32 = 0 // 等待玩家入房
	StateRunning int32 = 1 // 战斗中
	StateSettled int32 = 2 // 已结算（等待销毁）
)

// Player 房间内玩家。
type Player struct {
	PlayerID    int64
	BattleToken string
	Snapshot    []byte
	Result      int32 // 0 胜 1 负 2 平；-1 未上报
	Reported    bool
	JoinedAt    time.Time
}

// Room 战斗房间。
type Room struct {
	RoomID      uint64
	Mode        string
	ExpectCount int
	Timeout     time.Duration // 战斗时限，超时按平局兜底

	state   int32
	players map[int64]*Player
	tokens  map[string]int64 // token -> player_id（准入校验）
	settled map[int64]bool   // 已推送过结算的玩家（防重复）
	started time.Time

	// 下行能力（由 CrossServer 注入）
	sink   Sink
	logger *log.Logger
	pid    *actor.PID
}

// Sink 房间下行能力（把结算/同步帧发回参战游戏服）。
type Sink interface {
	// SendToGame 把帧推给指定游戏服节点。
	SendToGame(nodeID string, frame *internalv2.CrossFrame) error
	// NodeOf 查询玩家所在游戏服节点。
	NodeOf(playerID int64) (nodeID string, ok bool)
	// RoomClosed 房间关闭回调（从管理器摘除）。
	RoomClosed(roomID uint64)
}

// 消息类型。
type (
	// JoinMsg 玩家入房（快照）。
	JoinMsg struct {
		NodeID   string
		Snapshot *internalv2.PlayerSnapshot
	}
	// BattleMsgIn 战斗消息上行（来自游戏服）。
	BattleMsgIn struct {
		NodeID string
		Msg    *internalv2.BattleMsg
	}
	// ReportMsg 客户端上报战斗结论（经游戏服转发）。
	ReportMsg struct {
		NodeID string
		Result *internalv2.BattleResult
	}
	// CloseMsg 主动关房（优雅关闭 / 解散）。
	CloseMsg struct {
		Reason string
	}
	// tickMsg 超时检查。
	tickMsg struct{}
)

// NewRoom 创建房间 Actor。
func NewRoom(roomID uint64, mode string, expect int, timeout time.Duration, sink Sink, logger *log.Logger) *Room {
	if expect <= 0 {
		expect = 2
	}
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	return &Room{
		RoomID:      roomID,
		Mode:        mode,
		ExpectCount: expect,
		Timeout:     timeout,
		state:       StateWaiting,
		players:     make(map[int64]*Player),
		tokens:      make(map[string]int64),
		settled:     make(map[int64]bool),
		sink:        sink,
		logger:      logger,
	}
}

// SetPID 注入自身 PID（Spawn 后调用）。
func (r *Room) SetPID(pid *actor.PID) { r.pid = pid }

// Receive 实现 actor.Actor。
func (r *Room) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case *JoinMsg:
		r.onJoin(ctx, msg)
	case *BattleMsgIn:
		r.onBattleMsg(msg)
	case *ReportMsg:
		r.onReport(msg)
	case *CloseMsg:
		r.onClose(msg.Reason)
	case *tickMsg:
		r.onTick(ctx)
	}
}

// onJoin 玩家入房（校验令牌，满员即开战）。
func (r *Room) onJoin(ctx *actor.Context, msg *JoinMsg) {
	if r.state == StateSettled {
		r.logger.Debug("room settled, reject join", "room_id", r.RoomID, "player_id", msg.Snapshot.PlayerId)
		return
	}
	if msg.Snapshot == nil || msg.Snapshot.PlayerId == 0 {
		return
	}
	if _, ok := r.players[msg.Snapshot.PlayerId]; ok {
		return // 重复入房，幂等
	}
	if len(r.players) >= r.ExpectCount*2 {
		// 超出预期人数上限（防止 token 被滥用刷房）
		r.logger.Warn("room full, reject join", "room_id", r.RoomID, "player_id", msg.Snapshot.PlayerId)
		return
	}

	r.players[msg.Snapshot.PlayerId] = &Player{
		PlayerID:    msg.Snapshot.PlayerId,
		BattleToken: msg.Snapshot.BattleToken,
		Snapshot:    msg.Snapshot.Snapshot,
		Result:      -1,
		JoinedAt:    time.Now(),
	}
	r.tokens[msg.Snapshot.BattleToken] = msg.Snapshot.PlayerId

	r.logger.Info("player joined room", "room_id", r.RoomID, "player_id", msg.Snapshot.PlayerId,
		"count", len(r.players))

	// 满员 → 开战
	if len(r.players) >= r.ExpectCount {
		r.start(ctx)
	}
}

// start 房间开战：通知各游戏服房间就绪。
func (r *Room) start(ctx *actor.Context) {
	r.state = StateRunning
	r.started = time.Now()
	r.logger.Info("room started", "room_id", r.RoomID, "mode", r.Mode, "players", len(r.players))

	// 广播 ROOM_READY（游戏服据此可开始转发战斗消息）
	for pid := range r.players {
		nodeID, ok := r.sink.NodeOf(pid)
		if !ok {
			continue
		}
		frame := &internalv2.CrossFrame{
			Data: &internalv2.CrossFrame_Ctrl{
				Ctrl: &internalv2.CrossCtrl{
					Type:   internalv2.CrossCtrl_ROOM_READY,
					RoomId: r.RoomID,
				},
			},
		}
		_ = r.sink.SendToGame(nodeID, frame)
	}

	// 超时兜底：到点按平局结算（4.8 第 5 条）
	if r.pid != nil {
		ctx.ScheduleOnce(r.Timeout, r.pid, &tickMsg{})
	}
}

// onBattleMsg 战斗消息透传：转发给房间内其他玩家（原样，不解析 body）。
func (r *Room) onBattleMsg(msg *BattleMsgIn) {
	if r.state != StateRunning || msg.Msg == nil {
		return
	}
	if _, ok := r.players[msg.Msg.PlayerId]; !ok {
		return // 非本房间玩家
	}

	for pid := range r.players {
		if pid == msg.Msg.PlayerId {
			continue // 不回发给发送者自己
		}
		nodeID, ok := r.sink.NodeOf(pid)
		if !ok {
			continue
		}
		out := &internalv2.BattleMsg{
			RoomId:   r.RoomID,
			PlayerId: msg.Msg.PlayerId, // 保留来源，接收方据此渲染对手
			MsgId:    msg.Msg.MsgId,
			Body:     msg.Msg.Body,
		}
		frame := &internalv2.CrossFrame{Data: &internalv2.CrossFrame_Battle{Battle: out}}
		if err := r.sink.SendToGame(nodeID, frame); err != nil {
			r.logger.Warn("relay battle msg failed", "room_id", r.RoomID, "node", nodeID, "err", err)
		}
	}
}

// onReport 客户端上报结论（令牌校验 + 幂等 + 全员到齐才结算）。
func (r *Room) onReport(msg *ReportMsg) {
	if msg.Result == nil || r.state != StateRunning {
		return
	}
	playerID := msg.Result.PlayerId
	p, ok := r.players[playerID]
	if !ok {
		return
	}
	// 令牌校验（纵深防御：GS 已校验过一次，CrossServer 再校验防伪造结算）
	if p.BattleToken != "" && msg.Result.BattleToken != p.BattleToken {
		r.logger.Warn("battle token mismatch",
			"room_id", r.RoomID, "player_id", playerID,
			"want", p.BattleToken, "got", msg.Result.BattleToken)
		return
	}
	// 幂等：每人只认第一次上报
	if p.Reported {
		return
	}
	p.Reported = true
	p.Result = msg.Result.Result

	r.logger.Info("battle result reported", "room_id", r.RoomID, "player_id", playerID, "result", msg.Result.Result)

	// 全部玩家都上报 → 结算
	for _, pp := range r.players {
		if !pp.Reported {
			return
		}
	}
	r.settle("all_reported")
}

// onTick 超时兜底：按平局结算。
func (r *Room) onTick(_ *actor.Context) {
	if r.state != StateRunning {
		return
	}
	r.logger.Warn("battle timeout, settle as draw", "room_id", r.RoomID,
		"elapsed", time.Since(r.started).String())
	// 未上报的玩家按平局
	for _, p := range r.players {
		if !p.Reported {
			p.Reported = true
			p.Result = 2
		}
	}
	r.settle("timeout")
}

// settle 结算：把结果推给各参战游戏服（幂等，每人只推一次）。
func (r *Room) settle(reason string) {
	if r.state == StateSettled {
		return
	}
	r.state = StateSettled

	for playerID, p := range r.players {
		if r.settled[playerID] {
			continue
		}
		nodeID, ok := r.sink.NodeOf(playerID)
		if !ok {
			r.logger.Warn("settle: node not found, skip", "room_id", r.RoomID, "player_id", playerID)
			continue
		}
		frame := &internalv2.CrossFrame{
			Data: &internalv2.CrossFrame_Result{Result: &internalv2.BattleResult{
				RoomId:     r.RoomID,
				PlayerId:   playerID,
				Result:     p.Result,
				FinishedAt: time.Now().Unix(),
			}},
		}
		if err := r.sink.SendToGame(nodeID, frame); err != nil {
			r.logger.Warn("settle push failed", "room_id", r.RoomID, "node", nodeID, "err", err)
			continue
		}
		r.settled[playerID] = true
	}

	r.logger.Info("room settled", "room_id", r.RoomID, "reason", reason, "players", len(r.players))

	// 通知管控器摘除
	r.sink.RoomClosed(r.RoomID)
}

// onClose 主动关房（未结算的按平局兜底，避免玩家卡在战斗态）。
func (r *Room) onClose(reason string) {
	if r.state == StateSettled {
		return
	}
	for _, p := range r.players {
		if !p.Reported {
			p.Reported = true
			p.Result = 2
		}
	}
	if r.state == StateRunning {
		r.settle("closed:" + reason)
		return
	}
	// 未开战直接销毁
	r.state = StateSettled
	r.sink.RoomClosed(r.RoomID)
}

// Snapshot 返回房间只读快照（调试/管理接口用）。
func (r *Room) Snapshot() (state int32, players []int64) {
	players = make([]int64, 0, len(r.players))
	for pid := range r.players {
		players = append(players, pid)
	}
	return r.state, players
}

// Deadline 返回超时时间点（管理接口用）。
func (r *Room) Deadline() time.Time {
	if r.started.IsZero() {
		return time.Time{}
	}
	return r.started.Add(r.Timeout)
}

// String 便于日志。
func (r *Room) String() string {
	return fmt.Sprintf("room(%d,%s,%d players)", r.RoomID, r.Mode, len(r.players))
}
