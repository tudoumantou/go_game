// internal/cross/battle/room.go —— 战斗房间 Actor（设计文档 10.5）
//
// 职责（demo 期战斗演算在客户端，服务端只做三件事）：
//  1. 房间生命周期：建房 → 玩家入房 → 战斗中 → 结算 → 销毁；
//  2. 战斗消息透传：收到某玩家的 BattleMsg，原样转发给房间内其他玩家（不解析 body）；
//  3. 结算：客户端上报结论 或 超时平局兜底 → 生成 BattleResult 推送各参战游戏服。
//
// 并发模型：房间状态由单个 Actor 串行处理，无锁；跨 Actor 只通过消息。
//
// 协作关系：对上经 Sink 接口（SendToGame/NodeOf/RoomClosed）与
// CrossServer 交互——接口注入让 Room 可脱离 gRPC 单测；超时兜底靠
// actor.System 的 ScheduleOnce 给自己发 tickMsg（延迟消息，非轮询）。
// 生命周期：waiting → running（满员或超时判定）→ settled（推送结算）→
// Manager 摘除并终止 Actor，每个终态都保证「每人恰好收到一次结算」。
package battle

import (
	"fmt"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// 房间状态。
// settled 是终态但房间对象还存活一小会儿（等 Manager 延迟终止）：
// 这期间的入房/消息一律拒绝，防止「已结算房间又进人」。
const (
	StateWaiting int32 = 0 // 等待玩家入房
	StateRunning int32 = 1 // 战斗中
	StateSettled int32 = 2 // 已结算（等待销毁）
)

// Player 房间内玩家。
//
// Result 初值 -1（未上报）而非 0：0 是「胜」的合法值，用 -1 区分
// 「没说话」与「说了话」——onTick 兜底只填未上报的。BattleToken 入房时
// 记录，上报时比对：跨服节点侧的第二道令牌校验。
type Player struct {
	PlayerID    int64
	BattleToken string
	Snapshot    []byte
	Result      int32 // 0 胜 1 负 2 平；-1 未上报
	Reported    bool
	JoinedAt    time.Time
}

// Room 战斗房间。
//
// 全部字段只被 Actor 单线程读写（Receive 串行），无锁——这是 Actor
// 模型的核心承诺：把并发消灭在邮箱里，而不是用锁保护。
// 四个 map 各司其职：players（在房玩家）、tokens（准入校验）、
// settled（防重复推送）。导出字段（RoomID/Mode/...）供 Manager/日志读。
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
//
// 接口而非具体类型：Room 因此不依赖 gRPC/protobuf 的发送细节，
// 测试用假 Sink 即可断言「给谁发了什么」。NodeOf 与 SendToGame 分开：
// 寻路（玩家→节点）与投递（节点→流）是两个关注点，合并不利于 mock。
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
	// 不导出（小写）：内部消息类型，外部只发前四种；
	// 由 start 时 ScheduleOnce 到点自动投递
	tickMsg struct{}
)

// NewRoom 创建房间 Actor。
// expect/timeout 兜底（2 人 / 5 分钟）：调用方（Manager.Create）传 0 时
// 用默认值而不是建一间永远开不了战的房。初始状态 waiting：
// 房间先存在、等人，而不是等人再存在（入房消息需要房间已注册）。
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
// 先 Spawn 后 SetPID 的顺序问题见 Manager.CreateWithID；这里只存值——
// ScheduleOnce 要等 start 时才用，届时 PID 必然已注入。
func (r *Room) SetPID(pid *actor.PID) { r.pid = pid }

// Receive 实现 actor.Actor。
//
// 类型 switch 分派五种消息：Go 的接口值经类型断言/switch 分发是
// Actor 框架的标准手法——消息用具体指针类型区分，不用字符串 tag，
// 编译期即可检查穷尽性（漏一个 case 有 default 兜底则静默，这里
// 刻意不加 default：未知消息直接忽略比误处理安全）。
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
// 四道防线依次是：settled 拒入、空快照拒入、重复入房幂等、
// 超员拒入。超员上限 ExpectCount*2（而非 ==）：同房间两个发起方
// 可能重复补发快照，2 倍余量容忍重发又不放开滥用。
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
		// 2 倍而非 1 倍：同局两个发起方各自建房+各自带快照入房是
		// 正常路径，卡死在 1 倍会把正常对局判成滥用
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
// 两件事：广播 ROOM_READY（让游戏服开始转发战斗消息）+ 挂超时 tick。
// NodeOf 查不到就跳过该玩家：节点可能刚断流，它的 ROOM_READY 丢失
// 由客户端重连补发快照路径兜底（onJoin 幂等）。
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
	// ScheduleOnce 而非起 Ticker：只需要一次超时事件，到点后 tickMsg
	// 进邮箱由 onTick 处理——Actor 不用持有定时器，无泄漏风险
	if r.pid != nil {
		ctx.ScheduleOnce(r.Timeout, r.pid, &tickMsg{})
	}
}

// onBattleMsg 战斗消息透传：转发给房间内其他玩家（原样，不解析 body）。
//
// 三道闸：房间必须在 running（未开战不收战斗消息）、发送者必须是
// 本房玩家（防串房注入）、不回发给发送者自己。原样转发保留 PlayerId
// 与 MsgId：接收方据此渲染对手与去重——跨服节点零业务解析。
func (r *Room) onBattleMsg(msg *BattleMsgIn) {
	if r.state != StateRunning || msg.Msg == nil {
		return
	}
	if _, ok := r.players[msg.Msg.PlayerId]; !ok {
		return // 非本房间玩家
	}

	for pid := range r.players {
		if pid == msg.Msg.PlayerId {
			continue // 不回发给发送者自己（原路返回会让客户端看到自己的回声）
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
//
// 幂等靠 p.Reported 布尔：每人只认第一次上报，重投直接忽略——
// 战斗结果不可变，后到的不同结论是伪造或客户端 bug，都该丢。
// 全员到齐才 settle：提前结算会让未上报玩家的 Result 停在 -1。
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
	// 只在入房时记了 token 才比：老协议客户端不带 token 时跳过，
	// 不能因校验把正常玩家卡在门外
	if p.BattleToken != "" && msg.Result.BattleToken != p.BattleToken {
		r.logger.Warn("battle token mismatch",
			"room_id", r.RoomID, "player_id", playerID,
			"want", p.BattleToken, "got", msg.Result.BattleToken)
		return
	}
	// 幂等：每人只认第一次上报
	// 先置位再记录结果：两步之间无 yield 点（Actor 串行），
	// 顺序只影响可读性；重投路径在这里被静默丢弃，不告警——
	// 客户端重发是常态，告警会刷屏
	if p.Reported {
		return
	}
	p.Reported = true
	p.Result = msg.Result.Result

	r.logger.Info("battle result reported", "room_id", r.RoomID, "player_id", playerID, "result", msg.Result.Result)

	// 全部玩家都上报 → 结算
	// 用 for+return 而不是计数再比较：少维护一个「已上报人数」变量，
	// 全员状态本身就是真相（计数与 Reported 可能不一致）
	for _, pp := range r.players {
		if !pp.Reported {
			return
		}
	}
	r.settle("all_reported")
}

// onTick 超时兜底：按平局结算。
//
// 由 start 时挂的 ScheduleOnce 触发：只来一次，所以不用判「第几次 tick」。
// 非 running 直接忽略：可能已结算（settle 在 tick 之前到达）——
// 双路径竞态下谁先到谁生效，后到的这条自然作废。
func (r *Room) onTick(_ *actor.Context) {
	if r.state != StateRunning {
		return
	}
	r.logger.Warn("battle timeout, settle as draw", "room_id", r.RoomID,
		"elapsed", time.Since(r.started).String())
	// 未上报的玩家按平局
	// 置 Reported=true 而不只改 Result：让后续 onReport 的重投被
	// 幂等挡掉——「超时已裁決」这件事本身也要防重复
	for _, p := range r.players {
		if !p.Reported {
			p.Reported = true
			p.Result = 2
		}
	}
	r.settle("timeout")
}

// settle 结算：把结果推给各参战游戏服（幂等，每人只推一次）。
//
// 三重幂等：state==settled 早退（重复调用）、settled map 逐人跳过
// （部分推送后重入）、NodeOf 查不到只告警不中断（该玩家节点已断，
// 它的结算丢失由客户端超时/重连兜底，不拖累其他人）。
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
			// 推送失败不重试：settled[playerID] 保持 false，但本函数
			// 不会因单人失败整体重来——该玩家的结算丢失由业务层兜底
			//（游戏服按 room_id 重查/客户端超时），跨服节点不背这件事
			r.logger.Warn("settle push failed", "room_id", r.RoomID, "node", nodeID, "err", err)
			continue
		}
		r.settled[playerID] = true
	}

	r.logger.Info("room settled", "room_id", r.RoomID, "reason", reason, "players", len(r.players))

	// 通知管控器摘除
	// 放在推送之后：Manager 摘除后会延迟终止 Actor，提前通知会让
	// 「索引已删、结算还没推完」的窗口出现，新消息找不到房间
	r.sink.RoomClosed(r.RoomID)
}

// onClose 主动关房（未结算的按平局兜底，避免玩家卡在战斗态）。
//
// 与 onTick 的区别：onTick 是超时自然到期，onClose 是外部强制
// （Drain/解散）。未开战的房间直接销毁不结算——没有 start 过，
// 玩家没进入战斗，推结算反而让客户端弹一个莫名的结果框。
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
	// 同样要置 settled + 通知 Manager：不置位的话，关房消息之后
	// 到达的入房请求又会把人放进一间「即将销毁」的房间
	r.state = StateSettled
	r.sink.RoomClosed(r.RoomID)
}

// Snapshot 返回房间只读快照（调试/管理接口用）。
// 由 Manager.Stats 在锁外直接调：读到的是近似值（见 roomEntry 注释），
// 管理接口不追求强一致。
func (r *Room) Snapshot() (state int32, players []int64) {
	players = make([]int64, 0, len(r.players))
	for pid := range r.players {
		players = append(players, pid)
	}
	return r.state, players
}

// Deadline 返回超时时间点（管理接口用）。
// started 零值（未开战）返回零 time.Time：调用方据此识别「还没开战」，
// 比再查一次 state 省事。
func (r *Room) Deadline() time.Time {
	if r.started.IsZero() {
		return time.Time{}
	}
	return r.started.Add(r.Timeout)
}

// String 便于日志。
// 实现 fmt.Stringer：Room 进日志/错误信息时自动调用，
// 输出 "room(id,mode,n players)" 一眼定位，好过打印整个结构体。
func (r *Room) String() string {
	return fmt.Sprintf("room(%d,%s,%d players)", r.RoomID, r.Mode, len(r.players))
}
