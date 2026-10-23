// internal/game/scene/actor.go —— 场景 Actor：实体进出、移动校验与 AOI 广播（R9）
//
// 架构职责：一个场景一个 Actor，独占该地图的全部权威状态（实体表 ents +
// 九宫格索引 grid），处理进入/移动/离开/同步/心跳五类入站消息，并把视野
// 差集广播推给相关玩家。
//
// 协作关系：玩家 Actor 经 Enter/Move/Leave/Sync/Heartbeat 消息与本 Actor
// 交互；回执走 player.SceneResult（可能触发玩家快照 CAS），广播走
// player.ScenePush（纯下行，不触发）；manager.go 提供启动装配与查找。
//
// 设计取舍：
//   - 为什么位置权威只在场景：客户端上报的坐标只是「意图」，若两个
//     goroutine（玩家/场景）都能写坐标，超速与穿墙校验就形同虚设；
//     玩家侧连可写坐标都不持有，从结构上消除竞态；
//   - 为什么移动要限频 + 测速双保险：测速防瞬移，限频防「原地高频小步」
//     ——后者测速完全合法，但会把分片信箱与同屏玩家的收件箱打满；
//   - 为什么 sweep 兜底清理必须有：玩家 Actor 终止时的 LeaveMsg 可能因
//     信箱满/对端已停而丢失，只靠「下线通知」会留下永久幽灵实体。
package scene

import (
	"fmt"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	"game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 客户端错误码（SceneEnterResp / SceneMoveResp 的 code 字段）。
const (
	codeOK       int32 = 0
	codeNoScene  int32 = 4001 // 场景不存在
	codeFull     int32 = 4002 // 人数已满
	codeBusy     int32 = 4003 // 场景繁忙（Actor 信箱满）
	codeTooFast  int32 = 4004 // 移动超出速度上限
	codeNotIn    int32 = 4005 // 不在场景中
	codeNotReady int32 = 4006 // 场景未就绪
	codeThrottle int32 = 4007 // 移动过于频繁（限频）
)

// 移动速度上限（单位/秒）与抖动容忍。
//
// 客户端上报目标点，服务端按「距上次移动的时间 × 速度上限」判定。
// 超速时不移动、回权威坐标：钳制会让外挂靠高频小步前进绕过限制。
const (
	maxSpeedPerSec  = 30
	minMoveDistance = 10 // 网络抖动与首次移动的容忍量
)

// 移动限频：窗口内最多 moveQuota 次。
//
// 速度校验只能防瞬移，防不住「原地高频小步」——那会把分片打满，
// 也会把同屏玩家的信箱塞满。限频按实体计，不按连接计。
const (
	moveWindow = time.Second
	moveQuota  = 12
)

// 心跳兜底：默认 90 秒无消息即视为幽灵实体，每 30 秒扫一次。
//
// 为什么必须有：玩家 Actor 终止时的 LeaveScene 可能失败（信箱满 / 对端已停），
// 只靠「下线通知」会留下永久幽灵实体——它占名额、向别人广播、还挡住自己重进。
const (
	defaultIdleTimeout = 90 * time.Second
	sweepInterval      = 30 * time.Second
)

// Config 场景静态配置（来自 scene.xml）。
type Config struct {
	ID         int32
	Name       string
	Width      int32 // 地图宽（坐标上界，开区间）
	Height     int32
	Cell       int32 // AOI 单元格边长
	MaxPlayers int32
	// IdleTimeout 实体空闲多久后被兜底清理（0 用默认 90s）。
	// 可注入是为了让测试不必等 90 秒——生产值不接受配置，避免误配成 0 秒清场。
	IdleTimeout time.Duration
	// Clock 时钟；nil 时用 time.Now。
	//
	// 与 IdleTimeout 配套：测试要制造「空闲超时」时推进虚拟时钟即可，
	// 不必真的睡过阈值——真实时钟下「睡 60ms 跨 40ms 阈值」本就是在赌调度。
	// 注意实现必须并发安全（测试用 atomic 包装），Actor 在自己的 goroutine 里读。
	Clock func() time.Time
}

// entity 场景内实体。只存广播必需的最小信息。
//
// 不存玩家业务状态（等级/背包在玩家 Actor）：场景只需要「谁在哪、推给谁」，
// 多存一份就会多一套一致性负担。
type entity struct {
	id       int64
	pid      *actor.PID // 广播目标；玩家重连后会更新
	name     string
	level    int32
	x, y     int32
	cell     int64     // 当前所在格的键（九宫格索引）
	lastSeen time.Time // 心跳兜底与速度校验的时间基准

	// moveWindowStart/moveCount：限频滑窗状态（每秒 moveQuota 次，见常量注释）
	moveWindowStart time.Time
	moveCount       int
}

// Actor 场景 Actor。所有字段只在分片 goroutine 内读写，无锁。
//
// sys 是广播出口：视野差集推送没有 ctx 可用，只能用 System.Send（见 pushBody）。
type Actor struct {
	cfg    Config
	sys    *actor.System
	logger *log.Logger
	ents   map[int64]*entity
	grid   map[int64]map[int64]struct{} // 格键 → 实体 ID 集合
	// now 时钟。生产用 time.Now；测试注入可控时钟，避免为了制造
	// 「空闲超时」而真的 sleep —— 空闲阈值与睡眠时长的边界本来就靠调度运气。
	now func() time.Time
}

// NewActor 创建场景 Actor。
func NewActor(cfg Config, sys *actor.System, logger *log.Logger) *Actor {
	if cfg.IdleTimeout <= 0 {
		// 0/负值按默认 90s：生产不接受配置（防误配成 0 秒清场），
		// 测试显式传小值（见 Config.IdleTimeout 注释）
		cfg.IdleTimeout = defaultIdleTimeout
	}
	a := &Actor{
		cfg:    cfg,
		sys:    sys,
		logger: logger.With("scene_id", cfg.ID),
		ents:   make(map[int64]*entity, cfg.MaxPlayers),
		grid:   make(map[int64]map[int64]struct{}, 64),
		now:    cfg.Clock,
	}
	if a.now == nil {
		a.now = time.Now
	}
	return a
}

// 入站消息（玩家 Actor → 场景 Actor）。

// EnterMsg 进入场景。
type EnterMsg struct {
	PlayerID int64
	PID      *actor.PID
	Name     string
	Level    int32
	X, Y     int32
}

// MoveMsg 移动到目标点。
type MoveMsg struct {
	PlayerID int64
	X, Y     int32
	Seq      int32
}

// LeaveMsg 离开场景。
type LeaveMsg struct {
	PlayerID int64
}

// SyncMsg 请求全量同步（重连或客户端发现通知断档）。
type SyncMsg struct {
	PlayerID int64
	PID      *actor.PID
}

// HeartbeatMsg 玩家侧心跳（兜底清理的依据）。
type HeartbeatMsg struct {
	PlayerID int64
}

// SweepMsg 触发一次心跳超时扫描（自调度，每 sweepInterval 一次）。
type SweepMsg struct{}

// CountMsg 查询在场人数（测试与运维观测用）。
//
// 人数是场景内部状态，外部只能通过消息读取——直接读 map 会与分片 goroutine 竞争。
type CountMsg struct {
	Reply chan int
}

// Receive 实现 actor.Actor。
//
// 全部消息在同一分片 goroutine 内串行处理，因此下面的 handler 可以直接
// 读写 ents/grid 而不加锁；跨 Actor 的访问一律走消息，没有共享内存。
func (a *Actor) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case *EnterMsg:
		a.onEnter(ctx, msg)
	case *MoveMsg:
		a.onMove(ctx, msg)
	case *LeaveMsg:
		a.onLeave(ctx, msg)
	case *SyncMsg:
		a.onSync(ctx, msg)
	case *HeartbeatMsg:
		a.onHeartbeat(msg)
	case *SweepMsg:
		a.sweep(a.now())
		// 扫完立刻给自己排下一帧：自维持的周期扫描，不依赖外部定时器
		ctx.ScheduleOnce(sweepInterval, ctx.PID(), &SweepMsg{})
	case *CountMsg:
		// 唯一需要「拿结果」的消息：用调用方提供的同步通道回包
		msg.Reply <- len(a.ents)
	default:
		a.logger.Debug("scene unknown message", "type", fmt.Sprintf("%T", ctx.Message()))
	}
}

// onEnter 进入场景：重复进入只刷新绑定，名额满直接拒绝，成功后先回本人
// 视野快照、再向邻居广播出现。
func (a *Actor) onEnter(ctx *actor.Context, msg *EnterMsg) {
	// 重复进入（重连、客户端重发）：更新绑定与在场时间，不重复占名额。
	if e, ok := a.ents[msg.PlayerID]; ok {
		e.pid = msg.PID
		e.name = msg.Name
		e.level = msg.Level
		e.lastSeen = a.now()
		a.replyEnter(ctx, msg.PID, codeOK, e, true)
		return
	}

	if int32(len(a.ents)) >= a.cfg.MaxPlayers {
		// 人数满不踢人、不排队：直接拒绝，玩家留在原场景。
		a.replyEnter(ctx, msg.PID, codeFull, nil, false)
		return
	}

	now := a.now()
	// 进入即钳制：客户端可发任意坐标，非法坐标进网格会破坏
	//「格键恒非负」这个 cellKeyOf/viewSet 依赖的不变量
	e := &entity{
		id:              msg.PlayerID,
		pid:             msg.PID,
		name:            msg.Name,
		level:           msg.Level,
		x:               Clamp(msg.X, a.cfg.Width),
		y:               Clamp(msg.Y, a.cfg.Height),
		lastSeen:        now,
		moveWindowStart: now,
	}
	a.ents[e.id] = e
	a.gridAdd(e)

	// 先给本人视野快照，再向视野内其他人广播出现——
	// 顺序反了会让新玩家先收到别人的出现通知、再收到快照，客户端出现重复实体。
	a.replyEnter(ctx, msg.PID, codeOK, e, false)

	// 视野对称：新实体出现要通知视野内每一个其他人；
	// 自己的快照已在回执里，不需要再给自己推一条。
	for id := range a.viewSet(e.x, e.y) {
		if id == e.id {
			continue
		}
		if other := a.ents[id]; other != nil {
			a.pushBody(other, appearNotice(e))
		}
	}
}

// replyEnter 回进入结果：成功时附带视野内实体列表（玩家据此渲染同屏人）。
//
// existing=true 表示重复进入（重连）：此时不置 SetScene——Profile.SceneID
// 本来就对，重复置位会白走一次快照 CAS。
func (a *Actor) replyEnter(ctx *actor.Context, pid *actor.PID, code int32, self *entity, existing bool) {
	resp := &gamev1.SceneEnterResp{
		Code:    code,
		SceneId: a.cfg.ID,
		Width:   a.cfg.Width,
		Height:  a.cfg.Height,
	}
	if code == codeOK && self != nil {
		resp.X, resp.Y = self.x, self.y
		view := a.viewSet(self.x, self.y)
		resp.Entities = make([]*gamev1.SceneEntity, 0, len(view))
		for id := range view {
			if id == self.id {
				continue
			}
			if other := a.ents[id]; other != nil {
				resp.Entities = append(resp.Entities, snapshot(other))
			}
		}
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		a.logger.Error("marshal enter resp", "err", err)
		return
	}
	// SetScene 只在首次成功进入为 true：玩家 Actor 据此更新 Profile.SceneID
	// 并走一次快照 CAS（R2.1「一条消息 = 一次 CAS」）；重复进入不标脏，
	// 失败进入更不标脏——否则一次拒绝就白写一次快照。
	_ = ctx.Send(pid, &player.SceneResult{
		SceneID:  a.cfg.ID,
		MsgID:    codec.MsgSceneEnterResp,
		Body:     body,
		SetScene: code == codeOK && !existing,
	})
}

// onMove 移动处理流水线：存在性 → 限频 → 原地判定 → 测速 → 移格 → 视野差集。
//
// 顺序是性能与安全的权衡：限频最便宜且防放大，放在最前；测速要算时间差，
// 放在限频后；移格与广播最贵，只有全部校验通过才做。
func (a *Actor) onMove(ctx *actor.Context, msg *MoveMsg) {
	e, ok := a.ents[msg.PlayerID]
	if !ok {
		a.replyMove(ctx, msg.PlayerID, msg.Seq, codeNotIn, 0, 0)
		return
	}

	// 限频先于一切：超频时连速度校验都不做，直接丢弃，
	// 否则恶意客户端能用高频请求占满分片。
	now := a.now()
	if now.Sub(e.moveWindowStart) >= moveWindow {
		e.moveWindowStart = now
		e.moveCount = 0
	}
	e.moveCount++
	if e.moveCount > moveQuota {
		a.replyMove(ctx, msg.PlayerID, msg.Seq, codeThrottle, e.x, e.y)
		return
	}

	x := Clamp(msg.X, a.cfg.Width)
	y := Clamp(msg.Y, a.cfg.Height)
	if x == e.x && y == e.y {
		e.lastSeen = now
		a.replyMove(ctx, msg.PlayerID, msg.Seq, codeOK, x, y)
		return // 原地不动不广播，避免无意义的流量
	}

	// 速度校验：按距上次移动的时间给预算，超出则原地不动。
	dt := now.Sub(e.lastSeen).Seconds()
	allowed := maxSpeedPerSec*dt + minMoveDistance
	if abs32(x-e.x)+abs32(y-e.y) > int32(allowed) {
		a.replyMove(ctx, msg.PlayerID, msg.Seq, codeTooFast, e.x, e.y)
		return
	}

	// 旧视野必须在改坐标/移格之前算：gridMove 之后 e.cell 已变，旧视野就复原不出来
	oldView := a.viewSet(e.x, e.y)
	e.x, e.y = x, y
	e.lastSeen = now
	a.gridMove(e)

	a.replyMove(ctx, msg.PlayerID, msg.Seq, codeOK, x, y)

	newView := a.viewSet(x, y)
	a.applyViewDelta(e, oldView, newView)
}

// replyMove 回移动回执。实体已离场（被 sweep/离开）时静默返回——
// 移动到达时玩家可能已不在场景里，回执没有接收方。
func (a *Actor) replyMove(ctx *actor.Context, playerID int64, seq, code, x, y int32) {
	e, ok := a.ents[playerID]
	if !ok {
		return
	}
	body, err := proto.Marshal(&gamev1.SceneMoveResp{Code: code, X: x, Y: y, Seq: seq})
	if err != nil {
		return
	}
	_ = ctx.Send(e.pid, &player.SceneResult{
		SceneID: a.cfg.ID,
		MsgID:   codec.MsgSceneMoveResp,
		Body:    body,
		// 回执恒带服务端权威坐标（X/Y）：被拒（超速/限频）时客户端按它回滚，
		// 避免客户端预测位置与服务端长期分叉。与进入回执不同，移动回执
		// 不置 SetScene——位置权威在场景，不需要玩家侧标脏落库。
	})
}

// onLeave 主动离场；幂等——重复 Leave 不报错（重连/ sweep 可能已清过）。
func (a *Actor) onLeave(ctx *actor.Context, msg *LeaveMsg) {
	e, ok := a.ents[msg.PlayerID]
	if !ok {
		return // 幂等：重复离开不报错
	}

	a.removeEntity(e)
}

// removeEntity 从实体表与网格移除，并向视野内其他人广播 Leave。
//
// 视野必须在删除前算：删掉之后自己就不在格子里了，算出来会漏掉邻居。
func (a *Actor) removeEntity(e *entity) {
	view := a.viewSet(e.x, e.y)
	a.gridRemove(e)
	delete(a.ents, e.id)

	// R9 §5 不变量 3：离开场景/断线后，仍在场的其他人必收到 Leave，
	// 否则客户端会留下一个永远不消失的幽灵实体。
	for id := range view {
		if id == e.id {
			continue
		}
		if other := a.ents[id]; other != nil {
			a.pushBody(other, leaveNotice(e.id))
		}
	}
}

// sweep 清理空闲超时的实体。
func (a *Actor) sweep(now time.Time) {
	if len(a.ents) == 0 {
		// 空场景免扫：长尾时间里大多数场景无人，省掉一次全表遍历
		return
	}
	var stale []*entity
	for _, e := range a.ents {
		if now.Sub(e.lastSeen) > a.cfg.IdleTimeout {
			stale = append(stale, e)
		}
	}
	// 两阶段：先收集再移除。removeEntity 内部要广播，混在遍历里改 map 可读性差
	for _, e := range stale {
		a.removeEntity(e)
		// Warn 级：幽灵清理是异常路径（正常下线走 LeaveMsg），
		// 清理量大说明「下线通知离场」在静默失败，需要运维介入
		a.logger.Warn("scene entity timeout, removed",
			"player_id", e.id, "idle", now.Sub(e.lastSeen).Round(time.Second).String())
	}
}

// onSync 全量视野兜底：断线/序号断档时客户端与服务端重新对齐。
func (a *Actor) onSync(ctx *actor.Context, msg *SyncMsg) {
	e, ok := a.ents[msg.PlayerID]
	if !ok {
		// 不在场（例如场景 Actor 曾重启）：让客户端重新进入，而不是回空列表。
		a.logger.Debug("sync for absent entity", "player_id", msg.PlayerID)
		body, _ := proto.Marshal(&gamev1.SceneSyncResp{Code: codeNotIn, SceneId: a.cfg.ID})
		if msg.PID == nil {
			return
		}
		_ = ctx.Send(msg.PID, &player.SceneResult{SceneID: a.cfg.ID, MsgID: codec.MsgSceneSyncResp, Body: body})
		return
	}
	e.pid = msg.PID // 重连后 PID 可能变化
	e.lastSeen = a.now()

	// Sync 是全量兜底：广播不重试（见 pushBody），重连/序号断档时
	// 用它把视野一次补齐，客户端可直接复用进入回执的渲染逻辑
	view := a.viewSet(e.x, e.y)
	resp := &gamev1.SceneSyncResp{
		Code:     codeOK,
		SceneId:  a.cfg.ID,
		X:        e.x,
		Y:        e.y,
		Entities: make([]*gamev1.SceneEntity, 0, len(view)),
	}
	for id := range view {
		if id == e.id {
			continue
		}
		if other := a.ents[id]; other != nil {
			resp.Entities = append(resp.Entities, snapshot(other))
		}
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		return
	}
	_ = ctx.Send(e.pid, &player.SceneResult{SceneID: a.cfg.ID, MsgID: codec.MsgSceneSyncResp, Body: body})
}

// onHeartbeat 只刷新 lastSeen，不回包：心跳是纯保活信号（玩家 Actor 每 30s 一次），
// 回包只会让两个方向的车轮翻倍。
func (a *Actor) onHeartbeat(msg *HeartbeatMsg) {
	if e, ok := a.ents[msg.PlayerID]; ok {
		e.lastSeen = a.now()
	}
}

// pushBody 向单个实体推送一条场景通知。
//
// 推送失败（信箱满 / Actor 已终止）**不重试**：重试只会加剧拥堵，
// 客户端发现通知断档后走 Sync 全量补齐。
func (a *Actor) pushBody(e *entity, body []byte) {
	if e == nil || e.pid == nil || len(body) == 0 {
		return
	}
	// 走 ScenePush 而不是 SceneResult：实体通知是纯下行广播，
	// 不触发玩家 Actor 的快照 CAS——位置权威在场景，玩家侧坐标只是只读副本；
	// 只有进入回执（SceneResult + SetScene）才更新 Profile.SceneID。
	if err := a.sys.Send(e.pid, &player.ScenePush{
		SceneID: a.cfg.ID,
		MsgID:   codec.MsgSceneEntityNotify,
		Body:    body,
	}); err != nil {
		a.logger.Debug("scene push dropped", "player_id", e.id, "err", err)
	}
}

// Count 当前在场人数（测试与指标使用）。
func (a *Actor) Count() int { return len(a.ents) }

// snapshot 广播/回执共用的实体快照：只带客户端渲染必需的字段。
func snapshot(e *entity) *gamev1.SceneEntity {
	return &gamev1.SceneEntity{
		PlayerId: e.id,
		Name:     e.name,
		Level:    e.level,
		X:        e.x,
		Y:        e.y,
	}
}

// appearNotice Type=1（出现）：Entity 为全量快照，客户端据此创建实体。
func appearNotice(e *entity) []byte {
	body, err := proto.Marshal(&gamev1.SceneEntityNotify{Type: 1, Entity: snapshot(e)})
	if err != nil {
		return nil
	}
	return body
}

// moveNotice Type=2（移动）：同快照结构，客户端按 PlayerId 更新坐标。
func moveNotice(e *entity) []byte {
	body, err := proto.Marshal(&gamev1.SceneEntityNotify{Type: 2, Entity: snapshot(e)})
	if err != nil {
		return nil
	}
	return body
}

// leaveNotice Type=3（离开）：只带 id——客户端按 id 移除实体，不需要最后坐标。
func leaveNotice(playerID int64) []byte {
	body, err := proto.Marshal(&gamev1.SceneEntityNotify{
		Type:   3,
		Entity: &gamev1.SceneEntity{PlayerId: playerID},
	})
	if err != nil {
		return nil
	}
	return body
}

// abs32 曼哈顿距离用的绝对值；int32 版避免 int 转换噪音。
func abs32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}
