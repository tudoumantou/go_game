// Package scene 场景 Actor：地图边界、实体进出、位置与广播（R9）。
//
// 权威边界：**位置只由本 Actor 修改**。玩家 Actor 只发消息、收推送，
// 不持有可写坐标——这条约束避免了两个 goroutine 同时改同一份场景数据。
package scene

import (
	"fmt"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	"game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"

	"game_study/internal/game/player"
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
)

// 移动速度上限（单位/秒）与抖动容忍。
//
// 客户端上报目标点，服务端按「距上次移动的时间 × 速度上限」判定。
// 超速时不移动、回权威坐标：钳制会让外挂靠高频小步前进绕过限制。
const (
	maxSpeedPerSec  = 30
	minMoveDistance = 10 // 网络抖动与首次移动的容忍量
)

// Config 场景静态配置（来自 scene.xml）。
type Config struct {
	ID         int32
	Name       string
	Width      int32 // 地图宽（坐标上界，开区间）
	Height     int32
	Cell       int32 // AOI 单元格边长（R9.2 使用）
	MaxPlayers int32
}

// entity 场景内实体。只存广播必需的最小信息。
type entity struct {
	id       int64
	pid      *actor.PID // 广播目标；玩家重连后会更新
	name     string
	level    int32
	x, y     int32
	lastSeen time.Time // R9.3 心跳兜底使用
}

// Actor 场景 Actor。所有字段只在分片 goroutine 内读写，无锁。
type Actor struct {
	cfg    Config
	sys    *actor.System
	logger *log.Logger
	ents   map[int64]*entity
}

// NewActor 创建场景 Actor。
func NewActor(cfg Config, sys *actor.System, logger *log.Logger) *Actor {
	return &Actor{
		cfg:    cfg,
		sys:    sys,
		logger: logger.With("scene_id", cfg.ID),
		ents:   make(map[int64]*entity, cfg.MaxPlayers),
	}
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

// CountMsg 查询在场人数（测试与运维观测用）。
//
// 人数是场景内部状态，外部只能通过消息读取——直接读 map 会与分片 goroutine 竞争。
type CountMsg struct {
	Reply chan int
}

// HeartbeatMsg 玩家侧心跳（R9.3 兜底清理的依据）。
type HeartbeatMsg struct {
	PlayerID int64
}

// Receive 实现 actor.Actor。
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
	case *CountMsg:
		msg.Reply <- len(a.ents)
	default:
		a.logger.Debug("scene unknown message", "type", msgType(ctx.Message()))
	}
}

func (a *Actor) onEnter(ctx *actor.Context, msg *EnterMsg) {
	// 重复进入（重连、客户端重发）：更新绑定与在场时间，不重复占名额。
	if e, ok := a.ents[msg.PlayerID]; ok {
		e.pid = msg.PID
		e.name = msg.Name
		e.level = msg.Level
		e.lastSeen = time.Now()
		a.replyEnter(ctx, msg.PID, codeOK, e, true)
		return
	}

	if int32(len(a.ents)) >= a.cfg.MaxPlayers {
		// 人数满不踢人、不排队：直接拒绝，玩家留在原场景。
		a.replyEnter(ctx, msg.PID, codeFull, nil, false)
		return
	}

	e := &entity{
		id:       msg.PlayerID,
		pid:      msg.PID,
		name:     msg.Name,
		level:    msg.Level,
		x:        Clamp(msg.X, a.cfg.Width),
		y:        Clamp(msg.Y, a.cfg.Height),
		lastSeen: time.Now(),
	}
	a.ents[e.id] = e

	// 先给本人全量视野快照，再向其他人广播出现——
	// 顺序反了会让新玩家先收到别人的出现通知、再收到快照，客户端出现重复实体。
	a.replyEnter(ctx, msg.PID, codeOK, e, false)

	if body, err := proto.Marshal(&gamev1.SceneEntityNotify{
		Type:   1, // 出现
		Entity: snapshot(e),
	}); err == nil {
		a.broadcastExcept(e.id, codec.MsgSceneEntityNotify, body)
	}
}

func (a *Actor) replyEnter(ctx *actor.Context, pid *actor.PID, code int32, self *entity, existing bool) {
	resp := &gamev1.SceneEnterResp{
		Code:    code,
		SceneId: a.cfg.ID,
		Width:   a.cfg.Width,
		Height:  a.cfg.Height,
	}
	resp.Entities = make([]*gamev1.SceneEntity, 0, len(a.ents))
	if code == codeOK && self != nil {
		resp.X, resp.Y = self.x, self.y
		for id, e := range a.ents {
			if id == self.id {
				continue
			}
			resp.Entities = append(resp.Entities, snapshot(e))
		}
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		a.logger.Error("marshal enter resp", "err", err)
		return
	}
	_ = ctx.Send(pid, &player.SceneResult{
		SceneID:  a.cfg.ID,
		MsgID:    codec.MsgSceneEnterResp,
		Body:     body,
		SetScene: code == codeOK && !existing,
	})
}

func (a *Actor) onMove(ctx *actor.Context, msg *MoveMsg) {
	e, ok := a.ents[msg.PlayerID]
	if !ok {
		a.replyMove(ctx, msg.PlayerID, msg.Seq, codeNotIn, 0, 0)
		return
	}

	x := Clamp(msg.X, a.cfg.Width)
	y := Clamp(msg.Y, a.cfg.Height)

	// 速度校验：按距上次移动的时间给预算，超出则原地不动。
	now := time.Now()
	dt := now.Sub(e.lastSeen).Seconds()
	allowed := maxSpeedPerSec*dt + minMoveDistance
	if abs32(x-e.x)+abs32(y-e.y) > int32(allowed) {
		a.replyMove(ctx, msg.PlayerID, msg.Seq, codeTooFast, e.x, e.y)
		return
	}

	moved := x != e.x || y != e.y
	e.x, e.y = x, y
	e.lastSeen = now

	a.replyMove(ctx, msg.PlayerID, msg.Seq, codeOK, x, y)
	if !moved {
		return // 原地不动不广播，避免无意义的流量
	}

	if body, err := proto.Marshal(&gamev1.SceneEntityNotify{
		Type:   2, // 移动
		Entity: snapshot(e),
	}); err == nil {
		a.broadcastExcept(e.id, codec.MsgSceneEntityNotify, body)
	}
}

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
	})
}

func (a *Actor) onLeave(ctx *actor.Context, msg *LeaveMsg) {
	e, ok := a.ents[msg.PlayerID]
	if !ok {
		return // 幂等：重复离开不报错
	}
	delete(a.ents, msg.PlayerID)

	if body, err := proto.Marshal(&gamev1.SceneEntityNotify{
		Type:   3, // 离开
		Entity: &gamev1.SceneEntity{PlayerId: e.id},
	}); err == nil {
		a.broadcastExcept(e.id, codec.MsgSceneEntityNotify, body)
	}
}

func (a *Actor) onSync(ctx *actor.Context, msg *SyncMsg) {
	e, ok := a.ents[msg.PlayerID]
	if !ok {
		// 不在场（例如场景 Actor 曾重启）：让客户端重新进入，而不是回空列表。
		a.logger.Debug("sync for absent entity", "player_id", msg.PlayerID)
		body, _ := proto.Marshal(&gamev1.SceneSyncResp{Code: codeNotIn, SceneId: a.cfg.ID})
		pid := msg.PID
		if pid == nil {
			return
		}
		_ = ctx.Send(pid, &player.SceneResult{SceneID: a.cfg.ID, MsgID: codec.MsgSceneSyncResp, Body: body})
		return
	}
	e.pid = msg.PID // 重连后 PID 可能变化
	e.lastSeen = time.Now()

	resp := &gamev1.SceneSyncResp{
		Code:     codeOK,
		SceneId:  a.cfg.ID,
		X:        e.x,
		Y:        e.y,
		Entities: make([]*gamev1.SceneEntity, 0, len(a.ents)),
	}
	for id, other := range a.ents {
		if id == e.id {
			continue
		}
		resp.Entities = append(resp.Entities, snapshot(other))
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		return
	}
	_ = ctx.Send(e.pid, &player.SceneResult{SceneID: a.cfg.ID, MsgID: codec.MsgSceneSyncResp, Body: body})
}

func (a *Actor) onHeartbeat(msg *HeartbeatMsg) {
	if e, ok := a.ents[msg.PlayerID]; ok {
		e.lastSeen = time.Now()
	}
}

// broadcastExcept 向场景内除指定玩家外的所有实体推送。
//
// 推送失败（信箱满 / Actor 已终止）**不重试**：重试只会加剧拥堵，
// 客户端发现通知断档后走 Sync 全量补齐。R9.2 引入 AOI 后这里改为按视野推送。
func (a *Actor) broadcastExcept(self int64, msgID uint32, body []byte) {
	for id, e := range a.ents {
		if id == self {
			continue
		}
		if e.pid == nil {
			continue
		}
		if err := a.sys.Send(e.pid, &player.ScenePush{
			SceneID: a.cfg.ID,
			MsgID:   msgID,
			Body:    body,
		}); err != nil {
			a.logger.Debug("scene push dropped", "player_id", id, "err", err)
		}
	}
}

// Count 当前在场人数（测试与指标使用）。
func (a *Actor) Count() int { return len(a.ents) }

func snapshot(e *entity) *gamev1.SceneEntity {
	return &gamev1.SceneEntity{
		PlayerId: e.id,
		Name:     e.name,
		Level:    e.level,
		X:        e.x,
		Y:        e.y,
	}
}

func abs32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}

func msgType(v any) string { return fmt.Sprintf("%T", v) }
