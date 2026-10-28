// internal/game/module/seat/seat.go —— 抢位置玩法模块（协议 8001-8010）
//
// 架构职责：把客户端「占用/离开/查询」协议翻译成对房间 Actor 的消息，
// 并在登录时补发离线期间产生的座位奖励。
//
// 两种部署形态共用同一套协议：
//   - 单服：注入 router（本节点房间 Actor），操作直接投递、查询读本地快照；
//   - 跨服：注入 sender + cache（cross.Pool + 本服快照缓存），操作经
//     CrossFrame_SeatOp 发往指定 cross 节点，查询读 cross 广播来的缓存。
//
// 关键点：座位状态不在玩家快照里，而在共享的房间 Actor 中（一个房间被全服
// 玩家共享，不能放进任何单个玩家状态）。本模块因此只做「转发 + 查询 + 补发」。
package seat

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
	pkgseat "game_study/pkg/seat"
)

// RoomRouter 单服房间寻址与只读快照（由 internal/game/seat.Manager 实现）。
type RoomRouter interface {
	Occupy(tier pkgseat.Tier, roomIndex, seatIndex int, playerID int64) bool
	Leave(tier pkgseat.Tier, roomIndex, seatIndex int, playerID int64) bool
	AllSnapshots() []pkgseat.RoomSnapshot
}

// PendingStore 离线座位奖励表（由 internal/game/seat.PendingStore 实现）。
type PendingStore interface {
	List(ctx context.Context, playerID int64) ([]pkgseat.PendingReward, error)
	Delete(ctx context.Context, playerID, seq int64) error
}

// SeatSender 跨服座位操作发送器（由 game/cross.Pool 实现）。
type SeatSender interface {
	SendSeatOp(op *internalv2.SeatOp) error
}

// SeatCache 跨服座位快照缓存（由 game/seat.Cache 实现）。
type SeatCache interface {
	Snapshots() []pkgseat.RoomSnapshot
}

var (
	router  RoomRouter
	pending PendingStore
	sender  SeatSender
	cache   SeatCache
)

// SetRouter 注入单服房间路由器（组装层在 initSeat 调用）。
func SetRouter(r RoomRouter) { router = r }

// SetPending 注入离线奖励表（无 MySQL 环境可不注入）。
func SetPending(p PendingStore) { pending = p }

// SetCross 切换到跨服模式：操作走 sender，查询读 cache。
func SetCross(s SeatSender, c SeatCache) {
	sender = s
	cache = c
}

// M 抢位置模块唯一实例。
//
// 占用/离开/查询都不修改玩家状态（奖励另走 GrantItemsMsg），登录钩子只做
// 离线补发；因此无状态、可丢弃，也不依赖缓存权威路径。
var M = module.Define[struct{}]("seat").WithLogin(onLogin)

func init() {
	module.Route(M, &gamev1.SeatOccupyReq{}, onOccupy)
	module.Route(M, &gamev1.SeatLeaveReq{}, onLeave)
	M.HandleRoute(&gamev1.SeatQueryReq{}, onQuery)
}

// onOccupy 占用/抢夺。
func onOccupy(c *module.Ctx[struct{}], req *gamev1.SeatOccupyReq) error {
	resp := &gamev1.SeatOccupyResp{Code: int32(errcode.OK)}
	tier := pkgseat.Tier(req.GetTier())
	roomIndex, seatIndex := int(req.GetRoomIndex()), int(req.GetSeatIndex())

	// 坐标校验必须放在最前：跨服模式下非法坐标的帧发出去后，对端只会
	// 静默丢弃，玩家却收到成功回包——表现为「点了没反应」。
	// tier 传 0（未填/填错）同样在这里被拦下。
	if !pkgseat.ValidSeat(tier, roomIndex, seatIndex) {
		resp.Code = int32(errcode.ErrParam)
		return c.Reply(resp)
	}
	playerID := c.State.PlayerID()

	// 跨服模式：发往指定 cross 节点（房间在那边，一人一座由那边裁决）。
	if sender != nil {
		if err := sender.SendSeatOp(&internalv2.SeatOp{
			Action:    internalv2.SeatOp_OCCUPY,
			Tier:      int32(tier),
			RoomIndex: int32(roomIndex),
			SeatIndex: int32(seatIndex),
			PlayerId:  playerID,
		}); err != nil {
			resp.Code = int32(errcode.ErrServerBusy)
		}
		return c.Reply(resp)
	}

	// 单服模式：交给本节点管理器（含一人一座裁决与自动换座）。
	if router == nil {
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}
	if !router.Occupy(tier, roomIndex, seatIndex, playerID) {
		resp.Code = int32(errcode.ErrServerBusy)
	}
	return c.Reply(resp)
}

// onLeave 主动离开。
func onLeave(c *module.Ctx[struct{}], req *gamev1.SeatLeaveReq) error {
	resp := &gamev1.SeatLeaveResp{Code: int32(errcode.OK)}
	tier := pkgseat.Tier(req.GetTier())
	roomIndex, seatIndex := int(req.GetRoomIndex()), int(req.GetSeatIndex())
	if !pkgseat.ValidSeat(tier, roomIndex, seatIndex) {
		resp.Code = int32(errcode.ErrParam)
		return c.Reply(resp)
	}
	playerID := c.State.PlayerID()

	if sender != nil {
		if err := sender.SendSeatOp(&internalv2.SeatOp{
			Action:    internalv2.SeatOp_LEAVE,
			Tier:      int32(tier),
			RoomIndex: int32(roomIndex),
			SeatIndex: int32(seatIndex),
			PlayerId:  playerID,
		}); err != nil {
			resp.Code = int32(errcode.ErrServerBusy)
		}
		return c.Reply(resp)
	}

	if router == nil {
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}
	if !router.Leave(tier, roomIndex, seatIndex, playerID) {
		resp.Code = int32(errcode.ErrServerBusy)
	}
	return c.Reply(resp)
}

// onQuery 拉取全部房间/座位状态。
func onQuery(c *module.Ctx[struct{}]) error {
	resp := &gamev1.SeatQueryResp{Code: int32(errcode.OK)}

	var snaps []pkgseat.RoomSnapshot
	switch {
	case cache != nil: // 跨服：读 cross 广播来的缓存
		snaps = cache.Snapshots()
	case router != nil: // 单服：读本节点房间快照
		snaps = router.AllSnapshots()
	default:
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}

	for _, snap := range snaps {
		room := &gamev1.SeatRoom{Tier: int32(snap.Tier), RoomIndex: int32(snap.RoomIndex)}
		for _, sv := range snap.Seats {
			room.Seats = append(room.Seats, &gamev1.SeatSlot{
				Index:     int32(sv.Index),
				State:     int32(sv.State),
				Occupant:  sv.Occupant,
				RemainSec: remainSec(sv),
			})
		}
		resp.Rooms = append(resp.Rooms, room)
	}
	return c.Reply(resp)
}

// remainSec 按占用起始时间实时计算倒计时。
//
// 快照里的 RemainSec 是「生成快照那一刻」的值：单服缓存/跨服广播都只在
// 变更时刷新，直接下发会随时间失真（半小时没变更就多报半小时）。这里用
// OccupiedAt 现算，保证倒计时始终准确。
func remainSec(sv pkgseat.SeatView) int64 {
	if sv.State != pkgseat.SeatOccupied || sv.OccupiedAt <= 0 {
		return 0
	}
	d := pkgseat.HoldLimit - time.Since(time.Unix(sv.OccupiedAt, 0))
	if d <= 0 {
		return 0
	}
	return int64(d / time.Second)
}

// onLogin 补发离线座位奖励。
//
// 幂等双保险：ApplyOnce 的业务键随玩家快照落库（applied_grants），即使
// 删表失败，下次登录也不会重复发；满包时 ApplyOnce 不占用业务键，保留
// 挂起行待下次登录重试。
func onLogin(c *module.Ctx[struct{}]) error {
	if pending == nil {
		return nil
	}
	ctx := c.Actor.Context()
	playerID := c.State.PlayerID()

	rows, err := pending.List(ctx, playerID)
	if err != nil {
		c.Log().Warn("seat pending list failed", "player_id", playerID, "err", err)
		return nil
	}
	for _, r := range rows {
		reward := pkgseat.RewardOf(r.Tier, r.Minutes)
		if reward <= 0 {
			_ = pending.Delete(ctx, playerID, r.OccupyID)
			continue
		}
		key := fmt.Sprintf("seat_pending:%d", r.OccupyID)
		applied, gerr := c.ApplyOnce(key, func() error {
			_, e := module.Grant(c.State, []module.ItemGrant{
				{ItemID: pkgseat.RewardItem, Count: reward},
			})
			return e
		})
		if gerr != nil {
			continue // 满包/超限：保留挂起行，下次登录再补
		}
		if applied {
			_ = pending.Delete(ctx, playerID, r.OccupyID)
		}
	}
	return nil
}
