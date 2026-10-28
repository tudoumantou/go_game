// internal/game/module/seat/seat.go —— 抢位置玩法模块（协议 8001-8010）
//
// 只做三件事：把客户端请求翻译成对房间的动作（占座/离开）、把房间快照
// 翻译成查询响应、登录时补发离线期间的座位奖励。
//
// 座位状态不在玩家快照里（一个房间被全服玩家共享），所以本模块无状态，
// 也不改玩家任何字段——发奖走玩家 Actor 的 GrantItemsMsg。
package seat

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
	pkgseat "game_study/pkg/seat"
)

// Router 房间操作与快照（由 internal/game/seat.Manager 实现）。
type Router interface {
	Occupy(roomID int32, seatIndex int, playerID int64) bool
	Leave(roomID int32, seatIndex int, playerID int64) bool
	AllSnapshots() []pkgseat.RoomSnapshot
}

// PendingStore 待补发奖励表（由 internal/game/seat.PendingStore 实现）。
type PendingStore interface {
	List(ctx context.Context, playerID int64) ([]pkgseat.Pending, error)
	Delete(ctx context.Context, playerID, occupyID int64) error
}

var (
	router  Router
	pending PendingStore
)

// SetRouter 注入房间管理器（组装层调用）。
func SetRouter(r Router) { router = r }

// SetPending 注入待补发奖励表（无 MySQL 环境可不注入）。
func SetPending(p PendingStore) { pending = p }

// M 抢位置模块唯一实例。
var M = module.Define[struct{}]("seat").WithLogin(onLogin)

func init() {
	module.Route(M, &gamev1.SeatOccupyReq{}, onOccupy)
	module.Route(M, &gamev1.SeatLeaveReq{}, onLeave)
	M.HandleRoute(&gamev1.SeatQueryReq{}, onQuery)
}

// onOccupy 占座/抢夺。
func onOccupy(c *module.Ctx[struct{}], req *gamev1.SeatOccupyReq) error {
	resp := &gamev1.SeatOccupyResp{Code: int32(errcode.OK)}
	tier := pkgseat.Tier(req.GetTier())
	roomIndex, seatIndex := int(req.GetRoomIndex()), int(req.GetSeatIndex())
	if !pkgseat.ValidSeat(tier, roomIndex, seatIndex) {
		resp.Code = int32(errcode.ErrParam)
		return c.Reply(resp)
	}
	if router == nil {
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}
	if !router.Occupy(pkgseat.RoomID(tier, roomIndex), seatIndex, c.State.PlayerID()) {
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
	if router == nil {
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}
	if !router.Leave(pkgseat.RoomID(tier, roomIndex), seatIndex, c.State.PlayerID()) {
		resp.Code = int32(errcode.ErrServerBusy)
	}
	return c.Reply(resp)
}

// onQuery 拉取全部房间/座位状态。
func onQuery(c *module.Ctx[struct{}]) error {
	resp := &gamev1.SeatQueryResp{Code: int32(errcode.OK)}
	if router == nil {
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}
	for _, snap := range router.AllSnapshots() {
		room := &gamev1.SeatRoom{Tier: int32(snap.Tier), RoomIndex: int32(snap.Index)}
		for _, sl := range snap.Slots {
			room.Seats = append(room.Seats, &gamev1.SeatSlot{
				Index:     int32(sl.Index),
				State:     int32(sl.State),
				Occupant:  sl.Occupant,
				RemainSec: remainSec(sl),
			})
		}
		resp.Rooms = append(resp.Rooms, room)
	}
	return c.Reply(resp)
}

// remainSec 按占用起始时间现算倒计时。
//
// 快照里的占用时间是「生成快照那一刻」的值，直接下发会随时间失真
// （半小时没变更就多报半小时），所以查询时用当前时间重算。
func remainSec(sl pkgseat.Slot) int64 {
	if sl.State != pkgseat.SeatOccupied || sl.OccupiedAt <= 0 {
		return 0
	}
	d := pkgseat.HoldLimit - time.Since(time.Unix(sl.OccupiedAt, 0))
	if d <= 0 {
		return 0
	}
	return int64(d / time.Second)
}

// onLogin 补发离线期间的座位奖励。
//
// ApplyOnce 把业务键写进玩家快照（applied_grants），即使删表失败也不会
// 重复发；满包时 ApplyOnce 不占用业务键，保留挂起行下次登录重试。
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
		applied, gerr := c.ApplyOnce(fmt.Sprintf("seat:%d", r.OccupyID), func() error {
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
