// internal/game/module/seat/seat.go —— 抢位置玩法模块（协议 8001-8010）
//
// 只做「协议 ↔ 房间动作」的翻译：占座、离开、查询。
// 奖励投递不在这里——座位释放由组装层处理（在线直投 / 离线事件队列），
// 玩家登录时的补发也由离线段 drain 统一消费，模块本身无状态。
package seat

import (
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

var router Router

// SetRouter 注入房间管理器（组装层调用）。
func SetRouter(r Router) { router = r }

// M 抢位置模块唯一实例。
var M = module.Define[module.NoState]("seat")

func init() {
	module.Route(M, &gamev1.SeatOccupyReq{}, onOccupy)
	module.Route(M, &gamev1.SeatLeaveReq{}, onLeave)
	M.HandleRoute(&gamev1.SeatQueryReq{}, onQuery)
}

// onOccupy 占座/抢夺。
func onOccupy(c *module.Ctx[module.NoState], req *gamev1.SeatOccupyReq) error {
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
func onLeave(c *module.Ctx[module.NoState], req *gamev1.SeatLeaveReq) error {
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
func onQuery(c *module.Ctx[module.NoState]) error {
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
