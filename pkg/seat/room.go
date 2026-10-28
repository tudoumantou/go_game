// pkg/seat/room.go —— 房间 Actor（一个房间管 10 个座位）
//
// 并发模型：状态全在 Actor 内，Receive 串行调用，业务侧无锁。
// 持久化边界：本文件不碰数据库——变更经 onChange 交快照、释放经 onRelease
// 交结算事件，由组装层（internal/game/seat.Manager）负责落库与发奖。
package seat

import (
	"sync/atomic"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"
)

type (
	// OccupyMsg 占座（空座先到先得；他人已占则抢夺，原主先结算）。
	OccupyMsg struct {
		PlayerID  int64
		SeatIndex int
	}
	// LeaveMsg 主动离开（仅本人有效）。
	LeaveMsg struct {
		PlayerID  int64
		SeatIndex int
	}
	// RestoreMsg 进程启动时从库里恢复占用。
	RestoreMsg struct {
		Slots []Slot
	}
	// timeoutMsg 满 1h 的到期消息；带 OccupiedAt 以识别旧定时器。
	timeoutMsg struct {
		SeatIndex  int
		OccupiedAt time.Time
	}
)

// Room 一个座位房间。
type Room struct {
	id    int32 // 房间唯一 ID：tier*100+room_index
	tier  Tier
	index int
	seats [SeatsPerRoom]*Seat

	// snap 房间快照：Actor 内重建后 Store，外部无锁 Load。
	// 查询方直接读它，管理器不必再存一份副本。
	snap atomic.Value // RoomSnapshot（Slots 每次重建，不共享底层数组）

	onChange  func(RoomSnapshot) // 状态变更（落库）
	onSeat    func(playerID int64, seatIndex int, occupied bool, at time.Time) // 座位归属变化（同步玩家数据）
	onRelease func(Release)      // 座位释放（结算）

	logger *log.Logger
}

// NewRoom 创建房间（尚未 Spawn）。
func NewRoom(tier Tier, index int, logger *log.Logger) *Room {
	r := &Room{id: RoomID(tier, index), tier: tier, index: index, logger: logger}
	for i := range r.seats {
		r.seats[i] = &Seat{Index: i}
	}
	r.refresh() // 先放一份空快照：Spawn 前就可能有查询
	return r
}

// ID 房间唯一 ID。
func (r *Room) ID() int32 { return r.id }

// SetOnChange 注入状态变更回调（Spawn 前设置）。
func (r *Room) SetOnChange(fn func(RoomSnapshot)) { r.onChange = fn }

// SetOnSeat 注入座位归属变化回调（Spawn 前设置）。
func (r *Room) SetOnSeat(fn func(playerID int64, seatIndex int, occupied bool, at time.Time)) {
	r.onSeat = fn
}

// SetOnRelease 注入释放回调（Spawn 前设置）。
func (r *Room) SetOnRelease(fn func(Release)) { r.onRelease = fn }

// Receive 实现 actor.Actor。
func (r *Room) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case *OccupyMsg:
		r.onOccupy(ctx, msg)
	case *LeaveMsg:
		r.onLeave(msg)
	case *RestoreMsg:
		r.onRestore(ctx, msg)
	case *timeoutMsg:
		r.onTimeout(msg)
	}
}

// onOccupy 占座：空座直接占；他人已占则先结算原主；自己占自己幂等。
func (r *Room) onOccupy(ctx *actor.Context, msg *OccupyMsg) {
	if !ValidSeat(r.tier, r.index, msg.SeatIndex) || msg.PlayerID == 0 {
		return
	}
	s := r.seats[msg.SeatIndex]
	now := time.Now()

	if s.State == SeatOccupied {
		if s.Occupant == msg.PlayerID {
			return // 幂等：不重置计时，避免「自己占自己」无限续时
		}
		r.release(s, now) // 抢夺：原主按已占分钟数结算后让位
	}

	s.State = SeatOccupied
	s.Occupant = msg.PlayerID
	s.OccupyID = nextSeq()
	s.OccupiedAt = now

	// 自投定时器：用 ctx.PID() 即可，不必自己存一份 PID。
	// ctx 为 nil 只出现在单测里（直接调处理函数，不挂定时器）。
	if ctx != nil {
		if self := ctx.PID(); self != nil {
			ctx.ScheduleOnce(HoldLimit, self, &timeoutMsg{SeatIndex: msg.SeatIndex, OccupiedAt: now})
		}
	}
	r.logger.Info("seat occupied", "tier", r.tier.String(), "room", r.index,
		"seat", msg.SeatIndex, "player_id", msg.PlayerID)
	if r.onSeat != nil {
		r.onSeat(msg.PlayerID, msg.SeatIndex, true, now)
	}
	r.notifyChange()
}

// onLeave 主动离开：仅当前占用者本人可释放。
func (r *Room) onLeave(msg *LeaveMsg) {
	if !ValidSeat(r.tier, r.index, msg.SeatIndex) {
		return
	}
	s := r.seats[msg.SeatIndex]
	if s.State != SeatOccupied || s.Occupant != msg.PlayerID {
		return
	}
	r.release(s, time.Now())
	r.notifyChange()
}

// onRestore 重启恢复：已到期立即结算（定时器已随进程丢失），未到期重挂。
func (r *Room) onRestore(ctx *actor.Context, msg *RestoreMsg) {
	now := time.Now()
	for _, sl := range msg.Slots {
		if !ValidSeat(r.tier, r.index, sl.Index) || sl.Occupant <= 0 || sl.OccupiedAt <= 0 {
			continue
		}
		s := r.seats[sl.Index]
		s.State = SeatOccupied
		s.Occupant = sl.Occupant
		s.OccupyID = sl.OccupyID
		if s.OccupyID <= 0 {
			// 兜底：缺 ID 会变成幂等键 0，同一玩家所有奖励只发得出第一笔。
			s.OccupyID = nextSeq()
		}
		s.OccupiedAt = time.Unix(sl.OccupiedAt, 0)

		deadline := s.OccupiedAt.Add(HoldLimit)
		if !deadline.After(now) {
			r.release(s, now) // 内部会回调 onSeat(false)
			continue
		}
		if r.onSeat != nil {
			r.onSeat(s.Occupant, s.Index, true, s.OccupiedAt) // 重建玩家数据索引
		}
		if ctx != nil {
			if self := ctx.PID(); self != nil {
				ctx.ScheduleOnce(deadline.Sub(now), self, &timeoutMsg{
					SeatIndex: sl.Index, OccupiedAt: s.OccupiedAt,
				})
			}
		}
	}
	r.notifyChange()
}

// onTimeout 到期让座；只处理同一 OccupiedAt 的那次占用（旧定时器作废）。
func (r *Room) onTimeout(msg *timeoutMsg) {
	if !ValidSeat(r.tier, r.index, msg.SeatIndex) {
		return
	}
	s := r.seats[msg.SeatIndex]
	if s.State != SeatOccupied || !s.OccupiedAt.Equal(msg.OccupiedAt) {
		return
	}
	r.release(s, time.Now())
	r.notifyChange()
}

// notifyChange 重建快照并回调（先 Store 再通知，读方永远看到一致状态）。
func (r *Room) notifyChange() {
	snap := r.refresh()
	if r.onChange != nil {
		r.onChange(snap)
	}
}

// refresh 按当前座位重建快照并发布。
//
// 每次新建 Slots 切片而不是复用：快照会交给外部无锁读取，
// 复用底层数组等于让读方看到正在被改写的元素。
func (r *Room) refresh() RoomSnapshot {
	snap := RoomSnapshot{ID: r.id, Tier: r.tier, Index: r.index, Slots: make([]Slot, 0, SeatsPerRoom)}
	for _, s := range r.seats {
		sl := Slot{Index: s.Index, State: s.State, Occupant: s.Occupant, OccupyID: s.OccupyID}
		if s.State == SeatOccupied {
			sl.OccupiedAt = s.OccupiedAt.Unix()
		}
		snap.Slots = append(snap.Slots, sl)
	}
	r.snap.Store(snap)
	return snap
}

// Snapshot 房间快照（无锁读，供管理器和查询路径使用）。
func (r *Room) Snapshot() RoomSnapshot {
	if v, ok := r.snap.Load().(RoomSnapshot); ok {
		return v
	}
	return RoomSnapshot{ID: r.id, Tier: r.tier, Index: r.index}
}

// release 结算并清空座位（调用方保证 s 处于占用态）。
func (r *Room) release(s *Seat, now time.Time) {
	rel := Release{
		PlayerID:  s.Occupant,
		Tier:      r.tier,
		RoomIndex: r.index,
		SeatIndex: s.Index,
		OccupyID:  s.OccupyID,
		Minutes:   OccupancyMinutes(s.OccupiedAt, now),
	}
	if rel.Minutes > 0 && rel.PlayerID > 0 {
		rel.Amount = RewardOf(r.tier, rel.Minutes)
	}

	s.State, s.Occupant, s.OccupyID, s.OccupiedAt = SeatEmpty, 0, 0, time.Time{}

	if rel.PlayerID > 0 && r.onSeat != nil {
		r.onSeat(rel.PlayerID, rel.SeatIndex, false, time.Time{})
	}
	if rel.PlayerID > 0 && r.onRelease != nil {
		r.onRelease(rel)
	}
	if rel.Amount > 0 {
		r.logger.Info("seat released", "tier", r.tier.String(), "room", r.index,
			"seat", rel.SeatIndex, "player_id", rel.PlayerID,
			"minutes", rel.Minutes, "amount", rel.Amount)
	}
}
