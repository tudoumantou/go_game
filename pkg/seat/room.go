// pkg/seat/room.go —— 房间 Actor（单服/跨服共用的座位状态机）
//
// 并发模型：房间状态全部在 Actor 内，Receive 由分片 goroutine 串行调用，
// 业务侧无锁。跨 Actor 只通过消息：玩家侧发 Occupy/Leave，超时由
// ScheduleOnce 投递 timeoutMsg，重启恢复由 RestoreMsg 灌入。
//
// 与 battle.Room 的差异：战斗房随结算销毁，座位房是常驻设施（进程生命周期内
// 一直存在），因此没有 RoomClosed 回调，也不做「结算后拒绝入内」。
//
// 持久化边界：本文件不碰数据库。状态变更通过 onChange 把快照交给组装层落库，
// 座位释放通过 onRelease 把结算事件交给组装层（发奖 + 清挂起表）。
package seat

import (
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"
)

// 消息类型：用具体指针类型区分，Receive 里做类型 switch（编译期可查）。
type (
	// OccupyMsg 占用座位（空座=先到先得；已占他人=抢夺，原主先结算）。
	OccupyMsg struct {
		PlayerID  int64
		SeatIndex int
		Node      string // 跨服：玩家所在游戏服节点；单服留空
	}
	// LeaveMsg 主动离开（仅本人有效）。
	LeaveMsg struct {
		PlayerID  int64
		SeatIndex int
	}
	// RestoreMsg 进程启动时从库里恢复座位占用。
	//
	// 恢复不是简单回填：已到期的占用必须立刻结算（重启期间定时器丢了），
	// 未到期的按剩余时长重新挂 ScheduleOnce，否则玩家会白占一个不受
	// 1h 上限约束的座位。
	RestoreMsg struct {
		Slots []RestoredSlot
	}
	// timeoutMsg 占用满 1h 的到期消息。
	//
	// 带 OccupiedAt：同一座位可能在 1h 内被释放并重新占用，旧定时器随后才到，
	// 必须凭「起始时间」判定它属于哪一次占用——否则会把新占用者误踢掉。
	timeoutMsg struct {
		SeatIndex  int
		OccupiedAt time.Time
	}
)

// Room 单个座位房间。
type Room struct {
	tier  Tier
	index int
	seats [SeatsPerRoom]*Seat

	// onChange 状态变更回调（组装层用它落库、跨服用它广播给游戏服）。
	// 在 Actor 内调用并传入本房间快照——快照在 Actor 串行上下文中生成，无竞态。
	onChange func(RoomSnapshot)
	// onRelease 座位释放回调：无论是否产生奖励都会调用。
	//
	// 组装层用它做两件事：清「一人一座」登记（不足 1 分钟没奖也必须清，
	// 否则该玩家会永远被判定为已有座位）；结算奖励（Amount>0 才有）。
	onRelease func(ReleaseEvent)

	logger *log.Logger
	pid    *actor.PID
}

// NewRoom 创建房间 Actor（尚未 Spawn）。
func NewRoom(tier Tier, index int, logger *log.Logger) *Room {
	r := &Room{
		tier:   tier,
		index:  index,
		logger: logger,
	}
	for i := 0; i < SeatsPerRoom; i++ {
		r.seats[i] = &Seat{Index: i}
	}
	return r
}

// SetPID 注入自身 PID（Spawn 之后调用，ScheduleOnce 需要它）。
func (r *Room) SetPID(pid *actor.PID) { r.pid = pid }

// SetOnChange 注入状态变更回调（须在 Spawn 前设置）。
func (r *Room) SetOnChange(fn func(RoomSnapshot)) { r.onChange = fn }

// SetOnRelease 注入座位释放回调（须在 Spawn 前设置）。
func (r *Room) SetOnRelease(fn func(ReleaseEvent)) { r.onRelease = fn }

// Tier 房间档位。
func (r *Room) Tier() Tier { return r.tier }

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

// onOccupy 占用：空座直接占；已占他人则先结算原主（抢夺）；自己占自己幂等。
func (r *Room) onOccupy(ctx *actor.Context, msg *OccupyMsg) {
	if msg.SeatIndex < 0 || msg.SeatIndex >= SeatsPerRoom || msg.PlayerID == 0 {
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
	s.OccupyID = nextSeq() // 本次占用实例 ID，结算幂等键
	s.Node = msg.Node
	s.OccupiedAt = now

	// 1h 硬上限：到点自动结算并让座。
	if r.pid != nil {
		ctx.ScheduleOnce(HoldLimit, r.pid, &timeoutMsg{SeatIndex: msg.SeatIndex, OccupiedAt: now})
	}

	r.logger.Info("seat occupied", "tier", r.tier.String(), "room", r.index,
		"seat", msg.SeatIndex, "player_id", msg.PlayerID, "occupy_id", s.OccupyID)
	r.notifyChange()
}

// onLeave 主动离开：只有当前占用者本人可以释放自己的座位。
func (r *Room) onLeave(msg *LeaveMsg) {
	if msg.SeatIndex < 0 || msg.SeatIndex >= SeatsPerRoom {
		return
	}
	s := r.seats[msg.SeatIndex]
	if s.State != SeatOccupied || s.Occupant != msg.PlayerID {
		return // fail-closed：不是你的座位，拒绝
	}
	r.release(s, time.Now())
	r.notifyChange()
}

// onRestore 重启恢复：回填占用状态，已到期立即结算，未到期重挂定时器。
func (r *Room) onRestore(ctx *actor.Context, msg *RestoreMsg) {
	if len(msg.Slots) == 0 {
		return
	}
	now := time.Now()
	settled := 0
	for _, rs := range msg.Slots {
		if rs.SeatIndex < 0 || rs.SeatIndex >= SeatsPerRoom || rs.Occupant <= 0 {
			continue
		}
		s := r.seats[rs.SeatIndex]
		s.State = SeatOccupied
		s.Occupant = rs.Occupant
		s.OccupyID = rs.OccupyID
		if s.OccupyID <= 0 {
			// 兜底：库里缺 ID（历史数据/人工插入）时补一个。
			// 否则结算幂等键会是 0，同一玩家所有奖励共用一个 BizID，只发得出第一笔。
			s.OccupyID = nextSeq()
		}
		s.Node = rs.Node
		s.OccupiedAt = rs.OccupiedAt

		deadline := rs.OccupiedAt.Add(HoldLimit)
		if !deadline.After(now) {
			// 重启期间已经到期：定时器早已丢失，必须在这里补结算，
			// 否则这个座位会永久占住且不再受 1h 上限约束。
			r.release(s, now)
			settled++
			continue
		}
		if r.pid != nil {
			ctx.ScheduleOnce(deadline.Sub(now), r.pid, &timeoutMsg{
				SeatIndex:  rs.SeatIndex,
				OccupiedAt: rs.OccupiedAt,
			})
		}
	}
	r.logger.Info("seat room restored", "tier", r.tier.String(), "room", r.index,
		"slots", len(msg.Slots), "settled_on_restore", settled)
	r.notifyChange()
}

// onTimeout 超时让座：只处理「同一 OccupiedAt」的那次占用。
func (r *Room) onTimeout(msg *timeoutMsg) {
	if msg.SeatIndex < 0 || msg.SeatIndex >= SeatsPerRoom {
		return
	}
	s := r.seats[msg.SeatIndex]
	if s.State != SeatOccupied {
		return
	}
	if !s.OccupiedAt.Equal(msg.OccupiedAt) {
		return // 旧定时器：该座位已被释放/重新占用，这条到期消息作废
	}
	r.release(s, time.Now())
	r.notifyChange()
}

// notifyChange 状态变更回调（在 Actor 内取本房间快照，无竞态）。
func (r *Room) notifyChange() {
	if r.onChange == nil {
		return
	}
	r.onChange(r.Snapshot())
}

// release 结算并清空座位。调用方保证 s 当前处于占用态。
func (r *Room) release(s *Seat, now time.Time) {
	mins := OccupancyMinutes(s.OccupiedAt, now)
	ev := ReleaseEvent{
		PlayerID:  s.Occupant,
		Tier:      r.tier,
		RoomIndex: r.index,
		SeatIndex: s.Index,
		OccupyID:  s.OccupyID,
		Minutes:   mins,
		Node:      s.Node,
	}
	if mins > 0 && ev.PlayerID > 0 {
		ev.Amount = RewardOf(r.tier, mins)
	}

	s.State = SeatEmpty
	s.Occupant = 0
	s.OccupyID = 0
	s.Node = ""
	s.OccupiedAt = time.Time{}

	if ev.PlayerID > 0 && r.onRelease != nil {
		r.onRelease(ev)
	}
	if ev.Amount > 0 {
		r.logger.Info("seat released", "tier", r.tier.String(), "room", r.index,
			"seat", ev.SeatIndex, "player_id", ev.PlayerID,
			"minutes", ev.Minutes, "amount", ev.Amount, "occupy_id", ev.OccupyID)
	}
}

// Snapshot 返回房间只读快照（查询协议与落库的数据源）。
//
// 由组装层在 Actor 之外直接调用，读到的是「近似值」（可能与 Actor 内实时
// 状态差一拍）。查询路径实际读的是组装层缓存，不会走到这里；落库用的是
// Actor 内生成并传出的快照，因此不存在并发读。
func (r *Room) Snapshot() RoomSnapshot {
	out := RoomSnapshot{
		Tier:      r.tier,
		RoomIndex: r.index,
		Seats:     make([]SeatView, 0, SeatsPerRoom),
	}
	now := time.Now()
	for _, s := range r.seats {
		var remain, occupiedAt int64
		if s.State == SeatOccupied {
			occupiedAt = s.OccupiedAt.Unix()
			if d := HoldLimit - now.Sub(s.OccupiedAt); d > 0 {
				remain = int64(d / time.Second)
			}
		}
		out.Seats = append(out.Seats, SeatView{
			Index:      s.Index,
			State:      s.State,
			Occupant:   s.Occupant,
			OccupyID:   s.OccupyID,
			OccupiedAt: occupiedAt,
			RemainSec:  remain,
		})
	}
	return out
}
