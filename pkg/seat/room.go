// pkg/seat/room.go —— 房间 Actor（单服/跨服共用的座位状态机）
//
// 并发模型：房间状态全部在 Actor 内，Receive 由分片 goroutine 串行调用，
// 业务侧无锁。跨 Actor 只通过消息：玩家侧发 Occupy/Leave，超时由
// ScheduleOnce 投递 timeoutMsg。
//
// 与 battle.Room 的差异：战斗房随结算销毁，座位房是常驻设施（进程生命周期内
// 一直存在），因此没有 RoomClosed 回调，也不做「结算后拒绝入内」。
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
	tier   Tier
	index  int
	seats  [SeatsPerRoom]*Seat
	emit   func(Reward)
	// onChange 状态变更回调（跨服用它把房间快照广播给游戏服；单服可为 nil）。
	// 在 Actor 内调用并传入本房间快照——快照在 Actor 串行上下文中生成，无竞态。
	onChange func(RoomSnapshot)
	// onRelease 座位释放回调：无论是否产生奖励都会调用。
	//
	// 管理器用它清理「一人一座」登记——不足 1 分钟没奖也必须清，
	// 否则该玩家会永远被判定为「已有座位」，再也占不了新座。
	onRelease func(playerID int64, ref SeatRef)
	logger    *log.Logger
	pid       *actor.PID
}

// NewRoom 创建房间 Actor（尚未 Spawn）。
func NewRoom(tier Tier, index int, emit func(Reward), logger *log.Logger) *Room {
	r := &Room{
		tier:   tier,
		index:  index,
		emit:   emit,
		logger: logger,
	}
	for i := 0; i < SeatsPerRoom; i++ {
		r.seats[i] = &Seat{Index: i}
	}
	return r
}

// SetPID 注入自身 PID（Spawn 之后调用，ScheduleOnce 需要它）。
func (r *Room) SetPID(pid *actor.PID) { r.pid = pid }

// SetOnChange 注入状态变更回调（跨服广播用；须在 Spawn 前设置）。
func (r *Room) SetOnChange(fn func(RoomSnapshot)) { r.onChange = fn }

// SetOnRelease 注入座位释放回调（须在 Spawn 前设置）。
func (r *Room) SetOnRelease(fn func(playerID int64, ref SeatRef)) { r.onRelease = fn }

// Tier 房间档位。
func (r *Room) Tier() Tier { return r.tier }

// Receive 实现 actor.Actor。
func (r *Room) Receive(ctx *actor.Context) {
	switch msg := ctx.Message().(type) {
	case *OccupyMsg:
		r.onOccupy(ctx, msg)
	case *LeaveMsg:
		r.onLeave(msg)
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
	s.Node = msg.Node
	s.OccupiedAt = now

	// 1h 硬上限：到点自动结算并让座。
	if r.pid != nil {
		ctx.ScheduleOnce(HoldLimit, r.pid, &timeoutMsg{SeatIndex: msg.SeatIndex, OccupiedAt: now})
	}

	r.logger.Info("seat occupied", "tier", r.tier.String(), "room", r.index,
		"seat", msg.SeatIndex, "player_id", msg.PlayerID, "node", msg.Node)
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
	occupant := s.Occupant
	node := s.Node
	seq := nextSeq()
	ref := SeatRef{Tier: r.tier, RoomIndex: r.index, SeatIndex: s.Index}

	s.State = SeatEmpty
	s.Occupant = 0
	s.Node = ""
	s.OccupiedAt = time.Time{}

	// 无条件通知管理器清登记：不足 1 分钟不发奖，但座位确实空了，
	// 不清理会让该玩家被永久判定为「已有座位」。
	if occupant > 0 && r.onRelease != nil {
		r.onRelease(occupant, ref)
	}

	if mins <= 0 || occupant <= 0 {
		return // 不足 1 分钟不发奖（但座位照常释放）
	}
	amount := RewardOf(r.tier, mins)
	if amount <= 0 || r.emit == nil {
		return
	}
	r.emit(Reward{
		PlayerID: occupant,
		Tier:     r.tier,
		Minutes:  mins,
		Amount:   amount,
		Seq:      seq,
		Node:     node,
	})
	r.logger.Info("seat released", "tier", r.tier.String(), "room", r.index,
		"seat", s.Index, "player_id", occupant, "minutes", mins, "amount", amount)
}

// Snapshot 返回房间只读快照（查询协议用）。
//
// 由管理器/查询路径在 Actor 之外直接调用，读到的是「近似值」（可能与 Actor
// 内实时状态差一拍）。与 battle.Manager.Stats 同一取舍：查询是低频只读路径，
// 为它引入「请求-响应」消息对不值当，可接受近似。
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
			OccupiedAt: occupiedAt,
			RemainSec:  remain,
		})
	}
	return out
}
