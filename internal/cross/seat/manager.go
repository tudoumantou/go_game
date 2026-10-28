// internal/cross/seat/manager.go —— 跨服座位管理器（指定 cross 节点）
//
// 架构职责：在跨服节点上拉起「高 1 / 中 3 / 低 6」共 10 间常驻房间 Actor，
// 维护 房间名 → PID 索引；房间释放时经 Sink 把奖励帧精确推回玩家所在
// 游戏服节点；房间状态变化时把全量快照广播给所有游戏服（本地缓存后查询）。
//
// 为什么集中在一个 cross 节点：跨服的意义是不同游戏服的玩家抢同一批座位。
// 若每个 cross 节点各建一套房间，各游戏服看到的是各自的座位，就退化成
// 「单服」。因此所有游戏服统一把座位流量发到配置指定的那个 cross 节点
// （cross.seat_addr），房间只在那里存在。
//
// 与 internal/game/seat 的关系：房间状态机完全复用 pkg/seat.Room，差异只在
// 奖励发射器与状态广播——单服发 GrantItemsMsg，跨服发 CrossFrame_SeatReward
// 与 CrossFrame_SeatState。
package seat

import (
	"sync"

	"game_study/pkg/actor"
	"game_study/pkg/log"
	pkgseat "game_study/pkg/seat"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Sink 房间下行能力（由 cross/rpc.Server 实现）。
type Sink interface {
	// SendReward 把座位奖励推给玩家所在游戏服节点。
	SendReward(nodeID string, playerID int64, tier pkgseat.Tier, minutes, amount, seq int64) error
	// BroadcastState 把全量座位快照广播给所有已连接游戏服。
	BroadcastState(st *internalv2.SeatState) error
}

// Manager 跨服座位管理器。
type Manager struct {
	sys    *actor.System
	sink   Sink
	logger *log.Logger

	pids  map[string]*actor.PID
	order []string

	// occ 一人一座登记：房间在 cross 进程，登记也必须放在这里——
	// 游戏服侧无法跨进程裁决唯一性。
	occ *pkgseat.Occupancy

	// mu 保护 snaps：房间 Actor 在自己的 goroutine 里写，RPC 线程读。
	mu    sync.RWMutex
	snaps map[string]pkgseat.RoomSnapshot
}

// NewManager 创建管理器并拉起全部固定房间（actor 系统必须已 Start）。
func NewManager(sys *actor.System, sink Sink, logger *log.Logger) *Manager {
	m := &Manager{
		sys:    sys,
		sink:   sink,
		logger: logger,
		pids:   make(map[string]*actor.PID),
		snaps:  make(map[string]pkgseat.RoomSnapshot),
		occ:    pkgseat.NewOccupancy(),
	}

	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			name := pkgseat.RoomName(tier, i)
			room := pkgseat.NewRoom(tier, i, logger)
			room.SetOnChange(m.onChange)
			room.SetOnRelease(m.onRelease)
			pid, err := sys.Spawn(name, room)
			if err != nil {
				logger.Error("spawn cross seat room failed", "name", name, "err", err)
				continue
			}
			room.SetPID(pid)
			m.pids[name] = pid
			m.order = append(m.order, name)
			// 启动期无并发操作：直接取初始快照，供新连上的游戏服首帧同步。
			m.snaps[name] = room.Snapshot()
		}
	}
	logger.Info("cross seat rooms initialized", "rooms", len(m.order))
	return m
}

// onChange 房间状态变更：更新本地快照并广播全量状态。
//
// 由房间 Actor 在串行上下文中调用，传入的快照已由 Actor 生成（无竞态）。
// 广播走 peer 的非阻塞发送（队列满丢帧），不会长时间卡住房间 Actor。
func (m *Manager) onChange(snap pkgseat.RoomSnapshot) {
	name := pkgseat.RoomName(snap.Tier, snap.RoomIndex)
	m.mu.Lock()
	m.snaps[name] = snap
	m.mu.Unlock()

	if m.sink == nil {
		return
	}
	if err := m.sink.BroadcastState(m.BuildState()); err != nil {
		m.logger.Warn("seat state broadcast failed", "room", name, "err", err)
	}
}

// BuildState 组装全量座位快照（按稳定顺序）。
func (m *Manager) BuildState() *internalv2.SeatState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := &internalv2.SeatState{Rooms: make([]*internalv2.SeatRoomState, 0, len(m.order))}
	for _, name := range m.order {
		snap, ok := m.snaps[name]
		if !ok {
			continue
		}
		room := &internalv2.SeatRoomState{
			Tier:      int32(snap.Tier),
			RoomIndex: int32(snap.RoomIndex),
			Seats:     make([]*internalv2.SeatSlotState, 0, len(snap.Seats)),
		}
		for _, sv := range snap.Seats {
			room.Seats = append(room.Seats, &internalv2.SeatSlotState{
				Index:      int32(sv.Index),
				State:      int32(sv.State),
				Occupant:   sv.Occupant,
				RemainSec:  sv.RemainSec,
				OccupiedAt: sv.OccupiedAt,
			})
		}
		out.Rooms = append(out.Rooms, room)
	}
	return out
}

// Occupy 占用/抢夺（由 rpc 层按 BIND 的 nodeID 调用）。
//
// 一人一座：玩家已在别处有座时先释放旧座（照常结算奖励）再占新座，
// 即「自动换座」——避免单人占满 100 座按座叠加奖励。
func (m *Manager) Occupy(tier pkgseat.Tier, roomIndex, seatIndex int, playerID int64, node string) {
	if playerID == 0 || !pkgseat.ValidSeat(tier, roomIndex, seatIndex) {
		return
	}
	pid := m.pids[pkgseat.RoomName(tier, roomIndex)]
	if pid == nil {
		return
	}
	ref := pkgseat.SeatRef{Tier: tier, RoomIndex: roomIndex, SeatIndex: seatIndex}
	// 先登记再投递：旧座释放回调晚于此处到达，靠 ref 比对不会误删新登记。
	if old, had := m.occ.Claim(playerID, ref); had {
		m.sendLeave(old, playerID)
	}
	_ = m.sys.Send(pid, &pkgseat.OccupyMsg{
		PlayerID:  playerID,
		SeatIndex: seatIndex,
		Node:      node,
	})
}

// Leave 主动离开。
func (m *Manager) Leave(tier pkgseat.Tier, roomIndex, seatIndex int, playerID int64) {
	if playerID == 0 || !pkgseat.ValidSeat(tier, roomIndex, seatIndex) {
		return
	}
	pid := m.pids[pkgseat.RoomName(tier, roomIndex)]
	if pid == nil {
		return
	}
	_ = m.sys.Send(pid, &pkgseat.LeaveMsg{
		PlayerID:  playerID,
		SeatIndex: seatIndex,
	})
}

// sendLeave 向指定座位投递离开消息（换座时释放旧座用）。
func (m *Manager) sendLeave(ref pkgseat.SeatRef, playerID int64) {
	pid := m.pids[pkgseat.RoomName(ref.Tier, ref.RoomIndex)]
	if pid == nil {
		return
	}
	_ = m.sys.Send(pid, &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: ref.SeatIndex})
}

// onRelease 座位释放：清一人一座登记，并把奖励推回玩家所在游戏服。
//
// 跨服不做本地落库（本轮只做单服持久化）：奖励靠「游戏服收不到就写 outbox」
// 兜底，座位状态本身在 cross 节点重启后清空，玩家重抢即可。
func (m *Manager) onRelease(ev pkgseat.ReleaseEvent) {
	if ev.PlayerID <= 0 {
		return
	}
	m.occ.Release(ev.PlayerID, pkgseat.SeatRef{
		Tier: ev.Tier, RoomIndex: ev.RoomIndex, SeatIndex: ev.SeatIndex,
	})
	if ev.Amount <= 0 {
		return
	}
	if m.sink == nil {
		return
	}
	if ev.Node == "" {
		m.logger.Warn("seat reward without node, dropped",
			"player_id", ev.PlayerID, "occupy_id", ev.OccupyID)
		return
	}
	if err := m.sink.SendReward(ev.Node, ev.PlayerID, ev.Tier, ev.Minutes, ev.Amount, ev.OccupyID); err != nil {
		m.logger.Warn("seat reward push failed",
			"player_id", ev.PlayerID, "node", ev.Node, "occupy_id", ev.OccupyID, "err", err)
	}
}

// CurrentSeat 查询玩家当前占用的座位（调试/指标用）。
func (m *Manager) CurrentSeat(playerID int64) (pkgseat.SeatRef, bool) {
	return m.occ.Get(playerID)
}

// Count 房间总数（指标/日志用）。
func (m *Manager) Count() int { return len(m.order) }
