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
	SendReward(nodeID string, playerID int64, tier string, minutes, amount, seq int64) error
	// BroadcastState 把全量座位快照广播给所有已连接游戏服。
	BroadcastState(st *internalv2.SeatState) error
}

// Manager 跨服座位管理器。
type Manager struct {
	sys    *actor.System
	sink   Sink
	logger *log.Logger

	pids  map[string]*actor.PID
	rooms map[string]*pkgseat.Room
	order []string

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
		rooms:  make(map[string]*pkgseat.Room),
		snaps:  make(map[string]pkgseat.RoomSnapshot),
	}

	emit := func(rw pkgseat.Reward) {
		if rw.Node == "" {
			logger.Warn("seat reward without node, dropped",
				"player_id", rw.PlayerID, "seq", rw.Seq)
			return
		}
		if err := sink.SendReward(rw.Node, rw.PlayerID, rw.Tier, rw.Minutes, rw.Amount, rw.Seq); err != nil {
			logger.Warn("seat reward push failed",
				"player_id", rw.PlayerID, "node", rw.Node, "seq", rw.Seq, "err", err)
		}
	}

	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			name := pkgseat.RoomName(tier, i)
			room := pkgseat.NewRoom(tier, i, emit, logger)
			room.SetOnChange(m.onChange)
			pid, err := sys.Spawn(name, room)
			if err != nil {
				logger.Error("spawn cross seat room failed", "name", name, "err", err)
				continue
			}
			room.SetPID(pid)
			m.pids[name] = pid
			m.rooms[name] = room
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
			Tier:      snap.Tier,
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
func (m *Manager) Occupy(tier string, roomIndex, seatIndex int, playerID int64, node string) {
	pid := m.pids[pkgseat.RoomName(tier, roomIndex)]
	if pid == nil || playerID == 0 {
		return
	}
	_ = m.sys.Send(pid, &pkgseat.OccupyMsg{
		PlayerID:  playerID,
		SeatIndex: seatIndex,
		Node:      node,
	})
}

// Leave 主动离开。
func (m *Manager) Leave(tier string, roomIndex, seatIndex int, playerID int64) {
	pid := m.pids[pkgseat.RoomName(tier, roomIndex)]
	if pid == nil || playerID == 0 {
		return
	}
	_ = m.sys.Send(pid, &pkgseat.LeaveMsg{
		PlayerID:  playerID,
		SeatIndex: seatIndex,
	})
}

// Count 房间总数（指标/日志用）。
func (m *Manager) Count() int { return len(m.order) }
