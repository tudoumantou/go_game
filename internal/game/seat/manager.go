// internal/game/seat/manager.go —— 房间管理器
//
// 管理两样东西：
//   rooms  房间（每个房间管 10 个座位）
//   seats  玩家参与数据（player_id → 占着哪个房间的哪个座），O(1) 查
//
// 结算不自己发奖：只把 Release 事件回调给组装层（onSettle），由它决定
// 在线直投还是塞进离线事件队列。本包因此不依赖 player / presence。
//
// 房间快照由 Room 自持（atomic.Value），管理器不缓存副本，也没有锁
// 保护房间——只有 seats 用 sync.Map（多个房间 Actor 会并发写它）。
package seat

import (
	"context"
	"sync"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"
	pkgseat "game_study/pkg/seat"
)

// Manager 房间管理器。
type Manager struct {
	sys      *actor.System
	store    *Store
	logger   *log.Logger
	onSettle func(pkgseat.Release) // 结算回调（在线投递 / 离线入队由组装层决定）

	rooms map[int32]*pkgseat.Room // room_id → 房间
	seats sync.Map                // player_id → SeatRef（玩家参与数据）
}

// NewManager 创建管理器。
//
// store 可为 nil（无 MySQL 环境退化为纯内存）；onSettle 可为 nil（不投递奖励）。
func NewManager(sys *actor.System, store *Store, logger *log.Logger, onSettle func(pkgseat.Release)) *Manager {
	m := &Manager{
		sys:      sys,
		store:    store,
		logger:   logger,
		onSettle: onSettle,
		rooms:    make(map[int32]*pkgseat.Room),
	}

	saved := m.load() // 先把上次的占用读出来，房间收到 RestoreMsg 才能立即结算

	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			room := pkgseat.NewRoom(tier, i, logger)
			id := room.ID()

			room.SetOnChange(m.onChange)
			room.SetOnRelease(m.onRelease)
			room.SetOnSeat(func(playerID int64, seatIndex int, occupied bool, at time.Time) {
				m.onSeat(id, playerID, seatIndex, occupied, at)
			})

			pid, err := sys.Spawn(pkgseat.RoomName(id), room)
			if err != nil {
				logger.Error("spawn seat room failed", "room_id", id, "err", err)
				continue
			}
			room.SetPID(pid)
			m.rooms[id] = room

			if slots := saved[id]; len(slots) > 0 {
				if err := sys.Send(pid, &pkgseat.RestoreMsg{Slots: slots}); err != nil {
					logger.Warn("seat restore failed", "room_id", id, "err", err)
				}
			}
		}
	}
	return m
}

// load 读取上次落库的房间占用，并据此重建玩家数据。
//
// 玩家数据以房间快照为准（座位状态只有一份真相在房间里）：即使 seat_player
// 表因写入失败而落后，启动时也会被纠正过来。
// 读失败只降级为空状态启动：座位是玩法设施，宁可丢占用也不能让起服失败。
func (m *Manager) load() map[int32][]pkgseat.Slot {
	out := make(map[int32][]pkgseat.Slot)
	if m.store == nil {
		return out
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	empty := make([]pkgseat.RoomSnapshot, 0, 16)
	for _, id := range m.roomIDs() {
		tier, index := pkgseat.SplitRoomID(id)
		empty = append(empty, pkgseat.RoomSnapshot{ID: id, Tier: tier, Index: index})
	}
	if err := m.store.EnsureRooms(ctx, empty); err != nil {
		m.logger.Warn("seat ensure rooms failed", "err", err)
	}

	snaps, err := m.store.Load(ctx)
	if err != nil {
		m.logger.Error("seat load failed, start empty", "err", err)
		return out
	}
	occupied := 0
	for _, snap := range snaps {
		var slots []pkgseat.Slot
		for _, sl := range snap.Slots {
			if sl.State != pkgseat.SeatOccupied || sl.Occupant <= 0 || sl.OccupiedAt <= 0 {
				continue
			}
			slots = append(slots, sl)
			m.seats.Store(sl.Occupant, pkgseat.SeatRef{RoomID: snap.ID, SeatIndex: sl.Index})
			occupied++
		}
		if len(slots) > 0 {
			out[snap.ID] = slots
		}
	}
	if len(snaps) > 0 {
		m.logger.Info("seat state loaded", "rooms", len(snaps), "occupied", occupied)
	}
	return out
}

// onChange 房间状态变更：整房落库。
func (m *Manager) onChange(snap pkgseat.RoomSnapshot) {
	if m.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.store.Save(ctx, snap); err != nil {
		m.logger.Warn("seat room save failed", "room_id", snap.ID, "err", err)
	}
}

// onSeat 座位归属变化：更新玩家数据（内存 + seat_player 表）。
func (m *Manager) onSeat(roomID int32, playerID int64, seatIndex int, occupied bool, at time.Time) {
	if playerID <= 0 {
		return
	}
	ref := pkgseat.SeatRef{RoomID: roomID, SeatIndex: seatIndex}
	if occupied {
		m.seats.Store(playerID, ref)
	} else {
		// 比对后再删：换座时「旧座释放」晚于「新座写入」，直接删会把新座位抹掉。
		m.seats.CompareAndDelete(playerID, ref)
	}

	if m.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if occupied {
		if err := m.store.SavePlayer(ctx, playerID, ref, at.Unix()); err != nil {
			m.logger.Warn("seat player save failed", "player_id", playerID, "err", err)
		}
		return
	}
	if err := m.store.DeletePlayer(ctx, playerID, roomID); err != nil {
		m.logger.Warn("seat player delete failed", "player_id", playerID, "err", err)
	}
}

// onRelease 座位释放：把奖励事件交给组装层。
func (m *Manager) onRelease(rel pkgseat.Release) {
	if rel.PlayerID <= 0 || rel.Amount <= 0 || m.onSettle == nil {
		return
	}
	m.onSettle(rel)
}

// Occupy 占座/抢夺；玩家已在别处有座则先释放旧座（自动换座）。
func (m *Manager) Occupy(roomID int32, seatIndex int, playerID int64) bool {
	room := m.rooms[roomID]
	if room == nil || playerID == 0 || seatIndex < 0 || seatIndex >= pkgseat.SeatsPerRoom {
		return false
	}
	target := pkgseat.SeatRef{RoomID: roomID, SeatIndex: seatIndex}
	if old, ok := m.Seat(playerID); ok && old != target {
		if oldRoom := m.rooms[old.RoomID]; oldRoom != nil {
			_ = m.sys.Send(oldRoom.PID(), &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: old.SeatIndex})
		}
	}
	return m.sys.Send(room.PID(), &pkgseat.OccupyMsg{PlayerID: playerID, SeatIndex: seatIndex}) == nil
}

// Leave 主动离开。
func (m *Manager) Leave(roomID int32, seatIndex int, playerID int64) bool {
	room := m.rooms[roomID]
	if room == nil || playerID == 0 || seatIndex < 0 || seatIndex >= pkgseat.SeatsPerRoom {
		return false
	}
	return m.sys.Send(room.PID(), &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: seatIndex}) == nil
}

// Seat 查玩家当前坐哪（玩家参与数据，O(1)）。
func (m *Manager) Seat(playerID int64) (pkgseat.SeatRef, bool) {
	if v, ok := m.seats.Load(playerID); ok {
		return v.(pkgseat.SeatRef), true
	}
	return pkgseat.SeatRef{}, false
}

// roomIDs 全部房间 ID（按档位/房号顺序，保证遍历结果稳定）。
func (m *Manager) roomIDs() []int32 {
	ids := make([]int32, 0, 16)
	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			ids = append(ids, pkgseat.RoomID(tier, i))
		}
	}
	return ids
}

// Snapshot 单间房快照。
func (m *Manager) Snapshot(roomID int32) (pkgseat.RoomSnapshot, bool) {
	room := m.rooms[roomID]
	if room == nil {
		return pkgseat.RoomSnapshot{}, false
	}
	return room.Snapshot(), true
}

// AllSnapshots 全部房间快照。
func (m *Manager) AllSnapshots() []pkgseat.RoomSnapshot {
	out := make([]pkgseat.RoomSnapshot, 0, len(m.rooms))
	for _, id := range m.roomIDs() {
		if room := m.rooms[id]; room != nil {
			out = append(out, room.Snapshot())
		}
	}
	return out
}

// Count 房间总数。
func (m *Manager) Count() int { return len(m.rooms) }
