// internal/game/seat/manager.go —— 房间管理器
//
// 管理两样东西：
//
//	rooms  房间（每个房间管 10 个座位）
//	seats  玩家参与数据（player_id → 占着哪个房间的哪个座），O(1) 查
//
// 结算不自己发奖：只把 Release 事件回调给组装层（onSettle），由它决定
// 在线直投还是塞进离线事件队列。本包因此不依赖 player / presence。
//
// 落库不在房间 Actor 里做：变更只登记脏标记，由 writer 合并写（见
// flush.go）。房间 Actor 因此零 IO，DB 抖动不会反压到抢座消息。
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
	store    *Store // 启动时一次性读库（恢复用）
	logger   *log.Logger
	onSettle func(pkgseat.Release) // 结算回调（在线投递 / 离线入队由组装层决定）

	rooms map[int32]*pkgseat.Room // room_id → 房间
	seats sync.Map                // player_id → SeatRef（玩家参与数据）

	w *writer // 合并落库（store 为 nil 时为 nil，纯内存运行）
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

	// writer 先建好（暂不启动）：恢复过程若有状态变化（已到期结算、
	// 补齐 occupy_id），脏标记要能落到队列里，等首轮 tick 一起写出。
	if store != nil {
		m.w = newWriter(store, logger, func(roomID int32) (pkgseat.RoomSnapshot, bool) {
			room := m.rooms[roomID]
			if room == nil {
				return pkgseat.RoomSnapshot{}, false
			}
			return room.Snapshot(), true
		})
	}

	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			room := pkgseat.NewRoom(tier, i, logger)
			id := room.ID()

			room.SetOnChange(m.onChange)
			room.SetOnRelease(m.onRelease)
			room.SetOnSeat(func(playerID int64, seatIndex int, occupied bool, at time.Time) {
				m.onSeat(id, playerID, seatIndex, occupied, at)
			})

			name := pkgseat.RoomName(id)
			if _, err := sys.Spawn(name, room); err != nil {
				logger.Error("spawn seat room failed", "room_id", id, "err", err)
				continue
			}
			m.rooms[id] = room

			if slots := saved[id]; len(slots) > 0 {
				if err := sys.DeliverRemote(name, &pkgseat.RestoreMsg{Slots: slots}); err != nil {
					logger.Warn("seat restore failed", "room_id", id, "err", err)
				}
			}
		}
	}
	if m.w != nil {
		m.w.start()
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

// onChange 房间状态变更：只登记脏标记，落库由 writer 合并（不阻塞房间 Actor）。
func (m *Manager) onChange(snap pkgseat.RoomSnapshot) {
	if m.w == nil {
		return
	}
	m.w.markRoom(snap.ID)
}

// onSeat 座位归属变化：内存索引立即生效，落库走脏标记。
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

	if m.w == nil {
		return
	}
	var atUnix int64
	if occupied {
		atUnix = at.Unix()
	}
	m.w.markPlayer(playerID, ref, atUnix, occupied)
}

// onRelease 座位释放：把奖励事件交给组装层。
func (m *Manager) onRelease(rel pkgseat.Release) {
	if rel.PlayerID <= 0 || rel.Amount <= 0 || m.onSettle == nil {
		return
	}
	m.onSettle(rel)
}

// Occupy 占座/抢夺；玩家已在别处有座则先占新座、再释放旧座（自动换座）。
//
// 投递走 DeliverRemote（按 actorID 投递、sender 为 nil）：房间消息不需要
// Respond，因此不必为了发消息而保存一份 PID。
func (m *Manager) Occupy(roomID int32, seatIndex int, playerID int64) bool {
	return m.switchSeat(roomID, seatIndex, playerID, m.deliver)
}

// deliver 向房间 Actor 投递消息。
func (m *Manager) deliver(roomID int32, msg any) error {
	return m.sys.DeliverRemote(pkgseat.RoomName(roomID), msg)
}

// switchSeat 占座 + 换座。deliver 作为参数传入，便于单测构造投递失败。
//
// 顺序必须是「先占新、再放旧」：反过来一旦新座投递失败（邮箱满 / 房间
// 已摘），旧座已经释放，玩家两头空。先占新则失败时什么都没变。
func (m *Manager) switchSeat(roomID int32, seatIndex int, playerID int64,
	deliver func(roomID int32, msg any) error) bool {
	if m.rooms[roomID] == nil || playerID == 0 ||
		seatIndex < 0 || seatIndex >= pkgseat.SeatsPerRoom {
		return false
	}
	// 旧坐标必须在投递前读：新房间可能在我们读之前就处理完 Occupy，
	// 那时 seats 已指向新座，再读会漏掉旧座释放。
	old, hadOld := m.Seat(playerID)
	target := pkgseat.SeatRef{RoomID: roomID, SeatIndex: seatIndex}

	if err := deliver(roomID, &pkgseat.OccupyMsg{PlayerID: playerID, SeatIndex: seatIndex}); err != nil {
		return false
	}
	if hadOld && old != target && m.rooms[old.RoomID] != nil {
		if err := deliver(old.RoomID, &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: old.SeatIndex}); err != nil {
			// 旧座释放失败：玩家会短暂占两席，旧座在 ≤HoldLimit 后自然超时
			// 让座（奖励照发）；索引不会被误删——onSeat(false) 用 ref 比对。
			m.logger.Warn("seat release on switch failed", "player_id", playerID,
				"old_room_id", old.RoomID, "err", err)
		}
	}
	return true
}

// Leave 主动离开。
func (m *Manager) Leave(roomID int32, seatIndex int, playerID int64) bool {
	if m.rooms[roomID] == nil || playerID == 0 ||
		seatIndex < 0 || seatIndex >= pkgseat.SeatsPerRoom {
		return false
	}
	return m.deliver(roomID, &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: seatIndex}) == nil
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

// Stop 停掉合并落库 worker 并做最后一次写库。
//
// 关服顺序：actorSys 必须已 Drain（不会再产生新标记）后才调用。
func (m *Manager) Stop() error {
	if m.w == nil {
		return nil
	}
	return m.w.stop()
}
