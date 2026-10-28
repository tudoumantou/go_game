// internal/game/seat/manager.go —— 房间管理器
//
// 只做三件事：拉起 10 个房间、把请求转发给对应房间、把房间的回调落到库/发奖。
//
// 管理器不存状态副本：
//   - 房间快照由 Room 自己用 atomic.Value 持有，查询直接读房间；
//   - 「玩家占哪」也不另建登记表，占座前扫一遍房间快照即可（10 房 × 10 座）；
//   - 因此没有锁、没有缓存、没有遍历顺序表。
//
// 唯一的并发前提：玩家的占座请求由玩家 Actor 串行发起，同一玩家不会同时
// 发出两个占座；跨玩家冲突由房间 Actor 串行裁决（后到者视为抢夺）。
package seat

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	pkgseat "game_study/pkg/seat"

	"google.golang.org/protobuf/proto"
)

// Manager 房间管理器。
type Manager struct {
	sys     *actor.System
	players *player.Manager
	store   *Store
	pending *PendingStore
	logger  *log.Logger
	rooms   map[int32]*pkgseat.Room // room_id → 房间
}

// NewManager 创建管理器；store/pending 可为 nil（无 MySQL 环境退化为纯内存）。
func NewManager(sys *actor.System, players *player.Manager, store *Store, pending *PendingStore, logger *log.Logger) *Manager {
	m := &Manager{
		sys:     sys,
		players: players,
		store:   store,
		pending: pending,
		logger:  logger,
		rooms:   make(map[int32]*pkgseat.Room),
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

// load 读取上次落库的占用（按 room_id 分组）。
//
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
			if sl.State == pkgseat.SeatOccupied && sl.Occupant > 0 && sl.OccupiedAt > 0 {
				slots = append(slots, sl)
				occupied++
			}
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

// onSeat 座位归属变化：同步玩家数据（seat_player）。
func (m *Manager) onSeat(roomID int32, playerID int64, seatIndex int, occupied bool, at time.Time) {
	if m.store == nil || playerID <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if occupied {
		ref := pkgseat.SeatRef{RoomID: roomID, SeatIndex: seatIndex}
		if err := m.store.SavePlayer(ctx, playerID, ref, at.Unix()); err != nil {
			m.logger.Warn("seat player save failed", "player_id", playerID, "err", err)
		}
		return
	}
	if err := m.store.DeletePlayer(ctx, playerID, roomID); err != nil {
		m.logger.Warn("seat player delete failed", "player_id", playerID, "err", err)
	}
}

// onRelease 座位释放：有奖则结算。
func (m *Manager) onRelease(rel pkgseat.Release) {
	if rel.PlayerID > 0 && rel.Amount > 0 {
		m.settle(rel)
	}
}

// settle 发奖：在线直发，离线/投递失败写 outbox 等登录补发。
//
// 不需要「先写 outbox 再发奖」：occupy_id 在占座时就随房间快照落库了，
// 重启后重放结算拿到的仍是同一个 ID，BizID 幂等天然挡住重复发放。
func (m *Manager) settle(rel pkgseat.Release) {
	if m.players != nil {
		if pid, ok := m.players.Get(rel.PlayerID); ok {
			err := m.sys.Send(pid, &player.GrantItemsMsg{
				BizID:  fmt.Sprintf("%d", rel.OccupyID),
				Source: "seat",
				Items:  []player.GrantItem{{ItemID: pkgseat.RewardItem, Count: rel.Amount}},
			})
			if err == nil {
				if raw, e := proto.Marshal(&gamev1.SeatRewardNotify{
					Tier: int32(rel.Tier), Minutes: rel.Minutes, Reward: rel.Amount,
				}); e == nil {
					_ = m.sys.Send(pid, &player.PushMsg{MsgID: codec.MsgSeatRewardNotify, Body: raw})
				}
				return
			}
			m.logger.Warn("seat reward send failed, fallback to outbox",
				"player_id", rel.PlayerID, "err", err)
		}
	}

	if m.pending == nil {
		m.logger.Warn("seat reward dropped: offline and no outbox",
			"player_id", rel.PlayerID, "occupy_id", rel.OccupyID)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := m.pending.Add(ctx, rel.PlayerID, rel.OccupyID, rel.Tier, rel.Minutes); err != nil {
		m.logger.Warn("seat outbox write failed", "player_id", rel.PlayerID, "err", err)
	}
}

// PendReward 供跨服路径写入待补发奖励。
func (m *Manager) PendReward(ctx context.Context, playerID, occupyID int64, tier pkgseat.Tier, minutes int64) error {
	if m.pending == nil {
		return nil
	}
	return m.pending.Add(ctx, playerID, occupyID, tier, minutes)
}

// Occupy 占座/抢夺；玩家已在别处有座则先释放旧座（自动换座）。
func (m *Manager) Occupy(roomID int32, seatIndex int, playerID int64) bool {
	room := m.rooms[roomID]
	if room == nil || playerID == 0 || seatIndex < 0 || seatIndex >= pkgseat.SeatsPerRoom {
		return false
	}
	target := pkgseat.SeatRef{RoomID: roomID, SeatIndex: seatIndex}
	if old, ok := m.findSeat(playerID); ok && old != target {
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

// findSeat 查玩家当前坐哪（扫 10 房 × 10 座，占座是低频操作，不值得为它维护反向索引）。
func (m *Manager) findSeat(playerID int64) (pkgseat.SeatRef, bool) {
	for _, id := range m.roomIDs() {
		room := m.rooms[id]
		if room == nil {
			continue
		}
		for _, sl := range room.Snapshot().Slots {
			if sl.State == pkgseat.SeatOccupied && sl.Occupant == playerID {
				return pkgseat.SeatRef{RoomID: id, SeatIndex: sl.Index}, true
			}
		}
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
