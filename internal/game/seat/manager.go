// internal/game/seat/manager.go —— 房间管理器（房间 / 一人一座 / 落库 / 结算）
//
// 结构：Manager 管 10 个房间（按 room_id 索引），Room 管 10 个座位。
// Manager 做四件事：
//   1. 启动按「高 1 / 中 3 / 低 6」Spawn 房间，把上次落库的占用灌回去；
//   2. 裁决「一人一座」（玩家同时只能占一个座，换座自动释放旧座）；
//   3. 房间状态变更时整房落库，并按差异同步玩家数据（seat_player）；
//   4. 座位释放时发奖：在线直发，离线写 outbox 等登录补发。
//
// 玩家数据（占哪个房哪个座、从何时起）归这里管，不进玩家 Actor：
// 座位是房间的，不是玩家的私有状态；塞进玩家快照会变成 N 份副本。
package seat

import (
	"context"
	"fmt"
	"sync"
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

	rooms map[int32]*actor.PID // room_id → PID
	order []int32              // 稳定遍历顺序

	mu    sync.RWMutex
	snaps map[int32]pkgseat.RoomSnapshot
	occ   map[int64]pkgseat.SeatRef // 玩家 → 当前座位（一人一座）
}

// NewManager 创建管理器；store/pending 可为 nil（无 MySQL 环境退化为纯内存）。
func NewManager(sys *actor.System, players *player.Manager, store *Store, pending *PendingStore, logger *log.Logger) *Manager {
	m := &Manager{
		sys:     sys,
		players: players,
		store:   store,
		pending: pending,
		logger:  logger,
		rooms:   make(map[int32]*actor.PID),
		snaps:   make(map[int32]pkgseat.RoomSnapshot),
		occ:     make(map[int64]pkgseat.SeatRef),
	}

	saved := m.load() // 先把上次的占用读出来，房间收到 RestoreMsg 才能立即结算

	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			room := pkgseat.NewRoom(tier, i, logger)
			id := room.ID()
			room.SetOnChange(m.onChange)
			room.SetOnRelease(m.onRelease)
			pid, err := sys.Spawn(pkgseat.RoomName(id), room)
			if err != nil {
				logger.Error("spawn seat room failed", "room_id", id, "err", err)
				continue
			}
			room.SetPID(pid)
			m.rooms[id] = pid
			m.order = append(m.order, id)
			m.snaps[id] = room.Snapshot()

			slots := saved[id]
			if len(slots) == 0 {
				continue
			}
			// 先重建一人一座登记，再灌回房间；超时结算会经 onRelease 清掉。
			for _, sl := range slots {
				m.occ[sl.Occupant] = pkgseat.SeatRef{RoomID: id, SeatIndex: sl.Index}
			}
			if err := sys.Send(pid, &pkgseat.RestoreMsg{Slots: slots}); err != nil {
				logger.Warn("seat restore failed", "room_id", id, "err", err)
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
	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			empty = append(empty, pkgseat.RoomSnapshot{ID: pkgseat.RoomID(tier, i), Tier: tier, Index: i})
		}
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

// onChange 状态变更：更新查询缓存 → 整房落库 → 按差异同步玩家数据。
//
// 同步写而不是异步合并：座位变更是玩家点出来的低频操作，两条 upsert 只有
// 几毫秒；加超时兜底就够，不值得为它引入合并器与后台协程。
func (m *Manager) onChange(snap pkgseat.RoomSnapshot) {
	m.mu.Lock()
	old, had := m.snaps[snap.ID]
	m.snaps[snap.ID] = snap
	m.mu.Unlock()

	if m.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := m.store.Save(ctx, snap); err != nil {
		m.logger.Warn("seat room save failed", "room_id", snap.ID, "err", err)
		return // 房间都没写成功，玩家索引也没必要写（下次变更会一起补）
	}
	if had {
		m.syncPlayers(ctx, old, snap)
	}
}

// syncPlayers 按快照差异维护 seat_player（房间维度 → 玩家维度的投影）。
func (m *Manager) syncPlayers(ctx context.Context, old, cur pkgseat.RoomSnapshot) {
	prev, next := occupants(old), occupants(cur)

	for pid, seat := range next {
		if s, ok := prev[pid]; ok && s == seat {
			continue // 没变化
		}
		ref := pkgseat.SeatRef{RoomID: cur.ID, SeatIndex: seat}
		if err := m.store.SavePlayer(ctx, pid, ref, occupiedAt(cur, seat)); err != nil {
			m.logger.Warn("seat player save failed", "player_id", pid, "err", err)
		}
	}
	for pid := range prev {
		if _, ok := next[pid]; ok {
			continue
		}
		if err := m.store.DeletePlayer(ctx, pid, cur.ID); err != nil {
			m.logger.Warn("seat player delete failed", "player_id", pid, "err", err)
		}
	}
}

// occupants 座位号 → 玩家，返回 玩家 → 座位 的反查表。
func occupants(snap pkgseat.RoomSnapshot) map[int64]int {
	out := make(map[int64]int, len(snap.Slots))
	for _, sl := range snap.Slots {
		if sl.State == pkgseat.SeatOccupied && sl.Occupant > 0 {
			out[sl.Occupant] = sl.Index
		}
	}
	return out
}

func occupiedAt(snap pkgseat.RoomSnapshot, seatIndex int) int64 {
	for _, sl := range snap.Slots {
		if sl.Index == seatIndex {
			return sl.OccupiedAt
		}
	}
	return 0
}

// onRelease 座位释放：清一人一座登记，有奖则结算。
func (m *Manager) onRelease(rel pkgseat.Release) {
	if rel.PlayerID <= 0 {
		return
	}
	cur := pkgseat.SeatRef{RoomID: pkgseat.RoomID(rel.Tier, rel.RoomIndex), SeatIndex: rel.SeatIndex}
	m.mu.Lock()
	if ref, ok := m.occ[rel.PlayerID]; ok && ref == cur {
		delete(m.occ, rel.PlayerID)
	}
	m.mu.Unlock()

	if rel.Amount > 0 {
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
	pid := m.rooms[roomID]
	if pid == nil || playerID == 0 || seatIndex < 0 || seatIndex >= pkgseat.SeatsPerRoom {
		return false
	}

	ref := pkgseat.SeatRef{RoomID: roomID, SeatIndex: seatIndex}
	m.mu.Lock()
	old, had := m.occ[playerID]
	m.occ[playerID] = ref
	m.mu.Unlock()

	if had && old != ref {
		if oldPid := m.rooms[old.RoomID]; oldPid != nil {
			_ = m.sys.Send(oldPid, &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: old.SeatIndex})
		}
	}
	return m.sys.Send(pid, &pkgseat.OccupyMsg{PlayerID: playerID, SeatIndex: seatIndex}) == nil
}

// Leave 主动离开。
func (m *Manager) Leave(roomID int32, seatIndex int, playerID int64) bool {
	pid := m.rooms[roomID]
	if pid == nil || playerID == 0 || seatIndex < 0 || seatIndex >= pkgseat.SeatsPerRoom {
		return false
	}
	return m.sys.Send(pid, &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: seatIndex}) == nil
}

// Snapshot 单间房快照（读缓存）。
func (m *Manager) Snapshot(roomID int32) (pkgseat.RoomSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap, ok := m.snaps[roomID]
	return snap, ok
}

// AllSnapshots 全部房间快照（按稳定顺序）。
func (m *Manager) AllSnapshots() []pkgseat.RoomSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]pkgseat.RoomSnapshot, 0, len(m.order))
	for _, id := range m.order {
		if snap, ok := m.snaps[id]; ok {
			out = append(out, snap)
		}
	}
	return out
}

// Count 房间总数。
func (m *Manager) Count() int { return len(m.order) }
