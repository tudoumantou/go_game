// internal/game/seat/manager.go —— 单服座位管理器（房间 + 落库 + 重启恢复）
//
// 架构职责：
//  1. 启动时按「高 1 / 中 3 / 低 6」拉起常驻房间 Actor；
//  2. 从 seat_slot 恢复上次的座位占用：未到期的按剩余时长重挂定时器，
//     重启期间已到期的立即结算（否则这些座位会永久占住且不再受 1h 上限约束）；
//  3. 状态变更异步落库（脏合并，不阻塞房间 Actor）；
//  4. 座位释放时按 outbox 结算奖励：写 pending → 在线直发 → 成功删 pending。
//
// 协作关系：玩家侧 seat 模块经 Occupy/Leave 操作房间；房间 Actor 经 onChange
// 上报快照（落库 + 查询缓存）、经 onRelease 上报结算事件。
//
// 设计取舍：内存仍是权威，落库是「重启恢复 + 结算凭据」，不是每次读都回源。
// 合并窗口内崩溃最多丢几个座位的占用状态，玩家重抢即可；奖励正确性由
// occupy_id 幂等保证，不受影响。
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

// Manager 单服座位管理器。
type Manager struct {
	sys     *actor.System
	players *player.Manager
	pending *PendingStore
	store   *Store
	writer  *writer
	logger  *log.Logger

	pids  map[string]*actor.PID
	order []string

	// occ 一人一座登记：房间 Actor 各自只看得见自己的 10 个座，
	// 跨房间的唯一性必须由管理器统一裁决。
	occ *pkgseat.Occupancy

	// mu 保护 snaps：房间 Actor 在自己的 goroutine 里写，查询路径读。
	mu    sync.RWMutex
	snaps map[string]pkgseat.RoomSnapshot
}

// NewManager 创建管理器：拉起固定房间、恢复持久化状态。
//
// pending / store 均可为 nil（无 MySQL 环境）：此时退化为纯内存模式，
// 不落库、不恢复，但占座与在线发奖照常工作。
func NewManager(sys *actor.System, players *player.Manager, pending *PendingStore, store *Store, logger *log.Logger) *Manager {
	m := &Manager{
		sys:     sys,
		players: players,
		pending: pending,
		store:   store,
		logger:  logger,
		pids:    make(map[string]*actor.PID),
		occ:     pkgseat.NewOccupancy(),
		snaps:   make(map[string]pkgseat.RoomSnapshot),
	}
	if store != nil {
		m.writer = newWriter(store, logger)
	}

	// 先把上次的占用读出来，再建房间——房间收到 RestoreMsg 时才能立即结算。
	restored := m.loadPersisted()

	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			name := pkgseat.RoomName(tier, i)
			room := pkgseat.NewRoom(tier, i, logger)
			room.SetOnChange(m.onChange)
			room.SetOnRelease(m.onRelease)
			pid, err := sys.Spawn(name, room)
			if err != nil {
				logger.Error("spawn seat room failed", "name", name, "err", err)
				continue
			}
			room.SetPID(pid)
			m.pids[name] = pid
			m.order = append(m.order, name)
			// 启动期无并发：直接取初始快照。
			m.snaps[name] = room.Snapshot()

			if slots := restored[name]; len(slots) > 0 {
				// 先重建「一人一座」登记，再灌入房间；超时结算会经 onRelease
				// 把登记清掉，顺序反了会让已结算的玩家残留在登记表里。
				for _, rs := range slots {
					m.occ.Claim(rs.Occupant, pkgseat.SeatRef{Tier: tier, RoomIndex: i, SeatIndex: rs.SeatIndex})
				}
				if err := sys.Send(pid, &pkgseat.RestoreMsg{Slots: slots}); err != nil {
					logger.Warn("seat restore send failed", "room", name, "err", err)
				}
			}
		}
	}
	return m
}

// loadPersisted 读取上次落库的房间与座位状态，按房间名分组。
//
// 任何读失败都只降级为「空状态启动」而不阻塞起服：座位是玩法设施，
// 不是账号数据，宁可丢占用也不能让整个游戏服起不来。
func (m *Manager) loadPersisted() map[string][]pkgseat.RestoredSlot {
	out := make(map[string][]pkgseat.RestoredSlot)
	if m.store == nil {
		return out
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	refs := make([]pkgseat.SeatRef, 0, 16)
	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			refs = append(refs, pkgseat.SeatRef{Tier: tier, RoomIndex: i})
		}
	}
	if err := m.store.EnsureRooms(ctx, refs); err != nil {
		m.logger.Warn("seat room ensure failed", "err", err)
	}

	rows, err := m.store.LoadSlots(ctx)
	if err != nil {
		m.logger.Error("seat slot load failed, start with empty state", "err", err)
		return out
	}

	occupied := 0
	for _, r := range rows {
		if r.State != int32(pkgseat.SeatOccupied) || r.Occupant <= 0 || r.OccupiedAt <= 0 {
			continue
		}
		if !pkgseat.ValidSeat(r.Tier, r.RoomIndex, r.SeatIndex) {
			continue // 脏数据（非法档位/房号/座号）：忽略，不让它污染房间
		}
		name := pkgseat.RoomName(r.Tier, r.RoomIndex)
		out[name] = append(out[name], pkgseat.RestoredSlot{
			SeatIndex:  r.SeatIndex,
			Occupant:   r.Occupant,
			OccupyID:   r.OccupyID,
			OccupiedAt: time.Unix(r.OccupiedAt, 0),
		})
		occupied++
	}
	if len(rows) > 0 {
		m.logger.Info("seat state loaded", "rows", len(rows), "occupied", occupied)
	}
	return out
}

// onChange 房间状态变更：更新查询缓存，并标记落库。
func (m *Manager) onChange(snap pkgseat.RoomSnapshot) {
	name := pkgseat.RoomName(snap.Tier, snap.RoomIndex)
	m.mu.Lock()
	m.snaps[name] = snap
	m.mu.Unlock()

	if m.writer != nil {
		m.writer.mark(snap)
	}
}

// onRelease 座位释放：清一人一座登记，按需结算奖励。
func (m *Manager) onRelease(ev pkgseat.ReleaseEvent) {
	if ev.PlayerID <= 0 {
		return
	}
	m.occ.Release(ev.PlayerID, pkgseat.SeatRef{
		Tier: ev.Tier, RoomIndex: ev.RoomIndex, SeatIndex: ev.SeatIndex,
	})
	if ev.Amount <= 0 {
		return // 不足 1 分钟：座位照常释放，不发奖
	}
	m.settle(ev)
}

// settle 结算一笔座位奖励（outbox 模式）。
//
// 顺序不能颠倒：先写 pending 再发奖。若先发奖后写 pending，中间崩溃会
// 导致「奖励没发但座位已空」，无法重算；先写 pending 则任何崩溃点都能
// 在下次登录补发。
func (m *Manager) settle(ev pkgseat.ReleaseEvent) {
	if m.pending != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := m.pending.Add(ctx, ev.PlayerID, ev.OccupyID, ev.Tier, ev.Minutes)
		cancel()
		if err != nil {
			m.logger.Warn("seat outbox write failed",
				"player_id", ev.PlayerID, "occupy_id", ev.OccupyID, "err", err)
		}
	}

	// 在线：走玩家 Actor 的统一发奖通道（BizID 用 occupy_id，重投不二次发放）。
	if m.players != nil {
		if pid, ok := m.players.Get(ev.PlayerID); ok {
			err := m.sys.Send(pid, &player.GrantItemsMsg{
				BizID:  fmt.Sprintf("%d", ev.OccupyID),
				Source: "seat",
				Items:  []player.GrantItem{{ItemID: pkgseat.RewardItem, Count: ev.Amount}},
			})
			if err == nil {
				// 已在线送达：outbox 行可以删了。
				if m.pending != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					if derr := m.pending.Delete(ctx, ev.PlayerID, ev.OccupyID); derr != nil {
						m.logger.Warn("seat outbox delete failed",
							"player_id", ev.PlayerID, "occupy_id", ev.OccupyID, "err", derr)
					}
					cancel()
				}
				if raw, e := proto.Marshal(&gamev1.SeatRewardNotify{
					Tier: int32(ev.Tier), Minutes: ev.Minutes, Reward: ev.Amount,
				}); e == nil {
					_ = m.sys.Send(pid, &player.PushMsg{MsgID: codec.MsgSeatRewardNotify, Body: raw})
				}
				return
			}
			m.logger.Warn("seat reward send failed, keep outbox",
				"player_id", ev.PlayerID, "occupy_id", ev.OccupyID, "err", err)
		}
	}
	// 离线或投递失败：outbox 行保留，登录时由 seat 模块补发。
}

// PendReward 供跨服回推路径使用：把待补发奖励写入 outbox。
func (m *Manager) PendReward(ctx context.Context, playerID, occupyID int64, tier pkgseat.Tier, minutes int64) error {
	if m.pending == nil {
		return nil
	}
	return m.pending.Add(ctx, playerID, occupyID, tier, minutes)
}

// Occupy 占用/抢夺座位。
//
// 一人一座：若该玩家已在别处有座，先把旧座释放（照常结算奖励），再占新座，
// 即「自动换座」。玩家只能有一个座位，避免占满 100 座按座叠加奖励。
func (m *Manager) Occupy(tier pkgseat.Tier, roomIndex, seatIndex int, playerID int64) bool {
	if playerID == 0 || !pkgseat.ValidSeat(tier, roomIndex, seatIndex) {
		return false
	}
	pid := m.pids[pkgseat.RoomName(tier, roomIndex)]
	if pid == nil {
		return false
	}
	ref := pkgseat.SeatRef{Tier: tier, RoomIndex: roomIndex, SeatIndex: seatIndex}
	// 先登记、再投递：旧座的释放回调总是晚于这里到达，靠 ref 比对不会
	// 误删刚写入的新登记。
	if old, had := m.occ.Claim(playerID, ref); had {
		m.sendLeave(old, playerID)
	}
	return m.sys.Send(pid, &pkgseat.OccupyMsg{PlayerID: playerID, SeatIndex: seatIndex}) == nil
}

// Leave 主动离开座位。
func (m *Manager) Leave(tier pkgseat.Tier, roomIndex, seatIndex int, playerID int64) bool {
	if playerID == 0 || !pkgseat.ValidSeat(tier, roomIndex, seatIndex) {
		return false
	}
	pid := m.pids[pkgseat.RoomName(tier, roomIndex)]
	if pid == nil {
		return false
	}
	return m.sys.Send(pid, &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: seatIndex}) == nil
}

// sendLeave 向指定座位投递离开消息（换座时释放旧座用）。
func (m *Manager) sendLeave(ref pkgseat.SeatRef, playerID int64) {
	pid := m.pids[pkgseat.RoomName(ref.Tier, ref.RoomIndex)]
	if pid == nil {
		return
	}
	_ = m.sys.Send(pid, &pkgseat.LeaveMsg{PlayerID: playerID, SeatIndex: ref.SeatIndex})
}

// CurrentSeat 查询玩家当前占用的座位（查询/调试用）。
func (m *Manager) CurrentSeat(playerID int64) (pkgseat.SeatRef, bool) {
	return m.occ.Get(playerID)
}

// Snapshot 取单间房快照（读缓存，不触碰房间 Actor）。
func (m *Manager) Snapshot(tier pkgseat.Tier, index int) (pkgseat.RoomSnapshot, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap, ok := m.snaps[pkgseat.RoomName(tier, index)]
	return snap, ok
}

// AllSnapshots 按稳定顺序返回全部房间快照（读缓存）。
func (m *Manager) AllSnapshots() []pkgseat.RoomSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]pkgseat.RoomSnapshot, 0, len(m.order))
	for _, name := range m.order {
		if snap, ok := m.snaps[name]; ok {
			out = append(out, snap)
		}
	}
	return out
}

// Count 房间总数。
func (m *Manager) Count() int { return len(m.order) }

// Close 停止落库协程并做最后一次刷盘（进程退出时调用）。
func (m *Manager) Close() {
	if m.writer != nil {
		m.writer.Close()
	}
}
