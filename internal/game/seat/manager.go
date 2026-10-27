// internal/game/seat/manager.go —— 单服座位管理器
//
// 架构职责：启动时按「高 1 / 中 3 / 低 6」拉起居常驻房间 Actor，维护
// 房间名 → PID 索引；把房间释放产生的奖励派发给玩家：
//   - 在线：直接向玩家 Actor 投递 GrantItemsMsg（复用其 BizID 幂等）；
//   - 离线：写 seat_pending_reward，登录时由 seat 模块补发。
//
// 协作关系：玩家侧 seat 模块经 RoomPID/Snapshot 访问房间；房间 Actor
// 经注入的 emit 回调把奖励交回本管理器、经 onChange 回调上报快照。
//
// 设计取舍：房间是纯内存设施（与 battle.Room 一致），节点重启后清空，
// 未结算的占用随进程消失——重启是低频事件，玩家重抢即可。
package seat

import (
	"context"
	"fmt"
	"sync"

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
	logger  *log.Logger

	pids  map[string]*actor.PID
	rooms map[string]*pkgseat.Room
	order []string // 稳定遍历顺序

	// mu 保护 snaps：房间 Actor 在自己的 goroutine 里写，查询路径读。
	// 用缓存而不是直接读 Room 字段：避免玩家查询与房间 Actor 的并发读。
	mu    sync.RWMutex
	snaps map[string]pkgseat.RoomSnapshot
}

// NewManager 创建管理器并拉起全部固定房间。
//
// pending 可为 nil（无 MySQL 环境）：此时离线奖励不落库，只保证在线发奖。
func NewManager(sys *actor.System, players *player.Manager, pending *PendingStore, logger *log.Logger) *Manager {
	m := &Manager{
		sys:     sys,
		players: players,
		pending: pending,
		logger:  logger,
		pids:    make(map[string]*actor.PID),
		rooms:   make(map[string]*pkgseat.Room),
		snaps:   make(map[string]pkgseat.RoomSnapshot),
	}
	emit := func(rw pkgseat.Reward) { m.emit(rw) }

	for _, tier := range pkgseat.TierOrder {
		for i := 0; i < pkgseat.RoomCounts[tier]; i++ {
			name := pkgseat.RoomName(tier, i)
			room := pkgseat.NewRoom(tier, i, emit, logger)
			room.SetOnChange(m.onChange)
			pid, err := sys.Spawn(name, room)
			if err != nil {
				logger.Error("spawn seat room failed", "name", name, "err", err)
				continue
			}
			room.SetPID(pid)
			m.pids[name] = pid
			m.rooms[name] = room
			m.order = append(m.order, name)
			// 启动期无并发：直接取初始快照。
			m.snaps[name] = room.Snapshot()
		}
	}
	return m
}

// onChange 房间状态变更：更新本服快照缓存（供查询路径无锁读取）。
// 由房间 Actor 在串行上下文中调用，快照已在 Actor 内生成，无竞态。
func (m *Manager) onChange(snap pkgseat.RoomSnapshot) {
	name := pkgseat.RoomName(snap.Tier, snap.RoomIndex)
	m.mu.Lock()
	m.snaps[name] = snap
	m.mu.Unlock()
}

// emit 派发一笔座位奖励。
func (m *Manager) emit(rw pkgseat.Reward) {
	if rw.Amount <= 0 {
		return
	}
	items := []player.GrantItem{{ItemID: pkgseat.RewardItem, Count: rw.Amount}}

	if pid, ok := m.players.Get(rw.PlayerID); ok {
		// 在线：走玩家 Actor 的统一发奖通道（BizID 幂等，重投不会二次发放）。
		_ = m.sys.Send(pid, &player.GrantItemsMsg{
			BizID:  fmt.Sprintf("%d", rw.Seq),
			Source: "seat",
			Items:  items,
		})
		// 到账通知（best-effort，失败不影响发奖）。
		if raw, err := proto.Marshal(&gamev1.SeatRewardNotify{
			Tier: rw.Tier, Minutes: rw.Minutes, Reward: rw.Amount,
		}); err == nil {
			_ = m.sys.Send(pid, &player.PushMsg{MsgID: codec.MsgSeatRewardNotify, Body: raw})
		}
		return
	}

	// 离线：耐久化，登录补发。
	if m.pending == nil {
		m.logger.Warn("seat reward dropped: offline and no pending store",
			"player_id", rw.PlayerID, "seq", rw.Seq, "amount", rw.Amount)
		return
	}
	if err := m.pending.Add(context.Background(), rw.PlayerID, rw.Seq, rw.Tier, rw.Minutes); err != nil {
		m.logger.Warn("seat pending reward store failed",
			"player_id", rw.PlayerID, "seq", rw.Seq, "err", err)
	}
}

// PendReward 供跨服回推路径使用：把离线奖励写入挂起表。
func (m *Manager) PendReward(ctx context.Context, playerID, seq int64, tier string, minutes int64) error {
	if m.pending == nil {
		return nil
	}
	return m.pending.Add(ctx, playerID, seq, tier, minutes)
}

// RoomPID 按档位 + 房间序号取房间 Actor PID。
func (m *Manager) RoomPID(tier string, index int) *actor.PID {
	return m.pids[pkgseat.RoomName(tier, index)]
}

// Snapshot 取单间房快照（读缓存，不触碰房间 Actor）。
func (m *Manager) Snapshot(tier string, index int) (pkgseat.RoomSnapshot, bool) {
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
