// internal/cross/battle/manager.go —— 房间管理器（建房/摘除/优雅关闭）
package battle

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Manager 房间管理器。
//
// 职责：分配 room_id、Spawn 房间 Actor、维护 roomID → PID 索引、优雅关闭时批量兜底结算。
// 房间状态本身在 Room Actor 内，管理器只持有 PID 与关房句柄。
type Manager struct {
	sys    *actor.System
	logger *log.Logger
	sink   Sink

	mu    sync.RWMutex
	rooms map[uint64]*roomEntry

	seq     atomic.Uint64
	closing atomic.Bool
}

type roomEntry struct {
	pid  *actor.PID
	room *Room
}

// NewManager 创建房间管理器。
func NewManager(sys *actor.System, sink Sink, logger *log.Logger) *Manager {
	return &Manager{
		sys:    sys,
		logger: logger,
		sink:   sink,
		rooms:  make(map[uint64]*roomEntry),
	}
}

// Create 创建房间并返回 roomID。
//
// roomID = 时间戳低 16 位 << 32 | 进程内自增序号，避免节点重启后复用旧号
// （客户端可能用旧 room_id 上报，重号会导致串房）。
func (m *Manager) Create(mode string, expectPlayers int, timeout time.Duration) (uint64, error) {
	if m.closing.Load() {
		return 0, fmt.Errorf("cross server is draining, reject new room")
	}
	n := m.seq.Add(1)
	roomID := uint64(time.Now().Unix()&0xFFFF)<<32 | n

	room := NewRoom(roomID, mode, expectPlayers, timeout, m.sink, m.logger)
	pid, err := m.sys.Spawn(fmt.Sprintf("room:%d", roomID), room)
	if err != nil {
		return 0, fmt.Errorf("spawn room actor: %w", err)
	}
	room.SetPID(pid)

	m.mu.Lock()
	m.rooms[roomID] = &roomEntry{pid: pid, room: room}
	m.mu.Unlock()

	m.logger.Info("room created", "room_id", roomID, "mode", mode, "expect", expectPlayers)
	return roomID, nil
}

// Join 玩家入房（快照入队，满员自动开战）。
func (m *Manager) Join(nodeID string, snap *internalv2.PlayerSnapshot) error {
	e := m.get(snap.GetRoomId())
	if e == nil {
		return fmt.Errorf("room %d not found", snap.GetRoomId())
	}
	return m.sys.Send(e.pid, &JoinMsg{NodeID: nodeID, Snapshot: snap})
}

// RelayBattle 转发战斗消息到房间（房间再扇出给其他参战者）。
func (m *Manager) RelayBattle(nodeID string, msg *internalv2.BattleMsg) error {
	e := m.get(msg.GetRoomId())
	if e == nil {
		return fmt.Errorf("room %d not found", msg.GetRoomId())
	}
	return m.sys.Send(e.pid, &BattleMsgIn{NodeID: nodeID, Msg: msg})
}

// Report 转发结算上报到房间。
func (m *Manager) Report(nodeID string, res *internalv2.BattleResult) error {
	e := m.get(res.GetRoomId())
	if e == nil {
		return fmt.Errorf("room %d not found", res.GetRoomId())
	}
	return m.sys.Send(e.pid, &ReportMsg{NodeID: nodeID, Result: res})
}

// RoomClosed 实现 Sink：房间结算后从索引摘除并终止 Actor。
func (m *Manager) RoomClosed(roomID uint64) {
	m.mu.Lock()
	e, ok := m.rooms[roomID]
	if ok {
		delete(m.rooms, roomID)
	}
	m.mu.Unlock()

	if !ok {
		return
	}
	// 延迟终止：当前消息还在 Actor 内处理，直接 Terminate 会打断自身循环
	go func() {
		time.Sleep(100 * time.Millisecond)
		m.sys.Terminate(e.pid)
		m.logger.Info("room destroyed", "room_id", roomID)
	}()
}

// Count 当前房间数。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}

// Stats 房间状态快照（管理接口/指标用）。
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]int{"total": len(m.rooms), "waiting": 0, "running": 0, "settled": 0}
	for _, e := range m.rooms {
		state, _ := e.room.Snapshot()
		switch state {
		case StateWaiting:
			out["waiting"]++
		case StateRunning:
			out["running"]++
		case StateSettled:
			out["settled"]++
		}
	}
	return out
}

// Drain 优雅关闭：拒绝新房 → 存量房间兜底结算（超时强制）。
func (m *Manager) Drain(timeout time.Duration) {
	m.closing.Store(true)

	m.mu.RLock()
	entries := make([]*roomEntry, 0, len(m.rooms))
	for _, e := range m.rooms {
		entries = append(entries, e)
	}
	m.mu.RUnlock()

	if len(entries) == 0 {
		return
	}
	m.logger.Info("draining rooms", "count", len(entries), "timeout", timeout.String())

	deadline := time.Now().Add(timeout)
	for _, e := range entries {
		_ = m.sys.Send(e.pid, &CloseMsg{Reason: "server_drain"})
	}

	for time.Now().Before(deadline) {
		if m.Count() == 0 {
			m.logger.Info("all rooms drained")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	m.logger.Warn("drain timeout, remaining rooms", "count", m.Count())
}

func (m *Manager) get(roomID uint64) *roomEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rooms[roomID]
}
