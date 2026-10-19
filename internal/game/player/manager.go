// internal/game/player/manager.go —— 玩家 Actor 管理
package player

import (
	"fmt"
	"sync"

	"game_study/internal/game/module"
	"game_study/pkg/actor"
	"game_study/pkg/log"
)

// Manager 管理在线玩家 Actor。
type Manager struct {
	sys       *actor.System
	registry  *module.Registry
	logger    *log.Logger
	repo      *Repository
	preloader *Preloader
	onDirty   func(playerID int64)
	sendFunc  SendFunc
	sceneLink SceneLink // 场景心跳与重连同步（启动时注入一次，之后只读）

	mu    sync.RWMutex
	index map[int64]*actor.PID // player_id -> PID
}

// NewManager 创建 Manager。
func NewManager(sys *actor.System, registry *module.Registry, logger *log.Logger, repo *Repository, onDirty func(int64), sendFunc SendFunc) *Manager {
	return &Manager{
		sys:      sys,
		registry: registry,
		logger:   logger,
		repo:     repo,
		onDirty:  onDirty,
		sendFunc: sendFunc,
		index:    make(map[int64]*actor.PID),
	}
}

// SetPreloader 注入预加载器（12.6）。
func (m *Manager) SetPreloader(p *Preloader) { m.preloader = p }

// SetSceneLink 注入场景连接（启动时调用一次，须在首个玩家 Spawn 之前）。
func (m *Manager) SetSceneLink(l SceneLink) { m.sceneLink = l }

// GetOrSpawn 获取或创建玩家 Actor。
func (m *Manager) GetOrSpawn(playerID int64) (*actor.PID, error) {
	m.mu.RLock()
	pid, ok := m.index[playerID]
	m.mu.RUnlock()
	if ok {
		return pid, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if pid, ok := m.index[playerID]; ok {
		return pid, nil
	}

	id := fmt.Sprintf("player:%d", playerID)
	actor := NewActor(playerID, m.registry, m.logger, m.repo, m.onDirty, m.sendFunc)
	if m.preloader != nil {
		actor.WithPreloader(m.preloader)
	}
	if m.sceneLink != nil {
		actor.WithSceneLink(m.sceneLink)
	}
	pid, err := m.sys.Spawn(id, actor)
	if err != nil {
		return nil, err
	}
	m.index[playerID] = pid
	return pid, nil
}

// Get 查询 PID。
func (m *Manager) Get(playerID int64) (*actor.PID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pid, ok := m.index[playerID]
	return pid, ok
}

// Remove 移除玩家（下线）。
func (m *Manager) Remove(playerID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.index, playerID)
}

// Count 在线数。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.index)
}

// ForEach 遍历在线玩家。
func (m *Manager) ForEach(f func(playerID int64, pid *actor.PID)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, pid := range m.index {
		f(id, pid)
	}
}
