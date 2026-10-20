// internal/game/player/manager.go —— 在线玩家 Actor 索引（player_id → PID）
//
// 架构职责：进程内唯一一张「谁在线、Actor 在哪」的表。所有按 playerID
// 找 Actor 的路径（会话绑定、下行推送、Kafka 投递、保存）都经这里。
//
// 协作关系：被 server.go（会话编排）、mq 消费者组（Deps.Players 接口）、
// flusher（persistPlayer 查询）共同使用；GetOrSpawn 内部委托 actor.System。
//
// 设计取舍：
//   - 为什么 index 是 map[int64]*actor.PID 而不是「Actor 自己注册」：
//     PID 是轻量地址对象，进程级共享一份索引即可，无需每个 Actor 反向
//     登记；下线只删索引，Actor 由 Tier-2 保留策略稍后回收；
//   - 为什么用 RWMutex 双检而不是 sync.Map：在线表读远多于写，双检下
//     写路径（Spawn）只发生在首次上线；RWMutex 的语义也比 LoadOrStore
//     直白——创建失败时不会把半成品留在表里。
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

	// mu 保护 index 的全部读写。index 是普通 map：并发读写普通 map 会
	// 直接 panic（fatal error: concurrent map writes），且无法 recover，
	// 所以这里必须显式加锁，不能图省事。
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
//
// 双检锁（double-checked locking）：先 RLock 快路径覆盖绝大多数已在线
// 请求；未命中再 Lock 重检一次——两个会话同时为首个上线者时，只有一个
// 线程执行 Spawn，另一个拿到已创建的 PID，避免重复 Spawn 出两个 Actor。
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
//
// 注意：回调在 RLock 持有期间执行，回调内**不能**再调用 Manager 的任何
// 写方法（Remove/GetOrSpawn）——RWMutex 不是可重入锁，会死锁；
// 需要删除时先在回调外收集 ID。
func (m *Manager) ForEach(f func(playerID int64, pid *actor.PID)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, pid := range m.index {
		f(id, pid)
	}
}
