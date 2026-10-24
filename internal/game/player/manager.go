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
	"context"
	"fmt"
	"sync"

	"game_study/internal/game/module"
	"game_study/pkg/actor"
	"game_study/pkg/log"
)

// PresenceLookup 在线归属查询（*cache.Presence 实现；测试可注入内存实现）。
//
// 只声明本包用得到的方法：Manager 不依赖整个 cache 包，排査依赖时
// 一眼能看出「玩家索引只需要问一句谁在线」。
type PresenceLookup interface {
	// IsOnline 返回玩家的在线归属节点；不在线返回 ok=false。
	IsOnline(ctx context.Context, playerID int64) (nodeID string, ok bool)
}

// Manager 管理在线玩家 Actor。
type Manager struct {
	sys       *actor.System
	registry  *module.Registry
	logger    *log.Logger
	repo      *Repository
	preloader *Preloader
	onDirty   func(playerID int64)
	sendFunc  SendFunc
	// onKick 跨节点顶号回调（透传给每个 Actor；nil 表示不支持）
	onKick    func(playerID int64, fromNode string)
	sceneLink SceneLink // 场景心跳与重连同步（启动时注入一次，之后只读）

	// 跨节点（P0）：nodeID 是本节点标识；presence 是在线归属表。
	// 两者都在启动期注入一次，之后只读。缺 presence 时 Resolve 退化为
	// 纯本地查询（单机部署/测试环境）。
	nodeID   string
	presence PresenceLookup

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

// SetKickHook 注入跨节点顶号回调（启动时调用一次，须在首个玩家 Spawn 之前）。
//
// 与 SetSceneLink 同一约束：Actor 创建时读取，之后再设置对新 Actor 无效。
func (m *Manager) SetKickHook(fn func(playerID int64, fromNode string)) { m.onKick = fn }

// SetNodeID 设置本节点标识（跨节点路由基准，启动时调用一次）。
func (m *Manager) SetNodeID(id string) { m.nodeID = id }

// SetPresence 注入在线归属表（跨节点解析用，启动时调用一次）。
func (m *Manager) SetPresence(p PresenceLookup) { m.presence = p }

// playerPID 玩家 Actor 的全局唯一名（与 System.Spawn 的 key 一致）。
//
// 抽成函数是因为远程 PID 也要用同一个名字——本地 Spawn 的 key 与
// 远程投递的 actorID 必须是同一个字符串，否则消息到不了目标 Actor。
func playerPID(playerID int64) string { return fmt.Sprintf("player:%d", playerID) }

// ActorID 玩家 Actor 的全局 ID。
//
// 跨节点路由用：PID{ID: ActorID(playerID), Node: 目标节点}。
// 导出它是因为「给别的节点上的玩家发消息」是这个包之外的需求
// （跨节点顶号、远程推送），而 ID 格式必须与 Spawn 时一致。
func ActorID(playerID int64) string { return playerPID(playerID) }

// GetOrSpawn 获取或创建玩家 Actor。
//
// 双检锁（double-checked locking）：先 RLock 快路径覆盖绝大多数已在线
// 请求；未命中再 Lock 重检一次——两个会话同时为首个上线者时，只有一个
// 线程执行 Spawn，另一个拿到已创建的 PID，避免重复 Spawn 出两个 Actor。
//
// 跨节点守卫（P0）：本地未命中且归属表显示玩家在线在别的节点时，
// 返回该节点的远程 PID 而不是就地 Spawn——双 Actor 是双写分叉
// （两份内存各自 CAS 同一份 Redis 快照，后写的覆盖先写的）。
// Redis 查询放在写锁外：不把 IO 压在临界区上（bind 路径低频，
// 但锁内 IO 会放大成所有 GetOrSpawn 的尾延迟）。
func (m *Manager) GetOrSpawn(ctx context.Context, playerID int64) (*actor.PID, error) {
	m.mu.RLock()
	pid, ok := m.index[playerID]
	m.mu.RUnlock()
	if ok {
		return pid, nil
	}

	if m.presence != nil && m.nodeID != "" {
		if owner, online := m.presence.IsOnline(ctx, playerID); online && owner != m.nodeID {
			return &actor.PID{ID: playerPID(playerID), Node: owner}, nil
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if pid, ok := m.index[playerID]; ok {
		return pid, nil
	}

	id := playerPID(playerID)
	actor := NewActor(playerID, m.registry, m.logger, m.repo, m.onDirty, m.sendFunc)
	if m.onKick != nil {
		actor.SetKickHook(m.onKick)
	}
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

// Get 查询本地 PID（只查本节点表，不产生 Redis 访问）。
//
// 与 Resolve 的分工：Get 是「本节点一定持有」场景的快路径
// （会话绑定、本节点保存）；Resolve 是「玩家可能在线在别的节点」
// 场景的全解析（跨节点推送）。热路径不要为用不到的能力付 Redis RTT。
func (m *Manager) Get(playerID int64) (*actor.PID, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pid, ok := m.index[playerID]
	return pid, ok
}

// Resolve 解析玩家 Actor 位置：本地表 → 在线归属表（P0）。
//
// 本地未命中时查 presence：归属在别的节点 → 返回 Node=owner 的远程
// PID（Send 会自动走跨节点投递）；不在线任何节点 → ok=false。
// presence 未注入（单机/测试）时退化为纯本地查询。
func (m *Manager) Resolve(ctx context.Context, playerID int64) (*actor.PID, bool) {
	m.mu.RLock()
	pid, ok := m.index[playerID]
	m.mu.RUnlock()
	if ok {
		return pid, true
	}
	if m.presence == nil || m.nodeID == "" {
		return nil, false
	}
	if owner, online := m.presence.IsOnline(ctx, playerID); online && owner != m.nodeID {
		return &actor.PID{ID: playerPID(playerID), Node: owner}, true
	}
	return nil, false
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
