// internal/gateway/conn/manager.go —— 连接全局管理
//
// Package conn 网关接入连接层（见 conn.go 的包注释）：Manager 是进程内
// 所有活跃连接的登记处，负责 ID 分配、按 ID 检索、计数与遍历，
// 供网关限流（MaxConns）、状态查询与批量操作使用。
package conn

import (
	"sync"
	"sync/atomic"
)

// Manager 管理所有活跃连接。
//
// 线程安全：内部 RWMutex 保护 map；Add/Remove 由连接建立/销毁路径调用，
// Get/Count/NextID 可并发。NextID 用原子自增，与 map 锁独立，
// 避免 ID 分配被 map 竞争阻塞。
type Manager struct {
	// mu 保护 conns；seq 独立原子分配连接 ID
	mu    sync.RWMutex
	conns map[int64]*Conn
	seq   atomic.Int64
}

// NewManager 创建 Manager。
func NewManager() *Manager {
	return &Manager{conns: make(map[int64]*Conn)}
}

// Add 加入管理。
func (m *Manager) Add(c *Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[c.ID()] = c
}

// Remove 移除。
//
// 幂等：重复 Remove 同一连接只是 delete 空操作。连接关闭时
// Manager.Remove 与 router.OnClose 的清理是两条独立路径，都可能触发。
func (m *Manager) Remove(c *Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, c.ID())
}

// Get 按 ID 获取。
func (m *Manager) Get(id int64) *Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[id]
}

// Count 返回连接数。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// NextID 分配新连接 ID。
func (m *Manager) NextID() int64 {
	return m.seq.Add(1)
}

// Range 遍历所有连接。
//
// 注意死锁约束：遍历期间持读锁，回调 f 内不得再调用 Manager 的写方法
// （Add/Remove）；返回 false 可提前终止遍历。
func (m *Manager) Range(f func(*Conn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.conns {
		if !f(c) {
			return
		}
	}
}
