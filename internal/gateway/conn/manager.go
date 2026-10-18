// internal/gateway/conn/manager.go —— 连接全局管理
package conn

import (
	"sync"
	"sync/atomic"
)

// Manager 管理所有活跃连接。
type Manager struct {
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
func (m *Manager) Range(f func(*Conn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.conns {
		if !f(c) {
			return
		}
	}
}
