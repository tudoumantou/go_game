// internal/gateway/conn/manager.go —— 连接全局管理
//
// Package conn 网关接入连接层（见 conn.go 的包注释）：Manager 是进程内
// 所有活跃连接的登记处，负责 ID 分配、按 ID 检索、计数与遍历，
// 供网关限流（MaxConns）、状态查询与批量操作使用。
// 协作关系：server.onAccept 建连后 Add，router.OnClose 断连后 Remove；
// heartbeat handler 经 Get 找回连接对象；/status 探针读 Count。
// 设计取舍：map + RWMutex 而非 sync.Map——连接数在千级、读写都频繁，
// RWMutex 的读并发足够且代码直白；sync.Map 更适合只读多、key 稳定
// 的场景（如缓存）。ID 分配拆成独立 atomic，避免被 map 锁竞争拖慢。
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
// 知识点：Go 的 map 并发读写会直接 panic（fatal error: concurrent map
// writes），且不可 recover——因此所有访问点都必须走同一把锁，没有
// 「应该没问题」的例外；锁粒度到单次 map 操作，持锁期间不调回调。
type Manager struct {
	// mu 保护 conns；seq 独立原子分配连接 ID（见 NextID）
	mu    sync.RWMutex
	conns map[int64]*Conn
	seq   atomic.Int64
}

// NewManager 创建 Manager。
func NewManager() *Manager {
	return &Manager{conns: make(map[int64]*Conn)}
}

// Add 加入管理。
// 由 server.onAccept 在 Serve 之前调用，保证连接进入读写循环前已登记，
// Count 限流与 Get 检索不会漏掉这条连接。
func (m *Manager) Add(c *Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[c.ID()] = c
}

// Remove 移除。
//
// 幂等：重复 Remove 同一连接只是 delete 空操作。连接关闭时
// Manager.Remove 与 router.OnClose 的清理是两条独立路径，都可能触发。
// 知识点：delete 不存在的 key 是空操作，利用这一点把「清理」做成幂等，
// 调用方无需关心是否重复移除。
func (m *Manager) Remove(c *Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, c.ID())
}

// Get 按 ID 获取。
// 返回 nil 表示连接已消亡（调用方需判空，如 router.handleHeartbeat）。
func (m *Manager) Get(id int64) *Conn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[id]
}

// Count 返回连接数。
// 供 server.onAccept 的 MaxConns 限流与 /status 输出使用；持读锁，
// 与 Add/Remove 的写锁互斥，读到的是某一时刻的快照。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// NextID 分配新连接 ID。
// 知识点：atomic.Int64.Add(1) 是无锁原子操作，返回递增后的值；
// 从 1 开始天然避开「零值即合法 ID」的歧义。单调递增不回收，
// 重启后从零开始——ID 仅在进程内有效，不跨进程假设。
func (m *Manager) NextID() int64 {
	return m.seq.Add(1)
}

// Range 遍历所有连接。
//
// 注意死锁约束：遍历期间持读锁，回调 f 内不得再调用 Manager 的写方法
// （Add/Remove）；返回 false 可提前终止遍历。
// 知识点：RWMutex 不可重入——持读锁时再申请写锁会死锁（Go 不支持锁
// 升级），因此回调内只应做与 Manager 无关的只读工作。
func (m *Manager) Range(f func(*Conn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, c := range m.conns {
		if !f(c) {
			return
		}
	}
}
