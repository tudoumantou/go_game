// internal/gateway/session/manager.go —— 会话管理（SID 生成、reconnect 窗口、按 player_id 索引）
//
// Package session 网关会话层（见 session.go 的包注释）：Manager 是会话的
// 索引与生命周期管家——四张 map 分别按 SID/数值 ID/连接/玩家检索，
// 并提供「顶号检测、超时回收、批量关闭」三个运维必需能力。
// 协作关系：server.onAccept 调 NewSession；router.OnPacket 按连接查
// 会话；server.deliverToClient 按数值 ID 查；BindPlayer 由绑定流程调用；
// GC 由后台定时任务驱动（见 game_study 主程序），CloseAll 供优雅停机。
// 设计取舍：① 四张索引冗余换查询效率——不同调用方持有不同的键
// （connID/numID/SID/playerID），避免遍历；代价是任何增删都要四张
// 同步，由 Remove/GC/CloseAll 三个私有模式保证。② 顶号检测放在
// byPlayer 上：同一 player_id 只允许一个会话，是「双连接顶号」的
// 唯一防线。③ 无持久化：重启后全部会话随进程消失。
package session

import (
	"fmt"
	"sync"
	"time"

	"game_study/internal/common/constant"
	"game_study/internal/gateway/conn"
	"game_study/pkg/log"
)

// Manager 会话管理器。
//
// 四张索引必须保持一致：任何增删都要四张 map 同步（Remove/GC/CloseAll
// 都遵循同一模式）。seq 只在新会话创建时自增，用于生成可读 SID。
// 知识点：单把 RWMutex 保护四张 map 而不是四把——四张表的增删必须
// 原子完成（半同步状态会被并发读到），多锁无法表达这个复合不变量；
// 锁粒度取舍为「整组操作一把锁」。
type Manager struct {
	// mu 保护下面四张索引表；byPlayer 同时承担「顶号检测」职责
	// bySID：gw-{unix秒}-{seq} 字符串 → 会话（日志/运维检索）
	mu       sync.RWMutex
	bySID    map[string]*Session
	byNum    map[uint64]*Session // 数值会话 ID → 会话（gRPC/下行路由主键）
	byConn   map[int64]*Session  // 连接 ID → 会话（读循环收包主键）
	byPlayer map[int64]*Session  // player_id → 会话（顶号检测主键）

	// seq 会话序号（SID 后缀）；logger 只读
	seq    int64
	logger *log.Logger
}

// NewManager 创建 Manager。
func NewManager(logger *log.Logger) *Manager {
	return &Manager{
		bySID:    make(map[string]*Session),
		byNum:    make(map[uint64]*Session),
		byConn:   make(map[int64]*Session),
		byPlayer: make(map[int64]*Session),
		logger:   logger,
	}
}

// NewSession 为新连接创建会话。
//
// SID 格式 gw-{unix秒}-{seq}：可读且足够定位；numID 直接取连接 ID，
// 这是与 backend.Pool / gRPC BIND 帧的约定——下游全部按 numID 寻址。
// 知识点：seq 在锁内自增，与 SID 的生成原子绑定——否则两个并发
// NewSession 可能拿到同一 seq，SID 碰撞导致一张表覆盖另一张。
func (m *Manager) NewSession(c *conn.Conn) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	id := fmt.Sprintf("gw-%d-%d", time.Now().Unix(), m.seq)
	s := New(id, c, m.logger)
	s.SetNumID(uint64(c.ID()))
	m.bySID[id] = s
	m.byNum[uint64(c.ID())] = s
	m.byConn[c.ID()] = s
	return s
}

// GetBySID 按 SID 查。
func (m *Manager) GetBySID(sid string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bySID[sid]
}

// GetByNumID 按数值会话 ID 查（下行路由用）。
// server.deliverToClient 的主路径：游戏服回包只带 session_id。
func (m *Manager) GetByNumID(numID uint64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byNum[numID]
}

// GetByConn 按连接 ID 查。
// router.OnPacket/OnClose 的主路径：读循环只持有 conn.ID()。
func (m *Manager) GetByConn(connID int64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byConn[connID]
}

// GetByPlayer 按 player_id 查。
// 顶号检测与「玩家是否在线」查询用；未绑定时返回 nil。
func (m *Manager) GetByPlayer(playerID int64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byPlayer[playerID]
}

// BindPlayer 把会话绑定到玩家 ID。
//
// 双连接顶号保护：同一 player_id 已有别的会话时返回错误（由调用方决定
// 踢旧还是拒新）；同一会话重复绑定（如断线重连同会话）放行。
// 知识点：比较用指针相等（old != s）而非 ID 相等——同一会话重入
// （重连路径）放行，不同会话（顶号）拒绝，语义精确且无歧义。
func (m *Manager) BindPlayer(s *Session, playerID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.byPlayer[playerID]; ok && old != s {
		return fmt.Errorf("player %d already online", playerID)
	}
	m.byPlayer[playerID] = s
	s.SetState(StateBound)
	return nil
}

// Remove 移除会话。
// 四张表同步 delete；playerID=0 表示未绑定（无需清 byPlayer）。
// 幂等：对已移除的会话重复调用是空 delete。
func (m *Manager) Remove(s *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.bySID, s.ID())
	delete(m.byNum, s.NumID())
	if s.Conn() != nil {
		delete(m.byConn, s.Conn().ID())
	}
	if pid := s.PlayerID(); pid != 0 {
		delete(m.byPlayer, pid)
	}
}

// Count 返回会话数。
// 以 bySID 的长度为准——它是主表，其他表与它等长（不变量）。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bySID)
}

// CloseAll 关闭所有会话（优雅关闭用）。
//
// 先摘表再逐个 Close：避免 Close 回调（OnClose→Remove）在持锁状态下
// 重入 Manager 造成死锁。摘表后新到达的下行帧按「会话不存在」丢弃。
// 知识点：快照 + 换新 map 是「先摘后关」的标准实现——临界区内只做
// map 引用替换（O(1) 四次），阻塞时间与连接数无关；Close 在锁外
// 逐个执行，其回调重入 Remove 拿的是已经空了的新表。
func (m *Manager) CloseAll() {
	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.bySID))
	for _, s := range m.bySID {
		sessions = append(sessions, s)
	}
	m.bySID = make(map[string]*Session)
	m.byNum = make(map[uint64]*Session)
	m.byConn = make(map[int64]*Session)
	m.byPlayer = make(map[int64]*Session)
	m.mu.Unlock()

	for _, s := range sessions {
		s.Close(fmt.Errorf("server shutdown"))
	}
}

// GC 清理过期未认证会话（reconnect 窗口外）。
//
// 只处理两类：已关闭的残留条目（连接层已清理但索引未摘）与超过
// DefaultSessionReconnectTTL 仍未完成鉴权的半开连接。已鉴权会话不在此
// 回收——它们的生命周期由连接断开事件驱动（OnClose）。
// 知识点：遍历中 delete 是安全的（Go 规范保证），但只删当前 key；
// 这里删的是四张表里的关联条目，互不依赖，不怕遍历副作用。
func (m *Manager) GC(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for sid, s := range m.bySID {
		if s.State() == StateClosed {
			delete(m.bySID, sid)
			delete(m.byNum, s.NumID())
			if s.Conn() != nil {
				delete(m.byConn, s.Conn().ID())
			}
			if pid := s.PlayerID(); pid != 0 {
				delete(m.byPlayer, pid)
			}
			continue
		}
		// State < Authed：连接建立了但迟迟不握手/不鉴权（扫描器、僵死
		// 客户端）。超过 TTL 即断——半开连接同样占用 fd 与会话表。
		// now 由调用方注入（而非 time.Now()），便于单测控制时间。
		if s.State() < StateAuthed && now.Sub(s.LastAlive()) > constant.DefaultSessionReconnectTTL {
			m.logger.Warn("session timeout", "sid", sid)
			delete(m.bySID, sid)
			delete(m.byNum, s.NumID())
			if s.Conn() != nil {
				delete(m.byConn, s.Conn().ID())
			}
			s.Close(fmt.Errorf("auth timeout"))
		}
	}
}
