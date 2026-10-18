// internal/gateway/session/manager.go —— 会话管理（SID 生成、reconnect 窗口、按 player_id 索引）
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
type Manager struct {
	mu       sync.RWMutex
	bySID    map[string]*Session
	byNum    map[uint64]*Session
	byConn   map[int64]*Session
	byPlayer map[int64]*Session

	seq      int64
	logger   *log.Logger
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
func (m *Manager) GetByNumID(numID uint64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byNum[numID]
}

// GetByConn 按连接 ID 查。
func (m *Manager) GetByConn(connID int64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byConn[connID]
}

// GetByPlayer 按 player_id 查。
func (m *Manager) GetByPlayer(playerID int64) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byPlayer[playerID]
}

// BindPlayer 把会话绑定到玩家 ID。
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
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bySID)
}

// CloseAll 关闭所有会话（优雅关闭用）。
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
