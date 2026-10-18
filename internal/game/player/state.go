// internal/game/player/state.go —— 玩家内存状态
package player

import (
	"sync"
)

// Attr 玩家属性。
type Attr struct {
	Career int32
	Hp     int32
	Mp     int32
	Atk    int32
	Def    int32
	Spd    int32
}

// State 玩家内存状态（Actor 内单线程访问，外部只读时加锁）。
type State struct {
	playerID int64
	attr     Attr
	items    map[int32]int64 // item_id -> count

	dirty   map[string]bool // module -> dirty 标记
	mu      sync.RWMutex
}

// NewState 创建玩家状态。
func NewState(playerID int64) *State {
	return &State{
		playerID: playerID,
		items:    make(map[int32]int64),
		dirty:    make(map[string]bool),
	}
}

// PlayerID 返回玩家 ID。
func (s *State) PlayerID() int64 { return s.playerID }

// GetAttr 读取属性。
func (s *State) GetAttr(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch key {
	case "hp":
		return int64(s.attr.Hp)
	case "mp":
		return int64(s.attr.Mp)
	case "atk":
		return int64(s.attr.Atk)
	case "def":
		return int64(s.attr.Def)
	case "spd":
		return int64(s.attr.Spd)
	case "career":
		return int64(s.attr.Career)
	}
	return 0
}

// SetAttr 设置属性。
func (s *State) SetAttr(key string, val int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch key {
	case "hp":
		s.attr.Hp = int32(val)
	case "mp":
		s.attr.Mp = int32(val)
	case "atk":
		s.attr.Atk = int32(val)
	case "def":
		s.attr.Def = int32(val)
	case "spd":
		s.attr.Spd = int32(val)
	case "career":
		s.attr.Career = int32(val)
	}
}

// GetItemCount 查询道具数量。
func (s *State) GetItemCount(itemID int32) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.items[itemID]
}

// AddItem 增加道具。
func (s *State) AddItem(itemID int32, count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[itemID] += count
}

// RemoveItem 扣除道具，数量不足返回 false。
func (s *State) RemoveItem(itemID int32, count int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items[itemID] < count {
		return false
	}
	s.items[itemID] -= count
	if s.items[itemID] == 0 {
		delete(s.items, itemID)
	}
	return true
}

// MarkDirty 标记模块数据脏。
func (s *State) MarkDirty(module string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty[module] = true
}

// IsDirty 查询模块是否脏。
func (s *State) IsDirty(module string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dirty[module]
}

// ClearDirty 清除脏标记（落库成功后调用）。
func (s *State) ClearDirty(module string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.dirty, module)
}

// Attr 返回属性副本。
func (s *State) Attr() Attr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.attr
}

// SetAttrStruct 设置完整属性。
func (s *State) SetAttrStruct(a Attr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attr = a
}

// Items 返回道具副本。
func (s *State) Items() map[int32]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[int32]int64, len(s.items))
	for k, v := range s.items {
		out[k] = v
	}
	return out
}
