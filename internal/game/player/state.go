// internal/game/player/state.go —— 玩家内存状态
package player

import (
	"sync"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
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
	profile  module.Profile
	attr     Attr // 基础属性（落库）
	bonus    Attr // 装备等加成（登录时按已穿装备重算，不落库）
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

// Profile 返回角色基础信息副本。
func (s *State) Profile() module.Profile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.profile
}

// AddExp 增加经验并按 level.xml 连续升级（满级停止）。
func (s *State) AddExp(n int64) (levelUp bool, level int32) {
	if n <= 0 {
		return false, s.Profile().Level
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profile.Exp += n
	for {
		cfg, ok := gamedata.GetLevelCfg(int64(s.profile.Level) + 1)
		if !ok {
			break // 满级
		}
		need := cfg.Col1
		if need <= 0 || s.profile.Exp < need {
			break
		}
		s.profile.Exp -= need
		s.profile.Level++
		levelUp = true
	}
	return levelUp, s.profile.Level
}

// ExpMax 返回升到下一级所需经验（满级 0）。
func (s *State) ExpMax() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := gamedata.GetLevelCfg(int64(s.profile.Level) + 1)
	if !ok {
		return 0
	}
	return cfg.Col1
}

// SetProfile 设置角色基础信息。
func (s *State) SetProfile(p module.Profile) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.profile = p
}

// GetAttr 读取属性（基础值 + 装备加成）。
func (s *State) GetAttr(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	base := attrValue(s.attr, key)
	bonus := attrValue(s.bonus, key)
	if key == "career" {
		return base // 职业不受加成影响
	}
	return base + bonus
}

// AddBonus 增加/减少装备加成（delta 可为负）。
func (s *State) AddBonus(key string, delta int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch key {
	case "hp":
		s.bonus.Hp += int32(delta)
	case "mp":
		s.bonus.Mp += int32(delta)
	case "atk":
		s.bonus.Atk += int32(delta)
	case "def":
		s.bonus.Def += int32(delta)
	case "spd":
		s.bonus.Spd += int32(delta)
	}
}

// ClearBonus 清空全部加成（重算前调用）。
func (s *State) ClearBonus() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bonus = Attr{}
}

func attrValue(a Attr, key string) int64 {
	switch key {
	case "hp":
		return int64(a.Hp)
	case "mp":
		return int64(a.Mp)
	case "atk":
		return int64(a.Atk)
	case "def":
		return int64(a.Def)
	case "spd":
		return int64(a.Spd)
	case "career":
		return int64(a.Career)
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
