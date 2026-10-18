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

	dirty      map[string]bool // module -> dirty 标记
	dirtyItems map[int32]bool  // 变更过的道具（落库只写这些，见 persist.Save）

	// dirtyHook 任何数据变更后回调（由 Actor 注入 → flusher.MarkDirty）。
	//
	// 没有这个回调时，只有玩家下线（onUnbind）才会标脏 —— 意味着**在线玩家的
	// 所有改动都不会落库**，进程崩溃即全部丢失。P4 的 Write-Behind 一直缺这一环。
	dirtyHook func()

	mu sync.RWMutex
}

// NewState 创建玩家状态。
func NewState(playerID int64) *State {
	return &State{
		playerID: playerID,
		items:      make(map[int32]int64),
		dirty:      make(map[string]bool),
		dirtyItems: make(map[int32]bool),
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

// SetDirtyHook 注入变更回调（进程内一次，Actor 构造时设置）。
func (s *State) SetDirtyHook(hook func()) {
	s.mu.Lock()
	s.dirtyHook = hook
	s.mu.Unlock()
}

// notifyDirty 在锁外触发回调（避免持锁调用外部代码）。
func (s *State) notifyDirty() {
	s.mu.RLock()
	hook := s.dirtyHook
	s.mu.RUnlock()
	if hook != nil {
		hook()
	}
}

// AddItem 增加道具（标记脏，落库时只写变更项）。
func (s *State) AddItem(itemID int32, count int64) {
	s.mu.Lock()
	s.items[itemID] += count
	s.dirtyItems[itemID] = true
	s.mu.Unlock()
	s.notifyDirty()
}

// SeedItem 初始化道具（从库里加载时用，**不标脏**）。
//
// 与 AddItem 区分：预加载灌数据不应该产生写回，否则每次登录都会把所有道具
// 重写一遍，白白放大落库量。
func (s *State) SeedItem(itemID int32, count int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[itemID] = count
}

// RemoveItem 扣除道具，数量不足返回 false。
func (s *State) RemoveItem(itemID int32, count int64) bool {
	s.mu.Lock()
	if s.items[itemID] < count {
		s.mu.Unlock()
		return false
	}
	s.items[itemID] -= count
	s.dirtyItems[itemID] = true // 归零也要标脏：落库时转为 DELETE
	if s.items[itemID] == 0 {
		delete(s.items, itemID)
	}
	s.mu.Unlock()
	s.notifyDirty()
	return true
}

// DirtyItems 返回变更过的道具及其当前数量（0 表示应删除该行）。
//
// 落库只需处理这部分，而不是整包重写：背包 50 个道具里通常只有 1-2 个变化。
func (s *State) DirtyItems() map[int32]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[int32]int64, len(s.dirtyItems))
	for id := range s.dirtyItems {
		out[id] = s.items[id] // 不存在时为 0 → 落库转 DELETE
	}
	return out
}

// ClearItemsDirty 清空道具脏标记（落库成功后调用）。
func (s *State) ClearItemsDirty() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.dirtyItems)
}

// MarkDirty 标记模块数据脏，并通知 flusher 纳入本轮落库。
func (s *State) MarkDirty(module string) {
	s.mu.Lock()
	s.dirty[module] = true
	s.mu.Unlock()
	s.notifyDirty()
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
