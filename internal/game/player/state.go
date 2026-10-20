// internal/game/player/state.go —— 玩家内存状态（唯一可写的数据本体）
//
// 架构职责：State 是玩家全部内存数据的单一事实源——Profile/Attr/Items/
// Ext/appliedGrants 五类资产 + 两套脏标记，所有模块都经由它读写。
//
// 协作关系：Actor 持有并单线程操作它；snapshot.go 把它序列化进
// writebehind.Snapshot（Redis/落库的格式）；模块经 module.Context.State
// 访问，不直接持有字段。
//
// 设计取舍：
//   - 为什么锁只服务「Actor 外只读」：Actor 内单线程串行，加锁反而拖慢
//     且掩盖「谁在改」的问题；锁的存在是为了探针/测试/统计这类外部
//     读者不与写路径竞争（map 并发读写会直接 panic，见下方注释）；
//   - 为什么属性分三层（baseAttr/attr/bonus）：落库的只有职业基础值，
//     等级成长每次从 baseAttr 重算（幂等），bonus 登录时由装备模块重算
//     ——三层分离后，「升级」「换装」「重登」互不污染；
//   - 为什么 dirty 分模块级与道具级：模块级驱动 flusher 排轮，道具级
//     决定 Save 写哪几行——粒度不同，混用一张表会要么全量重写要么漏写。
//
// DB 列映射（与 snapshot/persist 一一对应）：
//
//	profile  → player(name, level, exp, coin, scene_id)
//	attr     → player_attr(career, hp, mp, atk, def, spd)
//	items    → player_item(item_id, count)
//	ext      → 各模块 Persister 的私有表（键 = Persister.Name）
//	appliedGrants → player_writebehind.applied_grants (JSON)
//	bonus/dirty*   → 不落库（在线派生状态与调度标记）
package player

import (
	"encoding/json"
	"sync"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/writebehind"
)

// Attr 玩家属性。
type Attr = writebehind.Attr

// State 玩家内存状态（Actor 内单线程访问，外部只读时加锁）。
//
// 三层属性的语义与落库边界：
//
//	baseAttr：职业初始值（player_attr 行），唯一落库的属性；
//	attr：    baseAttr + 等级成长，当前生效值，不落库；
//	bonus：   装备/伙伴加成，登录时重算，不落库（快照刻意不含它）。
type State struct {
	playerID      int64
	profile       module.Profile
	attr          Attr                       // 当前基础属性 = baseAttr + 等级成长（落库的是 baseAttr）
	baseAttr      Attr                       // 落库的职业基础属性（等级成长在它之上叠加，不写回）
	bonus         Attr                       // 装备/伙伴等加成（登录时重算，不落库）
	items         map[int32]int64            // item_id -> count
	ext           map[string]json.RawMessage // 模块私有状态（键 = writebehind.Persister.Name）
	appliedGrants map[string]bool

	dirty      map[string]bool // module -> dirty 标记
	dirtyItems map[int32]bool  // 变更过的道具（落库只写这些，见 persist.Save）

	// dirtyHook 任何数据变更后回调（由 Actor 注入 → flusher.MarkDirty）。
	//
	// 没有这个回调时，只有玩家下线（onUnbind）才会标脏 —— 意味着**在线玩家的
	// 所有改动都不会落库**，进程崩溃即全部丢失。P4 的 Write-Behind 一直缺这一环。
	dirtyHook func()

	// mu 保护所有字段。Actor 内串行访问本可不加锁，但探针/测试会在
	// Actor 外读状态；且 Go 的 map 并发读写会抛不可恢复的
	// 「concurrent map read and map write」fatal error，必须显式互斥。
	mu sync.RWMutex
}

// NewState 创建玩家状态。
func NewState(playerID int64) *State {
	return &State{
		playerID:   playerID,
		items:      make(map[int32]int64),
		ext:        make(map[string]json.RawMessage),
		dirty:      make(map[string]bool),
		dirtyItems: make(map[int32]bool),
	}
}

// Ext 返回模块私有状态的副本；未设置过时返回 nil。
//
// 返回副本而非原值：模块拿到 raw 后可能改写底层数组，副本保证 State 内
// 的快照不被外部污染（与 cloneExt 同理）。
func (s *State) Ext(name string) json.RawMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	raw := s.ext[name]
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

// SetExt 覆盖模块私有状态并标脏；raw 为 nil 表示清除该模块状态。
//
// 模块不需要自己序列化整个玩家：只提交自己那一段，框架负责搬运。
func (s *State) SetExt(name string, raw json.RawMessage) {
	s.mu.Lock()
	if raw == nil {
		delete(s.ext, name)
	} else {
		s.ext[name] = append(json.RawMessage(nil), raw...)
	}
	s.dirty[name] = true
	s.mu.Unlock()
	// notifyDirty 在锁外调用：持锁执行外部回调有死锁风险
	// （回调方若回头读 State 会与 Lock 自锁）
	s.notifyDirty()
}

// PlayerID 返回玩家 ID。
func (s *State) PlayerID() int64 { return s.playerID }

// ClaimBiz 占用业务唯一键，返回 false 表示该键已应用过（幂等命中）。
//
// 只在玩家 Actor 内单线程调用，与其它状态改动一样不加锁。键会随快照 CAS
// 并落库到 player_writebehind.applied_grants，进程重启或 Redis 丢失后仍有效。
func (s *State) ClaimBiz(key string) bool {
	if key == "" {
		return true
	}
	if s.appliedGrants == nil {
		s.appliedGrants = make(map[string]bool, 4)
	}
	if s.appliedGrants[key] {
		return false
	}
	s.appliedGrants[key] = true
	return true
}

// HasBiz 查询业务键是否已应用。
func (s *State) HasBiz(key string) bool { return s.appliedGrants[key] }

// Profile 返回角色基础信息副本。
func (s *State) Profile() module.Profile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.profile
}

// AddExp 增加经验并按 level.xml 连续升级（满级停止）。
//
// 升级后立即重算等级属性，在线就能看到属性变化，不用等重登。
func (s *State) AddExp(n int64) (levelUp bool, level int32) {
	if n <= 0 {
		return false, s.Profile().Level
	}
	s.mu.Lock()
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
	level = s.profile.Level
	s.mu.Unlock()
	if levelUp {
		s.RecalcAttrs()
	}
	return levelUp, level
}

// RecalcAttrs 按等级重算基础属性：落库基础 + attr_formula 的等级成长。
//
// 属性计算顺序（R4.2）：
//  1. baseAttr：职业初始值，创角写入 player_attr，不随等级变；
//  2. 等级成长：(level-1) * col3，登录与升级时重算，不落库；
//  3. bonus：装备/伙伴加成，登录时模块重算，不落库。
//
// 每次都从 baseAttr 重算，天然幂等；不碰 bonus，在线升级不会清掉已生效的装备加成。
func (s *State) RecalcAttrs() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recalcLocked()
}

// recalcLocked 重算主体（调用方已持锁）。
//
// 先拷贝 a := s.baseAttr 再累加：Attr 是值类型，直接改 s.baseAttr 会把
// 「成长」永久焊进基础值，下次重算重复叠加。
func (s *State) recalcLocked() {
	a := s.baseAttr
	levels := int64(s.profile.Level) - 1
	if levels < 0 {
		levels = 0
	}
	for _, cfg := range gamedata.AttrFormulaCfgList() {
		delta := cfg.Col3 * levels
		if delta == 0 {
			continue
		}
		switch cfg.Col1 {
		case "hp":
			a.Hp += int32(delta)
		case "mp":
			a.Mp += int32(delta)
		case "atk":
			a.Atk += int32(delta)
		case "def":
			a.Def += int32(delta)
		case "spd":
			a.Spd += int32(delta)
		}
	}
	s.attr = a
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

// SetSceneID 更新所在场景并标脏（0 = 不在场景中）。
func (s *State) SetSceneID(sceneID int32) {
	s.mu.Lock()
	s.profile.SceneID = sceneID
	s.mu.Unlock()
	s.notifyDirty()
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

// attrValue 按字符串键取属性值；未知键返回 0（配置错误的兜底，
// 由调用方表现为「加成无效」而不是 panic）。
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

// Capacity 背包容量（可容纳的道具**种类**数）：初始值 + 等级 * 每级扩容。
//
// 配置缺失时返回 0，表示不做容量限制（开发环境与旧存档友好）。
func (s *State) Capacity() int32 {
	s.mu.RLock()
	level := s.profile.Level
	s.mu.RUnlock()
	return capacityFor(level)
}

// CanHold 预检：这批道具是否都放得下。
//
// 只统计「新增种类」：已有道具继续叠加不受容量限制。先预检再统一发奖，
// 避免多物品奖励发到一半才发现满包，留下「部分到账」的状态。
func (s *State) CanHold(grants []writebehind.ItemGrant) bool {
	cap := s.Capacity()
	if cap <= 0 {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	kinds := 0
	for _, n := range s.items {
		if n > 0 {
			kinds++
		}
	}
	// 本批新增种类去重；已在背包里的直接跳过。
	seen := make(map[int32]bool, len(grants))
	for _, g := range grants {
		if g.Count <= 0 || g.ItemID <= 0 || s.items[g.ItemID] > 0 || seen[g.ItemID] {
			continue
		}
		seen[g.ItemID] = true
	}
	return kinds+len(seen) <= int(cap)
}

// capacityFor 按配置计算容量；无配置返回 0（不限制）。
func capacityFor(level int32) int32 {
	if len(gamedata.BagCapacityCfgList()) == 0 {
		return 0
	}
	cfg := gamedata.BagCapacityCfgList()[0]
	if cfg.Col1 <= 0 {
		return 0
	}
	return int32(cfg.Col1 + int64(level)*cfg.Col2)
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
// 返回新 map：调用方（persist/grant）会遍历甚至修改它，不能直接给内部 map。
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
