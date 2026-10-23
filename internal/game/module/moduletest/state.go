// Package moduletest 提供业务模块单测的内存桩。
//
// 存在的理由：模块单测只需要「一份玩家状态 + 一个能收下行的口子」，
// 但 module.PlayerState 有 17 个方法。每个模块测试各写一份 fakeState，
// 重复且容易漏方法；集中在这里，新增模块直接复用。
//
// 边界：只模拟**内存语义**，不模拟数据库、Actor、提交链路——那些属于
// 玩家层与集成测试的职责。这里的目标是让「领取是否幂等」「满包是否
// 不发奖」这类纯业务规则一秒钟跑完。
package moduletest

import (
	"encoding/json"
	"sync"

	"game_study/internal/game/module"
	"google.golang.org/protobuf/proto"
)

// State 是 module.PlayerState 的内存实现。
//
// 零值不可用，请用 NewState。字段读写不加锁：模块内测是单 goroutine 的
// 串行调用，与 Actor 内真实执行模型一致；加锁反而会掩盖「模块被并发调用」
// 这类问题。
type State struct {
	id      int64
	items   map[int32]int64
	ext     map[string]json.RawMessage
	biz     map[string]bool
	dirty   []string
	profile module.Profile
	attrs   map[string]int64
	canHold bool
	setExts int
}

// NewState 创建内存状态；默认允许持有任意道具。
func NewState(id int64) *State {
	return &State{
		id:      id,
		items:   map[int32]int64{},
		ext:     map[string]json.RawMessage{},
		biz:     map[string]bool{},
		attrs:   map[string]int64{},
		canHold: true,
	}
}

// SetCanHold 控制满包预检结果（false 模拟背包放不下）。
func (s *State) SetCanHold(v bool) { s.canHold = v }

// SetProfile 覆盖角色基础信息（等级/场景等）。
func (s *State) SetProfile(p module.Profile) { s.profile = p }

// SetSceneID 更新场景归属。
func (s *State) SetSceneID(id int32) { s.profile.SceneID = id }

// DirtyModules 返回被标脏的模块名（按调用顺序）。
func (s *State) DirtyModules() []string { return append([]string(nil), s.dirty...) }

// SetExt 写模块私有状态并标脏，与 player.State 行为一致。
func (s *State) SetExt(name string, raw json.RawMessage) {
	s.setExts++
	if raw == nil {
		delete(s.ext, name)
	} else {
		s.ext[name] = append(json.RawMessage(nil), raw...)
	}
	s.dirty = append(s.dirty, name)
}

// PlayerID 实现 module.PlayerState。
func (s *State) PlayerID() int64 { return s.id }

// Profile 实现 module.PlayerState。
func (s *State) Profile() module.Profile { return s.profile }

// ExpMax 实现 module.PlayerState。
func (s *State) ExpMax() int64 { return 0 }

// GetAttr 实现 module.PlayerState。
func (s *State) GetAttr(key string) int64 { return s.attrs[key] }

// SetAttr 实现 module.PlayerState。
func (s *State) SetAttr(key string, val int64) { s.attrs[key] = val }

// AddBonus 实现 module.PlayerState；内存桩不区分基础值与加成。
func (s *State) AddBonus(key string, delta int64) { s.attrs[key] += delta }

// ClearBonus 实现 module.PlayerState。
func (s *State) ClearBonus() { s.attrs = map[string]int64{} }

// GetItemCount 实现 module.PlayerState。
func (s *State) GetItemCount(itemID int32) int64 { return s.items[itemID] }

// Items 实现 module.PlayerState。
func (s *State) Items() map[int32]int64 { return s.items }

// AddItem 实现 module.PlayerState。
func (s *State) AddItem(itemID int32, count int64) { s.items[itemID] += count }

// RemoveItem 实现 module.PlayerState：不足时整体失败，不做部分扣减。
func (s *State) RemoveItem(itemID int32, count int64) bool {
	if s.items[itemID] < count {
		return false
	}
	s.items[itemID] -= count
	return true
}

// MarkDirty 实现 module.PlayerState。
func (s *State) MarkDirty(name string) { s.dirty = append(s.dirty, name) }

// Ext 实现 module.PlayerState。
func (s *State) Ext(name string) json.RawMessage {
	raw := s.ext[name]
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

// CanHold 实现 module.PlayerState。
func (s *State) CanHold([]module.ItemGrant) bool { return s.canHold }

// ClaimBiz 实现 module.PlayerState：首次占用返回 true。
func (s *State) ClaimBiz(key string) bool {
	if s.biz[key] {
		return false
	}
	s.biz[key] = true
	return true
}

// HasBiz 实现 module.PlayerState。
func (s *State) HasBiz(key string) bool { return s.biz[key] }

// Recorder 记录下行报文（按消息号保留最近一条，并统计条数）。
//
// 条数统计是专门为「一次请求只应回一个包」的断言准备的：多回一条会让
// 下一次请求读错包，而这种错位在客户端表现为「业务码莫名丢失」。
type Recorder struct {
	mu    sync.Mutex
	last  map[uint32][]byte
	count map[uint32]int
}

// NewRecorder 创建下行记录器。
func NewRecorder() *Recorder {
	return &Recorder{last: map[uint32][]byte{}, count: map[uint32]int{}}
}

// Send 实现 module.Sender。
func (r *Recorder) Send(msgID uint32, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last[msgID] = append([]byte(nil), body...)
	r.count[msgID]++
	return nil
}

// Last 返回某消息号最近一次的报文。
func (r *Recorder) Last(msgID uint32) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, ok := r.last[msgID]
	return raw, ok
}

// Count 返回某消息号的下行条数。
func (r *Recorder) Count(msgID uint32) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count[msgID]
}

// Total 返回全部下行条数。
func (r *Recorder) Total() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, c := range r.count {
		n += c
	}
	return n
}

// NewContext 组装模块执行上下文（缓存权威模式，State 已就绪）。
func NewContext(st *State, rec *Recorder) *module.Context {
	return &module.Context{State: st, CacheEnabled: true, Send: rec.Send}
}

// Call 发一条请求，返回对应响应报文。
//
// respID 由调用方显式给出（模块自己知道请求号 → 响应号），这里不猜。
func Call(m module.Module, ctx *module.Context, rec *Recorder, msgID, respID uint32, req proto.Message) ([]byte, error) {
	body, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	if err := m.OnMessage(ctx, msgID, body); err != nil {
		return nil, err
	}
	raw, _ := rec.Last(respID)
	return raw, nil
}
