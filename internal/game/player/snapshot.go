package player

import (
	"encoding/json"

	"game_study/pkg/writebehind"
)

// cloneExt 深拷贝模块私有状态，避免 Actor 回滚时与快照共享底层数组。
func cloneExt(src map[string]json.RawMessage) map[string]json.RawMessage {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(src))
	for name, raw := range src {
		out[name] = append(json.RawMessage(nil), raw...)
	}
	return out
}

func (s *State) snapshot() writebehind.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := writebehind.Snapshot{PlayerID: s.playerID, Profile: s.profile, Attr: s.attr, Items: make(map[int32]int64, len(s.items)), Ext: cloneExt(s.ext)}
	for id, count := range s.items {
		out.Items[id] = count
	}
	if s.appliedGrants != nil {
		out.AppliedGrants = make(map[string]bool, len(s.appliedGrants))
		for key, v := range s.appliedGrants {
			out.AppliedGrants[key] = v
		}
	}
	return out
}

func stateFromSnapshot(snapshot writebehind.Snapshot) *State {
	s := NewState(snapshot.PlayerID)
	s.profile = snapshot.Profile
	s.attr = snapshot.Attr
	s.ext = cloneExt(snapshot.Ext)
	if s.ext == nil {
		s.ext = make(map[string]json.RawMessage)
	}
	if snapshot.AppliedGrants != nil {
		s.appliedGrants = make(map[string]bool, len(snapshot.AppliedGrants))
		for key, v := range snapshot.AppliedGrants {
			s.appliedGrants[key] = v
		}
	}
	for id, count := range snapshot.Items {
		s.items[id] = count
	}
	return s
}

// clone 包含派生属性和脏标记，用于未成功写缓存时恢复本次内存修改。
func (s *State) clone() *State {
	out := stateFromSnapshot(s.snapshot())
	s.mu.RLock()
	defer s.mu.RUnlock()
	out.bonus = s.bonus
	out.dirtyHook = s.dirtyHook
	for name, dirty := range s.dirty {
		out.dirty[name] = dirty
	}
	for id, dirty := range s.dirtyItems {
		out.dirtyItems[id] = dirty
	}
	return out
}
