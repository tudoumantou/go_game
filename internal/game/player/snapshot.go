package player

import "game_study/pkg/writebehind"

func cloneSignin(data *writebehind.SigninData) *writebehind.SigninData {
	if data == nil {
		return nil
	}
	out := &writebehind.SigninData{AnchorDay: data.AnchorDay, Claims: make(map[int64][]writebehind.ItemGrant, len(data.Claims))}
	for day, items := range data.Claims {
		out.Claims[day] = append([]writebehind.ItemGrant(nil), items...)
	}
	return out
}

func (s *State) SigninData() *writebehind.SigninData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneSignin(s.signin)
}
func (s *State) SetSigninData(data *writebehind.SigninData) {
	s.mu.Lock()
	s.signin = cloneSignin(data)
	s.dirty["signin"] = true
	s.mu.Unlock()
	s.notifyDirty()
}

func (s *State) snapshot() writebehind.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := writebehind.Snapshot{PlayerID: s.playerID, Profile: s.profile, Attr: s.attr, Items: make(map[int32]int64, len(s.items)), Signin: cloneSignin(s.signin)}
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
	s.signin = cloneSignin(snapshot.Signin)
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
