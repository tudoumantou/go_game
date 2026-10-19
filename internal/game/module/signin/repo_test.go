package signin

import (
	"encoding/json"
	"testing"

	"game_study/internal/game/module"
)

// memorySignin 是最小状态实现：只提供模块自己那一段状态槽位。
type memorySignin struct{ ext map[string]json.RawMessage }

func (s *memorySignin) Ext(name string) json.RawMessage {
	if s.ext == nil {
		return nil
	}
	return s.ext[name]
}

func (s *memorySignin) SetExt(name string, raw json.RawMessage) {
	if s.ext == nil {
		s.ext = make(map[string]json.RawMessage)
	}
	s.ext[name] = raw
}

func TestLoadNeverStartsCycle(t *testing.T) {
	state := &memorySignin{}
	v, err := NewRepo().Load(state, 20000)
	if err != nil || v.Day != 1 || v.Start != 20000 || len(state.ext) != 0 {
		t.Fatalf("view=%+v err=%v ext=%v", v, err, state.ext)
	}
}

func TestClaimUsesActorStateAndStoredRewards(t *testing.T) {
	state := &memorySignin{}
	repo := NewRepo()
	first, already, err := repo.Claim(state, 20000, []module.ItemGrant{{ItemID: 1001, Count: 100}})
	if err != nil || already || first[0].Count != 100 {
		t.Fatal(first, already, err)
	}
	stored, already, err := repo.Claim(state, 20000, []module.ItemGrant{{ItemID: 1001, Count: 999}})
	if err != nil || !already || stored[0].Count != 100 {
		t.Fatal(stored, already, err)
	}
	v, err := repo.Load(state, 20007)
	if err != nil || v.Day != 1 || v.Start != 20007 || len(v.Claims) != 0 {
		t.Fatal(v, err)
	}
}

// 状态必须落在模块自己的槽位里，并且能通过 Persister 校验。
func TestClaimWritesModuleSlot(t *testing.T) {
	state := &memorySignin{}
	if _, _, err := NewRepo().Claim(state, 20000, []module.ItemGrant{{ItemID: 1001, Count: 3}}); err != nil {
		t.Fatal(err)
	}
	raw := state.Ext(stateKey)
	if len(raw) == 0 {
		t.Fatal("claim did not write module state")
	}
	if err := (persister{}).Validate(raw); err != nil {
		t.Fatal(err)
	}
	var d Data
	if err := json.Unmarshal(raw, &d); err != nil || d.AnchorDay != 20000 || len(d.Claims[20000]) != 1 {
		t.Fatalf("%+v %v", d, err)
	}
}
