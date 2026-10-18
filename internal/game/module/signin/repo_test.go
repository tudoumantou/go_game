package signin

import (
	"game_study/internal/game/module"
	"game_study/pkg/writebehind"
	"testing"
)

type memorySignin struct{ data *writebehind.SigninData }

func (s *memorySignin) SigninData() *writebehind.SigninData        { return s.data }
func (s *memorySignin) SetSigninData(data *writebehind.SigninData) { s.data = data }

func TestLoadNeverStartsCycle(t *testing.T) {
	state := &memorySignin{}
	v, err := NewRepo().Load(state, 20000)
	if err != nil || v.Day != 1 || v.Start != 20000 || state.data != nil {
		t.Fatalf("view=%+v err=%v", v, err)
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
