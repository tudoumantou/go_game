// internal/game/module/skill/skill_test.go —— 技能模块单测：学习/升级扣费/重复学习/落库形状。
//
// 统一取「带消耗」的第一个技能配置，保证升级路径能真正验证扣费。
package skill

import (
	"encoding/json"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

type fakeState struct {
	id    int64
	level int32
	items map[int32]int64
	ext   map[string]json.RawMessage
	biz   map[string]bool
}

func newFakeState(id int64) *fakeState {
	return &fakeState{id: id, level: 1, items: map[int32]int64{}, ext: map[string]json.RawMessage{}, biz: map[string]bool{}}
}

func (f *fakeState) PlayerID() int64               { return f.id }
func (f *fakeState) Profile() module.Profile       { return module.Profile{Level: f.level} }
func (f *fakeState) ExpMax() int64                 { return 0 }
func (f *fakeState) GetAttr(string) int64          { return 0 }
func (f *fakeState) SetAttr(string, int64)         {}
func (f *fakeState) AddBonus(string, int64)        {}
func (f *fakeState) ClearBonus()                   {}
func (f *fakeState) GetItemCount(id int32) int64   { return f.items[id] }
func (f *fakeState) Items() map[int32]int64        { return f.items }
func (f *fakeState) AddItem(id int32, count int64) { f.items[id] += count }
func (f *fakeState) RemoveItem(id int32, count int64) bool {
	if f.items[id] < count {
		return false
	}
	f.items[id] -= count
	return true
}
func (f *fakeState) MarkDirty(string)                {}
func (f *fakeState) SetSceneID(int32)                {}
func (f *fakeState) Ext(name string) json.RawMessage { return f.ext[name] }
func (f *fakeState) SetExt(name string, raw json.RawMessage) {
	f.ext[name] = raw
}
func (f *fakeState) CanHold([]module.ItemGrant) bool { return true }

func (f *fakeState) ClaimBiz(key string) bool {
	if f.biz[key] {
		return false
	}
	f.biz[key] = true
	return true
}
func (f *fakeState) HasBiz(key string) bool { return f.biz[key] }

func newCtx(st *fakeState) *module.Context {
	return &module.Context{
		State:        st,
		CacheEnabled: true,
		Send:         func(uint32, []byte) error { return nil },
	}
}

func firstSkill(t *testing.T) gamedata.SkillCfg {
	t.Helper()
	for _, cfg := range gamedata.SkillCfgList() {
		if cfg.Col3 > 0 {
			return cfg // 带消耗的技能，才能验证扣费
		}
	}
	t.Fatal("no paid skill configured")
	return gamedata.SkillCfg{}
}

// TestLearnAndUpgrade 学习落快照；升级扣材料并提升等级。
func TestLearnAndUpgrade(t *testing.T) {
	cfg := firstSkill(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)

	learn, _ := proto.Marshal(&gamev1.SkillLearnReq{SkillId: int32(cfg.Id)})
	if err := m.OnMessage(ctx, msgIDLearnReq, learn); err != nil {
		t.Fatal(err)
	}
	d, _ := load(st)
	if d.Levels[int32(cfg.Id)] != 1 {
		t.Fatalf("learn not persisted: %v", d.Levels)
	}

	st.items[upgradeItemID] = 1000
	upgrade, _ := proto.Marshal(&gamev1.SkillUpgradeReq{SkillId: int32(cfg.Id)})
	if err := m.OnMessage(ctx, msgIDUpgradeReq, upgrade); err != nil {
		t.Fatal(err)
	}
	d, _ = load(st)
	if d.Levels[int32(cfg.Id)] != 2 {
		t.Fatalf("upgrade not persisted: %v", d.Levels)
	}
	want := 1000 - int64(cfg.Col3)
	if st.GetItemCount(upgradeItemID) != want {
		t.Fatalf("cost not deducted: got %d want %d", st.GetItemCount(upgradeItemID), want)
	}
}

// TestLearnDuplicate 重复学习不重置等级。
func TestLearnDuplicate(t *testing.T) {
	cfg := firstSkill(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)
	if err := save(st, st.PlayerID(), &Data{Levels: map[int32]int32{int32(cfg.Id): 3}}); err != nil {
		t.Fatal(err)
	}
	learn, _ := proto.Marshal(&gamev1.SkillLearnReq{SkillId: int32(cfg.Id)})
	if err := m.OnMessage(ctx, msgIDLearnReq, learn); err != nil {
		t.Fatal(err)
	}
	d, _ := load(st)
	if d.Levels[int32(cfg.Id)] != 3 {
		t.Fatalf("level reset by duplicate learn: %v", d.Levels)
	}
}

// TestUpgradeWithoutResource 材料不足时不升级、不扣费。
func TestUpgradeWithoutResource(t *testing.T) {
	cfg := firstSkill(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)
	if err := save(st, st.PlayerID(), &Data{Levels: map[int32]int32{int32(cfg.Id): 1}}); err != nil {
		t.Fatal(err)
	}
	upgrade, _ := proto.Marshal(&gamev1.SkillUpgradeReq{SkillId: int32(cfg.Id)})
	if err := m.OnMessage(ctx, msgIDUpgradeReq, upgrade); err != nil {
		t.Fatal(err)
	}
	d, _ := load(st)
	if d.Levels[int32(cfg.Id)] != 1 {
		t.Fatal("level changed without resource")
	}
	if st.GetItemCount(upgradeItemID) != 0 {
		t.Fatal("item deducted on failed upgrade")
	}
}

// TestSnapshotShape 落库形状 {"player_id":..,"skills":"{...}"}。
func TestSnapshotShape(t *testing.T) {
	st := newFakeState(9)
	if err := save(st, st.PlayerID(), &Data{Levels: map[int32]int32{101: 4}}); err != nil {
		t.Fatal(err)
	}
	var row struct {
		PlayerId int64
		Skills   string
	}
	if err := json.Unmarshal(st.Ext(stateKey), &row); err != nil {
		t.Fatal(err)
	}
	if row.PlayerId != 9 || row.Skills != `{"101":4}` {
		t.Fatalf("unexpected row: %+v", row)
	}
}
