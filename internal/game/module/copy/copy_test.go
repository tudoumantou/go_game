package copy

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
	return &fakeState{id: id, level: 99, items: map[int32]int64{}, ext: map[string]json.RawMessage{}, biz: map[string]bool{}}
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
func (f *fakeState) Ext(name string) json.RawMessage { return f.ext[name] }
func (f *fakeState) SetExt(name string, raw json.RawMessage) {
	f.ext[name] = raw
}
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
		AddExp:       func(int64) (bool, int32) { return false, 1 },
	}
}

func firstCopy(t *testing.T) gamedata.CopyCfg {
	t.Helper()
	if len(gamedata.CopyCfgList) == 0 {
		t.Fatal("copy config is empty")
	}
	return gamedata.CopyCfgList[0]
}

// TestSettleFirstPassGrantsOnce 首通发奖一次，重复通关只累加次数。
func TestSettleFirstPassGrantsOnce(t *testing.T) {
	cfg := firstCopy(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)
	body, _ := proto.Marshal(&gamev1.CopySettleReq{CopyId: int32(cfg.Id), Star: 2})

	if err := m.OnMessage(ctx, msgIDSettleReq, body); err != nil {
		t.Fatal(err)
	}
	drops := 0
	for _, drop := range gamedata.CopyDropCfgList {
		if drop.Col1 == cfg.Id {
			drops++
		}
	}
	if drops == 0 {
		t.Skip("no drop configured for first copy")
	}

	before := map[int32]int64{}
	for id, n := range st.items {
		before[id] = n
	}
	if err := m.OnMessage(ctx, msgIDSettleReq, body); err != nil {
		t.Fatal(err)
	}
	for id, n := range st.items {
		if n != before[id] {
			t.Fatalf("duplicate settle granted again: item %d %d -> %d", id, before[id], n)
		}
	}
	d, _ := load(st)
	rec := d.Records[int32(cfg.Id)]
	if rec.PassCount != 2 || !rec.FirstPass || rec.Star != 2 {
		t.Fatalf("record not updated: %+v", rec)
	}
}

// TestStarKeepsBest 星级取历史最佳，不回退。
func TestStarKeepsBest(t *testing.T) {
	cfg := firstCopy(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)
	high, _ := proto.Marshal(&gamev1.CopySettleReq{CopyId: int32(cfg.Id), Star: 3})
	low, _ := proto.Marshal(&gamev1.CopySettleReq{CopyId: int32(cfg.Id), Star: 1})
	if err := m.OnMessage(ctx, msgIDSettleReq, high); err != nil {
		t.Fatal(err)
	}
	if err := m.OnMessage(ctx, msgIDSettleReq, low); err != nil {
		t.Fatal(err)
	}
	d, _ := load(st)
	if got := d.Records[int32(cfg.Id)].Star; got != 3 {
		t.Fatalf("star regressed to %d", got)
	}
}

// TestLevelGate 等级不足不结算、不发奖。
func TestLevelGate(t *testing.T) {
	var cfg gamedata.CopyCfg
	for _, c := range gamedata.CopyCfgList {
		if c.Col2 > 1 {
			cfg = c
			break
		}
	}
	if cfg.Id == 0 {
		t.Skip("no copy with level gate configured")
	}
	st := newFakeState(1)
	st.level = 1
	ctx := newCtx(st)
	m := New(nil)
	body, _ := proto.Marshal(&gamev1.CopySettleReq{CopyId: int32(cfg.Id), Star: 3})
	if err := m.OnMessage(ctx, msgIDSettleReq, body); err != nil {
		t.Fatal(err)
	}
	d, _ := load(st)
	if _, ok := d.Records[int32(cfg.Id)]; ok {
		t.Fatal("settle must be rejected below required level")
	}
}
