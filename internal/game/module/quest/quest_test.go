// internal/game/module/quest/quest_test.go —— 任务模块单测：接取→事件推进→领奖→幂等。
//
// 事件类型与目标次数直接取自配置首行，XML 变更时用例自动跟随，不写死数值。
package quest

import (
	"encoding/json"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// fakeState 是 module.PlayerState 的内存实现，只保留任务模块用到的能力。
type fakeState struct {
	id    int64
	items map[int32]int64
	ext   map[string]json.RawMessage
	biz   map[string]bool
	exp   int64
}

func newFakeState(id int64) *fakeState {
	return &fakeState{id: id, items: map[int32]int64{}, ext: map[string]json.RawMessage{}, biz: map[string]bool{}}
}

func (f *fakeState) PlayerID() int64               { return f.id }
func (f *fakeState) Profile() module.Profile       { return module.Profile{Exp: f.exp} }
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

func newCtx(st *fakeState) (*module.Context, *[]uint32) {
	var replies []uint32
	return &module.Context{
		State:        st,
		CacheEnabled: true,
		Send: func(id uint32, _ []byte) error {
			replies = append(replies, id)
			return nil
		},
		AddExp: func(n int64) (bool, int32) {
			st.exp += n
			return false, 1
		},
	}, &replies
}

func firstQuest(t *testing.T) gamedata.QuestCfg {
	t.Helper()
	if len(gamedata.QuestCfgList()) == 0 {
		t.Fatal("quest config is empty")
	}
	return gamedata.QuestCfgList()[0]
}

// TestAcceptProgressClaim 接取 → 事件推进 → 领奖，状态与奖励都落在快照里。
func TestAcceptProgressClaim(t *testing.T) {
	cfg := firstQuest(t)
	st := newFakeState(1)
	ctx, _ := newCtx(st)
	m := New(nil)

	acceptBody, _ := proto.Marshal(&gamev1.QuestAcceptReq{QuestId: int32(cfg.Id)})
	if err := m.OnMessage(ctx, msgIDAcceptReq, acceptBody); err != nil {
		t.Fatal(err)
	}
	// 事件推进（类型取自配置，增量给足）
	m.OnEvent(ctx, module.Event{Type: cfg.Col2, Delta: int64(cfg.Col3)})

	d, err := load(st)
	if err != nil {
		t.Fatal(err)
	}
	rec := d.Quests[int32(cfg.Id)]
	if rec.Status != statusDone || rec.Progress != int32(cfg.Col3) {
		t.Fatalf("progress not advanced: %+v", rec)
	}

	claimBody, _ := proto.Marshal(&gamev1.QuestClaimReq{QuestId: int32(cfg.Id)})
	if err := m.OnMessage(ctx, msgIDClaimReq, claimBody); err != nil {
		t.Fatal(err)
	}
	if st.GetItemCount(1001) != int64(cfg.Col3)*10 {
		t.Fatalf("item not granted: %d", st.GetItemCount(1001))
	}
	if st.exp != int64(cfg.Col3)*100 {
		t.Fatalf("exp not granted: %d", st.exp)
	}
	d, _ = load(st)
	if d.Quests[int32(cfg.Id)].Status != statusClaimed {
		t.Fatal("claim state not persisted in snapshot")
	}
}

// TestClaimIdempotent 重复领奖只返回已领，不再发奖。
func TestClaimIdempotent(t *testing.T) {
	cfg := firstQuest(t)
	st := newFakeState(1)
	ctx, _ := newCtx(st)
	m := New(nil)

	body, _ := proto.Marshal(&gamev1.QuestClaimReq{QuestId: int32(cfg.Id)})
	d := &Data{Quests: map[int32]Record{int32(cfg.Id): {Status: statusDone, Progress: int32(cfg.Col3)}}}
	if err := save(st, st.PlayerID(), d); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.OnMessage(ctx, msgIDClaimReq, body); err != nil {
			t.Fatal(err)
		}
	}
	if st.GetItemCount(1001) != int64(cfg.Col3)*10 {
		t.Fatalf("duplicate claim granted again: %d", st.GetItemCount(1001))
	}
}

// TestClaimUnfinished 进度不足时不发奖、不改状态。
func TestClaimUnfinished(t *testing.T) {
	cfg := firstQuest(t)
	st := newFakeState(1)
	ctx, _ := newCtx(st)
	m := New(nil)
	d := &Data{Quests: map[int32]Record{int32(cfg.Id): {Status: statusDoing}}}
	if err := save(st, st.PlayerID(), d); err != nil {
		t.Fatal(err)
	}
	body, _ := proto.Marshal(&gamev1.QuestClaimReq{QuestId: int32(cfg.Id)})
	if err := m.OnMessage(ctx, msgIDClaimReq, body); err != nil {
		t.Fatal(err)
	}
	if st.GetItemCount(1001) != 0 {
		t.Fatal("unfinished quest must not grant")
	}
	back, _ := load(st)
	if back.Quests[int32(cfg.Id)].Status != statusDoing {
		t.Fatal("status changed on failed claim")
	}
}

// TestSnapshotShape 落库形状是 {"quests":{...}}，与 model.PlayerQuest.Quests 对应。
func TestSnapshotShape(t *testing.T) {
	st := newFakeState(9)
	d := &Data{Quests: map[int32]Record{101: {Status: statusClaimed, Progress: 5}}}
	if err := save(st, st.PlayerID(), d); err != nil {
		t.Fatal(err)
	}
	var row struct {
		PlayerId int64
		Quests   string
	}
	if err := json.Unmarshal(st.Ext(stateKey), &row); err != nil {
		t.Fatal(err)
	}
	if row.PlayerId != 9 || row.Quests != `{"101":{"status":2,"progress":5}}` {
		t.Fatalf("unexpected row: %+v", row)
	}
}
