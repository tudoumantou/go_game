// internal/game/module/activity/activity_test.go —— 活动模块单测：领取幂等、进度截断。
//
// fakeState 的 Profile 固定 10 级；用例直接写快照进度，绕过事件累积。
package activity

import (
	"encoding/json"
	"fmt"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type fakeState struct {
	id    int64
	items map[int32]int64
	ext   map[string]json.RawMessage
	biz   map[string]bool
}

func newFakeState(id int64) *fakeState {
	return &fakeState{id: id, items: map[int32]int64{}, ext: map[string]json.RawMessage{}, biz: map[string]bool{}}
}

func (f *fakeState) PlayerID() int64               { return f.id }
func (f *fakeState) Profile() module.Profile       { return module.Profile{Level: 10} }
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
	return &module.Context{State: st, CacheEnabled: true, Send: func(uint32, []byte) error { return nil }}
}

func firstActivity(t *testing.T) gamedata.ActivityCfg {
	t.Helper()
	require.NotEmpty(t, gamedata.ActivityCfgList(), "activity config is empty")
	return gamedata.ActivityCfgList()[0]
}

// TestClaimOnce 达标领奖一次，重复领取不再发放。
func TestClaimOnce(t *testing.T) {
	cfg := firstActivity(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil, nil)
	if err := save(st, st.PlayerID(), &Data{Records: map[int32]Record{
		int32(cfg.Id): {Progress: int32(cfg.Col3)},
	}}); err != nil {
		t.Fatal(err)
	}
	body, _ := proto.Marshal(&gamev1.ActivityClaimReq{ActivityId: int32(cfg.Id)})
	for i := 0; i < 2; i++ {
		err := m.OnMessage(ctx, msgIDClaimReq, body)
		require.NoError(t, err)
	}
	want := int64(cfg.Col3) * 20
	require.EqualValues(t, want, st.GetItemCount(itemDiamond))
	d, _ := load(st)
	require.NotEqualValues(t, 0, d.Records[int32(cfg.Id)].RewardMask&1, "claim fact not persisted in snapshot")
}

// TestClaimUnfinished 进度不足不发奖。
func TestClaimUnfinished(t *testing.T) {
	cfg := firstActivity(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil, nil)
	body, _ := proto.Marshal(&gamev1.ActivityClaimReq{ActivityId: int32(cfg.Id)})
	err := m.OnMessage(ctx, msgIDClaimReq, body)
	require.NoError(t, err)
	require.EqualValues(t, 0, st.GetItemCount(itemDiamond), "unfinished activity must not grant")
}

// TestEventClampsProgress 事件推进不超过目标值。
func TestEventClampsProgress(t *testing.T) {
	cfg := firstActivity(t)
	if cfg.Col2 == "" || cfg.Col3 <= 0 {
		t.Skip("activity config lacks event type")
	}
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil, nil)
	m.OnEvent(ctx, module.Event{Type: cfg.Col2, Delta: int64(cfg.Col3) + 100})
	d, _ := load(st)
	got := d.Records[int32(cfg.Id)].Progress
	require.EqualValues(t, int32(cfg.Col3), got, fmt.Sprintf("progress not clamped: %d", got))
}
