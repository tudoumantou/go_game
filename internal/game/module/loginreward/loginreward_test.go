package loginreward

import (
	"encoding/json"
	"testing"
	"time"

	"game_study/internal/game/module"
)

// fakeState 是 module.PlayerState 的内存实现，只保留礼包用到的能力。
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
func (f *fakeState) Profile() module.Profile       { return module.Profile{Level: 1} }
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
	}
}

// day 返回北京时间某天的正午，避免边界歧义。
func day(n int) time.Time {
	return time.Date(2026, 10, 20, 12, 0, 0, 0, businessZone).AddDate(0, 0, n)
}

// TestFirstLoginGrants 首次登录发第 1 档，连续天数=1。
func TestFirstLoginGrants(t *testing.T) {
	st := newFakeState(1)
	m := New(nil, func() time.Time { return day(0) })
	if err := m.OnLogin(newCtx(st)); err != nil {
		t.Fatal(err)
	}
	if st.GetItemCount(1001) != 100 {
		t.Fatalf("first day reward: %d", st.GetItemCount(1001))
	}
	d, _ := load(st)
	if d.Streak != 1 || d.LastDay != businessDay(day(0)) {
		t.Fatalf("state: %+v", d)
	}
}

// TestConsecutiveStreak 连续第二天发第 2 档。
func TestConsecutiveStreak(t *testing.T) {
	st := newFakeState(1)
	ctx := newCtx(st)
	// 第一天
	if err := New(nil, func() time.Time { return day(0) }).OnLogin(ctx); err != nil {
		t.Fatal(err)
	}
	// 第二天
	if err := New(nil, func() time.Time { return day(1) }).OnLogin(ctx); err != nil {
		t.Fatal(err)
	}
	if st.GetItemCount(1002) != 5 {
		t.Fatalf("second day reward: %d", st.GetItemCount(1002))
	}
	d, _ := load(st)
	if d.Streak != 2 {
		t.Fatalf("streak: %d", d.Streak)
	}
}

// TestSameDayReconnectNoDoubleGrant 同一天重连（OnLogin 再次触发）不重复发奖。
func TestSameDayReconnectNoDoubleGrant(t *testing.T) {
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil, func() time.Time { return day(0) })
	for i := 0; i < 3; i++ {
		if err := m.OnLogin(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if st.GetItemCount(1001) != 100 {
		t.Fatalf("reconnect granted again: %d", st.GetItemCount(1001))
	}
	if len(st.biz) != 0 {
		t.Fatalf("login reward must not consume AppliedGrants: %v", st.biz)
	}
}

// TestBreakResetsStreak 断签一天后重置为第 1 档。
func TestBreakResetsStreak(t *testing.T) {
	st := newFakeState(1)
	ctx := newCtx(st)
	if err := New(nil, func() time.Time { return day(0) }).OnLogin(ctx); err != nil {
		t.Fatal(err)
	}
	// 跳过一天
	if err := New(nil, func() time.Time { return day(2) }).OnLogin(ctx); err != nil {
		t.Fatal(err)
	}
	d, _ := load(st)
	if d.Streak != 1 {
		t.Fatalf("streak should reset: %d", d.Streak)
	}
	if st.GetItemCount(1001) != 200 {
		t.Fatalf("day-1 reward should be granted again: %d", st.GetItemCount(1001))
	}
}

// TestBeyondMaxTier 超过最大档按最后一档发。
func TestBeyondMaxTier(t *testing.T) {
	st := newFakeState(1)
	if err := save(st, st.PlayerID(), &Data{LastDay: businessDay(day(0)), Streak: 2}); err != nil {
		t.Fatal(err)
	}
	if err := New(nil, func() time.Time { return day(1) }).OnLogin(newCtx(st)); err != nil {
		t.Fatal(err)
	}
	if st.GetItemCount(1002) != 5 {
		t.Fatalf("beyond max tier reward: %d", st.GetItemCount(1002))
	}
}

// TestParseRewardRejectsBadConfig 配置格式错误在 Init 期暴露。
func TestParseRewardRejectsBadConfig(t *testing.T) {
	for _, s := range []string{"", "1001", "1001,", ",5", "abc,1", "1001,0", "99999999,1"} {
		if _, err := parseReward(s); err == nil {
			t.Fatalf("bad config accepted: %q", s)
		}
	}
}
