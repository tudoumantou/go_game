package signin

import (
	"encoding/json"
	"testing"
	"time"

	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// fakeState 是 module.PlayerState 的最小内存实现，够验证领取与落库形状。
type fakeState struct {
	id    int64
	items map[int32]int64
	ext   map[string]json.RawMessage
	dirty []string
	biz   map[string]bool
}

func newFakeState(id int64) *fakeState {
	return &fakeState{id: id, items: map[int32]int64{}, ext: map[string]json.RawMessage{}, biz: map[string]bool{}}
}

func (f *fakeState) PlayerID() int64                 { return f.id }
func (f *fakeState) Profile() module.Profile         { return module.Profile{} }
func (f *fakeState) ExpMax() int64                   { return 0 }
func (f *fakeState) GetAttr(string) int64            { return 0 }
func (f *fakeState) SetAttr(string, int64)           {}
func (f *fakeState) AddBonus(string, int64)          {}
func (f *fakeState) ClearBonus()                     {}
func (f *fakeState) GetItemCount(id int32) int64     { return f.items[id] }
func (f *fakeState) Items() map[int32]int64          { return f.items }
func (f *fakeState) AddItem(id int32, count int64)   { f.items[id] += count }
func (f *fakeState) MarkDirty(name string)           { f.dirty = append(f.dirty, name) }
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
func (f *fakeState) RemoveItem(id int32, count int64) bool {
	if f.items[id] < count {
		return false
	}
	f.items[id] -= count
	return true
}

// recorder 按消息号保存最近一次下行，供测试断言回包内容。
type recorder struct{ last map[uint32][]byte }

func (r *recorder) send(id uint32, body []byte) error {
	r.last[id] = body
	return nil
}

func newCtx(st module.PlayerState) (*module.Context, *recorder) {
	r := &recorder{last: map[uint32][]byte{}}
	return &module.Context{State: st, CacheEnabled: true, Send: r.send}, r
}

// call 发送一次请求，返回 msgID+1 对应的回包。
func call(t *testing.T, m *Module, ctx *module.Context, rec *recorder, msgID uint32, req proto.Message) []byte {
	t.Helper()
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.OnMessage(ctx, msgID, body); err != nil {
		t.Fatal(err)
	}
	raw, ok := rec.last[msgID+1]
	if !ok {
		t.Fatalf("no reply for msg %d", msgID)
	}
	return raw
}

// TestWeekdayOf 验证北京时间下的周一至周日映射，与进程时区无关。
func TestWeekdayOf(t *testing.T) {
	// 2026-10-19 是周一，2026-10-25 是周日。
	monday := time.Date(2026, 10, 19, 10, 0, 0, 0, businessZone)
	for i := 0; i < 7; i++ {
		if got, want := weekdayOf(monday.Add(time.Duration(i)*24*time.Hour)), int32(i+1); got != want {
			t.Fatalf("day +%d: got %d want %d", i, got, want)
		}
	}
	// 北京周一 00:30 在 UTC 仍是周日 16:30，必须按北京时间算周一。
	utc := time.Date(2026, 10, 18, 16, 30, 0, 0, time.UTC)
	if got := weekdayOf(utc); got != 1 {
		t.Fatalf("utc boundary: got %d want 1", got)
	}
}

// TestDataAdd 验证天数升序、奖励累加、同一天不重复登记。
func TestDataAdd(t *testing.T) {
	d := &Data{}
	d.add(3, []module.ItemGrant{{ItemID: 1001, Count: 30}})
	d.add(1, []module.ItemGrant{{ItemID: 1001, Count: 10}})
	if len(d.Days) != 2 || d.Days[0] != 1 || d.Days[1] != 3 {
		t.Fatalf("days not sorted: %v", d.Days)
	}
	if d.Rewards[1001] != 40 {
		t.Fatalf("rewards not accumulated: %v", d.Rewards)
	}
	// 同一天再次登记不重复天数（重领由 claim 的 has 拦截，这里兜底）。
	d.add(3, nil)
	if len(d.Days) != 2 || d.Rewards[1001] != 40 {
		t.Fatalf("duplicate day registered: %v %v", d.Days, d.Rewards)
	}
}

// TestLoadSaveRoundTrip 验证表行与业务视图的编解码，空状态落库为合法 JSON。
func TestLoadSaveRoundTrip(t *testing.T) {
	st := newFakeState(7)
	d, err := load(st)
	if err != nil || d == nil || len(d.Days) != 0 {
		t.Fatalf("empty load: %v %v", d, err)
	}
	d.add(2, []module.ItemGrant{{ItemID: 1001, Count: 20}})
	if err := save(st, st.PlayerID(), d); err != nil {
		t.Fatal(err)
	}
	row := st.Ext(stateKey)
	if len(row) == 0 {
		t.Fatal("state not written")
	}
	var decoded struct {
		ClaimedDays    string
		ClaimedRewards string
	}
	if err := json.Unmarshal(row, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ClaimedDays != "[2]" || decoded.ClaimedRewards != `{"1001":20}` {
		t.Fatalf("unexpected row shape: %+v", decoded)
	}
	back, err := load(st)
	if err != nil || len(back.Days) != 1 || back.Days[0] != 2 || back.Rewards[1001] != 20 {
		t.Fatalf("round trip: %+v %v", back, err)
	}
}

// TestClaimFlow 验证领取发奖、重复领取幂等、状态汇总与回包内容。
func TestClaimFlow(t *testing.T) {
	st := newFakeState(1)
	ctx, rec := newCtx(st)
	m := New(nil, func() time.Time { return time.Date(2026, 10, 21, 9, 0, 0, 0, businessZone) }) // 周三

	// 信息：未领取，今天=3。
	info := &gamev1.SigninInfoResp{}
	if err := proto.Unmarshal(call(t, m, ctx, rec, MsgInfoReq, &gamev1.SigninInfoReq{}), info); err != nil {
		t.Fatal(err)
	}
	if info.Code != 0 || info.Today != 3 || info.ClaimedToday || len(info.ClaimedDays) != 0 {
		t.Fatalf("info: %+v", info)
	}

	// 首次领取：拿到第 3 天奖励，状态与背包同步更新。
	resp := &gamev1.SigninClaimResp{}
	if err := proto.Unmarshal(call(t, m, ctx, rec, MsgClaimReq, &gamev1.SigninClaimReq{}), resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != 0 || resp.Already || resp.Day != 3 || len(resp.Rewards) == 0 {
		t.Fatalf("claim: %+v", resp)
	}
	if len(resp.ClaimedDays) != 1 || resp.ClaimedDays[0] != 3 || resp.ClaimedRewards[1001] != 300 {
		t.Fatalf("claim state: days=%v rewards=%v", resp.ClaimedDays, resp.ClaimedRewards)
	}
	if st.GetItemCount(1001) != 300 {
		t.Fatalf("item not granted: %d", st.GetItemCount(1001))
	}

	// 重复领取：幂等，不叠加奖励。
	dup := &gamev1.SigninClaimResp{}
	if err := proto.Unmarshal(call(t, m, ctx, rec, MsgClaimReq, &gamev1.SigninClaimReq{}), dup); err != nil {
		t.Fatal(err)
	}
	if !dup.Already || dup.ClaimedRewards[1001] != 300 {
		t.Fatalf("duplicate claim: %+v", dup)
	}
	if st.GetItemCount(1001) != 300 {
		t.Fatalf("duplicate grant: %d", st.GetItemCount(1001))
	}
}

// TestRewardConfiguration 七天配置齐全，缺天在启动期暴露。
func TestRewardConfiguration(t *testing.T) {
	for day := int32(1); day <= 7; day++ {
		items, err := rewardsForDay(day)
		if err != nil || len(items) == 0 {
			t.Fatalf("day %d: %v", day, err)
		}
	}
	if _, err := rewardsForDay(8); err == nil {
		t.Fatal("day 8 must not have rewards")
	}
}
