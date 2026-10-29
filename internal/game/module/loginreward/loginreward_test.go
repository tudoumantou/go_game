// internal/game/module/loginreward/loginreward_test.go —— 登录礼包单测：连续/断签/同天重连/超档。
//
// 时钟用 M.SetClock 注入固定时间（与 signin 同一手法），避免依赖真实日期
// 导致用例偶发失败。ctx 每个场景新建：真实运行时每条消息也是新 Context，
// 复用会让「模块时钟只在 ctx.Clock 为空时生效」的语义把第二次注入吃掉。
package loginreward

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/timeutil"
	"github.com/stretchr/testify/require"
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

// at 把模块时钟固定到某一天，并在用例结束时还原成系统时间。
func at(t *testing.T, d time.Time) {
	t.Helper()
	M.SetClock(func() time.Time { return d })
	t.Cleanup(func() { M.SetClock(nil) })
}

// login 用当前注入的时钟跑一次 OnLogin。
func login(t *testing.T, st *fakeState) error {
	t.Helper()
	return M.OnLogin(newCtx(st))
}

// loadData 读出玩家槽位里的礼包状态；空槽位返回零值。
func loadData(t *testing.T, st *fakeState) Data {
	t.Helper()
	raw := st.Ext(stateKey)
	if raw == nil {
		return Data{}
	}
	var d Data
	require.NoError(t, json.Unmarshal(raw, &d))
	return d
}

// day 返回北京时间某天的正午，避免边界歧义。
func day(n int) time.Time {
	return time.Date(2026, 10, 20, 12, 0, 0, 0, timeutil.BusinessZone).AddDate(0, 0, n)
}

// TestFirstLoginGrants 首次登录发第 1 档，连续天数=1。
func TestFirstLoginGrants(t *testing.T) {
	st := newFakeState(1)
	at(t, day(0))
	require.NoError(t, login(t, st))

	require.EqualValues(t, 100, st.GetItemCount(1001))
	d := loadData(t, st)
	require.EqualValues(t, 1, d.Streak, fmt.Sprintf("state: %+v", d))
	require.EqualValues(t, businessDay(day(0)), d.LastDay)
}

// TestConsecutiveStreak 连续第二天发第 2 档。
func TestConsecutiveStreak(t *testing.T) {
	st := newFakeState(1)

	at(t, day(0))
	require.NoError(t, login(t, st))
	at(t, day(1))
	require.NoError(t, login(t, st))

	require.EqualValues(t, 5, st.GetItemCount(1002))
	require.EqualValues(t, 2, loadData(t, st).Streak)
}

// TestSameDayReconnectNoDoubleGrant 同一天重连（OnLogin 再次触发）不重复发奖。
func TestSameDayReconnectNoDoubleGrant(t *testing.T) {
	st := newFakeState(1)
	at(t, day(0))
	for i := 0; i < 3; i++ {
		require.NoError(t, login(t, st))
	}
	require.EqualValues(t, 100, st.GetItemCount(1001))
	require.Len(t, st.biz, 0, "礼包幂等靠状态本身，不该占用业务键")
}

// TestBreakResetsStreak 断签一天后重置为第 1 档。
func TestBreakResetsStreak(t *testing.T) {
	st := newFakeState(1)

	at(t, day(0))
	require.NoError(t, login(t, st))
	at(t, day(2)) // 跳过一天
	require.NoError(t, login(t, st))

	require.EqualValues(t, 1, loadData(t, st).Streak)
	require.EqualValues(t, 200, st.GetItemCount(1001))
}

// TestBeyondMaxTier 超过最大档按最后一档发。
func TestBeyondMaxTier(t *testing.T) {
	st := newFakeState(1)
	// 手工塞一个「昨天已领、连续 2 天」的状态，模拟已连续登录两天的玩家。
	raw, err := json.Marshal(&Data{LastDay: businessDay(day(0)), Streak: 2})
	require.NoError(t, err)
	st.SetExt(stateKey, raw)

	at(t, day(1))
	require.NoError(t, login(t, st))
	require.EqualValues(t, 5, st.GetItemCount(1002))
}

// TestParseRewardRejectsBadConfig 配置格式错误在 Init 期暴露。
func TestParseRewardRejectsBadConfig(t *testing.T) {
	for _, s := range []string{"", "1001", "1001,", ",5", "abc,1", "1001,0", "99999999,1"} {
		_, err := parseReward(s)
		require.Error(t, err, fmt.Sprintf("bad config accepted: %q", s))
	}
}

// TestDataDecodesLegacyExtJSON 锁住「新结构能吃旧格式」的兼容约定。
//
// 迁移前 Ext 槽位存的是 model.PlayerLogin 的 JSON（无 json tag，字段名大写，
// 多一个 PlayerId）。这行 JSON 是线上真实形态，改动 Data 的字段名或加小写
// tag 会让所有老玩家的连续天数归零、当天奖励重发一次。用例失败时不要改
// 断言，改 Data 之前先写数据迁移。
func TestDataDecodesLegacyExtJSON(t *testing.T) {
	legacy := []byte(`{"PlayerId":42,"LastDay":20726,"Streak":7}`)
	var d Data
	require.NoError(t, json.Unmarshal(legacy, &d))
	require.EqualValues(t, 20726, d.LastDay)
	require.EqualValues(t, 7, d.Streak)
}
