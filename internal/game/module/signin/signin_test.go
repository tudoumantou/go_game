// internal/game/module/signin/signin_test.go —— 签到模块单测：周几映射、状态编解码、领取幂等。
//
// 全部用例不碰数据库：fakeState 在内存里模拟 Ext 槽位，验证「领取事实 + 道具」
// 是否按设计落在同一份快照形状里。
package signin

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
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
func call(t *testing.T, m module.Module, ctx *module.Context, rec *recorder, msgID uint32, req proto.Message) []byte {
	t.Helper()
	body, err := proto.Marshal(req)
	require.NoError(t, err)
	err = m.OnMessage(ctx, msgID, body)
	require.NoError(t, err)
	raw, ok := rec.last[msgID+1]
	require.True(t, ok)
	return raw
}

// TestWeekdayOf 验证北京时间下的周一至周日映射，与进程时区无关。
func TestWeekdayOf(t *testing.T) {
	// 2026-10-19 是周一，2026-10-25 是周日。
	monday := time.Date(2026, 10, 19, 10, 0, 0, 0, businessZone)
	for i := 0; i < 7; i++ {
		got, want := weekdayOf(monday.Add(time.Duration(i)*24*time.Hour)), int32(i+1)
		require.EqualValues(t, want, got, fmt.Sprintf("day +%d: got %d want %d", i, got, want))
	}
	// 北京周一 00:30 在 UTC 仍是周日 16:30，必须按北京时间算周一。
	utc := time.Date(2026, 10, 18, 16, 30, 0, 0, time.UTC)
	got := weekdayOf(utc)
	require.EqualValues(t, 1, got, fmt.Sprintf("utc boundary: got %d want 1", got))
}

// TestDataAdd 验证天数升序、奖励累加、同一天不重复登记。
func TestDataAdd(t *testing.T) {
	d := &Data{}
	d.add(3, []module.ItemGrant{{ItemID: 1001, Count: 30}})
	d.add(1, []module.ItemGrant{{ItemID: 1001, Count: 10}})
	require.Len(t, d.Days, 2, fmt.Sprintf("days not sorted: %v", d.Days))
	require.EqualValues(t, 1, d.Days[0])
	require.EqualValues(t, 3, d.Days[1])
	require.EqualValues(t, 40, d.Rewards[1001])
	// 同一天再次登记不重复天数（重领由 claim 的 has 拦截，这里兜底）。
	d.add(3, nil)
	require.Len(t, d.Days, 2, fmt.Sprintf("duplicate day registered: %v %v", d.Days, d.Rewards))
	require.EqualValues(t, 40, d.Rewards[1001])
}

// TestStateRoundTrip 验证两段编解码：
//   - 业务结构 ↔ Ext（基座负责，存的是 Data 的 JSON）；
//   - 业务结构 ↔ 表行（persist.go 的 toRow/fromRow）。
func TestStateRoundTrip(t *testing.T) {
	st := newFakeState(7)
	ctx, _ := newCtx(st)
	m := M

	// 业务只调 Repo：Get 解码 → 改 → Save 编码并标脏。
	biz := m.NewCtx(ctx)
	d, err := biz.Repo.Get()
	require.NoError(t, err)
	d.add(2, []module.ItemGrant{{ItemID: 1001, Count: 20}})
	require.NoError(t, biz.Repo.Save(d))

	// Ext 里存的是业务结构（而不是表行），业务不必知道表长什么样。
	raw := st.Ext(stateKey)
	var decoded Data
	err = json.Unmarshal(raw, &decoded)
	require.NoError(t, err, fmt.Sprintf("Ext 不是 Data 结构: %v (%s)", err, raw))
	require.Len(t, decoded.Days, 1, fmt.Sprintf("Ext 内容不符: %+v", decoded))
	require.EqualValues(t, 2, decoded.Days[0])
	require.EqualValues(t, 20, decoded.Rewards[1001])

	// 落库：业务结构 → 表行 → 业务结构。
	row, err := toRow(7, &decoded)
	require.NoError(t, err)
	require.EqualValues(t, 7, row.PlayerId, fmt.Sprintf("行结构与表不符: %+v", row))
	require.EqualValues(t, "[2]", row.ClaimedDays)
	require.EqualValues(t, `{"1001":20}`, row.ClaimedRewards)
	back, err := fromRow(row)
	require.NoError(t, err, fmt.Sprintf("行回读失败: %+v %v", back, err))
	require.Len(t, back.Days, 1)
	require.EqualValues(t, 20, back.Rewards[1001])
}

// TestClaimFlow 验证领取发奖、重复领取幂等、状态汇总与回包内容。
func TestClaimFlow(t *testing.T) {
	st := newFakeState(1)
	ctx, rec := newCtx(st)
	M.SetClock(func() time.Time { return time.Date(2026, 10, 21, 9, 0, 0, 0, businessZone) }) // 周三
	m := M

	// 信息：未领取，今天=3。
	info := &gamev1.SigninInfoResp{}
	err := proto.Unmarshal(call(t, m, ctx, rec, codec.MsgSigninInfoReq, &gamev1.SigninInfoReq{}), info)
	require.NoError(t, err)
	require.False(t, info.Code != 0 || info.Today != 3 || info.ClaimedToday || len(info.ClaimedDays) != 0, fmt.Sprintf("info: %+v", info))

	// 首次领取：拿到第 3 天奖励，状态与背包同步更新。
	resp := &gamev1.SigninClaimResp{}
	err = proto.Unmarshal(call(t, m, ctx, rec, codec.MsgSigninClaimReq, &gamev1.SigninClaimReq{}), resp)
	require.NoError(t, err)
	require.False(t, resp.Code != 0 || resp.Already || resp.Day != 3 || len(resp.Rewards) == 0, fmt.Sprintf("claim: %+v", resp))
	require.Len(t, resp.ClaimedDays, 1, fmt.Sprintf("claim state: days=%v rewards=%v", resp.ClaimedDays, resp.ClaimedRewards))
	require.EqualValues(t, 3, resp.ClaimedDays[0])
	require.EqualValues(t, 300, resp.ClaimedRewards[1001])
	require.EqualValues(t, 300, st.GetItemCount(1001))

	// 重复领取：幂等，不叠加奖励。
	dup := &gamev1.SigninClaimResp{}
	err = proto.Unmarshal(call(t, m, ctx, rec, codec.MsgSigninClaimReq, &gamev1.SigninClaimReq{}), dup)
	require.NoError(t, err)
	require.False(t, !dup.Already || dup.ClaimedRewards[1001] != 300, fmt.Sprintf("duplicate claim: %+v", dup))
	require.EqualValues(t, 300, st.GetItemCount(1001))
}

// TestRewardConfiguration 七天配置齐全，缺天在启动期暴露。
func TestRewardConfiguration(t *testing.T) {
	for day := int32(1); day <= 7; day++ {
		items, err := rewardsForDay(day)
		require.NoError(t, err, fmt.Sprintf("day %d: %v", day, err))
		require.NotEmpty(t, items)
	}
	_, err := rewardsForDay(8)
	require.Error(t, err, "day 8 must not have rewards")
}
