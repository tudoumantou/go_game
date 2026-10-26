// internal/game/module/bag/bag_test.go —— 背包模块单测
//
// 道具是玩家资产，扣减路径的每条分支都必须可验证：
// 扣多了是刷道具，扣少了是贪污，不扣是白拿。
//
// 全部用例纯内存：道具直接放在 moduletest.State 里，配置走 gamedata 快照。
package bag

import (
	"fmt"
	"sort"
	"testing"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	itemCoin   int32 = 1001 // Col3=1：单次最多用 1 个
	itemPotion int32 = 1002 // Col3=10：单次最多用 10 个
)

func newTestModule() (*Module, *moduletest.State, *moduletest.Recorder, *module.Context) {
	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	m := New(log.New("bag-test", log.WithLevel("error")))
	return m, st, rec, moduletest.NewContext(st, rec)
}

func use(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, itemID, count int64) *gamev1.UseItemResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgUseItemReq, codec.MsgUseItemResp, &gamev1.UseItemReq{ItemUid: itemID, Count: int32(count)})
	require.NoError(t, err)
	require.NotNil(t, raw, "没有回包")
	resp := &gamev1.UseItemResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

// TestUseDeductsAndReportsRemain 正常使用：扣掉指定数量、回报剩余、发出事件。
func TestUseDeductsAndReportsRemain(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(itemPotion, 5)

	var events []module.Event
	ctx.Emit = func(e module.Event) { events = append(events, e) }

	resp := use(t, m, ctx, rec, int64(itemPotion), 3)
	require.EqualValues(t, int32(errcode.OK), resp.Code, fmt.Sprintf("use: %+v", resp))
	require.EqualValues(t, 2, resp.RemainCount)
	require.EqualValues(t, 2, st.GetItemCount(itemPotion), "库存必须真的扣掉")
	require.Len(t, events, 1, "使用道具要发事件驱动任务/成就")
	require.EqualValues(t, "use_item", events[0].Type)
	require.EqualValues(t, fmt.Sprintf("%d", itemPotion), events[0].Key)
	require.EqualValues(t, 3, events[0].Delta)
	require.Equal(t, 1, rec.Count(codec.MsgUseItemResp), "一次请求只能回一个包")
}

// TestUseClampsToConfigLimit 超过单次上限时截断而不是报错（与客户端批量使用体验一致）。
func TestUseClampsToConfigLimit(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(itemPotion, 20)

	resp := use(t, m, ctx, rec, int64(itemPotion), 99) // 上限 10
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 10, resp.RemainCount, "应按上限扣 10 个")
	require.EqualValues(t, 10, st.GetItemCount(itemPotion))
}

// TestUseDefaultsToOne 数量 <=0 时按 1 个处理。
func TestUseDefaultsToOne(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(itemCoin, 3)

	resp := use(t, m, ctx, rec, int64(itemCoin), 0)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 2, st.GetItemCount(itemCoin))
	require.EqualValues(t, 2, resp.RemainCount)
}

// TestUseNotEnough 库存不足：回 3002，一点都不能扣。
func TestUseNotEnough(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(itemPotion, 2)

	resp := use(t, m, ctx, rec, int64(itemPotion), 5)
	require.EqualValues(t, int32(errcode.ErrItemNotEnough), resp.Code, fmt.Sprintf("got %+v", resp))
	require.EqualValues(t, 2, st.GetItemCount(itemPotion), "失败请求不能部分扣减")
}

// TestUseUnknownItem 未知道具回 3001，且不碰库存。
func TestUseUnknownItem(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(itemPotion, 5)

	resp := use(t, m, ctx, rec, 9999, 1)
	require.EqualValues(t, int32(errcode.ErrItemNotExist), resp.Code)
	require.EqualValues(t, 5, st.GetItemCount(itemPotion))
}

// TestUseWithoutSubscriber 没有事件订阅方（Emit 为 nil）时不能 panic。
func TestUseWithoutSubscriber(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(itemCoin, 2)
	ctx.Emit = nil

	require.NotPanics(t, func() {
		resp := use(t, m, ctx, rec, int64(itemCoin), 1)
		require.EqualValues(t, int32(errcode.OK), resp.Code)
	})
}

// TestListSkipsNonPositive 列表过滤掉 0/负数脏数据，避免客户端显示「0 个金币」。
func TestListSkipsNonPositive(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(itemCoin, 10)
	st.AddItem(itemPotion, 3)
	st.AddItem(2001, 0)  // 脏数据
	st.AddItem(2002, -1) // 脏数据

	raw, err := moduletest.Call(m, ctx, rec, codec.MsgBagListReq, codec.MsgBagListResp, &gamev1.BagListReq{})
	require.NoError(t, err)
	resp := &gamev1.BagListResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, resp.Items, 2, fmt.Sprintf("items: %+v", resp.Items))

	// map 遍历顺序不定，按 ItemId 排一下再断言。
	sort.Slice(resp.Items, func(i, j int) bool { return resp.Items[i].ItemId < resp.Items[j].ItemId })
	require.EqualValues(t, itemCoin, resp.Items[0].ItemId)
	require.EqualValues(t, 10, resp.Items[0].Count)
	require.EqualValues(t, itemPotion, resp.Items[1].ItemId)
	require.EqualValues(t, 3, resp.Items[1].Count)
	require.Equal(t, 1, rec.Count(codec.MsgBagListResp), "一次请求只能回一个包")
}
