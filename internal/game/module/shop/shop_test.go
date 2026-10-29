// internal/game/module/shop/shop_test.go —— 商店模块单测
//
// 购买是「扣钱 + 发货 + 计数」三步，任何一步顺序错了都会变成资损：
// 先发货后扣钱 → 白拿；扣了钱没发货 → 客服补偿。
// 用例按这个顺序验：预检失败不能扣钱、扣钱失败不能发货。
package shop

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/timeutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func firstGoods(t *testing.T) gamedata.ShopGoodsCfg {
	t.Helper()
	require.NotEmpty(t, gamedata.ShopGoodsCfgList(), "shop config is empty")
	return gamedata.ShopGoodsCfgList()[0]
}

func newTestModule() (*module.Bound[Data], *moduletest.State, *moduletest.Recorder, *module.Context) {
	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	m := M
	return m, st, rec, moduletest.NewContext(st, rec)
}

func buy(t *testing.T, m *module.Bound[Data], ctx *module.Context, rec *moduletest.Recorder, goodsID, count int64) *gamev1.ShopBuyResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgShopBuyReq, codec.MsgShopBuyResp, &gamev1.ShopBuyReq{GoodsId: int32(goodsID), Count: int32(count)})
	require.NoError(t, err)
	require.NotNil(t, raw, "没有回包")
	resp := &gamev1.ShopBuyResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

// TestBuyDeductsAndGrants 正常购买：钻石减少、道具增加、次数累加。
func TestBuyDeductsAndGrants(t *testing.T) {
	cfg := firstGoods(t)
	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 1000)

	resp := buy(t, m, ctx, rec, cfg.Id, 2)
	require.EqualValues(t, int32(errcode.OK), resp.Code, fmt.Sprintf("buy: %+v", resp))
	require.EqualValues(t, 2, resp.Bought)
	require.EqualValues(t, 1000-2*cfg.Col3, st.GetItemCount(module.ItemGold), "钻石必须按单价×数量扣")
	require.EqualValues(t, 2*cfg.Col4, st.GetItemCount(int32(cfg.Col2)), "道具按数量发放")
	require.Equal(t, 1, rec.Count(codec.MsgShopBuyResp), "一次请求只能回一个包")
}

// TestBuyNotEnoughDiamond 钻石不足：不发货、次数不变。
func TestBuyNotEnoughDiamond(t *testing.T) {
	cfg := firstGoods(t)
	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, cfg.Col3-1) // 差一个

	resp := buy(t, m, ctx, rec, cfg.Id, 1)
	require.EqualValues(t, int32(errcode.ErrShopDiamondNotEnough), resp.Code, fmt.Sprintf("got %+v", resp))
	require.EqualValues(t, 0, st.GetItemCount(int32(cfg.Col2)), "扣费失败不能发货")
	require.EqualValues(t, cfg.Col3-1, st.GetItemCount(module.ItemGold), "失败请求不能部分扣费")
}

// TestBuyBagFullDoesNotCharge 背包放不下：预检阶段就拒绝，一分钱都不扣。
func TestBuyBagFullDoesNotCharge(t *testing.T) {
	cfg := firstGoods(t)
	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 1000)
	st.SetCanHold(false)

	resp := buy(t, m, ctx, rec, cfg.Id, 1)
	require.EqualValues(t, int32(errcode.ErrShopBagFull), resp.Code, fmt.Sprintf("got %+v", resp))
	require.EqualValues(t, 1000, st.GetItemCount(module.ItemGold), "满包时不能扣钱")
	require.EqualValues(t, 0, st.GetItemCount(int32(cfg.Col2)))
}

// TestBuyUnknownGoods 商品不存在：直接拒绝，不碰任何状态。
func TestBuyUnknownGoods(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 1000)

	resp := buy(t, m, ctx, rec, 999999, 1)
	require.EqualValues(t, int32(errcode.ErrShopGoodsNotExist), resp.Code)
	require.EqualValues(t, 1000, st.GetItemCount(module.ItemGold))
}

// TestBuyClampsCount 数量非法时按 1 处理，过大时截到 999。
func TestBuyClampsCount(t *testing.T) {
	cfg := firstGoods(t)
	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 100000)

	resp := buy(t, m, ctx, rec, cfg.Id, 0) // 按 1 个
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 100000-cfg.Col3, st.GetItemCount(module.ItemGold), "应只扣 1 个的价钱")
}

// TestListShowsBought 列表展示本周期已购次数。
func TestListShowsBought(t *testing.T) {
	cfg := firstGoods(t)
	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 1000)
	require.EqualValues(t, int32(errcode.OK), buy(t, m, ctx, rec, cfg.Id, 1).Code)

	raw, err := moduletest.Call(m, ctx, rec, codec.MsgShopListReq, codec.MsgShopListResp, &gamev1.ShopListReq{})
	require.NoError(t, err)
	resp := &gamev1.ShopListResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.NotEmpty(t, resp.Goods)
	for _, g := range resp.Goods {
		if int64(g.GoodsId) == cfg.Id {
			require.EqualValues(t, 1, g.Bought)
			return
		}
	}
	t.Fatalf("列表中找不到商品 %d", cfg.Id)
}

// TestNormalizeResetsAcrossDays 跨业务日后次数归零（daily 刷新配置下）。
func TestNormalizeResetsAcrossDays(t *testing.T) {
	if !dailyRefresh() {
		t.Skip("当前配置不按日刷新")
	}
	today := timeutil.BusinessDay(time.Now())
	d := &Data{Day: today - 1, Bought: map[string]int32{"1:1": 5}}
	require.True(t, normalize(d, today), "跨天应产生改动")
	require.Empty(t, d.Bought, "旧周期次数必须清零")
	require.EqualValues(t, today, d.Day)

	// 同一天重复 normalize 不应再改。
	require.False(t, normalize(d, today))
}

// swapConfig 临时替换 shop / shop_goods 两张表，测试结束恢复。
//
// 只动这两张表：其他包不读它们，跨包并行跑测试时不会互相干扰
// （gamedata 是全局快照，动别的表就有风险）。
func swapConfig(t *testing.T, shopJSON, goodsJSON string) {
	t.Helper()

	origShop, err := json.Marshal(gamedata.ShopCfgList())
	require.NoError(t, err)
	origGoods, err := json.Marshal(gamedata.ShopGoodsCfgList())
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err := gamedata.Replace(map[string][]byte{"shop": origShop, "shop_goods": origGoods})
		require.NoError(t, err, "恢复出厂配置失败")
	})

	_, err = gamedata.Replace(map[string][]byte{
		"shop":       []byte(shopJSON),
		"shop_goods": []byte(goodsJSON),
	})
	require.NoError(t, err, "热更配置失败")
}

// TestBuyKeyUsesConfiguredShopID 购买计数记在「商品归属的商店」下，而不是硬编码 1。
//
// 旧实现用 const defaultShopID = 1：把商品挂到别的商店（shop_goods.Col1）
// 后，计数还是记在 1 号商店的键上——运营配了归属却没效果，同一商品在
// 两个商店的限购也会串在一起。
//
// 默认配置里商品恰好挂在 1 号店，硬编码和查表结果一样，测不出区别；
// 这里热更一份「商品挂 2 号店」的配置。
func TestBuyKeyUsesConfiguredShopID(t *testing.T) {
	swapConfig(t,
		`[{"id":1,"col1":"道具商店"},{"id":2,"col1":"二号店"}]`,
		`[{"id":9,"col1":2,"col2":1002,"col3":5,"col4":1}]`)

	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 10000)

	resp := buy(t, m, ctx, rec, 9, 1)
	require.EqualValues(t, int32(errcode.OK), resp.Code)

	d, err := m.State(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, d.Bought["2:9"], "计数必须挂在商品归属的 2 号商店下")
	require.NotContains(t, d.Bought, "1:9", "不能再用硬编码的 1 号商店")
}

// TestGoodsInMissingShopHiddenAndUnbuyable 商品归属的商店不存在：不展示、不能买。
//
// 这是「下架」的实现方式：运营从 shop 表删掉一个商店，挂在它下面的商品
// 必须同时消失——否则玩家能看到列表、也能绕过列表直接发购买请求。
func TestGoodsInMissingShopHiddenAndUnbuyable(t *testing.T) {
	swapConfig(t,
		`[{"id":1,"col1":"道具商店"}]`,
		`[{"id":9,"col1":99,"col2":1002,"col3":5,"col4":1}]`)

	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 10000)

	// 列表不展示
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgShopListReq, codec.MsgShopListResp, &gamev1.ShopListReq{})
	require.NoError(t, err)
	listResp := &gamev1.ShopListResp{}
	require.NoError(t, proto.Unmarshal(raw, listResp))
	for _, g := range listResp.Goods {
		require.NotEqualValues(t, 9, g.GoodsId, "归属商店不存在的商品不能被展示")
	}

	// 直接买也不行
	resp := buy(t, m, ctx, rec, 9, 1)
	require.EqualValues(t, int32(errcode.ErrShopGoodsNotExist), resp.Code)
	require.EqualValues(t, 10000, st.GetItemCount(module.ItemGold), "拒绝时不能扣钱")
}

// TestWithinLimit 限购判定边界：0 表示不限、恰好买满允许、多一份拒绝。
//
// 抽成纯函数单测，是因为「0 = 不限」这条兼容语义最容易写反：新增 col5 之前
// 线上配置全是 0，若把 0 当成「一件都不能买」，热更配置会瞬间让商店停摆。
func TestWithinLimit(t *testing.T) {
	cases := []struct {
		name                string
		bought, want, limit int32
		allow               bool
	}{
		{"未配置限购列", 999, 999, 0, true},
		{"负数按不限处理", 5, 5, -1, true},
		{"首次购买在限内", 0, 1, 5, true},
		{"恰好买满", 4, 1, 5, true},
		{"已买满再买一份", 5, 1, 5, false},
		{"单次请求买超", 0, 6, 5, false},
		{"已超限（配置调小）再加", 6, 1, 5, false},
	}
	for _, c := range cases {
		require.Equal(t, c.allow, withinLimit(c.bought, c.want, c.limit), c.name)
	}
}

// TestBuyRejectsOverLimit 买满限购次数后拒绝，且不扣钱、不发货。
//
// 顺序即正确性：限购判定必须在扣费之前。放到扣费之后会变成
// 「钱扣了、货被限购挡下」，只能客服补偿。
func TestBuyRejectsOverLimit(t *testing.T) {
	swapConfig(t,
		`[{"id":1,"col1":"道具商店"}]`,
		`[{"id":1,"col1":1,"col2":1002,"col3":10,"col4":1,"col5":2}]`)

	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 1000)

	require.EqualValues(t, int32(errcode.OK), buy(t, m, ctx, rec, 1, 2).Code, "买满 2 份应当成功")
	diamondAfterBuy := st.GetItemCount(module.ItemGold)

	resp := buy(t, m, ctx, rec, 1, 1)
	require.EqualValues(t, int32(errcode.ErrShopLimitExceeded), resp.Code, fmt.Sprintf("got %+v", resp))
	require.EqualValues(t, diamondAfterBuy, st.GetItemCount(module.ItemGold), "被限购拒绝时不能扣钱")
	require.EqualValues(t, 2, st.GetItemCount(1002), "也不能多发货")
}

// TestBuySingleRequestOverLimit 一次请求就超过限购：整笔拒绝，不是「买满为止」。
//
// 不做部分成交：发一半货、扣一半钱的语义会让客户端难以展示，
// 且玩家会反复重试期望买到剩下的——直接拒绝最清晰。
func TestBuySingleRequestOverLimit(t *testing.T) {
	swapConfig(t,
		`[{"id":1,"col1":"道具商店"}]`,
		`[{"id":1,"col1":1,"col2":1002,"col3":10,"col4":1,"col5":2}]`)

	m, st, rec, ctx := newTestModule()
	st.AddItem(module.ItemGold, 1000)

	resp := buy(t, m, ctx, rec, 1, 3) // 一次要 3 份，限购 2
	require.EqualValues(t, int32(errcode.ErrShopLimitExceeded), resp.Code)
	require.EqualValues(t, 1000, st.GetItemCount(module.ItemGold), "拒绝时一分钱都不能扣")
	require.EqualValues(t, 0, st.GetItemCount(1002))
}

// TestListShowsLimit 列表把配置的限购次数带给客户端（客户端据此置灰按钮）。
func TestListShowsLimit(t *testing.T) {
	swapConfig(t,
		`[{"id":1,"col1":"道具商店"}]`,
		`[{"id":1,"col1":1,"col2":1002,"col3":10,"col4":1,"col5":7}]`)

	m, _, rec, ctx := newTestModule()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgShopListReq, codec.MsgShopListResp, &gamev1.ShopListReq{})
	require.NoError(t, err)
	resp := &gamev1.ShopListResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.NotEmpty(t, resp.Goods)
	require.EqualValues(t, 7, resp.Goods[0].Limit, "Limit 必须来自配置 col5，不能写死 0")
}
