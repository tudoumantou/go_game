// internal/game/module/equip/equip_test.go —— 装备模块单测
//
// 装备直接决定战力，穿戴/卸下的每条分支都要能验证：
// 加成多给是刷战力，少给是坑玩家，脱了不掉是永久虚高。
//
// 全部用例纯内存：装备列表直接写进 moduletest.State 的 Ext 槽位，
// 配置走 gamedata 快照（需要额外行时用 Replace 临时注入，Cleanup 还原）。
package equip

import (
	"encoding/json"
	"game_study/internal/common/errcode"
	"testing"

	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/model"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	equipSword int32 = 3001 // weapon，门槛 1，atk +10
	equipArmor int32 = 3002 // armor，门槛 1，atk +5
)

func newTestModule(t *testing.T) (*module.Bound[Data], *moduletest.State, *moduletest.Recorder, *module.Context) {
	t.Helper()
	st := moduletest.NewState(1)
	st.SetProfile(module.Profile{Level: 10}) // 默认满足 3001/3002 的等级门槛
	rec := moduletest.NewRecorder()
	m := M
	return m, st, rec, moduletest.NewContext(st, rec)
}

// seed 把装备列表写进模块状态（走 Mutate，与线上写入路径一致）。
func seed(t *testing.T, m *module.Bound[Data], ctx *module.Context, equips ...Entry) {
	t.Helper()
	require.NoError(t, m.Mutate(ctx, func(d *Data) bool {
		d.Equips = equips
		return true
	}))
}

func wear(t *testing.T, m *module.Bound[Data], ctx *module.Context, rec *moduletest.Recorder, uid int64) *gamev1.EquipWearResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgEquipWearReq, codec.MsgEquipWearResp, &gamev1.EquipWearReq{Uid: uid})
	require.NoError(t, err)
	require.NotNil(t, raw, "穿戴没有回包")
	resp := &gamev1.EquipWearResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

func takeOff(t *testing.T, m *module.Bound[Data], ctx *module.Context, rec *moduletest.Recorder, uid int64) *gamev1.EquipTakeOffResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgEquipTakeOffReq, codec.MsgEquipTakeOffResp, &gamev1.EquipTakeOffReq{Uid: uid})
	require.NoError(t, err)
	require.NotNil(t, raw, "卸下没有回包")
	resp := &gamev1.EquipTakeOffResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

func list(t *testing.T, m *module.Bound[Data], ctx *module.Context, rec *moduletest.Recorder) *gamev1.EquipListResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgEquipListReq, codec.MsgEquipListResp, &gamev1.EquipListReq{})
	require.NoError(t, err)
	require.NotNil(t, raw, "列表请求不能返回 nil")
	resp := &gamev1.EquipListResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

// TestWearAddsBonus 穿戴成功：置位 + 加攻击 + 面板刷新。
func TestWearAddsBonus(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword, Quality: 1})

	resp := wear(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 10, st.GetAttr("atk"), "穿戴必须加攻击")
	require.NotEmpty(t, resp.Attrs, "穿戴要回属性面板")
	require.Equal(t, 1, rec.Count(codec.MsgEquipWearResp), "一次请求只能回一个包")

	d, err := m.State(ctx)
	require.NoError(t, err)
	require.True(t, d.Equips[0].Equipped, "穿戴状态要写回快照")
}

// TestWearUnknownUID 不存在的实例：回 3101，不改状态。
func TestWearUnknownUID(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})

	resp := wear(t, m, ctx, rec, 999)
	require.EqualValues(t, int32(errcode.ErrEquipNotExist), resp.Code)
	require.EqualValues(t, 0, st.GetAttr("atk"), "失败不能加属性")
}

// TestWearLevelLow 等级不足：回 3102，不加属性、不置位。
func TestWearLevelLow(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	st.SetProfile(module.Profile{Level: 0})
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})

	resp := wear(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.ErrEquipLevelLow), resp.Code)
	require.EqualValues(t, 0, st.GetAttr("atk"))
	d, _ := m.State(ctx)
	require.False(t, d.Equips[0].Equipped)
}

// TestWearSlotConflict 同部位已有装备：回 3103，且不能把前一件挤掉。
func TestWearSlotConflict(t *testing.T) {
	// 注入一件同部位（weapon）的装备，否则内嵌配置里两个部位不同、测不出冲突。
	withExtraEquipCfg(t, gamedata.EquipCfg{Id: 3003, Col1: "木剑", Col2: 1, Col3: "weapon", Col4: 7})

	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx,
		Entry{UID: 100, EquipID: equipSword, Equipped: true},
		Entry{UID: 101, EquipID: 3003},
	)
	st.AddBonus("atk", 10) // 已穿戴的 3001 提供的加成

	resp := wear(t, m, ctx, rec, 101)
	require.EqualValues(t, int32(errcode.ErrEquipSlotConflict), resp.Code)
	require.EqualValues(t, 10, st.GetAttr("atk"), "冲突时不能重复加属性")

	d, _ := m.State(ctx)
	require.True(t, d.Equips[0].Equipped, "原装备不能被挤掉")
	require.False(t, d.Equips[1].Equipped)
}

// TestWearTwiceDoesNotDoubleBonus 重复穿戴幂等：属性只加一次，也不产生第二次提交。
func TestWearTwiceDoesNotDoubleBonus(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})

	require.EqualValues(t, int32(errcode.OK), wear(t, m, ctx, rec, 100).Code)
	require.EqualValues(t, 10, st.GetAttr("atk"))

	mark := ctx.State.(*moduletest.State).DirtyModules()
	require.EqualValues(t, int32(errcode.OK), wear(t, m, ctx, rec, 100).Code)
	require.EqualValues(t, 10, st.GetAttr("atk"), "重复穿戴不能二次加属性")
	require.Equal(t, mark, ctx.State.(*moduletest.State).DirtyModules(), "幂等路径不该标脏（不产生提交）")
}

// TestTakeOffRemovesBonus 卸下：扣回与穿戴等量的加成。
func TestTakeOffRemovesBonus(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword, Equipped: true})
	st.AddBonus("atk", 10)

	resp := takeOff(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 0, st.GetAttr("atk"), "卸下必须精确扣回")
	require.Equal(t, 1, rec.Count(codec.MsgEquipTakeOffResp))

	d, _ := m.State(ctx)
	require.False(t, d.Equips[0].Equipped)
}

// TestTakeOffNotEquipped 脱一件没穿的：回 3104。
func TestTakeOffNotEquipped(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})
	st.AddBonus("atk", 0)

	resp := takeOff(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.ErrEquipNotEquipped), resp.Code)
	require.EqualValues(t, 0, st.GetAttr("atk"))
}

// TestTakeOffUnknownUID 不存在的实例：回 3101。
func TestTakeOffUnknownUID(t *testing.T) {
	m, _, rec, ctx := newTestModule(t)
	resp := takeOff(t, m, ctx, rec, 404)
	require.EqualValues(t, int32(errcode.ErrEquipNotExist), resp.Code)
}

// TestLoginRecomputesBonus 登录按已穿戴件重算：只算穿着的，不重复累加。
func TestLoginRecomputesBonus(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx,
		Entry{UID: 100, EquipID: equipSword, Equipped: true},
		Entry{UID: 101, EquipID: equipArmor, Equipped: true},
		Entry{UID: 102, EquipID: equipArmor}, // 未穿戴，不计
	)

	require.NoError(t, m.OnLogin(ctx))
	require.EqualValues(t, 15, st.GetAttr("atk"), "10 + 5，未穿戴的不算")

	// 玩家层在 LoginAll 之前会 ClearBonus；这里模拟第二次登录不应翻倍。
	st.ClearBonus()
	require.NoError(t, m.OnLogin(ctx))
	require.EqualValues(t, 15, st.GetAttr("atk"), "重登不能翻倍")
	require.Equal(t, 0, rec.Total(), "登录钩子不下发任何包")
}

// TestListReturnsSnapshotOrder 列表按快照顺序下发，字段不丢。
func TestListReturnsSnapshotOrder(t *testing.T) {
	m, _, rec, ctx := newTestModule(t)
	seed(t, m, ctx,
		Entry{UID: 100, EquipID: equipSword, EnhanceLevel: 3, Quality: 2, Equipped: true},
		Entry{UID: 101, EquipID: equipArmor},
	)

	resp := list(t, m, ctx, rec)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, resp.Equips, 2)
	require.EqualValues(t, 100, resp.Equips[0].Uid, "顺序即持有顺序")
	require.EqualValues(t, 3, resp.Equips[0].EnhanceLevel)
	require.EqualValues(t, 2, resp.Equips[0].Quality)
	require.True(t, resp.Equips[0].Equipped)
	require.False(t, resp.Equips[1].Equipped)
	require.Equal(t, 1, rec.Count(codec.MsgEquipListResp))
}

// TestListEmpty 没有装备时也要正常回包（不能是 nil 报文）。
func TestListEmpty(t *testing.T) {
	m, _, rec, ctx := newTestModule(t)
	resp := list(t, m, ctx, rec)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Empty(t, resp.Equips)
}

// ============================================================================
// 持久化适配（persist.go）
// ============================================================================

// TestRowRoundTrip 表行往返不丢字段：Equipped=false 不能被 omitempty 吃掉。
func TestRowRoundTrip(t *testing.T) {
	in := &Data{Equips: []Entry{
		{UID: 100, EquipID: equipSword, EnhanceLevel: 2, Quality: 1, Equipped: true},
		{UID: 101, EquipID: equipArmor},
	}}
	row, err := toRow(7, in)
	require.NoError(t, err)
	require.EqualValues(t, 7, row.PlayerId)

	out, err := fromRow(row)
	require.NoError(t, err)
	require.Equal(t, in.Equips, out.Equips)
}

// TestFromRowEmptyColumn 空列等价于「一件装备都没有」，而不是报错。
func TestFromRowEmptyColumn(t *testing.T) {
	d, err := fromRow(&model.PlayerEquip{PlayerId: 7})
	require.NoError(t, err)
	require.Empty(t, d.Equips)
}

// TestFromRowBadJSON 坏数据必须报错（宁可整批写回失败，也不静默清空装备）。
func TestFromRowBadJSON(t *testing.T) {
	_, err := fromRow(&model.PlayerEquip{PlayerId: 7, Equips: `{"not":"array"}`})
	require.Error(t, err)
}

// TestValidate 结构性不变量：UID 重复、UID 非正、强化负值都要拒绝。
func TestValidate(t *testing.T) {
	cases := []struct {
		name  string
		in    *Data
		valid bool
	}{
		{"empty", newData(), true},
		{"normal", &Data{Equips: []Entry{{UID: 1, EquipID: equipSword}, {UID: 2, EquipID: equipArmor}}}, true},
		{"duplicate uid", &Data{Equips: []Entry{{UID: 1}, {UID: 1}}}, false},
		{"zero uid", &Data{Equips: []Entry{{UID: 0}}}, false},
		{"negative enhance", &Data{Equips: []Entry{{UID: 1, EnhanceLevel: -1}}}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validate(c.in)
			if c.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// ============================================================================
// 测试辅助
// ============================================================================

// withExtraEquipCfg 临时给 equip 表追加行（需要「同部位两件」才能测冲突）。
//
// Replace 是全局热更接口，这里用完立即还原；equip 包内用例串行执行，
// 不会与其他用例互相干扰。
func withExtraEquipCfg(t *testing.T, extra ...gamedata.EquipCfg) {
	t.Helper()
	orig := append([]gamedata.EquipCfg(nil), gamedata.EquipCfgList()...)
	rows := append(append([]gamedata.EquipCfg(nil), orig...), extra...)

	raw, err := json.Marshal(rows)
	require.NoError(t, err)
	_, err = gamedata.Replace(map[string][]byte{"equip": raw})
	require.NoError(t, err)

	t.Cleanup(func() {
		back, err := json.Marshal(orig)
		require.NoError(t, err)
		_, err = gamedata.Replace(map[string][]byte{"equip": back})
		require.NoError(t, err)
	})
}

func enhance(t *testing.T, m *module.Bound[Data], ctx *module.Context, rec *moduletest.Recorder, uid int64) *gamev1.EquipEnhanceResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgEquipEnhanceReq, codec.MsgEquipEnhanceResp, &gamev1.EquipEnhanceReq{Uid: uid})
	require.NoError(t, err)
	require.NotNil(t, raw, "强化没有回包")
	resp := &gamev1.EquipEnhanceResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

// TestEnhanceChargesAndLevelsUp 未穿戴的装备强化：扣钻石、等级 +1、面板不变。
//
// 面板不变是刻意的：未穿戴的装备不提供加成，提前加会在穿上的那一刻
// 再加一次，玩家白赚一份攻击力。
func TestEnhanceChargesAndLevelsUp(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	st.AddItem(itemDiamond, 1000)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})

	resp := enhance(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.OK), resp.Code, "强化应当成功")
	require.EqualValues(t, 1, resp.EnhanceLevel)
	require.EqualValues(t, 1000-100, st.GetItemCount(itemDiamond), "要扣一级的强化消耗")
	require.EqualValues(t, 0, st.GetAttr("atk"), "未穿戴的装备不提供加成")

	d, err := m.State(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, d.Equips[0].EnhanceLevel, "等级要写回快照")
}

// TestEnhanceEquippedUpdatesBonus 已穿戴的装备强化：攻击立刻按新等级重算。
func TestEnhanceEquippedUpdatesBonus(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	st.AddItem(itemDiamond, 1000)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})
	require.EqualValues(t, int32(errcode.OK), wear(t, m, ctx, rec, 100).Code, "先穿上")
	require.EqualValues(t, 10, st.GetAttr("atk"), "基础攻击来自装备配置")

	resp := enhance(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 1, resp.EnhanceLevel)
	require.EqualValues(t, 15, st.GetAttr("atk"), "1 级强化加 5 点攻击")
	require.NotEmpty(t, resp.Attrs, "强化后要回属性面板")
}

// TestEnhanceNoMaterial 材料不足：回 3106，等级与钻石都不变。
func TestEnhanceNoMaterial(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	st.AddItem(itemDiamond, 99) // 差 1 个
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})

	resp := enhance(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.ErrEquipNoMaterial), resp.Code)
	require.EqualValues(t, 99, st.GetItemCount(itemDiamond), "失败不能扣钱")
	d, _ := m.State(ctx)
	require.EqualValues(t, 0, d.Equips[0].EnhanceLevel, "失败不能涨等级")
}

// TestEnhanceMaxLevel 配置里没有下一级 = 已满级：回 3105，不扣钱。
//
// 上限判定用「配置缺失」而不是常量：运营加一行配置就放开一级。
func TestEnhanceMaxLevel(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	st.AddItem(itemDiamond, 10000)
	// 配置的等级是 1..3，seed 一个已到 3 级的实例
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword, EnhanceLevel: 3})

	resp := enhance(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.ErrEquipMaxEnhance), resp.Code)
	require.EqualValues(t, 10000, st.GetItemCount(itemDiamond), "满级不能再扣钱")
}

// TestEnhanceUnknownUID 不存在的实例：回 3101，不扣钱。
func TestEnhanceUnknownUID(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	st.AddItem(itemDiamond, 1000)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword})

	resp := enhance(t, m, ctx, rec, 999)
	require.EqualValues(t, int32(errcode.ErrEquipNotExist), resp.Code)
	require.EqualValues(t, 1000, st.GetItemCount(itemDiamond))
}

// TestTakeOffRevertsEnhancedBonus 强化过的装备卸下时，扣回的是「基础+强化」。
//
// 这是 equipBonus 必须四处共用的原因：只要卸下时只扣基础值，
// 反复穿脱就能把攻击力刷上去。
func TestTakeOffRevertsEnhancedBonus(t *testing.T) {
	m, st, rec, ctx := newTestModule(t)
	seed(t, m, ctx, Entry{UID: 100, EquipID: equipSword, EnhanceLevel: 2, Equipped: true})
	require.NoError(t, m.OnLogin(ctx))
	require.EqualValues(t, 22, st.GetAttr("atk"), "基础 10 + 2 级强化 12")

	resp := takeOff(t, m, ctx, rec, 100)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 0, st.GetAttr("atk"), "卸下必须扣干净，不能剩强化那部分")
}
