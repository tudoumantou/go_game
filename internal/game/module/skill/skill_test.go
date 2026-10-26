// internal/game/module/skill/skill_test.go —— 技能模块单测：学习/升级扣费/重复学习/落库形状。
//
// 统一取「带消耗」的第一个技能配置，保证升级路径能真正验证扣费。
package skill

import (
	"encoding/json"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
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
	return &fakeState{id: id, level: 1, items: map[int32]int64{}, ext: map[string]json.RawMessage{}, biz: map[string]bool{}}
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

// replies 记录下行包（按消息号），用于断言业务码。
type replies map[uint32][]byte

func newCtx(st *fakeState) *module.Context {
	return newCtxRec(st, nil)
}

func newCtxRec(st *fakeState, rec replies) *module.Context {
	return &module.Context{
		State:        st,
		CacheEnabled: true,
		Send: func(id uint32, body []byte) error {
			if rec != nil {
				rec[id] = append([]byte(nil), body...)
			}
			return nil
		},
	}
}

// upgradeCost 从配置查「从 level 升到 level+1」的消耗；ok=false 表示满级。
func upgradeCost(level int32) (int64, bool) {
	for _, row := range gamedata.SkillUpgradeCfgList() {
		if int32(row.Col1) == level {
			return row.Col2, true
		}
	}
	return 0, false
}

// maxConfiguredLevel 表里配置的最高「当前等级」。
//
// 注意它本身还能升级（该行就是「从这个等级再升一级」的消耗）；
// 真正的满级是 maxConfiguredLevel+1——那时表里已经没有对应的行了。
func maxConfiguredLevel(t *testing.T) int32 {
	t.Helper()
	var max int32
	for _, row := range gamedata.SkillUpgradeCfgList() {
		if int32(row.Col1) > max {
			max = int32(row.Col1)
		}
	}
	require.Greater(t, max, int32(0), "skill_upgrade 配置为空，测试前提不成立")
	return max
}

func firstSkill(t *testing.T) gamedata.SkillCfg {
	t.Helper()
	for _, cfg := range gamedata.SkillCfgList() {
		if cfg.Col3 > 0 {
			return cfg // 带消耗的技能，才能验证扣费
		}
	}
	t.Fatal("no paid skill configured")
	return gamedata.SkillCfg{}
}

// TestLearnAndUpgrade 学习落快照；升级扣材料并提升等级。
func TestLearnAndUpgrade(t *testing.T) {
	cfg := firstSkill(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)

	learn, _ := proto.Marshal(&gamev1.SkillLearnReq{SkillId: int32(cfg.Id)})
	err := m.OnMessage(ctx, msgIDLearnReq, learn)
	require.NoError(t, err)
	d, _ := m.State(ctx)
	require.EqualValues(t, 1, d.Levels[int32(cfg.Id)])

	st.items[upgradeItemID] = 1000
	upgrade, _ := proto.Marshal(&gamev1.SkillUpgradeReq{SkillId: int32(cfg.Id)})
	err = m.OnMessage(ctx, msgIDUpgradeReq, upgrade)
	require.NoError(t, err)
	d, _ = m.State(ctx)
	require.EqualValues(t, 2, d.Levels[int32(cfg.Id)])
	cost, ok := upgradeCost(1)
	require.True(t, ok, "1 级必须有升级配置")
	require.EqualValues(t, 1000-cost, st.GetItemCount(upgradeItemID),
		"消耗必须来自 skill_upgrade 表，而不是 skill.Col3 * level")
}

// TestLearnDuplicate 重复学习不重置等级。
func TestLearnDuplicate(t *testing.T) {
	cfg := firstSkill(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)
	err := m.Mutate(ctx, func(dst *Data) bool { *dst = Data{Levels: map[int32]int32{int32(cfg.Id): 3}}; return true })
	require.NoError(t, err)
	learn, _ := proto.Marshal(&gamev1.SkillLearnReq{SkillId: int32(cfg.Id)})
	err = m.OnMessage(ctx, msgIDLearnReq, learn)
	require.NoError(t, err)
	d, _ := m.State(ctx)
	require.EqualValues(t, 3, d.Levels[int32(cfg.Id)])
}

// TestUpgradeWithoutResource 材料不足时不升级、不扣费。
func TestUpgradeWithoutResource(t *testing.T) {
	cfg := firstSkill(t)
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)
	err := m.Mutate(ctx, func(dst *Data) bool { *dst = Data{Levels: map[int32]int32{int32(cfg.Id): 1}}; return true })
	require.NoError(t, err)
	upgrade, _ := proto.Marshal(&gamev1.SkillUpgradeReq{SkillId: int32(cfg.Id)})
	err = m.OnMessage(ctx, msgIDUpgradeReq, upgrade)
	require.NoError(t, err)
	d, _ := m.State(ctx)
	require.EqualValues(t, 1, d.Levels[int32(cfg.Id)], "level changed without resource")
	require.EqualValues(t, 0, st.GetItemCount(upgradeItemID), "item deducted on failed upgrade")
}

// TestUpgradeStopsAtMaxLevel 配置里没有「当前等级」的行 = 已满级。
//
// 旧实现是 cost = skill.Col3 * level，永远算得出钱，也永远没有上限——
// 技能能无限升，运营改 skill_upgrade 表毫无效果。
func TestUpgradeStopsAtMaxLevel(t *testing.T) {
	cfg := firstSkill(t)
	// 满级 = 配置里最高等级的下一级：那一级没有「升到再下一级」的行
	top := maxConfiguredLevel(t) + 1

	st := newFakeState(1)
	rec := replies{}
	ctx := newCtxRec(st, rec)
	m := New(nil)

	require.NoError(t, m.Mutate(ctx, func(dst *Data) bool {
		*dst = Data{Levels: map[int32]int32{int32(cfg.Id): top}}
		return true
	}))
	st.items[upgradeItemID] = 100000

	upgrade, _ := proto.Marshal(&gamev1.SkillUpgradeReq{SkillId: int32(cfg.Id)})
	require.NoError(t, m.OnMessage(ctx, msgIDUpgradeReq, upgrade))

	d, _ := m.State(ctx)
	require.EqualValues(t, top, d.Levels[int32(cfg.Id)], "满级后等级不能再涨")
	require.EqualValues(t, 100000, st.GetItemCount(upgradeItemID), "满级不能扣钱")

	resp := &gamev1.SkillUpgradeResp{}
	require.NoError(t, proto.Unmarshal(rec[msgIDUpgradeResp], resp))
	require.EqualValues(t, codeMaxLevel, resp.Code, "必须明确告诉客户端已满级")
}

// TestUpgradeCostComesFromTable 消耗按表逐级递增，不再线性乘算。
func TestUpgradeCostComesFromTable(t *testing.T) {
	cfg := firstSkill(t)

	// 1 级升 2 级
	st := newFakeState(1)
	ctx := newCtx(st)
	m := New(nil)
	require.NoError(t, m.Mutate(ctx, func(dst *Data) bool {
		*dst = Data{Levels: map[int32]int32{int32(cfg.Id): 1}}
		return true
	}))
	st.items[upgradeItemID] = 10000
	up, _ := proto.Marshal(&gamev1.SkillUpgradeReq{SkillId: int32(cfg.Id)})
	require.NoError(t, m.OnMessage(ctx, msgIDUpgradeReq, up))

	cost1, ok := upgradeCost(1)
	require.True(t, ok)
	require.EqualValues(t, 10000-cost1, st.GetItemCount(upgradeItemID))

	// 2 级升 3 级：表里是 300 而不是 100*2 的巧合值，能区分「查表」与「线性」
	if cost2, ok := upgradeCost(2); ok {
		require.NoError(t, m.OnMessage(ctx, msgIDUpgradeReq, up))
		require.EqualValues(t, 10000-cost1-cost2, st.GetItemCount(upgradeItemID))
	}
}

// TestSnapshotShape Ext 槽位存的是业务结构（Data），表行转换在 persist.go。
func TestSnapshotShape(t *testing.T) {
	st := newFakeState(9)
	ctx := newCtx(st)
	m := New(nil)

	err := m.Mutate(ctx, func(dst *Data) bool { *dst = Data{Levels: map[int32]int32{101: 4}}; return true })
	require.NoError(t, err)

	var decoded Data
	require.NoError(t, json.Unmarshal(st.Ext(stateKey), &decoded), "Ext 必须是 Data 结构")
	require.EqualValues(t, 4, decoded.Levels[101])

	// 落库形状由 toRow 决定：整份存 skills 列。
	row, err := toRow(9, &decoded)
	require.NoError(t, err)
	require.EqualValues(t, 9, row.PlayerId)
	require.EqualValues(t, `{"101":4}`, row.Skills)
}
