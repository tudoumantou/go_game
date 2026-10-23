package battle

import (
	"fmt"
	"testing"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
	"github.com/stretchr/testify/require"
)

// newTestState 构造一个可直接给 battle 用的 State（不启动 MGR）。
//
// 与 scenemap 各包的测试同一套路：battle 的所有入口都要求"已在 MGR
// goroutine 内"，纯状态对象正好模拟这个前提（owner 为 nil，Broadcast 丢弃）。
func newTestState() *mgr.State {
	return &mgr.State{
		Key:       scenemap.MapKey{MapID: 1},
		MapID:     1,
		Users:     make(map[scenemap.UUID]*mgr.UserState),
		NPES:      make(map[scenemap.UUID]*mgr.NpeAIInfo),
		Pets:      make(map[scenemap.UUID]scenemap.UUID),
		Bullets:   make(map[scenemap.UUID]*mgr.Bullet),
		Firewells: make(map[scenemap.UUID]*mgr.Firewell),
		Ext:       make(map[string]any),
		Index:     mgr.NewIndex(grid.Size{Width: 32, Height: 32}),
	}
}

// testRuntime 装一个带可控时钟的运行时。
func testRuntime(s *mgr.State, now *int64, opts *Options) *Runtime {
	o := Options{
		Table: DefaultTable(),
		Now:   func() int64 { return *now },
	}
	if opts != nil {
		o = *opts
		if o.Now == nil {
			o.Now = func() int64 { return *now }
		}
		if o.Table == nil {
			o.Table = DefaultTable()
		}
	}
	return Ensure(s, o)
}

// testPlayer 构造一个攻击方投影（属性写死，便于手算）。
func testPlayer(uuid scenemap.UUID, x, y int32) *Sprite {
	return &Sprite{
		UUID: uuid, Kind: scenemap.EntityKindPlayer, Owner: uuid, Camp: 1,
		HP: 1000, MaxHP: 1000,
		Atk: 100, Def: 20, Hit: PermilleOne,
		SpeedAtk: PermilleOne, ElementCoe: PermilleOne,
		FinalAddDmgCoe: PermilleOne, ThornsCoe: PermilleOne,
		Pos: scenemap.GridPos{X: x, Y: y},
	}
}

// testNpc 构造一个怪物投影。
func testNpc(uuid scenemap.UUID, hp int64, x, y int32) *Sprite {
	return &Sprite{
		UUID: uuid, Kind: scenemap.EntityKindNPE, Camp: 2,
		HP: hp, MaxHP: hp, Def: 20, Hit: PermilleOne,
		SpeedAtk: PermilleOne, ElementCoe: PermilleOne,
		FinalAddDmgCoe: PermilleOne, ThornsCoe: PermilleOne,
		Pos: scenemap.GridPos{X: x, Y: y},
	}
}

// TestDamageFormulaFixedValue 是规格 §16.4 要求的"固定输入 → 固定输出"。
//
// 手算：
//
//	基础   = max(100 - 20, 0) * 1.0 + 10 = 90
//	暴击   = 90 * 1.3（暴击率 100%，暴伤 +0）= 117
//	终伤   = 117 * 1.0 = 117
//
// 任何一步的常量/顺序被改动，这个用例都会红——它守的是"公式链顺序"，
// 不是"某个技能的数值"。
func TestDamageFormulaFixedValue(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)

	A := testPlayer(1, 5, 5)
	A.Crit = PermilleOne // 必定暴击
	T := testNpc(9, 1000, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T

	cfg := &SkillConfig{ID: 9999, HarmType: HarmTypeNormal, Coeff: PermilleOne, Harm: 10}
	sc := &SkillContext{SkillID: cfg.ID, Caster: A.UUID, Target: T.UUID, Action: ActionCast}
	out := rt.calcSkill(s, A, T, cfg, sc, now)

	got := 1000 - T.HP
	require.EqualValues(t, 117, got, fmt.Sprintf("伤害应为 117，实际 %d（debug=%s）", got, out.DebugInfo))
	require.False(t, !hasTag(out.Harms, HarmCrit), fmt.Sprintf("应带暴击标签: %+v", out.Harms))
	require.EqualValues(t, 117, out.RealHarm)
}

// TestDamageFormulaNoCrit 验证不暴击时只有基础伤害（顺序/默认值的对照组）。
func TestDamageFormulaNoCrit(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5) // Crit = 0
	T := testNpc(9, 1000, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T

	cfg := &SkillConfig{ID: 9998, HarmType: HarmTypeNormal, Coeff: PermilleOne, Harm: 10}
	sc := &SkillContext{SkillID: cfg.ID, Caster: A.UUID, Target: T.UUID, Action: ActionCast}
	rt.calcSkill(s, A, T, cfg, sc, now)

	got := 1000 - T.HP
	require.EqualValues(t, 90, got, fmt.Sprintf("不暴击伤害应为 90，实际 %d", got))
}

// TestDodgeFormula 验证闪避率归一化：命中 0 → 必闪，命中满 → 必中。
func TestDodgeFormula(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	A.Hit = 0
	T := testNpc(9, 1000, 6, 5)
	T.Dod = PermilleOne
	require.False(t, !rt.dodged(A, T), "命中 0 + 闪避满应判定闪避")
	A.Hit = PermilleOne
	T.Dod = 0
	require.False(t, rt.dodged(A, T), "命中满 + 闪避 0 不应闪避")
}

// TestShieldAndUndie 验证护盾吸收与免死截断（规格 §10.6.6）。
func TestShieldAndUndie(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	T := testNpc(9, 100, 6, 5)
	rt.sprites[T.UUID] = T

	// 护盾 50：打 30 全吸收，血量不变。
	require.False(t, !rt.applyBuff(T, 3004, 1, 0, now), "护盾 Buff 施加失败")
	real, dead, absorbed := rt.applyRealHarm(s, T, 30, 1, now)
	require.False(t, real != 0 || dead || absorbed != 30, fmt.Sprintf("护盾吸收异常: real=%d dead=%v absorbed=%d", real, dead, absorbed))
	require.EqualValues(t, 100, T.HP, fmt.Sprintf("护盾扣减异常: hp=%d shield=%d", T.HP, T.Shield))
	require.EqualValues(t, 20, T.Shield)

	// 免死：本应致死的伤害压到 1 点。
	T.HP = 10
	require.False(t, !rt.applyBuff(T, 3006, 1, 0, now), "免死 Buff 施加失败")
	real, dead, _ = rt.applyRealHarm(s, T, 1000, 1, now)
	require.False(t, dead || T.HP != 1, fmt.Sprintf("免死未生效: real=%d dead=%v hp=%d", real, dead, T.HP))
}

// TestBuffDotTicksAndExpires 验证 DoT 周期结算与到期移除。
func TestBuffDotTicksAndExpires(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	T := testNpc(9, 100, 6, 5)
	rt.sprites[T.UUID] = T
	// 灼烧：3 秒、每 500ms 掉 10。
	require.False(t, !rt.applyBuff(T, 3001, 1, 0, now), "DoT 施加失败")
	rt.tickBuffs(s, now+500)
	require.EqualValues(t, 90, T.HP)
	now += 1000
	rt.tickBuffs(s, now)
	require.EqualValues(t, 80, T.HP)
	// 跳到 3 秒之后：Buff 到期，血量不再变化。
	now += 3000
	rt.tickBuffs(s, now)
	require.Len(t, T.Buffs, 0)
	before := T.HP
	rt.tickBuffs(s, now+5000)
	require.EqualValues(t, before, T.HP)
}

// TestBuffStackCap 验证叠层上限（MaxStacks=3 的灼烧）。
func TestBuffStackCap(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	T := testNpc(9, 100, 6, 5)
	rt.sprites[T.UUID] = T
	for i := 0; i < 5; i++ {
		rt.applyBuff(T, 3001, 1, 0, now)
	}
	got := T.BuffStacks(3001)
	require.EqualValues(t, 3, got, fmt.Sprintf("层数应封顶 3，实际 %d", got))
}

// TestSeckillOnlySmallNpc 验证斩杀只对小怪生效且不走闪避链。
func TestSeckillOnlySmallNpc(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	T := testNpc(9, 100, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T
	cfg := &SkillConfig{ID: 9997, HarmType: HarmTypeNormal, Coeff: PermilleOne, SeckillPermille: PermilleOne}
	sc := &SkillContext{SkillID: cfg.ID, Caster: A.UUID, Target: T.UUID, Action: ActionCast}
	out := rt.calcSkill(s, A, T, cfg, sc, now)
	require.False(t, !hasTag(out.Harms, HarmSeckill), fmt.Sprintf("应触发斩杀标签: %+v", out.Harms))
	require.False(t, T.HP != 0 || !T.Dead, fmt.Sprintf("斩杀应直接致死: hp=%d dead=%v", T.HP, T.Dead))
}

// TestSuperBlocksDamage 验证无敌 Buff：0 伤害 + SUPER 标签。
func TestSuperBlocksDamage(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	T := testNpc(9, 100, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T
	rt.applyBuff(T, 3005, 1, 0, now) // 无敌
	cfg := &SkillConfig{ID: 9996, HarmType: HarmTypeNormal, Coeff: PermilleOne}
	sc := &SkillContext{SkillID: cfg.ID, Caster: A.UUID, Target: T.UUID, Action: ActionCast}
	out := rt.calcSkill(s, A, T, cfg, sc, now)
	require.EqualValues(t, 100, T.HP)
	require.False(t, !hasTag(out.Harms, HarmSuper), fmt.Sprintf("应带 SUPER 标签: %+v", out.Harms))
}
