package battle

import (
	"testing"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
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

	if got := 1000 - T.HP; got != 117 {
		t.Fatalf("伤害应为 117，实际 %d（debug=%s）", got, out.DebugInfo)
	}
	if !hasTag(out.Harms, HarmCrit) {
		t.Fatalf("应带暴击标签: %+v", out.Harms)
	}
	if out.RealHarm != 117 {
		t.Fatalf("real harm 应为 117，实际 %d", out.RealHarm)
	}
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

	if got := 1000 - T.HP; got != 90 {
		t.Fatalf("不暴击伤害应为 90，实际 %d", got)
	}
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
	if !rt.dodged(A, T) {
		t.Fatal("命中 0 + 闪避满应判定闪避")
	}
	A.Hit = PermilleOne
	T.Dod = 0
	if rt.dodged(A, T) {
		t.Fatal("命中满 + 闪避 0 不应闪避")
	}
}

// TestShieldAndUndie 验证护盾吸收与免死截断（规格 §10.6.6）。
func TestShieldAndUndie(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	T := testNpc(9, 100, 6, 5)
	rt.sprites[T.UUID] = T

	// 护盾 50：打 30 全吸收，血量不变。
	if !rt.applyBuff(T, 3004, 1, 0, now) {
		t.Fatal("护盾 Buff 施加失败")
	}
	real, dead, absorbed := rt.applyRealHarm(s, T, 30, 1, now)
	if real != 0 || dead || absorbed != 30 {
		t.Fatalf("护盾吸收异常: real=%d dead=%v absorbed=%d", real, dead, absorbed)
	}
	if T.HP != 100 || T.Shield != 20 {
		t.Fatalf("护盾扣减异常: hp=%d shield=%d", T.HP, T.Shield)
	}

	// 免死：本应致死的伤害压到 1 点。
	T.HP = 10
	if !rt.applyBuff(T, 3006, 1, 0, now) {
		t.Fatal("免死 Buff 施加失败")
	}
	real, dead, _ = rt.applyRealHarm(s, T, 1000, 1, now)
	if dead || T.HP != 1 {
		t.Fatalf("免死未生效: real=%d dead=%v hp=%d", real, dead, T.HP)
	}
}

// TestBuffDotTicksAndExpires 验证 DoT 周期结算与到期移除。
func TestBuffDotTicksAndExpires(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	T := testNpc(9, 100, 6, 5)
	rt.sprites[T.UUID] = T
	// 灼烧：3 秒、每 500ms 掉 10。
	if !rt.applyBuff(T, 3001, 1, 0, now) {
		t.Fatal("DoT 施加失败")
	}
	rt.tickBuffs(s, now+500)
	if T.HP != 90 {
		t.Fatalf("第一跳后应剩 90，实际 %d", T.HP)
	}
	now += 1000
	rt.tickBuffs(s, now)
	if T.HP != 80 {
		t.Fatalf("第二跳后应剩 80，实际 %d", T.HP)
	}
	// 跳到 3 秒之后：Buff 到期，血量不再变化。
	now += 3000
	rt.tickBuffs(s, now)
	if len(T.Buffs) != 0 {
		t.Fatalf("DoT 应已到期移除: %+v", T.Buffs)
	}
	before := T.HP
	rt.tickBuffs(s, now+5000)
	if T.HP != before {
		t.Fatalf("到期后不应继续掉血: %d → %d", before, T.HP)
	}
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
	if got := T.BuffStacks(3001); got != 3 {
		t.Fatalf("层数应封顶 3，实际 %d", got)
	}
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
	if !hasTag(out.Harms, HarmSeckill) {
		t.Fatalf("应触发斩杀标签: %+v", out.Harms)
	}
	if T.HP != 0 || !T.Dead {
		t.Fatalf("斩杀应直接致死: hp=%d dead=%v", T.HP, T.Dead)
	}
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
	if T.HP != 100 {
		t.Fatalf("无敌仍掉血: %d", T.HP)
	}
	if !hasTag(out.Harms, HarmSuper) {
		t.Fatalf("应带 SUPER 标签: %+v", out.Harms)
	}
}
