package battle

import (
	"testing"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/mgr"
	"github.com/stretchr/testify/require"
)

// addNpcState 在场景里放一只怪（NPES 权威数据），供 target/death 相关用例使用。
func addNpcState(s *mgr.State, id scenemap.UUID, cfgID int32, hp int64, pos scenemap.GridPos) {
	s.NPES[id] = &mgr.NpeAIInfo{
		UUID: id, TemplateID: cfgID, Kind: scenemap.EntityKindNPE,
		GridPos: pos, HP: hp, MaxHP: hp, AI: mgr.AIStateIdle,
		Hate: make([]mgr.HateEntry, 0, 2),
	}
}

// TestLaunchDoesNotHarmUntilCast 验证 §13.18：LAUNCH 不产生伤害，
// 只有 CAST（前摇到期）才扣血。
func TestLaunchDoesNotHarmUntilCast(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	T := testNpc(9, 1000, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T

	res := rt.cast(s, Cast{Caster: A.UUID, SkillID: 2001, Target: T.UUID})
	if !res.Accepted {
		t.Fatalf("出手应被受理: code=%d", res.Code)
	}
	if res.PrefixMS <= 0 {
		t.Fatalf("普攻前摇应大于 0，实际 %d", res.PrefixMS)
	}
	require.EqualValues(t, 1000, T.HP)
	require.EqualValues(t, 1, rt.PendingDelayed())

	now += 10000
	rt.RunDue(s, now)
	require.EqualValues(t, 910, T.HP)
}

// TestCastCooldownRejects 验证 CD 期间重复出手被拒（且不排 CAST）。
func TestCastCooldownRejects(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	T := testNpc(9, 1000, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T

	if res := rt.cast(s, Cast{Caster: A.UUID, SkillID: 2001, Target: T.UUID}); !res.Accepted {
		t.Fatalf("首次出手应受理: %d", res.Code)
	}
	if res := rt.cast(s, Cast{Caster: A.UUID, SkillID: 2001, Target: T.UUID}); res.Code != ErrCodeCooldown {
		t.Fatalf("第二次应报冷却，实际 code=%d", res.Code)
	}
}

// TestCastRejectsCityAndMissingSkill 验证 §13.9 与 §13.13：
// 主城不结算、技能缺失显式报错。
func TestCastRejectsCityAndMissingSkill(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, &Options{NoBattle: true})
	A := testPlayer(1, 5, 5)
	rt.sprites[A.UUID] = A
	if res := rt.cast(s, Cast{Caster: A.UUID, SkillID: 2001}); res.Code != ErrCodeNoBattle {
		t.Fatalf("主城应拒绝结算，实际 code=%d", res.Code)
	}

	s2 := newTestState()
	rt2 := testRuntime(s2, &now, nil)
	A2 := testPlayer(1, 5, 5)
	rt2.sprites[A2.UUID] = A2
	if res := rt2.cast(s2, Cast{Caster: A2.UUID, SkillID: 424242}); res.Code != ErrCodeMissingSkill {
		t.Fatalf("缺失技能应显式报错，实际 code=%d", res.Code)
	}
}

// TestMirrorWhitelist 验证 §13.15：镜像施法必须在地图白名单内。
func TestMirrorWhitelist(t *testing.T) {
	var now int64 = 1000
	s := newTestState()
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	A.Kind = scenemap.EntityKindMirror
	rt.sprites[A.UUID] = A
	if res := rt.cast(s, Cast{Caster: A.UUID, SkillID: 2001}); res.Code != ErrCodeMirrorForbidden {
		t.Fatalf("白名单为空时应拒绝镜像施法，实际 code=%d", res.Code)
	}

	s2 := newTestState()
	rt2 := testRuntime(s2, &now, &Options{MapType: 1, MirrorMaps: []int32{1}})
	A2 := testPlayer(1, 5, 5)
	A2.Kind = scenemap.EntityKindMirror
	rt2.sprites[A2.UUID] = A2
	if res := rt2.cast(s2, Cast{Caster: A2.UUID, SkillID: 2001}); !res.Accepted {
		t.Fatalf("白名单命中应允许镜像施法，实际 code=%d", res.Code)
	}
}

// TestMultiPartDamage 验证多段伤害按段间隔逐段结算（§10.3 step 3）。
func TestMultiPartDamage(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	// 自定义三段技：系数 0、固定 10，段间隔 100ms，无前摇。
	cfg := &SkillConfig{
		ID: 5001, HarmType: HarmTypeNormal, Coeff: 0, Harm: 10,
		Range: 2, TargetNum: 1, TargetType: TargetEnemy, Area: AreaSingle,
		Parts: 3, PartIntervalMS: 100, CooldownMS: 0,
	}
	rt.table.AddSkill(*cfg)
	A := testPlayer(1, 5, 5)
	T := testNpc(9, 1000, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T

	res := rt.cast(s, Cast{Caster: A.UUID, SkillID: cfg.ID, Target: T.UUID})
	if !res.Accepted {
		t.Fatalf("出手应受理: %d", res.Code)
	}
	// 第 0 段无前摇 → 立即结算。
	require.EqualValues(t, 990, T.HP)
	now += 150
	rt.RunDue(s, now)
	require.EqualValues(t, 980, T.HP)
	now += 150
	rt.RunDue(s, now)
	require.EqualValues(t, 970, T.HP)
	require.EqualValues(t, 0, rt.PendingDelayed())
}

// TestAreaSkillHitsMultiple 验证范围技按半径命中多人并按人数截断。
func TestAreaSkillHitsMultiple(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	rt.sprites[A.UUID] = A
	// 以 (7,5) 为中心，半径 2 内放三只怪。
	ids := []scenemap.UUID{91, 92, 93}
	pos := []scenemap.GridPos{{X: 7, Y: 5}, {X: 8, Y: 6}, {X: 6, Y: 4}}
	for i, id := range ids {
		T := testNpc(id, 1000, pos[i].X, pos[i].Y)
		rt.sprites[id] = T
		addNpcState(s, id, 9001, 1000, pos[i])
	}
	res := rt.cast(s, Cast{Caster: A.UUID, SkillID: 2002, Target: 91})
	if !res.Accepted {
		t.Fatalf("范围技应受理: %d", res.Code)
	}
	now += 10000
	rt.RunDue(s, now)
	for _, id := range ids {
		if rt.sprites[id].HP >= 1000 {
			t.Fatalf("怪 %d 未被范围技命中: hp=%d", id, rt.sprites[id].HP)
		}
	}
}

// TestKillIsIdempotent 验证 §13.5：同一只怪只走一次死亡链。
//
// 场景：一次致死伤害后再补一刀（模拟同拍多来源），OnNpcDead 只能被调用
// 一次，MonDeadEvent 也只能发一次；场景权威（NpeAIInfo）必须同步成死亡态。
func TestKillIsIdempotent(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	var deadCalls int
	var events []any
	rt := testRuntime(s, &now, &Options{
		OnNpcDead: func(st *mgr.State, uuid scenemap.UUID, npcID int32) { deadCalls++ },
		Notify:    func(uuid scenemap.UUID, evt any) { events = append(events, evt) },
	})
	A := testPlayer(1, 5, 5)
	rt.sprites[A.UUID] = A
	T := testNpc(9, 50, 6, 5)
	T.CfgID = 9001
	rt.sprites[T.UUID] = T
	addNpcState(s, 9, 9001, 50, scenemap.GridPos{X: 6, Y: 5})

	// 两次死亡结算（技能致死 + Dot 同拍致死）。
	rt.handleDead(s, T, A.UUID, now)
	rt.handleDead(s, T, A.UUID, now)

	require.EqualValues(t, 1, deadCalls)
	var monDead int
	for _, evt := range events {
		if m, ok := evt.(MonDeadEvent); ok {
			monDead++
			if m.Monster != 9 || m.NpcID != 9001 {
				t.Fatalf("死亡事件内容错误: %+v", m)
			}
		}
	}
	require.EqualValues(t, 1, monDead)
	info := s.NPES[9]
	if info.HP != 0 || info.AI != mgr.AIStateDead || info.DeadAt == 0 {
		t.Fatalf("场景权威未同步死亡态: %+v", info)
	}
}

// TestDeadTargetIgnoredByNewCast 验证死亡目标不会被后续技能再次结算。
func TestDeadTargetIgnoredByNewCast(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	T := testNpc(9, 10, 6, 5)
	rt.sprites[A.UUID] = A
	rt.sprites[T.UUID] = T
	addNpcState(s, 9, 9001, 10, scenemap.GridPos{X: 6, Y: 5})

	// 用一次高伤技能打死。
	cfg := &SkillConfig{ID: 5002, HarmType: HarmTypeNormal, Coeff: 100 * PermilleOne, Harm: 1000,
		Range: 2, TargetNum: 1, TargetType: TargetEnemy, Area: AreaSingle}
	rt.table.AddSkill(*cfg)
	rt.cast(s, Cast{Caster: A.UUID, SkillID: cfg.ID, Target: T.UUID})
	if !T.Dead {
		t.Fatalf("应已击杀: hp=%d", T.HP)
	}
	before := T.HP
	// 用另一个技能再打一次（绕过冷却），目标已死应被过滤。
	cfg2 := &SkillConfig{ID: 5003, HarmType: HarmTypeNormal, Coeff: PermilleOne, Range: 2,
		TargetNum: 1, TargetType: TargetEnemy, Area: AreaSingle}
	rt.table.AddSkill(*cfg2)
	now += 1000
	rt.cast(s, Cast{Caster: A.UUID, SkillID: cfg2.ID, Target: T.UUID})
	now += 1000
	rt.RunDue(s, now)
	require.EqualValues(t, before, T.HP)
}

// TestEnsureIsIdempotent 验证 §4.2：重复初始化复用同一份运行时。
func TestEnsureIsIdempotent(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt1 := testRuntime(s, &now, nil)
	rt2 := Ensure(s, Options{Table: DefaultTable(), Now: func() int64 { return now }, NoBattle: true})
	require.EqualValues(t, rt2, rt1, "重复 Ensure 应复用同一运行时")
	if rt2.NoBattle() {
		t.Fatal("复用后不应被第二次参数覆盖（单写者语义）")
	}
}

// TestAllySingleTargetsCasterByDefault 验证友方单体的默认目标是自己
// （治疗/增益技没点目标时不能打空）。
func TestAllySingleTargetsCasterByDefault(t *testing.T) {
	s := newTestState()
	var now int64 = 1000
	rt := testRuntime(s, &now, nil)
	A := testPlayer(1, 5, 5)
	rt.sprites[A.UUID] = A
	cfg := &SkillConfig{ID: 5004, HarmType: HarmTypeNotDamage,
		Range: 5, TargetNum: 1, TargetType: TargetAlly, Area: AreaSingle}
	sc := &SkillContext{SkillID: cfg.ID, Caster: A.UUID, Action: ActionCast}
	got := rt.selectTargets(s, A, cfg, sc)
	if len(got) != 1 || got[0].UUID != A.UUID {
		t.Fatalf("友方单体默认应选中自己，实际 %+v", got)
	}
}
