package entity

import (
	"testing"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// 测试脚手架
//
// 这些用例**不启动 MGR**：直接构造一个 State 就能测算法。理由有两个：
//   - entity 的函数本来就是"在 MGR goroutine 里跑的一段代码"，不需要 Actor；
//   - 单测要能精确控制 nowMS，用真实定时器就没法断言"第 3 拍才走"。
//
// 注意 State.owner 是 nil，所以 s.Now() 恒为 0；需要"当前时间"的地方
// 一律由用例显式传 nowMS，这也是生产代码不读时钟的原因之一。
// ============================================================================

func newTestState(size grid.Size) *mgr.State {
	return &mgr.State{
		Key:         scenemap.MapKey{MapID: 1},
		MapID:       1,
		Users:       make(map[scenemap.UUID]*mgr.UserState),
		NPES:        make(map[scenemap.UUID]*mgr.NpeAIInfo),
		Pets:        make(map[scenemap.UUID]scenemap.UUID),
		Bullets:     make(map[scenemap.UUID]*mgr.Bullet),
		Firewells:   make(map[scenemap.UUID]*mgr.Firewell),
		Ext:         make(map[string]any),
		RefreshData: mgr.NewRefreshData(),
		Index:       mgr.NewIndex(size),
	}
}

// addPlayer 往 State 里放一个玩家（不经过命令，只测 AI 的索敌）。
func addPlayer(t *testing.T, s *mgr.State, id scenemap.UUID, pos scenemap.GridPos) {
	t.Helper()
	s.Users[id] = &mgr.UserState{UUID: id, Kind: scenemap.EntityKindPlayer, GridPos: pos}
	if _, err := s.Index.Enter(id, pos, mgr.EnterOption{
		Kind:         scenemap.EntityKindPlayer,
		AllowOverlap: true,
	}); err != nil {
		t.Fatalf("玩家落格失败: %v", err)
	}
}

// mustSpawn 刷一只怪，失败直接 Fatal。
func mustSpawn(t *testing.T, s *mgr.State, spec Spec, pos scenemap.GridPos) scenemap.UUID {
	t.Helper()
	id, err := Spawn(s, spec, pos, grid.DefaultGeometry())
	if err != nil {
		t.Fatalf("Spawn(%s) 失败: %v", pos, err)
	}
	return id
}

// ============================================================================
// 创建 / 销毁
// ============================================================================

func TestSpawnWritesStateAndIndex(t *testing.T) {
	s := newTestState(grid.Size{Width: 8, Height: 8})
	id := mustSpawn(t, s, Spec{ConfigID: 101, AI: StateGuard, HP: 80, Attack: 7, Radius: 0}, scenemap.GridPos{X: 3, Y: 4})

	info := s.NPES[id]
	if info == nil {
		t.Fatalf("NPES 里没有 %s", id)
	}
	if info.TemplateID != 101 || info.HP != 80 || info.MaxHP != 80 || info.AI != StateGuard {
		t.Fatalf("状态写入不对: %+v", info)
	}
	if info.Hate == nil || len(info.Hate) != 0 {
		t.Fatalf("HateList 应为空切片, got %#v", info.Hate)
	}
	if info.BornAt != 0 {
		t.Fatalf("BornAt 应取 State.Now()=0, got %d", info.BornAt)
	}
	if pos, ok := s.Index.At(id); !ok || pos != (scenemap.GridPos{X: 3, Y: 4}) {
		t.Fatalf("索引落点不对: %s %v", pos, ok)
	}
	// 负数段 UUID：与玩家（正数）永不冲突。
	if id >= 0 {
		t.Fatalf("怪物 UUID 应为负数, got %d", id)
	}
}

func TestSpawnFallsBackToRingWhenBlocked(t *testing.T) {
	s := newTestState(grid.Size{Width: 9, Height: 9})
	first := mustSpawn(t, s, Spec{ConfigID: 1, HP: 10}, scenemap.GridPos{X: 4, Y: 4})
	second, err := Spawn(s, Spec{ConfigID: 1, HP: 10}, scenemap.GridPos{X: 4, Y: 4}, grid.DefaultGeometry())
	require.NoError(t, err, "第二只怪应能在附近找到空位")
	p1, _ := s.Index.At(first)
	p2, _ := s.Index.At(second)
	if p1 == p2 {
		t.Fatalf("两只怪重叠在 %s", p1)
	}
	if d := grid.Chebyshev(p1, p2); d > spawnSearchR {
		t.Fatalf("落点距离 %d 超过搜索半径 %d", d, spawnSearchR)
	}
}

func TestDespawnReturnsSpecAndReclaimsFootprint(t *testing.T) {
	s := newTestState(grid.Size{Width: 10, Height: 10})
	// 半径 1 的体积怪：占 (2*1+1)² = 9 格。
	id := mustSpawn(t, s, Spec{ConfigID: 7, HP: 30, Radius: 1}, scenemap.GridPos{X: 4, Y: 4})

	if got := s.Index.BlockCount(scenemap.GridPos{X: 3, Y: 3}); got != 1 {
		t.Fatalf("体积怪未登记动态阻挡, BlockCount=%d", got)
	}

	spec, ok := Despawn(s, id)
	if !ok {
		t.Fatal("Despawn 应返回 true")
	}
	if spec.ConfigID != 7 || spec.Radius != 1 {
		t.Fatalf("返回的 Spec 不对: %+v", spec)
	}
	if _, exists := s.NPES[id]; exists {
		t.Fatal("NPES 未删除")
	}
	if got := s.Index.BlockCount(scenemap.GridPos{X: 3, Y: 3}); got != 0 {
		t.Fatalf("动态阻挡未回收, BlockCount=%d", got)
	}
	if _, ok := Despawn(s, id); ok {
		t.Fatal("重复 Despawn 应返回 false（幂等）")
	}
}

func TestDamageDeathIsIdempotent(t *testing.T) {
	s := newTestState(grid.Size{Width: 8, Height: 8})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 20}, scenemap.GridPos{X: 2, Y: 2})

	if Damage(s, id, 5, 0) {
		t.Fatal("未致死却返回了 true")
	}
	if !Damage(s, id, 100, 0) {
		t.Fatal("致死应返回 true")
	}
	// 第二次致死必须返回 false：否则玩法会重复掉落、重复通关（规格 8.2）。
	if Damage(s, id, 100, 0) {
		t.Fatal("死亡结算不幂等：第二次仍返回 true")
	}
	info := s.NPES[id]
	if info.HP != 0 || info.AI != StateDead {
		t.Fatalf("死亡状态不对: HP=%d AI=%s", info.HP, info.AI)
	}
	if info.Alive() {
		t.Fatal("Alive() 应为 false")
	}
}

// ============================================================================
// AI 状态机
// ============================================================================

func TestGuardAcquiresNearestPlayer(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 100, AI: StateGuard}, scenemap.GridPos{X: 5, Y: 5})

	// 远的先插入（UUID 更小），近的后插入：断言"取最近"而不是"取先遇到"。
	addPlayer(t, s, 1, scenemap.GridPos{X: 10, Y: 5}) // 距离 5
	addPlayer(t, s, 2, scenemap.GridPos{X: 6, Y: 6})  // 距离 1
	TickAll(s, 1000)

	info := s.NPES[id]
	if info.AI != StateFight {
		t.Fatalf("应切到 FIGHT, got %s", info.AI)
	}
	if info.Target != 2 {
		t.Fatalf("应锁定最近的玩家 2, got %s", info.Target)
	}
	if len(info.Hate) != 1 || info.Hate[0].UUID != 2 {
		t.Fatalf("HateList 不对: %+v", info.Hate)
	}
}

func TestFixedNeverAcquires(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 100, AI: StateFixed}, scenemap.GridPos{X: 5, Y: 5})
	addPlayer(t, s, 1, scenemap.GridPos{X: 6, Y: 5})
	TickAll(s, 1000)

	if info := s.NPES[id]; info.AI != StateFixed || info.Target.Valid() {
		t.Fatalf("FIXED 不应索敌: AI=%s Target=%s", info.AI, info.Target)
	}
}

func TestFightAttackIsThrottledAndEmitsEvent(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 100, Attack: 12, AI: StateFight}, scenemap.GridPos{X: 5, Y: 5})
	addPlayer(t, s, 1, scenemap.GridPos{X: 6, Y: 5})
	s.NPES[id].Target = 1

	TickAll(s, 1000)
	evs := DrainEvents(s)
	if len(evs) != 1 {
		t.Fatalf("相邻应产生 1 条攻击事件, got %d", len(evs))
	}
	if evs[0].UUID != id || evs[0].Target != 1 || evs[0].Damage != 12 {
		t.Fatalf("事件内容不对: %+v", evs[0])
	}
	// 再取一次应为空：Drain 是"取走即清空"，避免同一刀结算两次。
	if again := DrainEvents(s); len(again) != 0 {
		t.Fatalf("事件应已清空, got %d", len(again))
	}

	// 1 秒内不再攻击。
	TickAll(s, 1500)
	if evs := DrainEvents(s); len(evs) != 0 {
		t.Fatalf("攻击节流失效: %d", len(evs))
	}
	// 到 2000ms 再攻击一次。
	TickAll(s, 2000)
	if evs := DrainEvents(s); len(evs) != 1 {
		t.Fatalf("2000ms 应再攻击一次, got %d", len(evs))
	}
	// 玩家 HP 不在这里扣（权威在玩家 Actor）：NpeAIInfo 没有玩家血量字段，
	// 这里只需断言"我们没动 Users"。
	if u := s.Users[1]; u == nil {
		t.Fatal("玩家不应被 AI 删掉")
	}
}

func TestFightGivesUpBeyondRange(t *testing.T) {
	s := newTestState(grid.Size{Width: 40, Height: 40})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 100, AI: StateFight}, scenemap.GridPos{X: 2, Y: 2})
	addPlayer(t, s, 1, scenemap.GridPos{X: 2 + ChaseGiveUp + 1, Y: 2})
	s.NPES[id].Target = 1
	s.NPES[id].Hate = []mgr.HateEntry{{UUID: 1, Hate: 5}}

	TickAll(s, 1000)
	info := s.NPES[id]
	if info.AI != StateReturn {
		t.Fatalf("超距应切 RETURN, got %s", info.AI)
	}
	if info.Target.Valid() || len(info.Hate) != 0 {
		t.Fatalf("RETURN 应清空目标与仇恨: %+v", info)
	}
}

func TestReturnWalksHomeHealsAndGuards(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 100, AI: StateGuard}, scenemap.GridPos{X: 7, Y: 5})
	home := scenemap.GridPos{X: 7, Y: 5}

	// 先把它挪远，再切成 RETURN。
	if err := MoveOne(s, id, scenemap.GridPos{X: 9, Y: 5}); err != nil {
		t.Fatalf("MoveOne 失败: %v", err)
	}
	info := s.NPES[id]
	info.HP = 50
	info.AI = StateReturn

	TickAll(s, 1000)
	if d := grid.Chebyshev(info.GridPos, home); d >= 2 {
		t.Fatalf("应朝出生点走近一步, got %s (距离 %d)", info.GridPos, d)
	}
	// 回血：每秒 5% of 100 = 5。
	if info.HP != 55 {
		t.Fatalf("第一秒应回 5 点血, got %d", info.HP)
	}

	// 继续走（每 500ms 一步），最多 4 拍必须到家。
	for now := int64(1500); now <= 3500 && info.GridPos != home; now += MoveIntervalMS {
		TickAll(s, now)
	}
	if info.GridPos != home {
		t.Fatalf("没有走回出生点, got %s", info.GridPos)
	}

	// 到家之后的第一拍才切 GUARD（状态迁移发生在 tick 开头）。
	TickAll(s, 4000)
	if info.AI != StateGuard {
		t.Fatalf("到家应切 GUARD, got %s", info.AI)
	}
	if info.HP <= 55 || info.HP > 100 {
		t.Fatalf("回血异常: %d", info.HP)
	}
}

func TestHateAccumulatesAndClearsAfterIdle(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 1000, AI: StateFixed}, scenemap.GridPos{X: 5, Y: 5})

	Damage(s, id, 30, 2)
	Damage(s, id, 10, 3)
	Damage(s, id, 5, 2)
	info := s.NPES[id]
	if info.Target != 2 {
		t.Fatalf("目标应为仇恨最高的 2, got %s", info.Target)
	}
	if len(info.Hate) != 2 || info.Hate[0].Hate != 35 {
		t.Fatalf("仇恨累加不对: %+v", info.Hate)
	}

	// 未到 5 秒：仇恨保留。
	TickAll(s, 1000)
	if len(s.NPES[id].Hate) == 0 {
		t.Fatal("未脱战就清空了仇恨")
	}
	// 超过 5 秒：清空（脱战）。
	TickAll(s, 6000)
	if got := s.NPES[id]; len(got.Hate) != 0 || got.Target.Valid() {
		t.Fatalf("脱战应清空仇恨: %+v", got)
	}
}

func TestGuardWanderIsDeterministicAndBounded(t *testing.T) {
	run := func() []scenemap.GridPos {
		s := newTestState(grid.Size{Width: 20, Height: 20})
		id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 100, AI: StateGuard}, scenemap.GridPos{X: 10, Y: 10})
		var trail []scenemap.GridPos
		for now := int64(0); now <= 3000; now += MoveIntervalMS {
			TickAll(s, now)
			trail = append(trail, s.NPES[id].GridPos)
		}
		return trail
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("轨迹长度不同: %d vs %d", len(a), len(b))
	}
	home := scenemap.GridPos{X: 10, Y: 10}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("同一局面两次运行结果不同（不可复现）: 第 %d 步 %s vs %s", i, a[i], b[i])
		}
		if d := grid.Chebyshev(home, a[i]); d > WanderRadius {
			t.Fatalf("游走超出出生点 %d 格: %s", WanderRadius, a[i])
		}
	}
}

func TestMoveOneRejectsBlockedCell(t *testing.T) {
	s := newTestState(grid.Size{Width: 10, Height: 10})
	id := mustSpawn(t, s, Spec{ConfigID: 1, HP: 10}, scenemap.GridPos{X: 2, Y: 2})
	other := mustSpawn(t, s, Spec{ConfigID: 1, HP: 10}, scenemap.GridPos{X: 3, Y: 2})

	err := MoveOne(s, id, scenemap.GridPos{X: 3, Y: 2})
	if err == nil {
		t.Fatalf("目标格被 %s 占着，应拒绝移动", other)
	}
	if pos, _ := s.Index.At(id); pos != (scenemap.GridPos{X: 2, Y: 2}) {
		t.Fatalf("被拒绝的移动不应改变位置, got %s", pos)
	}
}
