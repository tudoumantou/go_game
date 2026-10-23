package runtime

// battle_runtime_test.go 验证"到期事件 → 战斗裁决 → 队列前进"在真实
// 场景循环里跑通（文档 11.3/11.4）。
//
// 这些用例和 application 层的表驱动测试互补：那边只验证"计划长什么样"，
// 这里验证"计划被提交、应用到内存镜像、并触发下一条到期事件"。
//
// 白盒程度：需要直接往 scene.battles / scene.queues 里塞初始状态，
// 因为 Seed/LoadState 只接受对象与会话（战斗与队列属于"持久事实"，
// 由 store 恢复，M1 还没有对应的注入接口）。这些写入都发生在
// Start 之前，不违反单写者模型。

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

const (
	testTargetID   domain.ObjectID = 1000
	testAttackerID domain.ObjectID = 200
	testQueuedID   domain.ObjectID = 300
)

// newBattlePlanner 构造真实的 DefaultPlanner（不用桩）：
// 战斗超时、回城路线、建采集会话都由生产代码算出来。
func newBattlePlanner(t *testing.T) *application.DefaultPlanner {
	t.Helper()
	// 闭包捕获计数器：函数类型注入的好处是先声明变量、再在闭包里自增，
	// 不需要为"测试用 ID 生成器"定义一个类型。
	var idSeq int
	ids := func() string {
		idSeq++
		return fmt.Sprintf("bt-id-%d", idSeq)
	}
	var objectSeq uint64
	objectIDs := func() domain.ObjectID {
		objectSeq++
		return domain.ObjectID(9000 + objectSeq)
	}
	rules := application.Rules{
		MarchSpeedUnitsPerSec:    1000,
		CapacityUnits:            100,
		CollectRateMilliPerSec:   2500,
		MaxBattleQueuePerTarget:  100,
		BattleTimeoutMS:          30000,
		RefreshBatchSize:         3,
		ResourcePositionAttempts: 5,
		RichBlock: application.RichBlockRule{
			MaxCount: 4,
			Tiers: []application.RichTier{
				{ResourceType: 1, ItemID: 101, Weight: 1, Minimum: 1, TotalPerNode: 500},
			},
		},
	}
	seeds := func(block, generation, slot int64) int64 {
		return block*1000003 + generation*1009 + slot*31
	}
	p, err := application.NewPlanner(domain.DefaultTopology(1), rules, ids, objectIDs, seeds)
	require.NoError(t, err, "构造规划器失败")
	return p
}

// battleFixture 造一个"目标上正在开战 + 队列里有人"的场景。
type battleFixture struct {
	scene  *SceneLoop
	pool   *TxWorkerPool
	store  *MemoryCommitter
	clock  *FakeClock
	nowMS  int64
	battle *domain.BattleRecord
}

func newBattleFixture(t *testing.T) *battleFixture {
	t.Helper()
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	nowMS := NowMS(clock)
	store := NewMemoryCommitter()
	scene, pool := newTestScene(t, newBattlePlanner(t), store, clock)

	home := &domain.City{
		Header:   domain.ObjectHeader{WorldID: 1, ID: 100, Kind: domain.ObjectKindCity, OwnerScene: 1, Version: 1},
		PlayerID: 1,
		Position: domain.Pos{X: 0, Y: 0},
	}
	enemyHome := &domain.City{
		Header:   domain.ObjectHeader{WorldID: 1, ID: 101, Kind: domain.ObjectKindCity, OwnerScene: 1, Version: 1},
		PlayerID: 3,
		Position: domain.Pos{X: 1000, Y: 0},
	}
	res := &domain.ResourceNode{
		Header:    domain.ObjectHeader{WorldID: 1, ID: testTargetID, Kind: domain.ObjectKindResource, OwnerScene: 1, Version: 3},
		Position:  domain.Pos{X: 10000, Y: 0},
		Total:     500,
		Remaining: 500,
		ItemID:    101,
	}
	deadline := nowMS + 30000
	attacker := &domain.Troop{
		Header:        domain.ObjectHeader{WorldID: 1, ID: testAttackerID, Kind: domain.ObjectKindTroop, OwnerScene: 1, Version: 2},
		PlayerID:      1,
		HomeCityID:    100,
		Phase:         domain.TroopPhaseFighting,
		Busy:          domain.BusyState{Reason: domain.BusyReasonBattle, OpID: "b1", DeadlineMS: deadline},
		BattleID:      "b1",
		Target:        domain.TargetRef{TargetID: testTargetID, Intent: domain.MarchIntentAttack},
		Cargo:         map[int32]int64{},
		CapacityUnits: 100,
		Stats:         domain.CombatSnapshot{Attack: 100, Defense: 100, HP: 1000, RuleVersion: "m1"},
	}
	queued := &domain.Troop{
		Header:        domain.ObjectHeader{WorldID: 1, ID: testQueuedID, Kind: domain.ObjectKindTroop, OwnerScene: 1, Version: 1},
		PlayerID:      3,
		HomeCityID:    101,
		Phase:         domain.TroopPhaseQueued,
		Target:        domain.TargetRef{TargetID: testTargetID, Intent: domain.MarchIntentAttack},
		Cargo:         map[int32]int64{},
		CapacityUnits: 100,
		Stats:         domain.CombatSnapshot{Attack: 100, Defense: 100, HP: 1000, RuleVersion: "m1"},
	}
	objects := []domain.ObjectSnapshot{
		{Key: home.Header.Key(), Kind: domain.ObjectKindCity, OwnerScene: 1, Version: 1, PlayerID: 1, City: home},
		{Key: enemyHome.Header.Key(), Kind: domain.ObjectKindCity, OwnerScene: 1, Version: 1, PlayerID: 3, City: enemyHome},
		{Key: res.Header.Key(), Kind: domain.ObjectKindResource, OwnerScene: 1, Version: 3, Resource: res},
		{Key: attacker.Header.Key(), Kind: domain.ObjectKindTroop, OwnerScene: 1, Version: 2, PlayerID: 1, Troop: attacker},
		{Key: queued.Header.Key(), Kind: domain.ObjectKindTroop, OwnerScene: 1, Version: 1, PlayerID: 3, Troop: queued},
	}
	if err := scene.LoadState(objects, nil, 3); err != nil {
		t.Fatalf("注入对象失败: %v", err)
	}

	battle := &domain.BattleRecord{
		BattleID:      "b1",
		WorldID:       1,
		TargetID:      testTargetID,
		AttackerID:    testAttackerID,
		DefenderID:    testTargetID,
		InputHash:     domain.PayloadHashOf([]byte("b1")),
		AttackerStats: attacker.Stats,
		DefenderStats: domain.CombatSnapshot{Attack: 80, Defense: 120, HP: 1200, RuleVersion: "m1"},
		RuleVersion:   "m1",
		RandomSeed:    11,
		Status:        domain.BattleStatusPending,
		DeadlineMS:    deadline,
	}
	// 白盒注入：模拟"从 map_battle / map_battle_queue 恢复出来的事实"。
	scene.battles[testTargetID] = battle
	scene.queues[testTargetID] = []domain.QueueEntry{
		{TargetID: testTargetID, TroopID: testQueuedID, Seq: nowMS*1000 + 1},
	}
	// 队列已由显式表维护：不要再让 rebuildQueues 用旧格式覆盖它。
	scene.queueExplicit = true

	scene.heap.Schedule(domain.DueEvent{
		WorldID:  1,
		DueAtMS:  deadline,
		Kind:     domain.DueKindBattleTimeout,
		ObjectID: testTargetID,
		BattleID: "b1",
	})

	return &battleFixture{scene: scene, pool: pool, store: store, clock: clock, nowMS: nowMS, battle: battle}
}

func (f *battleFixture) run(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := f.scene.Start(ctx); err != nil {
		t.Fatalf("启动场景失败: %v", err)
	}
	t.Cleanup(func() { _ = f.scene.Shutdown(context.Background()) })
	t.Cleanup(func() { _ = f.pool.Shutdown(context.Background()) })
}

// battleOn 返回目标当前进行中的战斗（拷贝），持读锁避免与循环竞争。
func battleOn(s *SceneLoop, target domain.ObjectID) (domain.BattleRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.battles[target]
	if !ok {
		return domain.BattleRecord{}, false
	}
	return *b, true
}

// waitFor 轮询等待条件成立。到期事件由场景循环异步处理，
// 测试无法直接"等一个提交"，只能用有界轮询（禁止无界 sleep）。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

func findTroop(t *testing.T, scene *SceneLoop, player int64, id domain.ObjectID) *domain.Troop {
	t.Helper()
	for _, tr := range scene.QueryTroops(player) {
		if tr.Header.ID == id {
			return tr
		}
	}
	return nil
}

// TestBattleTimeoutEventResolvesAndAdvancesQueue 验证到期超时事件
// 真正产生裁决，并把队首拉起开战。
func TestBattleTimeoutEventResolvesAndAdvancesQueue(t *testing.T) {
	f := newBattleFixture(t)
	f.run(t)

	// 时间还早：事件不该触发（防止"不看 deadline 一律立即处理"）。
	f.clock.Advance(10 * time.Second)
	f.scene.Wakeup()
	time.Sleep(20 * time.Millisecond)
	if b, ok := battleOn(f.scene, testTargetID); !ok || b.BattleID != "b1" {
		t.Fatalf("未到期就换了战斗: %+v", b)
	}

	// 推进到超时点：PENDING→TIMED_OUT，攻方回城，队首开战。
	f.clock.Advance(21 * time.Second)
	f.scene.Wakeup()
	waitFor(t, "队首超时后下一名开战", func() bool {
		b, ok := battleOn(f.scene, testTargetID)
		return ok && b.BattleID != "b1" && b.Status == domain.BattleStatusPending && b.AttackerID == testQueuedID
	})

	attacker := findTroop(t, f.scene, 1, testAttackerID)
	if attacker == nil {
		t.Fatal("攻方部队丢失")
	}
	if attacker.Phase != domain.TroopPhaseReturning {
		t.Fatalf("超时后攻方 Phase=%v, want RETURNING", attacker.Phase)
	}
	if attacker.Move == nil {
		t.Fatal("超时后攻方必须生成回城路线（否则没有到达事件）")
	}
	if attacker.BattleID != "" || attacker.Busy.Reason != domain.BusyReasonNone {
		t.Fatalf("超时后战斗锁未解除: battle_id=%q busy=%+v", attacker.BattleID, attacker.Busy)
	}

	queued := findTroop(t, f.scene, 3, testQueuedID)
	if queued == nil {
		t.Fatal("队列成员丢失")
	}
	next, _ := battleOn(f.scene, testTargetID)
	if queued.Phase != domain.TroopPhaseFighting || queued.BattleID != next.BattleID {
		t.Fatalf("队首未开战: phase=%v battle_id=%q, want FIGHTING %q", queued.Phase, queued.BattleID, next.BattleID)
	}
	if queued.Busy.Reason != domain.BusyReasonBattle {
		t.Fatalf("队首缺少战斗锁: %+v", queued.Busy)
	}
	if q := f.scene.Snapshot().QueuedOn(testTargetID); len(q) != 0 {
		t.Fatalf("队首出队后队列应为空: %+v", q)
	}

	// 超时裁决的事实在回执里：TIMED_OUT 且无 Resolver 结果。
	rc, ok := f.store.Receipt(domain.CommandKey{
		WorldID:   1,
		PlayerID:  1, // 内部命令用攻方玩家身份
		RequestID: domain.BattleResultKey("b1"),
	})
	if !ok {
		t.Fatal("超时命令必须落回执")
	}
	var payload map[string]any
	if err := json.Unmarshal(rc.ResultJSON, &payload); err != nil {
		t.Fatalf("回执解析失败: %v (%s)", err, rc.ResultJSON)
	}
	if payload["status"] != "TIMED_OUT" {
		t.Fatalf("回执状态=%v, want TIMED_OUT", payload["status"])
	}
	if payload["timed_out"] != true {
		t.Fatalf("回执未标记超时: %v", payload)
	}
}

// TestBattleResultCommandAdvancesQueue 验证结果命令经运行时的完整链路：
// 提交 → 提交成功 → 应用 → 队首开战。
func TestBattleResultCommandAdvancesQueue(t *testing.T) {
	f := newBattleFixture(t)
	f.run(t)

	// 守方（资源点）获胜：攻方回城，队首开始下一战。
	env := envOf("result-b1")
	env.PlayerID = 1
	err := f.scene.SubmitSync(env, application.Command{
		Kind:         application.CmdBattleResult,
		BattleResult: application.BattleResultParams{BattleID: "b1", Winner: testTargetID},
	})
	require.NoError(t, err, "提交裁决失败")

	attacker := findTroop(t, f.scene, 1, testAttackerID)
	if attacker == nil || attacker.Phase != domain.TroopPhaseReturning {
		t.Fatalf("败方攻方应回城: %+v", attacker)
	}
	queued := findTroop(t, f.scene, 3, testQueuedID)
	if queued == nil || queued.Phase != domain.TroopPhaseFighting {
		t.Fatalf("队首应立即开战: %+v", queued)
	}
	next, ok := battleOn(f.scene, testTargetID)
	if !ok || next.BattleID == "b1" || next.AttackerID != testQueuedID {
		t.Fatalf("目标上应是新战斗: %+v", next)
	}
	if q := f.scene.Snapshot().QueuedOn(testTargetID); len(q) != 0 {
		t.Fatalf("队列应已清空: %+v", q)
	}

	// 同一请求重放（客户端超时重试）：命中 map_command 回执，
	// 不得再次推进队列或推进部队版本。
	beforeBattle := next.BattleID
	beforeVersion := queued.Header.Version
	if err := f.scene.SubmitSync(env, application.Command{
		Kind:         application.CmdBattleResult,
		BattleResult: application.BattleResultParams{BattleID: "b1", Winner: 200},
	}); err != nil {
		t.Fatalf("重复结果应幂等成功: %v", err)
	}
	after, _ := battleOn(f.scene, testTargetID)
	if after.BattleID != beforeBattle {
		t.Fatalf("重复结果改变了当前战斗: %s → %s", beforeBattle, after.BattleID)
	}
	if got := findTroop(t, f.scene, 3, testQueuedID); got == nil || got.Header.Version != beforeVersion {
		t.Fatalf("重复结果推进了队首版本: %+v", got)
	}
}

// TestApplyQueueChangesKeepsFIFO 直接验证内存队列的排序与去重：
// 入队按 Seq 排、出队按 troop 删、重复入队忽略。
func TestApplyQueueChangesKeepsFIFO(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	scene, pool := newTestScene(t, &stubPlanner{}, NewMemoryCommitter(), clock)
	defer func() { _ = pool.Shutdown(context.Background()) }()

	scene.applyPlan(&domain.MutationPlan{
		QueueEnqueue: []domain.QueueEntry{
			{TargetID: 1000, TroopID: 30, Seq: 300},
			{TargetID: 1000, TroopID: 10, Seq: 100},
			{TargetID: 1000, TroopID: 20, Seq: 200},
		},
	})
	if !scene.queueExplicit {
		t.Fatal("出现显式队列变更后必须置 queueExplicit")
	}
	if got := scene.queues[1000]; len(got) != 3 || got[0].TroopID != 10 || got[1].TroopID != 20 || got[2].TroopID != 30 {
		t.Fatalf("队列未按 Seq 排序: %+v", got)
	}

	// 重复入队：UNIQUE(world,target,troop) 的内存预检，直接忽略。
	scene.applyPlan(&domain.MutationPlan{
		QueueEnqueue: []domain.QueueEntry{{TargetID: 1000, TroopID: 20, Seq: 999}},
	})
	if got := scene.queues[1000]; len(got) != 3 {
		t.Fatalf("重复入队应被忽略: %+v", got)
	}

	// 出队中间成员：其余顺序不变。
	scene.applyPlan(&domain.MutationPlan{
		QueueDequeue: []domain.QueueEntry{{TargetID: 1000, TroopID: 20}},
	})
	if got := scene.queues[1000]; len(got) != 2 || got[0].TroopID != 10 || got[1].TroopID != 30 {
		t.Fatalf("出队后顺序错误: %+v", got)
	}

	// 全部出队后删除空队列，避免快照里出现空切片。
	scene.applyPlan(&domain.MutationPlan{
		QueueDequeue: []domain.QueueEntry{
			{TargetID: 1000, TroopID: 10},
			{TargetID: 1000, TroopID: 30},
		},
	})
	if _, ok := scene.queues[1000]; ok {
		t.Fatalf("空队列应被删除: %+v", scene.queues[1000])
	}
}

// TestRebuildQueuesLegacyMarkers 验证 M1 历史数据（战斗记录 + 标记）
// 仍能重建队列，且顺序确定（DeadlineMS 升序）。
func TestRebuildQueuesLegacyMarkers(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	scene, pool := newTestScene(t, &stubPlanner{}, NewMemoryCommitter(), clock)
	defer func() { _ = pool.Shutdown(context.Background()) }()

	scene.battles[1000] = &domain.BattleRecord{
		BattleID: "q2", TargetID: 1000, AttackerID: 20,
		Status: domain.BattleStatusPending, DeadlineMS: 200, AppliedOperationID: queuedMarker,
	}
	scene.battles[1001] = &domain.BattleRecord{
		BattleID: "q1", TargetID: 1001, AttackerID: 10,
		Status: domain.BattleStatusPending, DeadlineMS: 100, AppliedOperationID: queuedMarker,
	}
	// 真正在打的战斗（无标记）不进队列。
	scene.battles[1002] = &domain.BattleRecord{
		BattleID: "live", TargetID: 1002, AttackerID: 30,
		Status: domain.BattleStatusPending, DeadlineMS: 50,
	}
	scene.applyPlan(&domain.MutationPlan{})
	if got := scene.queues[1000]; len(got) != 1 || got[0].TroopID != 20 {
		t.Fatalf("旧格式队列重建失败: %+v", got)
	}
	if got := scene.queues[1002]; len(got) != 0 {
		t.Fatalf("无标记的队首不应进队列: %+v", got)
	}
}
