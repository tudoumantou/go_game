package application

// battle_test.go 覆盖文档 11.3/11.4 的裁决落地与队列前进：
//   - 结果应用：战斗终态、双方阶段与版本、资源点胜者转采集；
//   - 幂等：终态/重复请求只回执，不重算、不推进版本；
//   - 超时：攻击无效、防守保留、无战损；
//   - 队列：队首结束下一名开战，FIFO 顺序稳定。
//
// 断言全部落在 MutationPlan 的形状上：这一层不直接改库，
// "计划里有没有这条写入"才是可验证的契约。

import (
	"errors"
	"fmt"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

// fightingTroop 造一支正在战斗的部队（FIGHTING + battle_id + Busy=Battle
// 三件套齐全，符合 ValidateTroop 的反向约束）。
func fightingTroop(id domain.ObjectID, player int64, battleID string, deadline int64) *domain.Troop {
	tr := newTroop(id, player, domain.TroopPhaseFighting)
	tr.BattleID = battleID
	tr.Busy = domain.BusyState{
		Reason:     domain.BusyReasonBattle,
		OpID:       battleID,
		SinceMS:    0,
		DeadlineMS: deadline,
	}
	tr.Target = domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentAttack}
	return tr
}

// queuedTroop 造一支在目标队列中排队的部队。
func queuedTroop(id domain.ObjectID, player int64) *domain.Troop {
	tr := newTroop(id, player, domain.TroopPhaseQueued)
	tr.Target = domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentAttack}
	return tr
}

// pendingBattle 造一条进行中的战斗记录。defenderStats 用守军快照
// （资源点没有 Troop 对象，属性只能来自记录，11.1）。
func pendingBattle(id string, attacker, defender domain.ObjectID) *domain.BattleRecord {
	return &domain.BattleRecord{
		BattleID:      id,
		WorldID:       testWorld,
		TargetID:      1000,
		AttackerID:    attacker,
		DefenderID:    defender,
		InputHash:     domain.PayloadHashOf([]byte(id)),
		RuleVersion:   m1RuleVersion,
		RandomSeed:    7,
		Status:        domain.BattleStatusPending,
		DeadlineMS:    40000,
		DefenderStats: m1GuardStats(0),
	}
}

// TestPlanBattleResultAttackerWinsOnResource 验证攻方打赢资源点后：
// 战斗 RESOLVED、攻方转采集、版本 +1、回执含 winner/loser/queue_next。
func TestPlanBattleResultAttackerWinsOnResource(t *testing.T) {
	h := newHarness(t)
	attacker := fightingTroop(200, 1, "b1", 40000)
	res := newResource(1000, 10000, 0, 100)
	snap := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{
			cityObject(100, 1, 0, 0), troopObject(attacker), resourceObject(res),
		},
		Battles: []*domain.BattleRecord{pendingBattle("b1", 200, 1000)},
	})
	env := testEnv(1, "req-battle-win", 21)
	plan, err := h.planner.Plan(snap, env, Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "b1", Winner: 200, AttackerWon: true},
	}, 10000)
	require.NoError(t, err, "裁决计划失败")

	require.Len(t, plan.BattleUpsert, 1)
	rec := plan.BattleUpsert[0]
	require.EqualValues(t, domain.BattleStatusResolved, rec.Status)
	require.NotNil(t, rec.Result, fmt.Sprintf("结果未写入: %+v", rec.Result))
	require.EqualValues(t, 200, rec.Result.WinnerID)
	require.EqualValues(t, true, rec.Result.AttackerWon)
	// INV-06 的落点：applied_operation_id 必须是命令的内部键，
	// 迟到/重复命令据此识别"这个 battle 已有终态"。
	require.EqualValues(t, env.RequestID, rec.AppliedOperationID)

	up, ok := findUpsert(plan, domain.ObjectKindTroop)
	require.True(t, ok, "缺少部队 upsert")
	require.EqualValues(t, domain.TroopPhaseCollecting, up.Troop.Phase)
	require.EqualValues(t, attacker.Header.Version+1, up.Troop.Header.Version)
	require.EqualValues(t, "", up.Troop.BattleID, fmt.Sprintf("战斗态未解除: battle_id=%q busy=%+v", up.Troop.BattleID, up.Troop.Busy))
	require.EqualValues(t, domain.BusyReasonNone, up.Troop.Busy.Reason)
	require.Len(t, plan.CollectStart, 1, fmt.Sprintf("胜者应新建采集会话: %+v", plan.CollectStart))
	require.EqualValues(t, 200, plan.CollectStart[0].TroopID)

	m := receiptMap(t, plan)
	require.EqualValues(t, 200, jsonInt(t, m, "winner"), fmt.Sprintf("回执胜负不符: %v", m))
	require.EqualValues(t, 1000, jsonInt(t, m, "loser"))
	require.EqualValues(t, "", jsonStr(t, m, "queue_next"))
	require.EqualValues(t, "RESOLVED", jsonStr(t, m, "status"))
}

// TestPlanBattleResultDefenderTroopWins 验证守方（部队）获胜：
// 双方都回城、版本各 +1、败者保留 Cargo（10.4 无掠夺）。
func TestPlanBattleResultDefenderTroopWins(t *testing.T) {
	h := newHarness(t)
	attacker := fightingTroop(200, 1, "b1", 40000)
	attacker.Cargo = map[int32]int64{101: 5}
	attacker.Header.Version = 3
	defender := newTroop(900, 2, domain.TroopPhaseCollecting)
	defender.Cargo = map[int32]int64{102: 8}
	defender.Header.Version = 5
	defender.HomeCityID = 101
	snap := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{
			cityObject(100, 1, 0, 0), cityObject(101, 2, 20000, 0),
			troopObject(attacker), troopObject(defender), resourceObject(newResource(1000, 10000, 0, 100)),
		},
		Battles: []*domain.BattleRecord{pendingBattle("b1", 200, 900)},
	})
	plan, err := h.planner.Plan(snap, testEnv(1, "req-battle-lose", 22), Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "b1", Winner: 900},
	}, 10000)
	require.NoError(t, err, "裁决计划失败")
	require.Len(t, plan.ObjectsUpsert, 2)
	for _, o := range plan.ObjectsUpsert {
		if o.Kind != domain.ObjectKindTroop {
			continue
		}
		require.EqualValues(t, domain.TroopPhaseReturning, o.Troop.Phase)
		require.NotNil(t, o.Troop.Move)
		switch o.Troop.Header.ID {
		case 200:
			require.EqualValues(t, 4, o.Troop.Header.Version)
			require.EqualValues(t, 5, o.Troop.Cargo[101])
		case 900:
			require.EqualValues(t, 6, o.Troop.Header.Version)
			require.EqualValues(t, 8, o.Troop.Cargo[102])
		}
	}
	require.Len(t, plan.CollectStart, 0)
	m := receiptMap(t, plan)
	require.EqualValues(t, 900, jsonInt(t, m, "winner"), fmt.Sprintf("回执=%v", m))
	require.EqualValues(t, "RETURNING", jsonStr(t, m, "defender_route"))
}

// TestPlanBattleResultIdempotent 验证重复结果不重算、不推进版本。
func TestPlanBattleResultIdempotent(t *testing.T) {
	h := newHarness(t)
	attacker := fightingTroop(200, 1, "b1", 40000)
	objects := []domain.ObjectSnapshot{
		cityObject(100, 1, 0, 0), troopObject(attacker), resourceObject(newResource(1000, 10000, 0, 100)),
	}
	env := testEnv(1, "req-battle-once", 23)
	cmd := Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "b1", Winner: 200},
	}

	first, err := h.planner.Plan(snapshot(SnapshotOptions{
		Objects: objects,
		Battles: []*domain.BattleRecord{pendingBattle("b1", 200, 1000)},
	}), env, cmd, 10000)
	require.NoError(t, err, "首次裁决失败")

	// (a) 同一请求重放：快照里已有回执，规划器直接返回回执，零写入。
	replay, err := h.planner.Plan(snapshot(SnapshotOptions{
		Objects:  objects,
		Battles:  []*domain.BattleRecord{pendingBattle("b1", 200, 1000)},
		Receipts: []domain.CommandReceipt{*first.CommandReceipt},
	}), env, cmd, 20000)
	require.NoError(t, err, "重放失败")
	require.Empty(t, replay.ObjectsUpsert, fmt.Sprintf("重放不得产生业务写入: %+v", replay))
	require.Empty(t, replay.BattleUpsert)
	require.Empty(t, replay.CollectStart)
	require.NotNil(t, replay.CommandReceipt, "重放回执应与首次逐字节一致")
	require.EqualValues(t, string(first.CommandReceipt.ResultJSON), string(replay.CommandReceipt.ResultJSON))

	// (b) 不同请求、但战斗已终态（模拟迟到结果）：只回执，且状态保持首次。
	lateSnap := snapshot(SnapshotOptions{
		Objects: objects,
		Battles: []*domain.BattleRecord{{
			BattleID: "b1", WorldID: testWorld, TargetID: 1000,
			AttackerID: 200, DefenderID: 1000,
			Status:             domain.BattleStatusResolved,
			AppliedOperationID: "battle-result:b1",
			Result:             first.BattleUpsert[0].Result,
		}},
	})
	late, err := h.planner.Plan(lateSnap, testEnv(1, "req-battle-late", 24), Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "b1", Winner: 1000},
	}, 30000)
	require.NoError(t, err, "迟到结果应返回幂等回执而不是错误")
	require.Empty(t, late.ObjectsUpsert, fmt.Sprintf("迟到结果不得改任何状态: %+v", late))
	require.Empty(t, late.BattleUpsert)
	require.Empty(t, late.QueueEnqueue)
	m := receiptMap(t, late)
	require.False(t, !jsonBool(t, m, "idempotent") || jsonStr(t, m, "status") != "RESOLVED", fmt.Sprintf("幂等回执=%v", m))
}

// TestPlanBattleResultUnknownAndInvalid 验证不存在的战斗与非法胜者。
func TestPlanBattleResultUnknownAndInvalid(t *testing.T) {
	h := newHarness(t)
	snap := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0)},
	})
	_, err := h.planner.Plan(snap, testEnv(1, "req-battle-404", 25), Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "nope", Winner: 200},
	}, 10000)
	require.False(t, !errors.Is(err, domain.ErrObjectNotFound), fmt.Sprintf("战斗不存在应返回 ErrObjectNotFound, got %v", err))

	snap2 := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{
			cityObject(100, 1, 0, 0),
			troopObject(fightingTroop(200, 1, "b1", 40000)),
			resourceObject(newResource(1000, 10000, 0, 100)),
		},
		Battles: []*domain.BattleRecord{pendingBattle("b1", 200, 1000)},
	})
	_, err = h.planner.Plan(snap2, testEnv(1, "req-battle-third", 26), Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "b1", Winner: 777},
	}, 10000)
	require.False(t, !errors.Is(err, domain.ErrInvalidArgument), fmt.Sprintf("第三方胜者应被拒绝, got %v", err))
}

// TestPlanBattleTimeoutIdempotent 验证超时命令遇到终态战斗时只回执：
// 已 RESOLVED 的不能被改成 TIMED_OUT（11.3：谁先提交谁生效）。
func TestPlanBattleTimeoutIdempotent(t *testing.T) {
	h := newHarness(t)
	attacker := fightingTroop(200, 1, "b1", 40000)
	for _, status := range []domain.BattleStatus{domain.BattleStatusResolved, domain.BattleStatusTimedOut} {
		t.Run(battleStatusName(status), func(t *testing.T) {
			snap := snapshot(SnapshotOptions{
				Objects: []domain.ObjectSnapshot{
					cityObject(100, 1, 0, 0),
					troopObject(attacker),
					resourceObject(newResource(1000, 10000, 0, 100)),
				},
				Battles: []*domain.BattleRecord{{
					BattleID: "b1", WorldID: testWorld, TargetID: 1000,
					AttackerID: 200, DefenderID: 1000, Status: status,
					AppliedOperationID: "battle-result:b1",
				}},
			})
			plan, err := h.planner.Plan(snap, testEnv(1, "req-timeout-"+battleStatusName(status), 33), Command{
				Kind:          CmdBattleTimeout,
				BattleTimeout: BattleTimeoutParams{BattleID: "b1"},
			}, 40000)
			require.NoError(t, err, "终态战斗的超时应幂等返回")
			require.Empty(t, plan.ObjectsUpsert, fmt.Sprintf("终态战斗不得被改写: %+v", plan))
			require.Empty(t, plan.BattleUpsert)
			m := receiptMap(t, plan)
			require.False(t, !jsonBool(t, m, "idempotent") || jsonStr(t, m, "status") != battleStatusName(status), fmt.Sprintf("幂等回执=%v, want status=%s", m, battleStatusName(status)))
		})
	}
}

// TestPlanBattleTimeoutNoDamage 验证超时裁决：攻方回城、防守保留、
// 无战损（战斗记录没有 Result，防守方版本与 Cargo 不动）。
func TestPlanBattleTimeoutNoDamage(t *testing.T) {
	h := newHarness(t)
	attacker := fightingTroop(200, 1, "b1", 40000)
	attacker.Cargo = map[int32]int64{101: 7}
	defender := newTroop(900, 2, domain.TroopPhaseCollecting)
	defender.Cargo = map[int32]int64{102: 9}
	defender.Header.Version = 4
	snap := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{
			cityObject(100, 1, 0, 0),
			troopObject(attacker), troopObject(defender), resourceObject(newResource(1000, 10000, 0, 100)),
		},
		Battles: []*domain.BattleRecord{pendingBattle("b1", 200, 900)},
	})
	plan, err := h.planner.Plan(snap, testEnv(1, "req-battle-timeout", 27), Command{
		Kind:          CmdBattleTimeout,
		BattleTimeout: BattleTimeoutParams{BattleID: "b1"},
	}, 40000)
	require.NoError(t, err, "超时计划失败")
	require.Len(t, plan.BattleUpsert, 1)
	rec := plan.BattleUpsert[0]
	require.EqualValues(t, domain.BattleStatusTimedOut, rec.Status)
	require.Nil(t, rec.Result)
	require.False(t, rec.AppliedOperationID != "req-battle-timeout" && rec.AppliedOperationID != domain.BattleResultKey("b1"), fmt.Sprintf("AppliedOperationID=%q", rec.AppliedOperationID))

	for _, o := range plan.ObjectsUpsert {
		if o.Kind != domain.ObjectKindTroop {
			continue
		}
		require.NotEqualValues(t, 900, o.Troop.Header.ID, fmt.Sprintf("防守方必须保留（不得回城）: %+v", o.Troop))
	}
	up, ok := findUpsert(plan, domain.ObjectKindTroop)
	require.True(t, ok, fmt.Sprintf("攻方应回城: %+v", up.Troop))
	require.EqualValues(t, 200, up.Troop.Header.ID)
	require.EqualValues(t, domain.TroopPhaseReturning, up.Troop.Phase, fmt.Sprintf("攻方 Phase=%v move=%v, want RETURNING", up.Troop.Phase, up.Troop.Move))
	require.NotNil(t, up.Troop.Move)
	require.EqualValues(t, 7, up.Troop.Cargo[101])
	require.EqualValues(t, "", up.Troop.BattleID, fmt.Sprintf("战斗锁未解除: %+v", up.Troop))
	require.EqualValues(t, domain.BusyReasonNone, up.Troop.Busy.Reason)
	require.Empty(t, plan.CollectStart, "超时不得改变采集状态")
	require.Empty(t, plan.CollectSettle)
	m := receiptMap(t, plan)
	require.False(t, !jsonBool(t, m, "timed_out") || jsonStr(t, m, "status") != "TIMED_OUT", fmt.Sprintf("回执=%v", m))
}

// TestPlanBattleResultAdvancesQueue 验证"队首结束 → 下一名开始"。
func TestPlanBattleResultAdvancesQueue(t *testing.T) {
	h := newHarness(t)
	attacker := fightingTroop(200, 1, "b1", 40000)
	next := queuedTroop(300, 3)
	waiting := queuedTroop(400, 4)
	snap := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{
			cityObject(100, 1, 0, 0),
			troopObject(attacker), troopObject(next), troopObject(waiting),
			resourceObject(newResource(1000, 10000, 0, 100)),
		},
		Queues:  map[domain.ObjectID][]domain.ObjectID{1000: {300, 400}},
		Battles: []*domain.BattleRecord{pendingBattle("b1", 200, 1000)},
	})
	plan, err := h.planner.Plan(snap, testEnv(1, "req-battle-queue", 28), Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "b1", Winner: 1000},
	}, 10000)
	require.NoError(t, err, "裁决计划失败")

	// 只出队队首；第二名继续等待。
	require.Len(t, plan.QueueDequeue, 1, fmt.Sprintf("QueueDequeue=%+v, want 只有队首 300", plan.QueueDequeue))
	require.EqualValues(t, 300, plan.QueueDequeue[0].TroopID)
	require.Len(t, plan.QueueEnqueue, 0)

	var nextBattle *domain.BattleRecord
	for i := range plan.BattleUpsert {
		if plan.BattleUpsert[i].AttackerID == 300 {
			nextBattle = &plan.BattleUpsert[i]
		}
	}
	require.NotNil(t, nextBattle)
	require.EqualValues(t, domain.BattleStatusPending, nextBattle.Status)
	require.EqualValues(t, 10000+h.rules.BattleTimeoutMS, nextBattle.DeadlineMS)
	require.NotEqualValues(t, "b1", nextBattle.BattleID, "新战斗必须有新 BattleID")

	var started bool
	for _, o := range plan.ObjectsUpsert {
		if o.Kind != domain.ObjectKindTroop {
			continue
		}
		require.NotEqualValues(t, 400, o.Troop.Header.ID, fmt.Sprintf("非队首成员不得被拉起: %+v", o.Troop))
		if o.Troop.Header.ID != 300 {
			continue
		}
		started = true
		require.EqualValues(t, domain.TroopPhaseFighting, o.Troop.Phase, fmt.Sprintf("队首未进入战斗: %+v", o.Troop))
		require.EqualValues(t, nextBattle.BattleID, o.Troop.BattleID)
		require.EqualValues(t, domain.BusyReasonBattle, o.Troop.Busy.Reason, fmt.Sprintf("队首缺少战斗锁: %+v", o.Troop.Busy))
		require.EqualValues(t, nextBattle.BattleID, o.Troop.Busy.OpID)
	}
	require.True(t, started, "队首部队不在计划里")
	m := receiptMap(t, plan)
	require.EqualValues(t, nextBattle.BattleID, jsonStr(t, m, "queue_next"), fmt.Sprintf("回执 queue_next=%v, want %s", m["queue_next"], nextBattle.BattleID))
}

// TestBattleQueueFIFO 验证两轮推进的顺序：先 300 后 400。
func TestBattleQueueFIFO(t *testing.T) {
	h := newHarness(t)
	// 第一轮：队首 300 获胜，队列剩 [400]。
	firstSnap := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{
			cityObject(100, 3, 0, 0),
			troopObject(fightingTroop(300, 3, "b2", 40000)),
			troopObject(queuedTroop(400, 4)),
			resourceObject(newResource(1000, 10000, 0, 100)),
		},
		Queues:  map[domain.ObjectID][]domain.ObjectID{1000: {400}},
		Battles: []*domain.BattleRecord{pendingBattle("b2", 300, 1000)},
	})
	first, err := h.planner.Plan(firstSnap, testEnv(3, "req-fifo-1", 29), Command{
		Kind:         CmdBattleResult,
		BattleResult: BattleResultParams{BattleID: "b2", Winner: 300},
	}, 20000)
	require.NoError(t, err, "第一轮失败")
	require.Len(t, first.QueueDequeue, 1, fmt.Sprintf("第一轮应出队 400: %+v", first.QueueDequeue))
	require.EqualValues(t, 400, first.QueueDequeue[0].TroopID)

	// 第二轮：300 的战斗结束，400 成为队首（新一轮队列为空）。
	secondSnap := snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{
			cityObject(100, 3, 0, 0),
			troopObject(fightingTroop(400, 4, "b4", 50000)),
			resourceObject(newResource(1000, 10000, 0, 100)),
		},
		Battles: []*domain.BattleRecord{pendingBattle("b4", 400, 1000)},
	})
	second, err := h.planner.Plan(secondSnap, testEnv(4, "req-fifo-2", 30), Command{
		Kind:          CmdBattleTimeout,
		BattleTimeout: BattleTimeoutParams{BattleID: "b4"},
	}, 30000)
	require.NoError(t, err, "第二轮失败")
	require.Len(t, second.QueueDequeue, 0)
	m := receiptMap(t, second)
	require.EqualValues(t, "", jsonStr(t, m, "queue_next"), fmt.Sprintf("没有下一名时 queue_next 应为空: %v", m))
}

// TestEnqueueSeqMonotonic 验证入队序号在同一目标上严格递增
// （FIFO 的唯一跨进程依据）。
func TestEnqueueSeqMonotonic(t *testing.T) {
	h := newHarness(t)
	defender := newTroop(900, 2, domain.TroopPhaseCollecting)
	res := newResource(1000, 10000, 0, 100)
	res.ActiveCollectID = "sess-enemy"
	battle := pendingBattle("b-live", 300, 900)

	build := func(tr *domain.Troop, queued []domain.ObjectID, env domain.CommandEnvelope) *domain.MutationPlan {
		t.Helper()
		snap := snapshot(SnapshotOptions{
			Objects: []domain.ObjectSnapshot{
				cityObject(100, 1, 0, 0), troopObject(tr), troopObject(defender), resourceObject(res),
			},
			Sessions: []*domain.CollectSession{{
				ID: "sess-enemy", TroopID: 900, ResourceID: 1000,
				StartAtMS: 0, EndAtMS: 40000, RateMilliItemsPerSec: 2500,
				PlannedQty: 100, Status: domain.CollectStatusActive,
			}},
			Queues:  map[domain.ObjectID][]domain.ObjectID{1000: queued},
			Battles: []*domain.BattleRecord{battle},
		})
		plan, err := h.planner.Plan(snap, env, Command{
			Kind:   CmdArrive,
			Arrive: ArriveParams{TroopID: tr.Header.ID, MoveID: tr.Move.MoveID},
		}, 10000)
		require.NoError(t, err, "入队计划失败")
		return plan
	}

	first := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-1", domain.MarchIntentAttack)
	p1 := build(first, nil, testEnv(1, "req-q1", 31))
	require.Len(t, p1.QueueEnqueue, 1)
	seq1 := p1.QueueEnqueue[0].Seq

	second := arrivedTroop(t, 201, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-2", domain.MarchIntentAttack)
	p2 := build(second, []domain.ObjectID{200}, testEnv(1, "req-q2", 32))
	require.Len(t, p2.QueueEnqueue, 1)
	seq2 := p2.QueueEnqueue[0].Seq
	require.False(t, seq2 <= seq1, fmt.Sprintf("同一目标的入队序号必须严格递增: seq1=%d seq2=%d", seq1, seq2))
}
