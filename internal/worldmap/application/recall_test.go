package application

// recall_test.go 覆盖文档 9.4 的召回分桶表。
// 重点不是“返回值对不对”，而是“哪些对象进了同一个计划”
// （COLLECTING 的 INV-04）以及“幂等分支是否真的零副作用”。

import (
	"errors"
	"strings"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

func TestPlanRecall(t *testing.T) {
	cases := []struct {
		name    string
		env     domain.CommandEnvelope
		nowMS   int64
		build   func(t *testing.T) (*SceneSnapshot, Command)
		wantErr error
		check   func(t *testing.T, h *harness, plan *domain.MutationPlan)
	}{
		{
			name:  "MARCHING 按当前位置截断回城",
			env:   testEnv(1, "req-marching", 1),
			nowMS: 4000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseMarching)
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 10000, Y: 0}, 1000, 0, "m-out")
				tr.Target = domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentCollect}
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 100))},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.EqualValues(t, domain.CodeOK, plan.CommandReceipt.ResultCode)
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "缺少 Troop upsert")
				tr := up.Troop
				require.EqualValues(t, domain.TroopPhaseReturning, tr.Phase)
				require.EqualValues(t, 2, tr.Header.Version)
				if tr.Move.MoveID == "m-out" {
					t.Fatalf("回城必须生成新 MoveID")
				}
				// now=4000 时在 (4000,0)，回主堡 (0,0) 需要 4 秒。
				require.EqualValues(t, (domain.Pos{X: 4000, Y: 0}), tr.Move.From)
				require.EqualValues(t, (domain.Pos{X: 0, Y: 0}), tr.Move.To)
				require.EqualValues(t, 8000, tr.Move.EndAtMS)
				m := receiptMap(t, plan)
				if jsonInt(t, m, "arrive_at_ms") != 8000 || jsonStr(t, m, "move_id") != tr.Move.MoveID {
					t.Fatalf("回执与移动段不一致: %v", m)
				}
			},
		},
		{
			name:  "COLLECTING 部分结算后回城（INV-04 同一计划）",
			env:   testEnv(1, "req-collect", 2),
			nowMS: 3000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseCollecting)
				tr.Header.Version = 3
				tr.Cargo = map[int32]int64{101: 83} // capacityLeft = 100-83 = 17
				tr.Target = domain.TargetRef{TargetID: 500, Intent: domain.MarchIntentCollect}
				res := newResource(500, 10000, 0, 100)
				res.ActiveCollectID = "sess-1"
				sess := &domain.CollectSession{
					ID: "sess-1", TroopID: 200, ResourceID: 500,
					StartAtMS: 0, EndAtMS: 6800,
					RateMilliItemsPerSec: 2500, PlannedQty: 17,
					Status: domain.CollectStatusActive,
				}
				return snapshot(SnapshotOptions{
					Objects:  []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(res)},
					Sessions: []*domain.CollectSession{sess},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				// 文档例子：rate=2500、capacityLeft=17、remaining=100、3000ms → 7。
				require.Len(t, plan.CollectSettle, 1)
				require.EqualValues(t, 7, plan.CollectSettle[0].SettledQty)
				require.EqualValues(t, "sess-1", plan.CollectSettle[0].SessionID)
				resUp, ok := findUpsert(plan, domain.ObjectKindResource)
				require.True(t, ok, "INV-04: 资源扣减不在计划里")
				require.EqualValues(t, 93, resUp.Resource.Remaining)
				require.EqualValues(t, "", resUp.Resource.ActiveCollectID, "结算后必须清 active_collect_id")
				trUp, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "INV-04: Cargo 增加不在计划里")
				require.EqualValues(t, 90, trUp.Troop.Cargo[101])
				if trUp.Troop.Phase != domain.TroopPhaseReturning || trUp.Troop.Header.Version != 4 {
					t.Fatalf("Phase=%v Version=%d", trUp.Troop.Phase, trUp.Troop.Header.Version)
				}
			},
		},
		{
			name:  "QUEUED 退出队列后回城",
			env:   testEnv(1, "req-queued", 3),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseQueued)
				tr.Target = domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentAttack}
				battle := &domain.BattleRecord{
					BattleID: "battle-1", WorldID: testWorld, TargetID: 1000,
					AttackerID: 300, DefenderID: 400, Status: domain.BattleStatusPending,
				}
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 100))},
					Queues:  map[domain.ObjectID][]domain.ObjectID{1000: {200}},
					Battles: []*domain.BattleRecord{battle},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				// 退出队列 = 显式删除队列行（map_battle_queue），
				// 与 Phase 变更同一事务；不再把战斗记录置 TIMED_OUT。
				require.Len(t, plan.QueueDequeue, 1)
				if de := plan.QueueDequeue[0]; de.TargetID != 1000 || de.TroopID != 200 {
					t.Fatalf("出队记录=%+v", de)
				}
				require.Len(t, plan.BattleUpsert, 0)
				trUp, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok || trUp.Troop.Phase != domain.TroopPhaseReturning {
					t.Fatalf("部队未转回城")
				}
				m := receiptMap(t, plan)
				if !jsonBool(t, m, "queued_left") {
					t.Fatalf("回执未标记退出队列: %v", m)
				}
				require.EqualValues(t, "battle-1", jsonStr(t, m, "battle_id"))
			},
		},
		{
			name:  "FIGHTING 保存 Deferred 且 Phase 不变",
			env:   testEnv(1, "req-fight", 4),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseFighting)
				tr.Header.Version = 5
				tr.BattleID = "battle-9"
				tr.Busy = domain.BusyState{Reason: domain.BusyReasonBattle, OpID: "battle-9", DeadlineMS: 20000}
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "缺少 Troop upsert")
				tr := up.Troop
				require.EqualValues(t, domain.TroopPhaseFighting, tr.Phase)
				if tr.Busy.Reason != domain.BusyReasonBattle || tr.BattleID != "battle-9" {
					t.Fatalf("不得清战斗锁: %+v", tr.Busy)
				}
				if tr.Deferred == nil || tr.Deferred.Kind != domain.DeferredKindRecall {
					t.Fatalf("Deferred=%+v", tr.Deferred)
				}
				require.EqualValues(t, "req-fight", tr.Deferred.RequestID)
				require.EqualValues(t, 6, tr.Header.Version)
				m := receiptMap(t, plan)
				if !jsonBool(t, m, "accepted") || !jsonBool(t, m, "deferred") {
					t.Fatalf("回执=%v", m)
				}
			},
		},
		{
			name:  "已有 ForceReturn 时玩家 Recall 不覆盖",
			env:   testEnv(1, "req-downgrade", 5),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseFighting)
				tr.Header.Version = 5
				tr.BattleID = "battle-9"
				tr.Busy = domain.BusyState{Reason: domain.BusyReasonBattle, OpID: "battle-9"}
				tr.Deferred = &domain.DeferredAction{Kind: domain.DeferredKindForceReturn, RequestID: "req-admin"}
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200, ExpectedVersion: 5}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Len(t, plan.ObjectsUpsert, 0)
				m := receiptMap(t, plan)
				if !jsonBool(t, m, "deferred") || jsonStr(t, m, "deferred_kind") != "FORCE_RETURN" {
					t.Fatalf("回执=%v", m)
				}
			},
		},
		{
			name:  "ForceReturn 升级覆盖 Recall",
			env:   testEnv(1, "req-upgrade", 6),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseFighting)
				tr.Header.Version = 5
				tr.BattleID = "battle-9"
				tr.Busy = domain.BusyState{Reason: domain.BusyReasonBattle, OpID: "battle-9"}
				tr.Deferred = &domain.DeferredAction{Kind: domain.DeferredKindRecall, RequestID: "req-player"}
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200, Force: true}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "升级必须写对象")
				require.EqualValues(t, domain.DeferredKindForceReturn, up.Troop.Deferred.Kind)
				require.EqualValues(t, 6, up.Troop.Header.Version)
			},
		},
		{
			name:  "RETURNING 幂等",
			env:   testEnv(1, "req-returning", 7),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseReturning)
				tr.Header.Version = 4
				tr.Move = makeMove(t, domain.Pos{X: 4000, Y: 0}, domain.Pos{X: 0, Y: 0}, 1000, 1000, "m-back")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Len(t, plan.ObjectsUpsert, 0, "幂等分支不应写对象")
				m := receiptMap(t, plan)
				if jsonStr(t, m, "move_id") != "m-back" || !jsonBool(t, m, "idempotent") {
					t.Fatalf("回执=%v", m)
				}
				require.EqualValues(t, 5000, jsonInt(t, m, "arrive_at_ms"))
			},
		},
		{
			name:  "SETTLING 返回交接单",
			env:   testEnv(1, "req-settling", 8),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseSettling)
				tr.Header.Version = 5
				tr.SettlementID = "delivery-77"
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Len(t, plan.ObjectsUpsert, 0, "SETTLING 不得改状态")
				m := receiptMap(t, plan)
				require.EqualValues(t, "delivery-77", jsonStr(t, m, "settlement_id"))
			},
		},
		{
			name:  "部队不存在",
			env:   testEnv(1, "req-missing", 9),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				return snapshot(SnapshotOptions{Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0)}}),
					Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 999}}
			},
			wantErr: domain.ErrObjectNotFound,
		},
		{
			name:  "不是自己的部队",
			env:   testEnv(1, "req-owner", 10),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 2, domain.TroopPhaseMarching)
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 1000, Y: 0}, 1000, 0, "m-x")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			wantErr: domain.ErrNotOwner,
		},
		{
			name:  "版本过期",
			env:   testEnv(1, "req-stale", 11),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseMarching)
				tr.Header.Version = 4
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 1000, Y: 0}, 1000, 0, "m-y")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200, ExpectedVersion: 3}}
			},
			wantErr: domain.ErrStaleVersion,
		},
		{
			name:  "Busy=Transfer 返回 Pending",
			env:   testEnv(1, "req-transfer", 12),
			nowMS: 5000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseMarching)
				tr.Busy = domain.BusyState{Reason: domain.BusyReasonTransfer, OpID: "op-42"}
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 1000, Y: 0}, 1000, 0, "m-z")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdRecall, Recall: RecallParams{TroopID: 200}}
			},
			wantErr: domain.ErrOperationPending,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			snap, cmd := tc.build(t)
			plan, err := h.planner.Plan(snap, tc.env, cmd, tc.nowMS)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err=%v, want %v", err, tc.wantErr)
				}
				if tc.wantErr == domain.ErrOperationPending && !strings.Contains(err.Error(), "op-42") {
					t.Fatalf("错误信息应带 op_id: %v", err)
				}
				return
			}
			require.NoError(t, err, "意外错误")
			tc.check(t, h, plan)
		})
	}
}
