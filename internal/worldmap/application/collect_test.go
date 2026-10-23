package application

// collect_test.go 覆盖开始采集与结算。
// 采集的不变量是“资源↔Cargo 同生共死”，因此成功用例一律断言
// 两侧对象都在同一个 MutationPlan 里。

import (
	"errors"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

func TestPlanCollectStart(t *testing.T) {
	cases := []struct {
		name    string
		build   func(t *testing.T) (*SceneSnapshot, Command)
		wantErr error
		check   func(t *testing.T, h *harness, plan *domain.MutationPlan)
	}{
		{
			name: "三件套写入同一计划",
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseMarching)
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 10000, Y: 0}, 1000, 0, "m-in")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 100))},
				}), Command{Kind: CmdCollectStart, CollectStart: CollectStartParams{TroopID: 200, ResourceID: 1000}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				if len(plan.CollectStart) != 1 {
					t.Fatalf("CollectStart=%+v", plan.CollectStart)
				}
				sess := plan.CollectStart[0]
				if sess.RateMilliItemsPerSec != h.rules.CollectRateMilliPerSec ||
					sess.PlannedQty != 100 || sess.EndAtMS != 10000+40000 {
					t.Fatalf("会话=%+v", sess)
				}
				// 三件套：会话 + 资源 active_collect_id + Troop→COLLECTING。
				if countUpsert(plan, domain.ObjectKindResource) != 1 || countUpsert(plan, domain.ObjectKindTroop) != 1 {
					t.Fatalf("对象数量=%d", len(plan.ObjectsUpsert))
				}
				resUp, _ := findUpsert(plan, domain.ObjectKindResource)
				if resUp.Resource.ActiveCollectID != sess.ID || resUp.Resource.Header.Version != 2 {
					t.Fatalf("资源=%+v", resUp.Resource)
				}
				trUp, _ := findUpsert(plan, domain.ObjectKindTroop)
				if trUp.Troop.Phase != domain.TroopPhaseCollecting || trUp.Troop.Header.Version != 2 {
					t.Fatalf("部队=%+v", trUp.Troop)
				}
				if trUp.Troop.Move != nil {
					t.Fatalf("采集阶段应清 Move")
				}
				if plan.CommandReceipt.ResultCode != domain.CodeOK {
					t.Fatalf("ResultCode=%d", plan.CommandReceipt.ResultCode)
				}
			},
		},
		{
			name: "未共置拒绝",
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseMarching)
				// 起点在 (0,0)，资源点在 (10000,0)，距离 10 格。
				// 在 now=10000 时已停在 (5000,0)，距资源点 (10000,0) 有 5 格。
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 5000, Y: 0}, 1000, 0, "m-far")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 100))},
				}), Command{Kind: CmdCollectStart, CollectStart: CollectStartParams{TroopID: 200, ResourceID: 1000}}
			},
			wantErr: domain.ErrInvalidArgument,
		},
		{
			name: "已有活动会话拒绝",
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseMarching)
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 10000, Y: 0}, 1000, 0, "m-in2")
				res := newResource(1000, 10000, 0, 100)
				res.ActiveCollectID = "sess-busy"
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(res)},
					Sessions: []*domain.CollectSession{{
						ID: "sess-busy", TroopID: 999, ResourceID: 1000,
						StartAtMS: 0, EndAtMS: 40000, RateMilliItemsPerSec: 2500,
						PlannedQty: 100, Status: domain.CollectStatusActive,
					}},
				}), Command{Kind: CmdCollectStart, CollectStart: CollectStartParams{TroopID: 200, ResourceID: 1000}}
			},
			wantErr: domain.ErrTargetOccupied,
		},
		{
			name: "资源枯竭拒绝",
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseMarching)
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 10000, Y: 0}, 1000, 0, "m-empty")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 0))},
				}), Command{Kind: CmdCollectStart, CollectStart: CollectStartParams{TroopID: 200, ResourceID: 1000}}
			},
			wantErr: domain.ErrCapacityExhausted,
		},
		{
			name: "不是自己的部队",
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 2, domain.TroopPhaseMarching)
				tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 10000, Y: 0}, 1000, 0, "m-own")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 100))},
				}), Command{Kind: CmdCollectStart, CollectStart: CollectStartParams{TroopID: 200, ResourceID: 1000}}
			},
			wantErr: domain.ErrNotOwner,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			snap, cmd := tc.build(t)
			plan, err := h.planner.Plan(snap, testEnv(1, "req-cs-"+tc.name, 1), cmd, 10000)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err=%v, want %v", err, tc.wantErr)
				}
				return
			}
			require.NoError(t, err, "意外错误")
			tc.check(t, h, plan)
		})
	}
}

// settleFixture 造一个活动会话 + 资源 + 部队的快照。
func settleFixture(t *testing.T, remaining int64) *SceneSnapshot {
	t.Helper()
	tr := newTroop(200, 1, domain.TroopPhaseCollecting)
	tr.Header.Version = 2
	tr.Target = domain.TargetRef{TargetID: 500, Intent: domain.MarchIntentCollect}
	res := newResource(500, 10000, 0, remaining)
	res.Header.Version = 2
	res.ActiveCollectID = "sess-1"
	return snapshot(SnapshotOptions{
		Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(res)},
		Sessions: []*domain.CollectSession{{
			ID: "sess-1", TroopID: 200, ResourceID: 500,
			StartAtMS: 0, EndAtMS: 40000,
			RateMilliItemsPerSec: 2500, PlannedQty: 100,
			Status: domain.CollectStatusActive,
		}},
	})
}

func TestPlanCollectSettle(t *testing.T) {
	t.Run("正常结算", func(t *testing.T) {
		h := newHarness(t)
		plan, err := h.planner.Plan(settleFixture(t, 100), testEnv(1, "req-settle", 1),
			Command{Kind: CmdCollectSettle, CollectSettle: CollectSettleParams{SessionID: "sess-1", SettleAtMS: 3000}}, 3000)
		require.NoError(t, err, "结算")
		if len(plan.CollectSettle) != 1 || plan.CollectSettle[0].SettledQty != 7 {
			t.Fatalf("CollectSettle=%+v", plan.CollectSettle)
		}
		resUp, ok := findUpsert(plan, domain.ObjectKindResource)
		if !ok || resUp.Resource.Remaining != 93 {
			t.Fatalf("资源余量=%+v", resUp.Resource)
		}
		if resUp.Resource.ActiveCollectID != "" {
			t.Fatalf("必须清 active_collect_id")
		}
		if resUp.Resource.PendingDestroy {
			t.Fatalf("仍有剩余时不应标删除")
		}
		trUp, ok := findUpsert(plan, domain.ObjectKindTroop)
		if !ok || trUp.Troop.Cargo[101] != 7 {
			t.Fatalf("Cargo=%+v", trUp.Troop)
		}
		// 结算只做账务，阶段转换交给召回/回城命令。
		if trUp.Troop.Phase != domain.TroopPhaseCollecting {
			t.Fatalf("结算不应改 Phase: %v", trUp.Troop.Phase)
		}
	})

	t.Run("余量为 0 标记删除", func(t *testing.T) {
		h := newHarness(t)
		plan, err := h.planner.Plan(settleFixture(t, 5), testEnv(1, "req-settle-zero", 1),
			Command{Kind: CmdCollectSettle, CollectSettle: CollectSettleParams{SessionID: "sess-1", SettleAtMS: 3000}}, 3000)
		require.NoError(t, err, "结算")
		if plan.CollectSettle[0].SettledQty != 5 {
			t.Fatalf("SettledQty=%d", plan.CollectSettle[0].SettledQty)
		}
		resUp, _ := findUpsert(plan, domain.ObjectKindResource)
		if resUp.Resource.Remaining != 0 || !resUp.Resource.PendingDestroy {
			t.Fatalf("资源=%+v", resUp.Resource)
		}
	})

	t.Run("重复结算幂等", func(t *testing.T) {
		h := newHarness(t)
		// 第一次结算（活动会话）。
		first, err := h.planner.Plan(settleFixture(t, 100), testEnv(1, "req-settle-a", 1),
			Command{Kind: CmdCollectSettle, CollectSettle: CollectSettleParams{SessionID: "sess-1", SettleAtMS: 3000}}, 3000)
		require.NoError(t, err, "首次结算")
		qty := first.CollectSettle[0].SettledQty

		// 第二次：快照里会话已 SETTLED（不同 request_id，仍有回执可比）。
		snap := snapshot(SnapshotOptions{
			Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0)},
			Sessions: []*domain.CollectSession{{
				ID: "sess-1", TroopID: 200, ResourceID: 500,
				StartAtMS: 0, EndAtMS: 40000,
				RateMilliItemsPerSec: 2500, PlannedQty: 100,
				Status: domain.CollectStatusSettled, SettledQty: qty,
			}},
		})
		second, err := h.planner.Plan(snap, testEnv(1, "req-settle-b", 1),
			Command{Kind: CmdCollectSettle, CollectSettle: CollectSettleParams{SessionID: "sess-1", SettleAtMS: 9000}}, 9000)
		require.NoError(t, err, "重复结算")
		if len(second.ObjectsUpsert) != 0 || len(second.CollectSettle) != 0 {
			t.Fatalf("重复结算不得改状态: %+v", second)
		}
		m := receiptMap(t, second)
		if jsonInt(t, m, "settled_qty") != qty || !jsonBool(t, m, "idempotent") {
			t.Fatalf("回执=%v, want qty=%d", m, qty)
		}
	})

	t.Run("会话不存在", func(t *testing.T) {
		h := newHarness(t)
		_, err := h.planner.Plan(snapshot(SnapshotOptions{Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0)}}),
			testEnv(1, "req-settle-missing", 1),
			Command{Kind: CmdCollectSettle, CollectSettle: CollectSettleParams{SessionID: "nope"}}, 3000)
		if !errors.Is(err, domain.ErrObjectNotFound) {
			t.Fatalf("err=%v", err)
		}
	})
}
