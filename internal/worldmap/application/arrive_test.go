package application

// arrive_test.go 覆盖文档 9.3 的到达仲裁表（按顺序判断）。
// 每个用例都断言“是否回城”以及回执 ResultCode，因为仲裁表的
// 关键正是“先命中哪个分支”。

import (
	"errors"
	"fmt"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

// arrivedTroop 造一支刚好到达 at 的部队：MoveID 用于过期判定。
func arrivedTroop(t *testing.T, id domain.ObjectID, player int64, target domain.ObjectID, at domain.Pos, moveID string, intent domain.MarchIntent) *domain.Troop {
	t.Helper()
	tr := newTroop(id, player, domain.TroopPhaseMarching)
	tr.Target = domain.TargetRef{TargetID: target, Intent: intent}
	tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, at, 1000, 0, moveID)
	return tr
}

func TestPlanArrive(t *testing.T) {
	cases := []struct {
		name    string
		nowMS   int64
		build   func(t *testing.T) (*SceneSnapshot, Command)
		wantErr error
		check   func(t *testing.T, h *harness, plan *domain.MutationPlan)
	}{
		{
			name:  "过期 MoveID 零副作用",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-new", domain.MarchIntentCollect)
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 100))},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-old"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Empty(t, plan.ObjectsUpsert, "过期任务不得改任何状态")
				require.Empty(t, plan.CollectStart)
				require.Empty(t, plan.BattleUpsert)
				m := receiptMap(t, plan)
				require.False(t, !jsonBool(t, m, "stale"), fmt.Sprintf("回执=%v", m))
				require.EqualValues(t, domain.CodeOK, plan.CommandReceipt.ResultCode)
			},
		},
		{
			name:  "目标消失回城",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-1", domain.MarchIntentCollect)
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-1"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.EqualValues(t, domain.CodeObjectNotFound, plan.CommandReceipt.ResultCode)
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "目标消失必须回城")
				require.EqualValues(t, domain.TroopPhaseReturning, up.Troop.Phase)
				require.EqualValues(t, (domain.Pos{X: 0, Y: 0}), up.Troop.Move.To)
			},
		},
		{
			name:  "资源无剩余回城",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-2", domain.MarchIntentCollect)
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 0))},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-2"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.EqualValues(t, domain.CodeCapacityExhausted, plan.CommandReceipt.ResultCode)
				require.Len(t, plan.CollectStart, 0, "不得为空资源建会话")
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "必须回城")
				require.EqualValues(t, domain.TroopPhaseReturning, up.Troop.Phase)
			},
		},
		{
			name:  "空资源建会话（三件套同一计划）",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-3", domain.MarchIntentCollect)
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), resourceObject(newResource(1000, 10000, 0, 100))},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-3"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Len(t, plan.CollectStart, 1)
				sess := plan.CollectStart[0]
				require.EqualValues(t, h.rules.CollectRateMilliPerSec, sess.RateMilliItemsPerSec)
				// capacity=100、cargo=0、remaining=100 → planned=100，
				// need_ms = ceil(100*1e6/2500) = 40000。
				require.EqualValues(t, 100, sess.PlannedQty, fmt.Sprintf("会话=%+v", sess))
				require.EqualValues(t, 50000, sess.EndAtMS)
				require.EqualValues(t, domain.CollectStatusActive, sess.Status)
				resUp, ok := findUpsert(plan, domain.ObjectKindResource)
				require.True(t, ok, fmt.Sprintf("资源未绑定活动会话: %+v", resUp.Resource))
				require.EqualValues(t, sess.ID, resUp.Resource.ActiveCollectID)
				trUp, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "部队未转 COLLECTING")
				require.EqualValues(t, domain.TroopPhaseCollecting, trUp.Troop.Phase)
				require.Nil(t, trUp.Troop.Move, "采集阶段应清 Move")
				m := receiptMap(t, plan)
				require.EqualValues(t, sess.ID, jsonStr(t, m, "session_id"))
			},
		},
		{
			name:  "同玩家另一部队已采集则回城",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-4", domain.MarchIntentCollect)
				other := newTroop(201, 1, domain.TroopPhaseCollecting)
				other.Target = domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentCollect}
				res := newResource(1000, 10000, 0, 100)
				res.ActiveCollectID = "sess-other"
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), troopObject(other), resourceObject(res)},
					Sessions: []*domain.CollectSession{{
						ID: "sess-other", TroopID: 201, ResourceID: 1000,
						StartAtMS: 0, EndAtMS: 40000, RateMilliItemsPerSec: 2500,
						PlannedQty: 100, Status: domain.CollectStatusActive,
					}},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-4"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.EqualValues(t, domain.CodeTargetOccupied, plan.CommandReceipt.ResultCode)
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "必须回城，不排采集 FIFO")
				require.EqualValues(t, domain.TroopPhaseReturning, up.Troop.Phase)
			},
		},
		{
			name:  "敌方空闲则建 PENDING 战斗",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-5", domain.MarchIntentAttack)
				defender := newTroop(900, 2, domain.TroopPhaseCollecting)
				defender.Target = domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentCollect}
				defender.Stats = domain.CombatSnapshot{Attack: 55, Defense: 66, HP: 777, RuleVersion: m1RuleVersion}
				res := newResource(1000, 10000, 0, 100)
				res.ActiveCollectID = "sess-enemy"
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), troopObject(defender), resourceObject(res)},
					Sessions: []*domain.CollectSession{{
						ID: "sess-enemy", TroopID: 900, ResourceID: 1000,
						StartAtMS: 0, EndAtMS: 40000, RateMilliItemsPerSec: 2500,
						PlannedQty: 100, Status: domain.CollectStatusActive,
					}},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-5"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Len(t, plan.BattleUpsert, 1)
				rec := plan.BattleUpsert[0]
				require.EqualValues(t, domain.BattleStatusPending, rec.Status, fmt.Sprintf("战斗记录=%+v", rec))
				require.EqualValues(t, 10000+h.rules.BattleTimeoutMS, rec.DeadlineMS)
				require.EqualValues(t, 200, rec.AttackerID, fmt.Sprintf("攻守/目标=%+v", rec))
				require.EqualValues(t, 900, rec.DefenderID)
				require.EqualValues(t, 1000, rec.TargetID)
				require.EqualValues(t, 55, rec.DefenderStats.Attack)
				require.NotEqualValues(t, ([32]byte{}), rec.InputHash, "InputHash 不能为空")
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "缺少部队 upsert")
				require.EqualValues(t, domain.TroopPhaseFighting, up.Troop.Phase, fmt.Sprintf("部队未进战斗: %+v", up.Troop))
				require.EqualValues(t, rec.BattleID, up.Troop.BattleID)
				require.EqualValues(t, domain.BusyReasonBattle, up.Troop.Busy.Reason, fmt.Sprintf("Busy=%+v", up.Troop.Busy))
				require.EqualValues(t, rec.BattleID, up.Troop.Busy.OpID)
			},
		},
		{
			name:  "敌方战斗中入队",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-6", domain.MarchIntentAttack)
				defender := newTroop(900, 2, domain.TroopPhaseCollecting)
				res := newResource(1000, 10000, 0, 100)
				res.ActiveCollectID = "sess-enemy"
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), troopObject(defender), resourceObject(res)},
					Sessions: []*domain.CollectSession{{
						ID: "sess-enemy", TroopID: 900, ResourceID: 1000,
						StartAtMS: 0, EndAtMS: 40000, RateMilliItemsPerSec: 2500,
						PlannedQty: 100, Status: domain.CollectStatusActive,
					}},
					Battles: []*domain.BattleRecord{{
						BattleID: "battle-live", WorldID: testWorld, TargetID: 1000,
						AttackerID: 300, DefenderID: 900, Status: domain.BattleStatusPending,
					}},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-6"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				// 队列是显式持久事实（map_battle_queue），不再用
				// BattleRecord + 标记表示排队成员。
				require.Len(t, plan.BattleUpsert, 0)
				require.Len(t, plan.QueueEnqueue, 1)
				en := plan.QueueEnqueue[0]
				require.EqualValues(t, 1000, en.TargetID, fmt.Sprintf("入队记录=%+v", en))
				require.EqualValues(t, 200, en.TroopID)
				require.False(t, en.Seq <= 0, fmt.Sprintf("入队序号必须为正（FIFO 依据）: %d", en.Seq))
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "部队未进队列")
				require.EqualValues(t, domain.TroopPhaseQueued, up.Troop.Phase)
				require.EqualValues(t, "", up.Troop.BattleID, fmt.Sprintf("排队不得挂战斗锁: %+v", up.Troop))
				require.EqualValues(t, domain.BusyReasonNone, up.Troop.Busy.Reason)
				m := receiptMap(t, plan)
				require.False(t, !jsonBool(t, m, "queued") || jsonStr(t, m, "battle_id") != "battle-live", fmt.Sprintf("回执=%v", m))
				require.EqualValues(t, en.Seq, jsonInt(t, m, "enqueue_seq"))
			},
		},
		{
			name:  "队列满则回城",
			nowMS: 10000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := arrivedTroop(t, 200, 1, 1000, domain.Pos{X: 10000, Y: 0}, "m-7", domain.MarchIntentAttack)
				defender := newTroop(900, 2, domain.TroopPhaseCollecting)
				res := newResource(1000, 10000, 0, 100)
				res.ActiveCollectID = "sess-enemy"
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr), troopObject(defender), resourceObject(res)},
					Sessions: []*domain.CollectSession{{
						ID: "sess-enemy", TroopID: 900, ResourceID: 1000,
						StartAtMS: 0, EndAtMS: 40000, RateMilliItemsPerSec: 2500,
						PlannedQty: 100, Status: domain.CollectStatusActive,
					}},
					Queues: map[domain.ObjectID][]domain.ObjectID{1000: {11, 22}},
					Battles: []*domain.BattleRecord{{
						BattleID: "battle-live", WorldID: testWorld, TargetID: 1000,
						AttackerID: 300, DefenderID: 900, Status: domain.BattleStatusPending,
					}},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-7"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.EqualValues(t, domain.CodeQueueFull, plan.CommandReceipt.ResultCode)
				require.Len(t, plan.BattleUpsert, 0, "队列满不得再写战斗记录")
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "必须回城")
				require.EqualValues(t, domain.TroopPhaseReturning, up.Troop.Phase)
			},
		},
		{
			name:  "回主堡转 SETTLING 并生成交接单",
			nowMS: 15000,
			build: func(t *testing.T) (*SceneSnapshot, Command) {
				tr := newTroop(200, 1, domain.TroopPhaseReturning)
				tr.Header.Version = 3
				tr.Cargo = map[int32]int64{101: 5, 102: 2}
				tr.Move = makeMove(t, domain.Pos{X: 10000, Y: 0}, domain.Pos{X: 0, Y: 0}, 1000, 5000, "m-home")
				return snapshot(SnapshotOptions{
					Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0), troopObject(tr)},
				}), Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 200, MoveID: "m-home"}}
			},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Len(t, plan.DeliveryCreate, 1)
				d := plan.DeliveryCreate[0]
				require.EqualValues(t, domain.DeliveryStatusPending, d.Status, fmt.Sprintf("交接单=%+v", d))
				require.EqualValues(t, 1, d.PlayerID)
				require.EqualValues(t, 3, d.SettlementNo)
				require.EqualValues(t, 5, d.Cargo[101], fmt.Sprintf("交接单 Cargo=%v", d.Cargo))
				require.EqualValues(t, 2, d.Cargo[102])
				require.EqualValues(t, cargoHash(d.Cargo), d.PayloadHash, "PayloadHash 与 Cargo 不一致")
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				require.True(t, ok, "缺少部队 upsert")
				tr := up.Troop
				require.EqualValues(t, domain.TroopPhaseSettling, tr.Phase)
				require.EqualValues(t, d.DeliveryID, tr.SettlementID)
				// 文档 9.5：先留货再交接，Cargo 不能在到达时清空。
				require.EqualValues(t, 5, tr.Cargo[101], fmt.Sprintf("Cargo 被提前清空: %v", tr.Cargo))
				require.EqualValues(t, 2, tr.Cargo[102])
				require.Nil(t, tr.Move, "SETTLING 应清除 Move")
				require.EqualValues(t, 4, tr.Header.Version)
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			snap, cmd := tc.build(t)
			plan, err := h.planner.Plan(snap, testEnv(1, "req-arrive-"+tc.name, 1), cmd, tc.nowMS)
			if tc.wantErr != nil {
				require.False(t, !errors.Is(err, tc.wantErr), fmt.Sprintf("err=%v, want %v", err, tc.wantErr))
				return
			}
			require.NoError(t, err, "意外错误")
			tc.check(t, h, plan)
		})
	}
}

// TestPlanArriveTroopMissing 单独覆盖“部队不存在”这个早退分支。
func TestPlanArriveTroopMissing(t *testing.T) {
	h := newHarness(t)
	snap := snapshot(SnapshotOptions{Objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0)}})
	_, err := h.planner.Plan(snap, testEnv(1, "req-arrive-missing", 1),
		Command{Kind: CmdArrive, Arrive: ArriveParams{TroopID: 404, MoveID: "m"}}, 1000)
	require.False(t, !errors.Is(err, domain.ErrObjectNotFound), fmt.Sprintf("err=%v", err))
}
