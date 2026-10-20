package application

// arrive_test.go 覆盖文档 9.3 的到达仲裁表（按顺序判断）。
// 每个用例都断言“是否回城”以及回执 ResultCode，因为仲裁表的
// 关键正是“先命中哪个分支”。

import (
	"errors"
	"testing"

	"game_study/internal/worldmap/domain"
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
				if len(plan.ObjectsUpsert) != 0 || len(plan.CollectStart) != 0 || len(plan.BattleUpsert) != 0 {
					t.Fatalf("过期任务不得改任何状态")
				}
				m := receiptMap(t, plan)
				if !jsonBool(t, m, "stale") {
					t.Fatalf("回执=%v", m)
				}
				if plan.CommandReceipt.ResultCode != domain.CodeOK {
					t.Fatalf("过期是正常路径，不应是失败码: %d", plan.CommandReceipt.ResultCode)
				}
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
				if plan.CommandReceipt.ResultCode != domain.CodeObjectNotFound {
					t.Fatalf("ResultCode=%d, want %d", plan.CommandReceipt.ResultCode, domain.CodeObjectNotFound)
				}
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok || up.Troop.Phase != domain.TroopPhaseReturning {
					t.Fatalf("目标消失必须回城")
				}
				if up.Troop.Move.To != (domain.Pos{X: 0, Y: 0}) {
					t.Fatalf("回城终点=%+v", up.Troop.Move.To)
				}
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
				if plan.CommandReceipt.ResultCode != domain.CodeCapacityExhausted {
					t.Fatalf("ResultCode=%d, want %d", plan.CommandReceipt.ResultCode, domain.CodeCapacityExhausted)
				}
				if len(plan.CollectStart) != 0 {
					t.Fatalf("不得为空资源建会话")
				}
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok || up.Troop.Phase != domain.TroopPhaseReturning {
					t.Fatalf("必须回城")
				}
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
				if len(plan.CollectStart) != 1 {
					t.Fatalf("CollectStart=%+v", plan.CollectStart)
				}
				sess := plan.CollectStart[0]
				if sess.RateMilliItemsPerSec != h.rules.CollectRateMilliPerSec {
					t.Fatalf("Rate=%d", sess.RateMilliItemsPerSec)
				}
				// capacity=100、cargo=0、remaining=100 → planned=100，
				// need_ms = ceil(100*1e6/2500) = 40000。
				if sess.PlannedQty != 100 || sess.EndAtMS != 50000 {
					t.Fatalf("会话=%+v", sess)
				}
				if sess.Status != domain.CollectStatusActive {
					t.Fatalf("Status=%v", sess.Status)
				}
				resUp, ok := findUpsert(plan, domain.ObjectKindResource)
				if !ok || resUp.Resource.ActiveCollectID != sess.ID {
					t.Fatalf("资源未绑定活动会话: %+v", resUp.Resource)
				}
				trUp, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok || trUp.Troop.Phase != domain.TroopPhaseCollecting {
					t.Fatalf("部队未转 COLLECTING")
				}
				if trUp.Troop.Move != nil {
					t.Fatalf("采集阶段应清 Move")
				}
				m := receiptMap(t, plan)
				if jsonStr(t, m, "session_id") != sess.ID {
					t.Fatalf("回执=%v", m)
				}
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
				if plan.CommandReceipt.ResultCode != domain.CodeTargetOccupied {
					t.Fatalf("ResultCode=%d", plan.CommandReceipt.ResultCode)
				}
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok || up.Troop.Phase != domain.TroopPhaseReturning {
					t.Fatalf("必须回城，不排采集 FIFO")
				}
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
				if len(plan.BattleUpsert) != 1 {
					t.Fatalf("BattleUpsert=%+v", plan.BattleUpsert)
				}
				rec := plan.BattleUpsert[0]
				if rec.Status != domain.BattleStatusPending || rec.DeadlineMS != 10000+h.rules.BattleTimeoutMS {
					t.Fatalf("战斗记录=%+v", rec)
				}
				if rec.AttackerID != 200 || rec.DefenderID != 900 || rec.TargetID != 1000 {
					t.Fatalf("攻守/目标=%+v", rec)
				}
				if rec.DefenderStats.Attack != 55 {
					t.Fatalf("防守方快照应取被攻击部队: %+v", rec.DefenderStats)
				}
				if rec.InputHash == ([32]byte{}) {
					t.Fatalf("InputHash 不能为空")
				}
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok {
					t.Fatalf("缺少部队 upsert")
				}
				if up.Troop.Phase != domain.TroopPhaseFighting || up.Troop.BattleID != rec.BattleID {
					t.Fatalf("部队未进战斗: %+v", up.Troop)
				}
				if up.Troop.Busy.Reason != domain.BusyReasonBattle || up.Troop.Busy.OpID != rec.BattleID {
					t.Fatalf("Busy=%+v", up.Troop.Busy)
				}
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
				if len(plan.BattleUpsert) != 0 {
					t.Fatalf("排队不得再写占位战斗记录: %+v", plan.BattleUpsert)
				}
				if len(plan.QueueEnqueue) != 1 {
					t.Fatalf("QueueEnqueue=%+v", plan.QueueEnqueue)
				}
				en := plan.QueueEnqueue[0]
				if en.TargetID != 1000 || en.TroopID != 200 {
					t.Fatalf("入队记录=%+v", en)
				}
				if en.Seq <= 0 {
					t.Fatalf("入队序号必须为正（FIFO 依据）: %d", en.Seq)
				}
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok || up.Troop.Phase != domain.TroopPhaseQueued {
					t.Fatalf("部队未进队列")
				}
				if up.Troop.BattleID != "" || up.Troop.Busy.Reason != domain.BusyReasonNone {
					t.Fatalf("排队不得挂战斗锁: %+v", up.Troop)
				}
				m := receiptMap(t, plan)
				if !jsonBool(t, m, "queued") || jsonStr(t, m, "battle_id") != "battle-live" {
					t.Fatalf("回执=%v", m)
				}
				if jsonInt(t, m, "enqueue_seq") != en.Seq {
					t.Fatalf("回执与队列序号不一致: %v", m)
				}
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
				if plan.CommandReceipt.ResultCode != domain.CodeQueueFull {
					t.Fatalf("ResultCode=%d, want %d", plan.CommandReceipt.ResultCode, domain.CodeQueueFull)
				}
				if len(plan.BattleUpsert) != 0 {
					t.Fatalf("队列满不得再写战斗记录")
				}
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok || up.Troop.Phase != domain.TroopPhaseReturning {
					t.Fatalf("必须回城")
				}
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
				if len(plan.DeliveryCreate) != 1 {
					t.Fatalf("DeliveryCreate=%+v", plan.DeliveryCreate)
				}
				d := plan.DeliveryCreate[0]
				if d.Status != domain.DeliveryStatusPending || d.PlayerID != 1 {
					t.Fatalf("交接单=%+v", d)
				}
				if d.SettlementNo != 3 {
					t.Fatalf("SettlementNo=%d, want 3", d.SettlementNo)
				}
				if d.Cargo[101] != 5 || d.Cargo[102] != 2 {
					t.Fatalf("交接单 Cargo=%v", d.Cargo)
				}
				if d.PayloadHash != cargoHash(d.Cargo) {
					t.Fatalf("PayloadHash 与 Cargo 不一致")
				}
				up, ok := findUpsert(plan, domain.ObjectKindTroop)
				if !ok {
					t.Fatalf("缺少部队 upsert")
				}
				tr := up.Troop
				if tr.Phase != domain.TroopPhaseSettling {
					t.Fatalf("Phase=%v", tr.Phase)
				}
				if tr.SettlementID != d.DeliveryID {
					t.Fatalf("SettlementID=%q, delivery=%q", tr.SettlementID, d.DeliveryID)
				}
				// 文档 9.5：先留货再交接，Cargo 不能在到达时清空。
				if tr.Cargo[101] != 5 || tr.Cargo[102] != 2 {
					t.Fatalf("Cargo 被提前清空: %v", tr.Cargo)
				}
				if tr.Move != nil {
					t.Fatalf("SETTLING 应清除 Move")
				}
				if tr.Header.Version != 4 {
					t.Fatalf("Version=%d", tr.Header.Version)
				}
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
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err=%v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("意外错误: %v", err)
			}
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
	if !errors.Is(err, domain.ErrObjectNotFound) {
		t.Fatalf("err=%v", err)
	}
}
