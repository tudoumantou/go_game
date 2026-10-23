package application

// march_test.go 除了测试出征，还承载所有测试文件共用的 fixture
// （同一个测试包内共享，避免每个文件重复造快照）。
//
// 测试策略：
//   - 只依赖真实的 domain/spatial 实现，不 mock 路径计算——路径与
//     结算公式本身就是被测契约的一部分；
//   - 用闭包计数器实现 IDGen/ObjectIDGen（函数类型注入的价值：
//     一个捕获变量就能造出确定性生成器）；
//   - 断言“计划的形状”（哪些字段进了同一个 plan）而不只是返回值，
//     因为 INV-04 这类不变量只有在计划层面才能看见。

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/spatial"
	"github.com/stretchr/testify/require"
)

const (
	testWorld domain.WorldID = 7
	testScene domain.SceneID = 1
)

// testRules 返回一套自洽的最小规则。
func testRules() Rules {
	return Rules{
		MarchSpeedUnitsPerSec:    1000,
		CapacityUnits:            100,
		CollectRateMilliPerSec:   2500,
		MaxBattleQueuePerTarget:  2,
		BattleTimeoutMS:          30000,
		RefreshBatchSize:         3,
		ResourcePositionAttempts: 5,
		RichBlock: RichBlockRule{
			MaxCount: 6,
			Tiers: []RichTier{
				{ResourceType: 1, ItemID: 101, Weight: 1, Minimum: 2, TotalPerNode: 500},
				{ResourceType: 2, ItemID: 102, Weight: 3, Minimum: 1, TotalPerNode: 300},
			},
		},
	}
}

type harness struct {
	t        *testing.T
	topo     domain.Topology
	rules    Rules
	planner  *DefaultPlanner
	ids      IDGen
	idCount  *int
	objects  ObjectIDGen
	objCount *uint64
	seeds    SeedGen
}

func newHarness(t *testing.T, mutate ...func(*Rules)) *harness {
	t.Helper()
	rules := testRules()
	for _, m := range mutate {
		m(&rules)
	}
	ids, idCount := seqIDs("cmd-")
	objs, objCount := seqObjectIDs(1000)
	h := &harness{
		t: t, topo: domain.DefaultTopology(testWorld), rules: rules,
		ids: ids, idCount: idCount, objects: objs, objCount: objCount, seeds: fixedSeeds,
	}
	p, err := NewPlanner(h.topo, rules, ids, objs, h.seeds)
	require.NoError(t, err, "NewPlanner")
	h.planner = p
	return h
}

// seqIDs 生成 "cmd-1"、"cmd-2"…… 测试里只关心“是否新生成”，
// 不关心 UUID 形式。
func seqIDs(prefix string) (IDGen, *int) {
	n := 0
	return func() string { n++; return fmt.Sprintf("%s%d", prefix, n) }, &n
}

func seqObjectIDs(start domain.ObjectID) (ObjectIDGen, *uint64) {
	v := uint64(start)
	return func() domain.ObjectID { id := v; v++; return domain.ObjectID(id) }, &v
}

// fixedSeeds 是确定性种子函数：线性混合三元组，跨平台稳定。
func fixedSeeds(block, generation, slot int64) int64 {
	return block*1000003 + generation*1009 + slot*31
}

func testEnv(player int64, requestID string, hashByte byte) domain.CommandEnvelope {
	var h [32]byte
	h[0] = hashByte
	return domain.CommandEnvelope{
		WorldID:     testWorld,
		PlayerID:    player,
		RequestID:   requestID,
		PayloadHash: h,
		OperationID: "op-" + requestID,
	}
}

func snapshot(opts SnapshotOptions) *SceneSnapshot {
	opts.SceneID = testScene
	opts.Epoch = 3
	opts.Revision = 11
	opts.Topo = domain.DefaultTopology(testWorld)
	if opts.NowMS == 0 {
		opts.NowMS = 1000
	}
	return NewSceneSnapshot(opts)
}

func cityObject(id domain.ObjectID, player int64, x, y int64) domain.ObjectSnapshot {
	return domain.ObjectSnapshot{
		Key:        domain.ObjectKey{WorldID: testWorld, ID: id},
		Kind:       domain.ObjectKindCity,
		OwnerScene: testScene,
		Version:    1,
		City: &domain.City{
			Header: domain.ObjectHeader{
				WorldID: testWorld, ID: id, Kind: domain.ObjectKindCity,
				OwnerScene: testScene, Version: 1,
			},
			PlayerID: player,
			Position: domain.Pos{X: x, Y: y},
		},
	}
}

func troopObject(t *domain.Troop) domain.ObjectSnapshot {
	return domain.ObjectSnapshot{
		Key:        t.Header.Key(),
		Kind:       domain.ObjectKindTroop,
		OwnerScene: t.Header.OwnerScene,
		Version:    t.Header.Version,
		PlayerID:   t.PlayerID,
		Troop:      t,
	}
}

func resourceObject(r *domain.ResourceNode) domain.ObjectSnapshot {
	return domain.ObjectSnapshot{
		Key:        r.Header.Key(),
		Kind:       domain.ObjectKindResource,
		OwnerScene: r.Header.OwnerScene,
		Version:    r.Header.Version,
		Resource:   r,
	}
}

// newTroop 造一支部队，Header/负载都按生产路径的常见形态填。
func newTroop(id domain.ObjectID, player int64, phase domain.TroopPhase) *domain.Troop {
	return &domain.Troop{
		Header: domain.ObjectHeader{
			WorldID: testWorld, ID: id, Kind: domain.ObjectKindTroop,
			OwnerScene: testScene, Version: 1,
		},
		PlayerID:      player,
		SlotID:        1,
		HomeCityID:    100,
		Phase:         phase,
		Cargo:         map[int32]int64{},
		CapacityUnits: 100,
		Stats: domain.CombatSnapshot{
			ConfigID: 1, Level: 1, Attack: 100, Defense: 100, HP: 1000,
			Speed: 1000, RuleVersion: m1RuleVersion,
		},
	}
}

func newResource(id domain.ObjectID, x, y int64, remaining int64) *domain.ResourceNode {
	return &domain.ResourceNode{
		Header: domain.ObjectHeader{
			WorldID: testWorld, ID: id, Kind: domain.ObjectKindResource,
			OwnerScene: testScene, Version: 1,
		},
		Position:     domain.Pos{X: x, Y: y},
		ResourceType: 1,
		ItemID:       101,
		Total:        remaining,
		Remaining:    remaining,
	}
}

// makeMove 用真实 spatial.BuildMove 造移动段，避免测试自己编时间公式。
func makeMove(t *testing.T, from, to domain.Pos, speed, startMS int64, moveID string) *domain.MoveSegment {
	t.Helper()
	seg, err := spatial.BuildMove(from, to, speed, startMS, moveID, domain.DefaultTopology(testWorld).InBounds)
	require.NoError(t, err, "BuildMove")
	return seg
}

// receiptMap 把回执 JSON 解成 map，数字保留 json.Number 以免被 float64
// 截断（ID 一旦上 2^53 就会丢精度，这正是 ResultJSON 用整数的原因）。
func receiptMap(t *testing.T, plan *domain.MutationPlan) map[string]any {
	t.Helper()
	if plan == nil || plan.CommandReceipt == nil {
		t.Fatalf("计划缺少回执")
	}
	dec := json.NewDecoder(bytes.NewReader(plan.CommandReceipt.ResultJSON))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("回执 JSON 解析失败: %v (%s)", err, plan.CommandReceipt.ResultJSON)
	}
	return m
}

func jsonInt(t *testing.T, m map[string]any, key string) int64 {
	t.Helper()
	v, ok := m[key]
	require.True(t, ok)
	n, ok := v.(json.Number)
	require.True(t, ok)
	i, err := n.Int64()
	require.NoError(t, err)
	return i
}

func jsonStr(t *testing.T, m map[string]any, key string) string {
	t.Helper()
	v, ok := m[key]
	require.True(t, ok)
	s, ok := v.(string)
	require.True(t, ok)
	return s
}

func jsonBool(t *testing.T, m map[string]any, key string) bool {
	t.Helper()
	v, ok := m[key]
	require.True(t, ok)
	b, ok := v.(bool)
	require.True(t, ok)
	return b
}

// findUpsert 按种类找计划里的对象快照。计划内顺序不是契约，
// 测试按 kind 查而不是按下标，避免实现微调就挂。
func findUpsert(plan *domain.MutationPlan, kind domain.ObjectKind) (domain.ObjectSnapshot, bool) {
	for _, o := range plan.ObjectsUpsert {
		if o.Kind == kind {
			return o, true
		}
	}
	return domain.ObjectSnapshot{}, false
}

func countUpsert(plan *domain.MutationPlan, kind domain.ObjectKind) int {
	n := 0
	for _, o := range plan.ObjectsUpsert {
		if o.Kind == kind {
			n++
		}
	}
	return n
}

func marchScene(extra ...domain.ObjectSnapshot) []domain.ObjectSnapshot {
	objs := []domain.ObjectSnapshot{
		cityObject(100, 1, 0, 0),
		resourceObject(newResource(1000, 10000, 0, 100)),
	}
	return append(objs, extra...)
}

// TestPlanMarchStart 覆盖文档 9.1 六步的各个拒绝分支与正常路径。
func TestPlanMarchStart(t *testing.T) {
	target := domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentCollect}

	cases := []struct {
		name    string
		objects []domain.ObjectSnapshot
		cmd     Command
		wantErr error
		check   func(t *testing.T, h *harness, plan *domain.MutationPlan)
	}{
		{
			name:    "正常出征",
			objects: marchScene(),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.EqualValues(t, domain.CodeOK, plan.CommandReceipt.ResultCode)
				require.Len(t, plan.ObjectsUpsert, 1)
				got := plan.ObjectsUpsert[0]
				if got.Kind != domain.ObjectKindTroop || got.Troop == nil {
					t.Fatalf("对象种类=%s", got.Kind)
				}
				tr := got.Troop
				require.EqualValues(t, domain.TroopPhaseMarching, tr.Phase)
				if tr.Header.Version != 1 || tr.Header.OwnerScene != testScene {
					t.Fatalf("Header=%+v", tr.Header)
				}
				if tr.Cargo == nil || len(tr.Cargo) != 0 {
					t.Fatalf("Cargo 必须是非 nil 空表: %v", tr.Cargo)
				}
				require.EqualValues(t, h.rules.CapacityUnits, tr.CapacityUnits)
				// 速度只能来自规则；MarchParams 根本没有速度字段。
				require.EqualValues(t, h.rules.MarchSpeedUnitsPerSec, tr.Move.SpeedUnitsPerSec)
				// 到达时间 = ceil(len/speed)*1000 + now。
				length, err := spatial.SegmentLengthUnits(domain.Pos{X: 0, Y: 0}, domain.Pos{X: 10000, Y: 0})
				require.NoError(t, err, "SegmentLengthUnits")
				wantArrive := int64(1000) + ((length+h.rules.MarchSpeedUnitsPerSec-1)/h.rules.MarchSpeedUnitsPerSec)*1000
				require.EqualValues(t, wantArrive, tr.Move.EndAtMS)
				m := receiptMap(t, plan)
				if got := jsonInt(t, m, "arrive_at_ms"); got != wantArrive {
					t.Fatalf("回执 arrive_at_ms=%d, want %d", got, wantArrive)
				}
				if got := jsonInt(t, m, "troop_id"); got != int64(tr.Header.ID) {
					t.Fatalf("回执 troop_id=%d, want %d", got, tr.Header.ID)
				}
				if got := jsonStr(t, m, "move_id"); got != tr.Move.MoveID {
					t.Fatalf("回执 move_id=%q, want %q", got, tr.Move.MoveID)
				}
				if len(plan.OutboxEvents) != 1 || plan.OutboxEvents[0].Kind != "TroopMoved" {
					t.Fatalf("outbox=%+v", plan.OutboxEvents)
				}
			},
		},
		{
			name:    "槽位被行军中部队占用",
			objects: marchScene(troopObject(mustMarchingTroop(t, 200, 1))),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			wantErr: domain.ErrSlotBusy,
		},
		{
			name: "SETTLING 仍占槽位",
			objects: marchScene(func() domain.ObjectSnapshot {
				tr := newTroop(201, 1, domain.TroopPhaseSettling)
				tr.SettlementID = "delivery-1"
				return troopObject(tr)
			}()),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			wantErr: domain.ErrSlotBusy,
		},
		{
			name:    "CLOSED 不占槽位",
			objects: marchScene(troopObject(func() *domain.Troop { tr := newTroop(202, 1, domain.TroopPhaseClosed); return tr }())),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			check: func(t *testing.T, h *harness, plan *domain.MutationPlan) {
				require.Len(t, plan.ObjectsUpsert, 1)
			},
		},
		{
			name:    "主堡属于别人",
			objects: []domain.ObjectSnapshot{cityObject(100, 2, 0, 0), resourceObject(newResource(1000, 10000, 0, 100))},
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			wantErr: domain.ErrNotOwner,
		},
		{
			name:    "主堡不存在",
			objects: []domain.ObjectSnapshot{resourceObject(newResource(1000, 10000, 0, 100))},
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			wantErr: domain.ErrObjectNotFound,
		},
		{
			name:    "目标不存在",
			objects: []domain.ObjectSnapshot{cityObject(100, 1, 0, 0)},
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			wantErr: domain.ErrObjectNotFound,
		},
		{
			name:    "采集意图指向部队",
			objects: marchScene(troopObject(mustMarchingTroop(t, 200, 2))),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100,
				Target: domain.TargetRef{TargetID: 200, Intent: domain.MarchIntentCollect},
			}},
			wantErr: domain.ErrInvalidArgument,
		},
		{
			name:    "攻击意图指向资源点",
			objects: marchScene(),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100,
				Target: domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentAttack},
			}},
			wantErr: domain.ErrInvalidArgument,
		},
		{
			name:    "意图未指定",
			objects: marchScene(),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100,
				Target: domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentUnspecified},
			}},
			wantErr: domain.ErrInvalidArgument,
		},
		{
			name:    "路径被阻挡",
			objects: marchScene(),
			cmd: Command{Kind: CmdMarchStart, March: MarchParams{
				SlotID: 1, HomeCityID: 100, Target: target,
			}},
			wantErr: domain.ErrPathBlocked,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var blocked [][2]int64
			if tc.name == "路径被阻挡" {
				// 直线 y=0 上放一堵墙（格 (5,0)）。
				blocked = [][2]int64{{5, 0}}
			}
			snap := snapshot(SnapshotOptions{Objects: tc.objects, BlockedCells: blocked})
			plan, err := h.planner.Plan(snap, testEnv(1, "req-1", 1), tc.cmd, 1000)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err=%v, want %v", err, tc.wantErr)
				}
				return
			}
			require.NoError(t, err, "意外错误")
			if tc.check != nil {
				tc.check(t, h, plan)
			}
		})
	}
}

// mustMarchingTroop 造一支带移动段的部队（用于槽位/目标用例）。
func mustMarchingTroop(t *testing.T, id domain.ObjectID, player int64) *domain.Troop {
	t.Helper()
	tr := newTroop(id, player, domain.TroopPhaseMarching)
	tr.Move = makeMove(t, domain.Pos{X: 0, Y: 0}, domain.Pos{X: 5000, Y: 0}, 1000, 0, fmt.Sprintf("m-%d", id))
	return tr
}

// TestPlanMarchStartIdempotentReplay 覆盖同键同 hash 的重放：
// 返回同 CommandID、只含回执、不生成第二个 Troop。
func TestPlanMarchStartIdempotentReplay(t *testing.T) {
	h := newHarness(t)
	env := testEnv(1, "req-replay", 7)
	cmd := Command{Kind: CmdMarchStart, March: MarchParams{
		SlotID: 1, HomeCityID: 100,
		Target: domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentCollect},
	}}

	first, err := h.planner.Plan(snapshot(SnapshotOptions{Objects: marchScene()}), env, cmd, 1000)
	require.NoError(t, err, "首次规划")
	if h.objCount == nil || *h.objCount != 1001 {
		t.Fatalf("应分配一个对象 ID, got %d", *h.objCount)
	}
	firstTroopID := first.ObjectsUpsert[0].Troop.Header.ID

	// 模拟首次已提交：快照里带上回执。
	replaySnap := snapshot(SnapshotOptions{
		Objects:  marchScene(),
		Receipts: []domain.CommandReceipt{*first.CommandReceipt},
	})
	second, err := h.planner.Plan(replaySnap, env, cmd, 2000)
	require.NoError(t, err, "重放")
	require.EqualValues(t, first.CommandID, second.CommandID)
	require.Len(t, second.ObjectsUpsert, 0)
	require.EqualValues(t, 1001, *h.objCount)
	if second.CommandReceipt == nil || !bytes.Equal(second.CommandReceipt.ResultJSON, first.CommandReceipt.ResultJSON) {
		t.Fatalf("重放回执内容不一致")
	}
	m := receiptMap(t, second)
	require.EqualValues(t, int64(firstTroopID), jsonInt(t, m, "troop_id"), "重放回执 troop_id 应复用首次结果")
}

// TestPlanMarchStartIdempotencyConflict 覆盖同键不同 hash。
func TestPlanMarchStartIdempotencyConflict(t *testing.T) {
	h := newHarness(t)
	env := testEnv(1, "req-conflict", 7)
	cmd := Command{Kind: CmdMarchStart, March: MarchParams{
		SlotID: 1, HomeCityID: 100,
		Target: domain.TargetRef{TargetID: 1000, Intent: domain.MarchIntentCollect},
	}}
	first, err := h.planner.Plan(snapshot(SnapshotOptions{Objects: marchScene()}), env, cmd, 1000)
	require.NoError(t, err, "首次规划")
	snap := snapshot(SnapshotOptions{
		Objects:  marchScene(),
		Receipts: []domain.CommandReceipt{*first.CommandReceipt},
	})
	// 同 request_id，不同 payload_hash。
	other := testEnv(1, "req-conflict", 8)
	if _, err := h.planner.Plan(snap, other, cmd, 1000); !errors.Is(err, domain.ErrIdempotencyConflict) {
		t.Fatalf("err=%v, want ErrIdempotencyConflict", err)
	}
}

// TestPlanRejectsInvalidCommand 覆盖信封与命令种类的入口校验。
func TestPlanRejectsInvalidCommand(t *testing.T) {
	h := newHarness(t)
	snap := snapshot(SnapshotOptions{Objects: marchScene()})

	if _, err := h.planner.Plan(snap, testEnv(1, "req-bad", 1), Command{Kind: CommandKind(99)}, 1000); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("未知 Kind: err=%v", err)
	}
	if _, err := h.planner.Plan(snap, domain.CommandEnvelope{}, Command{Kind: CmdMarchStart}, 1000); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("信封非法: err=%v", err)
	}
	for _, kind := range []CommandKind{CmdTransferPrepare, CmdTransferActivate, CmdTransferRelease} {
		if _, err := h.planner.Plan(snap, testEnv(1, "req-t", 1), Command{Kind: kind}, 1000); !errors.Is(err, domain.ErrFeatureDisabled) {
			t.Fatalf("%s: err=%v, want ErrFeatureDisabled", kind, err)
		}
	}
	if _, err := h.planner.Plan(snap, testEnv(1, "req-scene", 1), Command{Kind: CmdRefresh, TargetScene: 2}, 1000); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("场景不符: err=%v", err)
	}
}
