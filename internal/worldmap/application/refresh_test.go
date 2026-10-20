package application

// refresh_test.go 覆盖富饶块刷新（文档 10.5）：
// 批次上限、选址尝试上限、幂等、规则校验、种子确定性。

import (
	"errors"
	"testing"

	"game_study/internal/worldmap/domain"
)

func refreshCmd(block, generation int64, slot int32) Command {
	return Command{Kind: CmdRefresh, Refresh: RefreshParams{
		RichBlockID: block, Generation: generation, Slot: slot,
	}}
}

// blockResources 造 n 个落在富饶块 0 内、指定 generation 的资源点。
func blockResources(n int, generation int64) []domain.ObjectSnapshot {
	out := make([]domain.ObjectSnapshot, 0, n)
	for i := 0; i < n; i++ {
		r := newResource(domain.ObjectID(5000+i), int64(i)*1000, 0, 100)
		r.RichBlockID = 0
		r.RefreshGeneration = generation
		out = append(out, resourceObject(r))
	}
	return out
}

func TestPlanRefresh(t *testing.T) {
	t.Run("批次上限", func(t *testing.T) {
		h := newHarness(t)
		plan, err := h.planner.Plan(snapshot(SnapshotOptions{}), testEnv(1, "rf-batch", 1), refreshCmd(0, 1, 0), 1000)
		if err != nil {
			t.Fatalf("刷新: %v", err)
		}
		if len(plan.ObjectsUpsert) != h.rules.RefreshBatchSize {
			t.Fatalf("created=%d, want %d", len(plan.ObjectsUpsert), h.rules.RefreshBatchSize)
		}
		if len(plan.OccupancyClaims) != len(plan.ObjectsUpsert) {
			t.Fatalf("每个新资源都要占位: %d/%d", len(plan.OccupancyClaims), len(plan.ObjectsUpsert))
		}
		seen := make(map[cellKey]struct{}, len(plan.ObjectsUpsert))
		for _, o := range plan.ObjectsUpsert {
			r := o.Resource
			if r == nil || o.Kind != domain.ObjectKindResource {
				t.Fatalf("对象种类=%s", o.Kind)
			}
			if r.Total != r.Remaining {
				t.Fatalf("Total/Remaining 不一致: %+v", r)
			}
			if r.RefreshGeneration != 1 || r.ConfigVersion != m1RuleVersion {
				t.Fatalf("资源=%+v", r)
			}
			if r.Header.OwnerScene != testScene || r.Header.Version != 1 {
				t.Fatalf("Header=%+v", r.Header)
			}
			// 富饶块 0 是格 [0,50)×[0,50)，换算成 units 必须落在 [0,50000)。
			if r.Position.X < 0 || r.Position.X >= 50000 || r.Position.Y < 0 || r.Position.Y >= 50000 {
				t.Fatalf("位置越出富饶块: %+v", r.Position)
			}
			cell := cellKey{r.Position.X / 1000, r.Position.Y / 1000}
			if _, dup := seen[cell]; dup {
				t.Fatalf("批内位置重复: %+v", cell)
			}
			seen[cell] = struct{}{}
		}
		m := receiptMap(t, plan)
		if jsonInt(t, m, "created") != int64(len(plan.ObjectsUpsert)) {
			t.Fatalf("回执=%v", m)
		}
	})

	t.Run("选址尝试上限（全块阻挡）", func(t *testing.T) {
		h := newHarness(t)
		blocked := make([][2]int64, 0, 50*50)
		for x := int64(0); x < 50; x++ {
			for y := int64(0); y < 50; y++ {
				blocked = append(blocked, [2]int64{x, y})
			}
		}
		snap := snapshot(SnapshotOptions{BlockedCells: blocked})
		plan, err := h.planner.Plan(snap, testEnv(1, "rf-blocked", 1), refreshCmd(0, 2, 0), 1000)
		if err != nil {
			t.Fatalf("刷新: %v", err)
		}
		if len(plan.ObjectsUpsert) != 0 {
			t.Fatalf("全阻挡时不应创建对象, got %d", len(plan.ObjectsUpsert))
		}
		m := receiptMap(t, plan)
		if jsonInt(t, m, "skipped") != int64(h.rules.RefreshBatchSize) {
			t.Fatalf("skipped=%v, want %d", m["skipped"], h.rules.RefreshBatchSize)
		}
	})

	t.Run("幂等重放", func(t *testing.T) {
		h := newHarness(t)
		// 该 generation 已达到 MaxCount：整批视为已完成。
		snap := snapshot(SnapshotOptions{Objects: blockResources(h.rules.RichBlock.MaxCount, 5)})
		plan, err := h.planner.Plan(snap, testEnv(1, "rf-idem", 1), refreshCmd(0, 5, 0), 1000)
		if err != nil {
			t.Fatalf("刷新: %v", err)
		}
		if len(plan.ObjectsUpsert) != 0 {
			t.Fatalf("幂等分支不应创建对象")
		}
		m := receiptMap(t, plan)
		if !jsonBool(t, m, "idempotent") {
			t.Fatalf("回执=%v", m)
		}
	})

	t.Run("按最低数量补齐", func(t *testing.T) {
		h := newHarness(t)
		// 现有 2 个类型 2（tier2 的 min=1 已满足），tier1 min=2 仍缺 2。
		objs := []domain.ObjectSnapshot{
			resourceObject(func() *domain.ResourceNode {
				r := newResource(6000, 1000, 0, 100)
				r.RichBlockID = 0
				r.ResourceType = 2
				r.RefreshGeneration = 4
				return r
			}()),
			resourceObject(func() *domain.ResourceNode {
				r := newResource(6001, 2000, 0, 100)
				r.RichBlockID = 0
				r.ResourceType = 2
				r.RefreshGeneration = 4
				return r
			}()),
		}
		snap := snapshot(SnapshotOptions{Objects: objs})
		plan, err := h.planner.Plan(snap, testEnv(1, "rf-min", 1), refreshCmd(0, 5, 0), 1000)
		if err != nil {
			t.Fatalf("刷新: %v", err)
		}
		if len(plan.ObjectsUpsert) != h.rules.RefreshBatchSize {
			t.Fatalf("created=%d", len(plan.ObjectsUpsert))
		}
		// 批次 3 里前两个必须是 tier1（缺 2 个最低量），第三个按权重抽。
		for i := 0; i < 2; i++ {
			if got := plan.ObjectsUpsert[i].Resource.ResourceType; got != 1 {
				t.Fatalf("第 %d 个应是 tier1, got %d", i, got)
			}
		}
	})

	t.Run("种子确定性", func(t *testing.T) {
		h1 := newHarness(t, func(r *Rules) { r.RefreshBatchSize = 6 })
		h2 := newHarness(t, func(r *Rules) { r.RefreshBatchSize = 6 })
		p1, err := h1.planner.Plan(snapshot(SnapshotOptions{}), testEnv(1, "rf-seed-1", 1), refreshCmd(0, 9, 0), 1000)
		if err != nil {
			t.Fatalf("刷新 1: %v", err)
		}
		p2, err := h2.planner.Plan(snapshot(SnapshotOptions{}), testEnv(1, "rf-seed-2", 1), refreshCmd(0, 9, 0), 1000)
		if err != nil {
			t.Fatalf("刷新 2: %v", err)
		}
		if len(p1.ObjectsUpsert) != 6 || len(p2.ObjectsUpsert) != 6 {
			t.Fatalf("batches=%d/%d", len(p1.ObjectsUpsert), len(p2.ObjectsUpsert))
		}
		for i := range p1.ObjectsUpsert {
			a, b := p1.ObjectsUpsert[i].Resource, p2.ObjectsUpsert[i].Resource
			if a.Position != b.Position {
				t.Fatalf("第 %d 个位置不一致: %+v vs %+v", i, a.Position, b.Position)
			}
			if a.ResourceType != b.ResourceType || a.ItemID != b.ItemID {
				t.Fatalf("第 %d 个档位不一致: %+v vs %+v", i, a, b)
			}
		}
	})
}

// TestNewPlannerRejectsInvalidRichBlock 覆盖 ΣMinimum > MaxCount 的规则校验。
func TestNewPlannerRejectsInvalidRichBlock(t *testing.T) {
	rules := testRules()
	rules.RichBlock.MaxCount = 2
	rules.RichBlock.Tiers = []RichTier{
		{ResourceType: 1, ItemID: 101, Weight: 1, Minimum: 2, TotalPerNode: 100},
		{ResourceType: 2, ItemID: 102, Weight: 1, Minimum: 1, TotalPerNode: 100},
	}
	ids, _ := seqIDs("x-")
	objs, _ := seqObjectIDs(1)
	if _, err := NewPlanner(domain.DefaultTopology(testWorld), rules, ids, objs, fixedSeeds); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("err=%v, want ErrInvalidArgument", err)
	}
}
