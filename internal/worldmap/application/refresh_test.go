package application

// refresh_test.go 覆盖富饶块刷新（文档 10.5）：
// 批次上限、选址尝试上限、幂等、规则校验、种子确定性。

import (
	"errors"
	"fmt"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
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
		require.NoError(t, err, "刷新")
		require.Len(t, plan.ObjectsUpsert, h.rules.RefreshBatchSize)
		require.EqualValues(t, len(plan.ObjectsUpsert), len(plan.OccupancyClaims))
		seen := make(map[cellKey]struct{}, len(plan.ObjectsUpsert))
		for _, o := range plan.ObjectsUpsert {
			r := o.Resource
			require.NotNil(t, r, fmt.Sprintf("对象种类=%s", o.Kind))
			require.EqualValues(t, domain.ObjectKindResource, o.Kind)
			require.EqualValues(t, r.Remaining, r.Total)
			require.EqualValues(t, 1, r.RefreshGeneration, fmt.Sprintf("资源=%+v", r))
			require.EqualValues(t, m1RuleVersion, r.ConfigVersion)
			require.EqualValues(t, testScene, r.Header.OwnerScene, fmt.Sprintf("Header=%+v", r.Header))
			require.EqualValues(t, 1, r.Header.Version)
			// 富饶块 0 是格 [0,50)×[0,50)，换算成 units 必须落在 [0,50000)。
			require.False(t, r.Position.X < 0 || r.Position.X >= 50000 || r.Position.Y < 0 || r.Position.Y >= 50000, fmt.Sprintf("位置越出富饶块: %+v", r.Position))
			cell := cellKey{r.Position.X / 1000, r.Position.Y / 1000}
			_, dup := seen[cell]
			require.False(t, dup, fmt.Sprintf("批内位置重复: %+v", cell))
			seen[cell] = struct{}{}
		}
		m := receiptMap(t, plan)
		require.EqualValues(t, int64(len(plan.ObjectsUpsert)), jsonInt(t, m, "created"))
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
		require.NoError(t, err, "刷新")
		require.Len(t, plan.ObjectsUpsert, 0)
		m := receiptMap(t, plan)
		require.EqualValues(t, int64(h.rules.RefreshBatchSize), jsonInt(t, m, "skipped"))
	})

	t.Run("幂等重放", func(t *testing.T) {
		h := newHarness(t)
		// 该 generation 已达到 MaxCount：整批视为已完成。
		snap := snapshot(SnapshotOptions{Objects: blockResources(h.rules.RichBlock.MaxCount, 5)})
		plan, err := h.planner.Plan(snap, testEnv(1, "rf-idem", 1), refreshCmd(0, 5, 0), 1000)
		require.NoError(t, err, "刷新")
		require.Len(t, plan.ObjectsUpsert, 0, "幂等分支不应创建对象")
		m := receiptMap(t, plan)
		require.False(t, !jsonBool(t, m, "idempotent"), fmt.Sprintf("回执=%v", m))
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
		require.NoError(t, err, "刷新")
		require.Len(t, plan.ObjectsUpsert, h.rules.RefreshBatchSize)
		// 批次 3 里前两个必须是 tier1（缺 2 个最低量），第三个按权重抽。
		for i := 0; i < 2; i++ {
			got := plan.ObjectsUpsert[i].Resource.ResourceType
			require.EqualValues(t, 1, got, fmt.Sprintf("第 %d 个应是 tier1, got %d", i, got))
		}
	})

	t.Run("种子确定性", func(t *testing.T) {
		h1 := newHarness(t, func(r *Rules) { r.RefreshBatchSize = 6 })
		h2 := newHarness(t, func(r *Rules) { r.RefreshBatchSize = 6 })
		p1, err := h1.planner.Plan(snapshot(SnapshotOptions{}), testEnv(1, "rf-seed-1", 1), refreshCmd(0, 9, 0), 1000)
		require.NoError(t, err, "刷新 1")
		p2, err := h2.planner.Plan(snapshot(SnapshotOptions{}), testEnv(1, "rf-seed-2", 1), refreshCmd(0, 9, 0), 1000)
		require.NoError(t, err, "刷新 2")
		require.Len(t, p1.ObjectsUpsert, 6, fmt.Sprintf("batches=%d/%d", len(p1.ObjectsUpsert), len(p2.ObjectsUpsert)))
		require.Len(t, p2.ObjectsUpsert, 6)
		for i := range p1.ObjectsUpsert {
			a, b := p1.ObjectsUpsert[i].Resource, p2.ObjectsUpsert[i].Resource
			require.EqualValues(t, b.Position, a.Position)
			require.EqualValues(t, b.ResourceType, a.ResourceType, fmt.Sprintf("第 %d 个档位不一致: %+v vs %+v", i, a, b))
			require.EqualValues(t, b.ItemID, a.ItemID)
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
	_, err := NewPlanner(domain.DefaultTopology(testWorld), rules, ids, objs, fixedSeeds)
	require.False(t, !errors.Is(err, domain.ErrInvalidArgument), fmt.Sprintf("err=%v, want ErrInvalidArgument", err))
}
