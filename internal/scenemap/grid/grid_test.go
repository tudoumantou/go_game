package grid

import (
	"fmt"
	"testing"

	"game_study/internal/scenemap"
	"github.com/stretchr/testify/require"
)

// 这批测试只覆盖"纯函数 + 边界"，不涉及 Actor。
// 规格 13 的 P0 验收里"跨越阻挡的移动被拒"就是从这里开始的。

func TestGeometryCellConversions(t *testing.T) {
	g := Geometry{CellW: 48, CellH: 24, AoiGrid: 6}

	// 常规换算
	got1 := g.CellOf(95, 47)
	require.EqualValues(t, (scenemap.GridPos{X: 1, Y: 1}), got1, "CellOf(95,47) = %s, want (1,1)", got1)
	// 负数必须向下取整：Go 的截断除法会得到 0，等于把图外当成图内
	got2 := g.CellOf(-1, -1)
	require.EqualValues(t, (scenemap.GridPos{X: -1, Y: -1}), got2, "CellOf(-1,-1) = %s, want (-1,-1)", got2)
	// 格中心
	got3 := g.CellCenter(scenemap.GridPos{X: 2, Y: 3})
	require.EqualValues(t, (scenemap.PixelPos{X: 2*48 + 24, Y: 3*24 + 12}), got3, "CellCenter = %+v", got3)
	// 视野格 = 行走格 / aoi_grid
	got4 := g.ViewOf(scenemap.GridPos{X: 13, Y: 5})
	require.EqualValues(t, (scenemap.GridPos{X: 2, Y: 0}), got4, "ViewOf(13,5) = %s, want (2,0)", got4)
}

func TestGeometryValidate(t *testing.T) {
	cases := []struct {
		name string
		g    Geometry
		ok   bool
	}{
		{"默认", DefaultGeometry(), true},
		{"零格宽", Geometry{CellW: 0, CellH: 24, AoiGrid: 6}, false},
		{"零视野倍率", Geometry{CellW: 48, CellH: 24, AoiGrid: 0}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err5 := tc.g.Validate()
			require.EqualValues(t, tc.ok, (err5 == nil), "Validate() err=%v, want ok=%v", err5, tc.ok)
		})
	}
}

func TestDistances(t *testing.T) {
	a := scenemap.GridPos{X: 1, Y: 1}
	b := scenemap.GridPos{X: 4, Y: 3}

	got6 := Chebyshev(a, b)
	require.EqualValues(t, 3, got6, "Chebyshev = %d, want 3", got6)
	got7 := Manhattan(a, b)
	require.EqualValues(t, 5, got7, "Manhattan = %d, want 5", got7)
	got8 := Euclidean2(a, b)
	require.EqualValues(t, 13, got8, "Euclidean2 = %d, want 13", got8)
	// (1,1)→(4,3) 的 dx=3, dy=2，距离 √13≈3.6：
	// 半径 4 包含，半径 3 不包含。
	require.False(t, !WithinRadius(a, b, 4), "半径 4 应包含 (4,3)（√13≈3.6）")
	require.False(t, WithinRadius(a, b, 3), "半径 3 不应包含 (4,3)")
}

func TestInSector(t *testing.T) {
	origin := scenemap.GridPos{X: 5, Y: 5}
	cases := []struct {
		name string
		dst  scenemap.GridPos
		dir  scenemap.Dir
		half int32
		r    int32
		want bool
	}{
		{"正前方", scenemap.GridPos{X: 8, Y: 5}, scenemap.DirRight, 45, 5, true},
		{"正后方", scenemap.GridPos{X: 2, Y: 5}, scenemap.DirRight, 45, 5, false},
		{"超出半径", scenemap.GridPos{X: 12, Y: 5}, scenemap.DirRight, 45, 5, false},
		{"无朝向退化为圆", scenemap.GridPos{X: 2, Y: 5}, scenemap.DirNone, 45, 5, true},
		{"半角 180 退化为圆", scenemap.GridPos{X: 2, Y: 5}, scenemap.DirRight, 180, 5, true},
		{"侧后方 135 度", scenemap.GridPos{X: 4, Y: 7}, scenemap.DirRight, 45, 5, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got9 := InSector(origin, tc.dst, tc.dir, tc.half, tc.r)
			require.EqualValues(t, tc.want, got9, "InSector = %v, want %v", got9, tc.want)
		})
	}
}

func TestRectClipAndContains(t *testing.T) {
	r := Rect{MinX: -5, MinY: -5, MaxX: 15, MaxY: 15}.Clip(Size{Width: 10, Height: 8})
	require.EqualValues(t, (Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 8}), r)
	// 左闭右开：MaxX 本身不在矩形内
	require.False(t, r.Contains(scenemap.GridPos{X: 10, Y: 0}), "右边界应开区间")
	require.False(t, !r.Contains(scenemap.GridPos{X: 9, Y: 7}), "(9,7) 应在矩形内")
}

func TestBlockerStaticDynamicAndFootprint(t *testing.T) {
	b, err := NewBlockerFromRows([]string{
		"....",
		".##.",
		"....",
	})
	require.NoError(t, err, "构造失败")
	require.False(t, b.Walkable(scenemap.GridPos{X: 1, Y: 1}), "墙格应不可走")
	require.False(t, !b.Walkable(scenemap.GridPos{X: 0, Y: 0}), "空地应可走")
	// 越界视为阻挡
	require.False(t, b.Walkable(scenemap.GridPos{X: -1, Y: 0}) || b.Walkable(scenemap.GridPos{X: 4, Y: 0}), "越界应不可走")
	// 动态阻挡是计数：加两次，删一次仍阻挡
	p := scenemap.GridPos{X: 2, Y: 0}
	b.AddDynamic(p)
	b.AddDynamic(p)
	b.RemoveDynamic(p)
	require.False(t, b.Walkable(p), "两次加、一次删后仍应阻挡")
	b.RemoveDynamic(p)
	require.False(t, !b.Walkable(p), "计数归零后应恢复可走")
	// 体积：4×3 的地图上，中心 (1,1) 半径 2 的 5×5 会被裁剪到 12 格
	fp := b.Footprint(scenemap.GridPos{X: 1, Y: 1}, 2)
	require.Len(t, fp, 12)
	// 角上的体积被裁剪
	fp = b.Footprint(scenemap.GridPos{X: 0, Y: 0}, 1)
	require.Len(t, fp, 4)
}

func TestBlockerRejectsBadMatrix(t *testing.T) {
	_, err10 := NewBlocker(Size{Width: 2, Height: 2}, []byte{0})
	require.Error(t, err10, "矩阵长度不符应报错")
	_, err11 := NewBlockerFromRows([]string{"..", "..."})
	require.Error(t, err11, "行长度不一致应报错")
}

func TestAStarStraightAndAroundWall(t *testing.T) {
	b, err := NewBlockerFromRows([]string{
		".....",
		".###.",
		".....",
	})
	require.NoError(t, err, "构造失败")
	// 同行直达
	path, err := AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 3, Y: 0}, 1000)
	require.NoError(t, err, "直线寻路失败")
	require.Len(t, path, 3, fmt.Sprintf("直线路径异常: %v", path))
	require.EqualValues(t, (scenemap.GridPos{X: 3, Y: 0}), path[len(path)-1])
	// 绕墙：必须从第 0 行或第 2 行绕过去，且不穿墙
	path, err = AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 4, Y: 2}, 1000)
	require.NoError(t, err, "绕墙寻路失败")
	for _, p := range path {
		require.False(t, !b.Walkable(p), fmt.Sprintf("路径穿墙: %s", p))
	}
	require.EqualValues(t, (scenemap.GridPos{X: 4, Y: 2}), path[len(path)-1])
}

func TestAStarRejectsBlockedTargetAndBudget(t *testing.T) {
	b, _ := NewBlockerFromRows([]string{
		"....",
		"..#.",
		"....",
	})
	_, err12 := AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 2, Y: 1}, 1000)
	require.Error(t, err12, "目标是墙应返回错误")
	// 预算 1：第一步之后就会超限
	_, err13 := AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 3, Y: 2}, 1)
	require.Error(t, err13, "超出节点预算应返回错误")
	// 起点即终点
	path, err := AStar(b, scenemap.GridPos{X: 1, Y: 1}, scenemap.GridPos{X: 1, Y: 1}, 10)
	require.NoError(t, err, fmt.Sprintf("原地应返回空路径, got %v err=%v", path, err))
	require.Empty(t, path)
}

func TestAStarNoDiagonalCornerCutting(t *testing.T) {
	// 两面墙夹出一个斜角：(1,0) 与 (0,1) 都不可走，
	// 不允许从 (0,0) 斜穿到 (1,1)。
	b, _ := NewBlockerFromRows([]string{
		".#",
		"#.",
	})
	_, err14 := AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 1, Y: 1}, 100)
	require.Error(t, err14, "不应允许斜穿墙角")
}

func TestRingAndRandomCell(t *testing.T) {
	ring := Ring(scenemap.GridPos{X: 5, Y: 5}, 2)
	require.Len(t, ring, 16)
	got15 := Ring(scenemap.GridPos{X: 5, Y: 5}, 0)
	require.EqualValues(t, 1, len(got15), "半径 0 应只有中心: %v", got15)
	require.EqualValues(t, (scenemap.GridPos{X: 5, Y: 5}), got15[0], "半径 0 应只有中心: %v", got15)
	// 全部格都在切比雪夫距离 2 上
	for _, p := range ring {
		require.EqualValues(t, 2, Chebyshev(p, scenemap.GridPos{X: 5, Y: 5}))
	}

	size := Size{Width: 10, Height: 10}
	walkable := func(p scenemap.GridPos) bool { return p.X != 6 }
	// 注入确定性随机：永远取第 0 个候选
	p, err := RandomCellInRing(scenemap.GridPos{X: 5, Y: 5}, 1, 3, size,
		walkable, func(int) int { return 0 })
	require.NoError(t, err, "应能找到落点")
	require.NotEqualValues(t, (scenemap.GridPos{X: 6, Y: 4}), p, fmt.Sprintf("落点撞上了不可走格: %s", p))
	require.NotEqualValues(t, (scenemap.GridPos{X: 6, Y: 5}), p)
	require.NotEqualValues(t, (scenemap.GridPos{X: 6, Y: 6}), p)
	// 整张图都不可走
	if _, err := RandomCellInRing(scenemap.GridPos{X: 5, Y: 5}, 1, 2, size,
		func(scenemap.GridPos) bool { return false }, nil); err == nil {
		t.Fatal("没有可走格应返回 ErrNoWalkableCell")
	}
}

func TestStraightStepAndReachable(t *testing.T) {
	b, _ := NewBlockerFromRows([]string{
		"....",
		"..#.",
		"....",
	})
	// 朝目标走一步
	next := StraightStep(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 2, Y: 0})
	require.EqualValues(t, (scenemap.GridPos{X: 1, Y: 0}), next)
	// 前方是墙：原地不动
	next = StraightStep(b, scenemap.GridPos{X: 1, Y: 1}, scenemap.GridPos{X: 3, Y: 1})
	require.EqualValues(t, (scenemap.GridPos{X: 1, Y: 1}), next)
	// 绕行可达
	require.False(t, !Reachable(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 3, Y: 2}, 100), "应可达")
}
