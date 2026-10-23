package grid

import (
	"testing"

	"game_study/internal/scenemap"
	"github.com/stretchr/testify/require"
)

// 这批测试只覆盖"纯函数 + 边界"，不涉及 Actor。
// 规格 13 的 P0 验收里"跨越阻挡的移动被拒"就是从这里开始的。

func TestGeometryCellConversions(t *testing.T) {
	g := Geometry{CellW: 48, CellH: 24, AoiGrid: 6}

	// 常规换算
	if got := g.CellOf(95, 47); got != (scenemap.GridPos{X: 1, Y: 1}) {
		t.Fatalf("CellOf(95,47) = %s, want (1,1)", got)
	}
	// 负数必须向下取整：Go 的截断除法会得到 0，等于把图外当成图内
	if got := g.CellOf(-1, -1); got != (scenemap.GridPos{X: -1, Y: -1}) {
		t.Fatalf("CellOf(-1,-1) = %s, want (-1,-1)", got)
	}
	// 格中心
	if got := g.CellCenter(scenemap.GridPos{X: 2, Y: 3}); got != (scenemap.PixelPos{X: 2*48 + 24, Y: 3*24 + 12}) {
		t.Fatalf("CellCenter = %+v", got)
	}
	// 视野格 = 行走格 / aoi_grid
	if got := g.ViewOf(scenemap.GridPos{X: 13, Y: 5}); got != (scenemap.GridPos{X: 2, Y: 0}) {
		t.Fatalf("ViewOf(13,5) = %s, want (2,0)", got)
	}
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
			if err := tc.g.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() err=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestDistances(t *testing.T) {
	a := scenemap.GridPos{X: 1, Y: 1}
	b := scenemap.GridPos{X: 4, Y: 3}

	if got := Chebyshev(a, b); got != 3 {
		t.Fatalf("Chebyshev = %d, want 3", got)
	}
	if got := Manhattan(a, b); got != 5 {
		t.Fatalf("Manhattan = %d, want 5", got)
	}
	if got := Euclidean2(a, b); got != 13 {
		t.Fatalf("Euclidean2 = %d, want 13", got)
	}
	// (1,1)→(4,3) 的 dx=3, dy=2，距离 √13≈3.6：
	// 半径 4 包含，半径 3 不包含。
	if !WithinRadius(a, b, 4) {
		t.Fatal("半径 4 应包含 (4,3)（√13≈3.6）")
	}
	if WithinRadius(a, b, 3) {
		t.Fatal("半径 3 不应包含 (4,3)")
	}
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
			if got := InSector(origin, tc.dst, tc.dir, tc.half, tc.r); got != tc.want {
				t.Fatalf("InSector = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRectClipAndContains(t *testing.T) {
	r := Rect{MinX: -5, MinY: -5, MaxX: 15, MaxY: 15}.Clip(Size{Width: 10, Height: 8})
	if r != (Rect{MinX: 0, MinY: 0, MaxX: 10, MaxY: 8}) {
		t.Fatalf("Clip = %+v", r)
	}
	// 左闭右开：MaxX 本身不在矩形内
	if r.Contains(scenemap.GridPos{X: 10, Y: 0}) {
		t.Fatal("右边界应开区间")
	}
	if !r.Contains(scenemap.GridPos{X: 9, Y: 7}) {
		t.Fatal("(9,7) 应在矩形内")
	}
}

func TestBlockerStaticDynamicAndFootprint(t *testing.T) {
	b, err := NewBlockerFromRows([]string{
		"....",
		".##.",
		"....",
	})
	require.NoError(t, err, "构造失败")
	if b.Walkable(scenemap.GridPos{X: 1, Y: 1}) {
		t.Fatal("墙格应不可走")
	}
	if !b.Walkable(scenemap.GridPos{X: 0, Y: 0}) {
		t.Fatal("空地应可走")
	}
	// 越界视为阻挡
	if b.Walkable(scenemap.GridPos{X: -1, Y: 0}) || b.Walkable(scenemap.GridPos{X: 4, Y: 0}) {
		t.Fatal("越界应不可走")
	}
	// 动态阻挡是计数：加两次，删一次仍阻挡
	p := scenemap.GridPos{X: 2, Y: 0}
	b.AddDynamic(p)
	b.AddDynamic(p)
	b.RemoveDynamic(p)
	if b.Walkable(p) {
		t.Fatal("两次加、一次删后仍应阻挡")
	}
	b.RemoveDynamic(p)
	if !b.Walkable(p) {
		t.Fatal("计数归零后应恢复可走")
	}
	// 体积：4×3 的地图上，中心 (1,1) 半径 2 的 5×5 会被裁剪到 12 格
	fp := b.Footprint(scenemap.GridPos{X: 1, Y: 1}, 2)
	if len(fp) != 12 {
		t.Fatalf("Footprint 在 4x3 地图上应裁到 12 格，实际 %d", len(fp))
	}
	// 角上的体积被裁剪
	fp = b.Footprint(scenemap.GridPos{X: 0, Y: 0}, 1)
	if len(fp) != 4 {
		t.Fatalf("角落 Footprint 应裁到 4 格，实际 %d", len(fp))
	}
}

func TestBlockerRejectsBadMatrix(t *testing.T) {
	if _, err := NewBlocker(Size{Width: 2, Height: 2}, []byte{0}); err == nil {
		t.Fatal("矩阵长度不符应报错")
	}
	if _, err := NewBlockerFromRows([]string{"..", "..."}); err == nil {
		t.Fatal("行长度不一致应报错")
	}
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
	if len(path) != 3 || path[len(path)-1] != (scenemap.GridPos{X: 3, Y: 0}) {
		t.Fatalf("直线路径异常: %v", path)
	}
	// 绕墙：必须从第 0 行或第 2 行绕过去，且不穿墙
	path, err = AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 4, Y: 2}, 1000)
	require.NoError(t, err, "绕墙寻路失败")
	for _, p := range path {
		if !b.Walkable(p) {
			t.Fatalf("路径穿墙: %s", p)
		}
	}
	if path[len(path)-1] != (scenemap.GridPos{X: 4, Y: 2}) {
		t.Fatalf("终点不对: %v", path)
	}
}

func TestAStarRejectsBlockedTargetAndBudget(t *testing.T) {
	b, _ := NewBlockerFromRows([]string{
		"....",
		"..#.",
		"....",
	})
	if _, err := AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 2, Y: 1}, 1000); err == nil {
		t.Fatal("目标是墙应返回错误")
	}
	// 预算 1：第一步之后就会超限
	if _, err := AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 3, Y: 2}, 1); err == nil {
		t.Fatal("超出节点预算应返回错误")
	}
	// 起点即终点
	path, err := AStar(b, scenemap.GridPos{X: 1, Y: 1}, scenemap.GridPos{X: 1, Y: 1}, 10)
	if err != nil || len(path) != 0 {
		t.Fatalf("原地应返回空路径, got %v err=%v", path, err)
	}
}

func TestAStarNoDiagonalCornerCutting(t *testing.T) {
	// 两面墙夹出一个斜角：(1,0) 与 (0,1) 都不可走，
	// 不允许从 (0,0) 斜穿到 (1,1)。
	b, _ := NewBlockerFromRows([]string{
		".#",
		"#.",
	})
	if _, err := AStar(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 1, Y: 1}, 100); err == nil {
		t.Fatal("不应允许斜穿墙角")
	}
}

func TestRingAndRandomCell(t *testing.T) {
	ring := Ring(scenemap.GridPos{X: 5, Y: 5}, 2)
	if len(ring) != 16 {
		t.Fatalf("半径 2 的环应有 16 格，实际 %d", len(ring))
	}
	if got := Ring(scenemap.GridPos{X: 5, Y: 5}, 0); len(got) != 1 || got[0] != (scenemap.GridPos{X: 5, Y: 5}) {
		t.Fatalf("半径 0 应只有中心: %v", got)
	}
	// 全部格都在切比雪夫距离 2 上
	for _, p := range ring {
		if Chebyshev(p, scenemap.GridPos{X: 5, Y: 5}) != 2 {
			t.Fatalf("环上格 %s 距离不是 2", p)
		}
	}

	size := Size{Width: 10, Height: 10}
	walkable := func(p scenemap.GridPos) bool { return p.X != 6 }
	// 注入确定性随机：永远取第 0 个候选
	p, err := RandomCellInRing(scenemap.GridPos{X: 5, Y: 5}, 1, 3, size,
		walkable, func(int) int { return 0 })
	require.NoError(t, err, "应能找到落点")
	if p == (scenemap.GridPos{X: 6, Y: 4}) || p == (scenemap.GridPos{X: 6, Y: 5}) || p == (scenemap.GridPos{X: 6, Y: 6}) {
		t.Fatalf("落点撞上了不可走格: %s", p)
	}
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
	if next != (scenemap.GridPos{X: 1, Y: 0}) {
		t.Fatalf("StraightStep = %s", next)
	}
	// 前方是墙：原地不动
	next = StraightStep(b, scenemap.GridPos{X: 1, Y: 1}, scenemap.GridPos{X: 3, Y: 1})
	if next != (scenemap.GridPos{X: 1, Y: 1}) {
		t.Fatalf("撞墙应原地, got %s", next)
	}
	// 绕行可达
	if !Reachable(b, scenemap.GridPos{X: 0, Y: 0}, scenemap.GridPos{X: 3, Y: 2}, 100) {
		t.Fatal("应可达")
	}
}
