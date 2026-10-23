// Package grid 是场景坐标与几何的纯函数库（规格 3.5）。
//
// 无状态、无全局变量、可被任意 goroutine 并发调用。所有函数只吃参数、
// 只返回结果，不读时钟、不读配置单例——这条纪律让几何逻辑可以被
// 表驱动测试穷举，也让它能安全地在 MGR 与 AOI 两个 Actor 里同时用。
//
// 两套格子必须区分（这是本框架最常见的 bug 来源）：
//
//	行走格  CellSize 像素（如 48×24 等距），决定站位/阻挡/射程/寻路
//	视野格  CellSize × AoiGrid（默认 6），只决定"谁看得见谁"
//
// 两者混用的后果：视野缩小 6 倍（把视野格当行走格算）或广播范围放大 36 倍
// （把行走格当视野格算）。本包用不同函数名强制区分：
// PixelToCell / CellToPixel 是行走格，CellToView / ViewOf 是视野格。
package grid

import (
	"errors"
	"fmt"
	"math"

	"game_study/internal/scenemap"
)

// Size 是地图的格子尺寸（行走格）。
type Size struct {
	Width  int32
	Height int32
}

// Valid 报告尺寸是否可用。
func (s Size) Valid() bool { return s.Width > 0 && s.Height > 0 }

// Contains 报告格是否在地图内（左闭右开）。
func (s Size) Contains(p scenemap.GridPos) bool {
	return p.X >= 0 && p.Y >= 0 && p.X < s.Width && p.Y < s.Height
}

// Clamp 把格夹到地图内。用于"落点越界修正"（例如随机出生点算到边缘外）。
func (s Size) Clamp(p scenemap.GridPos) scenemap.GridPos {
	if p.X < 0 {
		p.X = 0
	}
	if p.Y < 0 {
		p.Y = 0
	}
	if p.X >= s.Width {
		p.X = s.Width - 1
	}
	if p.Y >= s.Height {
		p.Y = s.Height - 1
	}
	return p
}

// Geometry 是像素↔格换算参数。
//
// CellW/CellH 是单格的像素宽高（等距地图常见 48×24，宽高不等）。
// AoiGrid 是视野格相对行走格的倍率（默认 6）。
type Geometry struct {
	CellW   int32
	CellH   int32
	AoiGrid int32
}

// DefaultGeometry 返回规格默认值。
func DefaultGeometry() Geometry { return Geometry{CellW: 48, CellH: 24, AoiGrid: 6} }

// Validate 校验几何参数。
func (g Geometry) Validate() error {
	if g.CellW <= 0 || g.CellH <= 0 {
		return fmt.Errorf("%w: 格尺寸必须为正, got %dx%d", scenemap.ErrInvalidArgument, g.CellW, g.CellH)
	}
	if g.AoiGrid <= 0 {
		return fmt.Errorf("%w: aoi_grid 必须为正, got %d", scenemap.ErrInvalidArgument, g.AoiGrid)
	}
	return nil
}

// CellOf 把像素坐标换算成行走格。
//
// 负数像素（地图外）会被算成负数格——这里不夹紧，因为调用方需要
// 区分"在图外"与"在图内边缘"：静默夹紧会让异常坐标看起来合法。
// 用 math.Floor 语义（向下取整）而不是 Go 的截断除法：
// 截断对负数是向零取整，-1 像素会算出格 0，等于把图外当图内。
func (g Geometry) CellOf(px, py int32) scenemap.GridPos {
	return scenemap.GridPos{
		X: int32(math.Floor(float64(px) / float64(g.CellW))),
		Y: int32(math.Floor(float64(py) / float64(g.CellH))),
	}
}

// CellCenter 返回格中心像素。广播"到达某格"用中心点更稳，
// 用左上角会让客户端插值出现半格偏移。
func (g Geometry) CellCenter(p scenemap.GridPos) scenemap.PixelPos {
	return scenemap.PixelPos{
		X: p.X*g.CellW + g.CellW/2,
		Y: p.Y*g.CellH + g.CellH/2,
	}
}

// ViewOf 返回行走格所属的视野格。
//
// 视野格坐标 = 行走格 / aoi_grid（向下取整）。视野格的"大小"
// 是 aoi_grid 个行走格，不是像素——所以这里只做整数除法。
func (g Geometry) ViewOf(p scenemap.GridPos) scenemap.GridPos {
	return scenemap.GridPos{X: p.X / g.AoiGrid, Y: p.Y / g.AoiGrid}
}

// ViewCell 返回视野格的标识（用于 map 键）。
func (g Geometry) ViewCell(p scenemap.GridPos) scenemap.GridPos { return g.ViewOf(p) }

// Chebyshev 返回切比雪夫距离（八方向步数）。
//
// 为什么移动校验用它：玩家一次可以走八方向，一步最多改变 1 格；
// 用曼哈顿距离会把斜走算成 2 步，误判为异常包。
func Chebyshev(a, b scenemap.GridPos) int32 {
	dx := abs32(a.X - b.X)
	dy := abs32(a.Y - b.Y)
	if dx > dy {
		return dx
	}
	return dy
}

// Manhattan 返回曼哈顿距离（四方向步数）。技能射程按格数算时常用。
func Manhattan(a, b scenemap.GridPos) int32 {
	return abs32(a.X-b.X) + abs32(a.Y-b.Y)
}

// Euclidean2 返回欧氏距离的平方。比较距离大小时用平方，避免开方与浮点。
func Euclidean2(a, b scenemap.GridPos) int64 {
	dx := int64(a.X - b.X)
	dy := int64(a.Y - b.Y)
	return dx*dx + dy*dy
}

// WithinRadius 报告 b 是否在以 a 为圆心、r 为半径的圆内（含边界）。
func WithinRadius(a, b scenemap.GridPos, r int32) bool {
	if r < 0 {
		return false
	}
	return Euclidean2(a, b) <= int64(r)*int64(r)
}

// Rect 是矩形范围（左闭右开）。
type Rect struct {
	MinX, MinY, MaxX, MaxY int32
}

// Contains 报告格是否在矩形内。
func (r Rect) Contains(p scenemap.GridPos) bool {
	return p.X >= r.MinX && p.X < r.MaxX && p.Y >= r.MinY && p.Y < r.MaxY
}

// Width/Height 返回矩形尺寸。
func (r Rect) Width() int32  { return r.MaxX - r.MinX }
func (r Rect) Height() int32 { return r.MaxY - r.MinY }

// Empty 报告矩形是否为空。
func (r Rect) Empty() bool { return r.Width() <= 0 || r.Height() <= 0 }

// Square 返回以中心点、半径 r 的外接正方形（格）。
func Square(center scenemap.GridPos, r int32) Rect {
	return Rect{MinX: center.X - r, MinY: center.Y - r, MaxX: center.X + r + 1, MaxY: center.Y + r + 1}
}

// Clip 把矩形裁剪到地图内。
func (r Rect) Clip(s Size) Rect {
	if r.MinX < 0 {
		r.MinX = 0
	}
	if r.MinY < 0 {
		r.MinY = 0
	}
	if r.MaxX > s.Width {
		r.MaxX = s.Width
	}
	if r.MaxY > s.Height {
		r.MaxY = s.Height
	}
	return r
}

// InSector 报告 target 是否在以 origin 为顶点、朝向 dir、半张角 halfAngleDeg、
// 半径 r 的扇形内（技能范围判定）。
//
// 实现用整数叉积与点积，不引入三角函数：
//   - 朝向向量按 8 方向查表；
//   - 夹角判定用 |cross| <= tan(half) * dot 的形式，避免 acos。
//
// halfAngleDeg >= 180 时退化为圆形；dir 为 DirNone 时同样退化为圆形
// （"没有朝向"的技能不该要求朝向）。
func InSector(origin, target scenemap.GridPos, dir scenemap.Dir, halfAngleDeg int32, r int32) bool {
	if !WithinRadius(origin, target, r) {
		return false
	}
	if dir == scenemap.DirNone || halfAngleDeg >= 180 {
		return true
	}
	vx, vy := dirVector(dir)
	dx := int64(target.X - origin.X)
	dy := int64(target.Y - origin.Y)
	if dx == 0 && dy == 0 {
		return true // 顶点本身永远在扇形内
	}
	dot := float64(vx)*float64(dx) + float64(vy)*float64(dy)
	if dot <= 0 {
		return false
	}
	cross := math.Abs(float64(vx)*float64(dy) - float64(vy)*float64(dx))
	tanHalf := math.Tan(float64(halfAngleDeg) * math.Pi / 360) // 半角转弧度
	return cross <= tanHalf*dot+1e-9
}

// dirVector 返回 8 方向的整数向量。斜向用 (±1,±1)，
// 与 Chebyshev 的"一步斜走算 1 步"保持一致。
func dirVector(d scenemap.Dir) (int32, int32) {
	switch d {
	case scenemap.DirRight:
		return 1, 0
	case scenemap.DirDownRight:
		return 1, 1
	case scenemap.DirDown:
		return 0, 1
	case scenemap.DirDownLeft:
		return -1, 1
	case scenemap.DirLeft:
		return -1, 0
	case scenemap.DirUpLeft:
		return -1, -1
	case scenemap.DirUp:
		return 0, -1
	case scenemap.DirUpRight:
		return 1, -1
	default:
		return 0, 0
	}
}

// DirOf 返回从 from 到 to 的 8 方向。同格返回 DirNone。
func DirOf(from, to scenemap.GridPos) scenemap.Dir {
	dx := sign32(to.X - from.X)
	dy := sign32(to.Y - from.Y)
	switch {
	case dx == 1 && dy == 0:
		return scenemap.DirRight
	case dx == 1 && dy == 1:
		return scenemap.DirDownRight
	case dx == 0 && dy == 1:
		return scenemap.DirDown
	case dx == -1 && dy == 1:
		return scenemap.DirDownLeft
	case dx == -1 && dy == 0:
		return scenemap.DirLeft
	case dx == -1 && dy == -1:
		return scenemap.DirUpLeft
	case dx == 0 && dy == -1:
		return scenemap.DirUp
	case dx == 1 && dy == -1:
		return scenemap.DirUpRight
	default:
		return scenemap.DirNone
	}
}

// Neighbors8 返回 8 邻域（不含自身）。顺序固定为「右→右下→下→…」，
// 保证同样的输入在任何进程里得到同样的顺序（AI 选目标要可复现）。
func Neighbors8(p scenemap.GridPos) [8]scenemap.GridPos {
	return [8]scenemap.GridPos{
		{X: p.X + 1, Y: p.Y},
		{X: p.X + 1, Y: p.Y + 1},
		{X: p.X, Y: p.Y + 1},
		{X: p.X - 1, Y: p.Y + 1},
		{X: p.X - 1, Y: p.Y},
		{X: p.X - 1, Y: p.Y - 1},
		{X: p.X, Y: p.Y - 1},
		{X: p.X + 1, Y: p.Y - 1},
	}
}

// Ring 返回距中心恰好 r 步的格（切比雪夫环），按顺时针排列。
//
// 用途：环形随机落点（刷怪）、AOE 外圈。r=0 返回中心本身。
// 环上格数 = 8r（r>0）。
func Ring(center scenemap.GridPos, r int32) []scenemap.GridPos {
	if r <= 0 {
		return []scenemap.GridPos{center}
	}
	out := make([]scenemap.GridPos, 0, 8*r)
	// 上边与下边（含两端）
	for x := center.X - r; x <= center.X+r; x++ {
		out = append(out, scenemap.GridPos{X: x, Y: center.Y - r})
		out = append(out, scenemap.GridPos{X: x, Y: center.Y + r})
	}
	// 左右两边（不含已算过的角）
	for y := center.Y - r + 1; y <= center.Y+r-1; y++ {
		out = append(out, scenemap.GridPos{X: center.X - r, Y: y})
		out = append(out, scenemap.GridPos{X: center.X + r, Y: y})
	}
	return out
}

// ErrNoWalkableCell 环形落点找不到可走格。
var ErrNoWalkableCell = errors.New("scenemap/grid: 没有可走格")

// RandomCellInRing 在环形范围内挑一个可走格。
//
// rand 由调用方注入（通常是 MGR 内的确定性伪随机），这样刷怪结果
// 可以用同一个 seed 复现——线上"这只怪为什么刷在墙里"只能靠回放查。
// 从 r=1 开始向外扩张到 maxR，先内后外，保证落点尽量靠近中心。
func RandomCellInRing(center scenemap.GridPos, minR, maxR int32, size Size, walkable func(scenemap.GridPos) bool, randIntn func(n int) int) (scenemap.GridPos, error) {
	if minR < 0 {
		minR = 0
	}
	if maxR < minR {
		maxR = minR
	}
	for r := minR; r <= maxR; r++ {
		ring := Ring(center, r)
		// 先收集可走格再随机：直接"随机试 N 次"在环上可走格很少时
		// 会误判为"整环不可走"，而实际上有解。
		cands := ring[:0]
		for _, p := range ring {
			if size.Contains(p) && (walkable == nil || walkable(p)) {
				cands = append(cands, p)
			}
		}
		if len(cands) == 0 {
			continue
		}
		if randIntn == nil {
			return cands[0], nil
		}
		return cands[randIntn(len(cands))], nil
	}
	return scenemap.GridPos{}, ErrNoWalkableCell
}

func abs32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}

func sign32(v int32) int32 {
	switch {
	case v > 0:
		return 1
	case v < 0:
		return -1
	default:
		return 0
	}
}
