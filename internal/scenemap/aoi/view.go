package aoi

import (
	"sort"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
)

// AOIGridSize 是视野格相对行走格的默认倍率（规格 3.5 / 6.5）。
//
// 视野格的边长 = AOIGridSize 个行走格，而不是像素。默认 6 时：
//
//	1 个视野格 = 6×6 行走格
//	九宫格     = 3×3 视野格 = 18×18 行走格
//
// 一张 100×100 的图只有 17×17 个视野格，索引体量是行走格索引的 1/36；
// 这是"用视野格而不是行走格做 AOI 键"的全部理由。
//
// 两者混用是这套框架最常见的 bug：把行走格当视野格用，广播范围会放大
// 36 倍（每个格子的邻居全在九宫格里）；反过来视野会缩小 6 倍。
const AOIGridSize int32 = 6

// ViewRadius 是九宫格半径（单位：视野格）。3×3 = 半径 1 的切比雪夫邻域。
//
// 单独定义而不是写死 1，是为了让"五宫格/二十五宫格"这类性能开关
// 有一个明确的落点（改这里 + Neighbors9 的生成方式即可）。
const ViewRadius int32 = 1

// CellKey 是视野格坐标（不是行走格、更不是像素）。
//
// 用结构体而不是 cx<<32|cy 的位运算键：可读性优先，且 Go 的
// 结构体 map 键只要字段可比较就没有额外成本（编译器会用 memhash）。
// 位运算键在日志与调试里表现为一个巨大的负数，排查一线问题时得不偿失。
type CellKey struct {
	X int32
	Y int32
}

// String 便于日志与测试失败信息。
func (c CellKey) String() string {
	return "cell(" + itoa(c.X) + "," + itoa(c.Y) + ")"
}

// ChebyshevCell 返回两个视野格之间的切比雪夫距离（八方向步数）。
//
// 用切比雪夫而不是曼哈顿：九宫格是正方形邻域，斜向相邻的两格
// 曼哈顿距离是 2，会被误判为"不相邻"。
func ChebyshevCell(a, b CellKey) int32 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	if dx < 0 {
		dx = -dx
	}
	if dy < 0 {
		dy = -dy
	}
	if dx > dy {
		return dx
	}
	return dy
}

// ASeeB 报告两个视野格是否互相可见（九宫格语义，含自身）。
//
// 注意这是"格级"判定：格子相邻 ⇒ 双方可见。实体级的可见性还要过
// Behavior.ASeeBForAOI（怪物看不见怪物这类业务规则）。
func ASeeB(a, b CellKey) bool {
	return ChebyshevCell(a, b) <= ViewRadius
}

// Neighbors9 返回九宫格（含中心），顺序固定：
//
//	中心 → 右 → 右下 → 下 → 左下 → 左 → 左上 → 上 → 右上
//
// 为什么要固定顺序：`map` 遍历是随机序。若这里从 map 生成邻居，
// 同一份输入在不同进程/不同次运行里会产生不同的广播顺序，客户端
// 收到的事件次序抖动，测试也会变成"偶发失败"。
func Neighbors9(cell CellKey) []CellKey {
	return []CellKey{
		{cell.X, cell.Y},
		{cell.X + 1, cell.Y},
		{cell.X + 1, cell.Y + 1},
		{cell.X, cell.Y + 1},
		{cell.X - 1, cell.Y + 1},
		{cell.X - 1, cell.Y},
		{cell.X - 1, cell.Y - 1},
		{cell.X, cell.Y - 1},
		{cell.X + 1, cell.Y - 1},
	}
}

// ViewCellOf 把像素坐标换算成视野格。
//
// 两跳换算：像素 → 行走格（geo.CellOf）→ 视野格（geo.ViewOf）。
// 不做这一步而直接 px/(CellW*AoiGrid) 在等距地图上是错的：
// 行走格的宽高比不是 1:1，除法顺序会引入半格偏差。
func ViewCellOf(geo grid.Geometry, px, py int32) CellKey {
	if geo.AoiGrid <= 0 {
		geo.AoiGrid = AOIGridSize
	}
	v := geo.ViewOf(geo.CellOf(px, py))
	return CellKey{X: v.X, Y: v.Y}
}

// ViewGrid 是"视野格 → 实体集合"的倒排索引（只属于 AOI 的 goroutine）。
//
// 两级 map：
//
//	cells[cell]  = set(uuid)   某个视野格里的实体（为算视野集合服务）
//	uuid2cell    = uuid → cell 反向查（为移动时算新旧九宫格服务）
//
// 两级索引必须同步维护：只维护正向会导致移动时不知道"旧格"，
// 差集算不出来；只维护反向则每次算视野都要遍历全图实体。
//
// 并发契约：**非并发安全**，只允许 AOI 的 goroutine 读写。
// 跨 goroutine 读走 Snapshot（不可变 + atomic.Pointer），不要在这里加锁——
// 加了锁也解决不了"读方拿到半更新索引"的问题，只是把竞态变成死锁风险。
type ViewGrid struct {
	cells     map[CellKey]map[scenemap.UUID]struct{}
	uuid2cell map[scenemap.UUID]CellKey

	// 视野格索引的地图边界（左闭右开，单位：视野格）。
	// bounded=false 表示不限制（测试里的小地图/无尺寸场景）。
	minX, minY, maxX, maxY int32
	bounded                bool
}

// NewViewGrid 创建空索引。
func NewViewGrid() *ViewGrid {
	return &ViewGrid{
		cells:     make(map[CellKey]map[scenemap.UUID]struct{}),
		uuid2cell: make(map[scenemap.UUID]CellKey),
	}
}

// SetBounds 设置视野格索引的地图范围（左闭右开，单位：视野格）。
// w/h <= 0 表示不限制。
//
// 越界裁剪的意义不是"防止数组越界"（map 不会越界），而是让边缘实体的
// 九宫格只包含真实存在的格子：地图左上角的实体只需查 4 个格，
// 而不是 9 个（其中 5 个永远是空的）。
func (g *ViewGrid) SetBounds(w, h int32) {
	if w <= 0 || h <= 0 {
		g.bounded = false
		g.minX, g.minY, g.maxX, g.maxY = 0, 0, 0, 0
		return
	}
	g.bounded = true
	g.minX, g.minY, g.maxX, g.maxY = 0, 0, w, h
}

// InBounds 报告视野格是否在索引范围内。未设边界时恒为 true。
func (g *ViewGrid) InBounds(c CellKey) bool {
	if !g.bounded {
		return true
	}
	return c.X >= g.minX && c.Y >= g.minY && c.X < g.maxX && c.Y < g.maxY
}

// Add 把实体放入某个视野格。实体已存在时会先摘除旧格（防索引泄漏）。
func (g *ViewGrid) Add(uuid scenemap.UUID, cell CellKey) {
	if old, ok := g.uuid2cell[uuid]; ok {
		if old == cell {
			return
		}
		g.detach(uuid, old)
	}
	bucket := g.cells[cell]
	if bucket == nil {
		bucket = make(map[scenemap.UUID]struct{}, 4)
		g.cells[cell] = bucket
	}
	bucket[uuid] = struct{}{}
	g.uuid2cell[uuid] = cell
}

// Remove 摘除实体，返回它原来所在的格。
//
// 空格立即回收：一张长局副本里实体在几万个格子上流动过，
// 不回收会让 cells 的键数量随"历史到过的格子数"单调增长。
func (g *ViewGrid) Remove(uuid scenemap.UUID) (CellKey, bool) {
	cell, ok := g.uuid2cell[uuid]
	if !ok {
		return CellKey{}, false
	}
	g.detach(uuid, cell)
	delete(g.uuid2cell, uuid)
	return cell, true
}

// detach 只从正向索引移除，不动 uuid2cell。
func (g *ViewGrid) detach(uuid scenemap.UUID, cell CellKey) {
	bucket := g.cells[cell]
	if bucket == nil {
		return
	}
	delete(bucket, uuid)
	if len(bucket) == 0 {
		delete(g.cells, cell)
	}
}

// Move 把实体迁移到新格，返回旧格与"是否真的换了格"。
//
// 同格不迁移是刻意的：调用方（AOI）需要区分"同格微调"与"跨格"，
// 前者只更新快照位置、不广播移动包；若这里把同格也当成迁移，
// 上层就得再判一次，且会多一次 map 写。
//
// 实体不存在时等价于 Add，返回 (零值, true)。
func (g *ViewGrid) Move(uuid scenemap.UUID, cell CellKey) (CellKey, bool) {
	old, ok := g.uuid2cell[uuid]
	if !ok {
		g.Add(uuid, cell)
		return CellKey{}, true
	}
	if old == cell {
		return old, false
	}
	g.detach(uuid, old)
	bucket := g.cells[cell]
	if bucket == nil {
		bucket = make(map[scenemap.UUID]struct{}, 4)
		g.cells[cell] = bucket
	}
	bucket[uuid] = struct{}{}
	g.uuid2cell[uuid] = cell
	return old, true
}

// CellOf 返回实体所在视野格。
func (g *ViewGrid) CellOf(uuid scenemap.UUID) (CellKey, bool) {
	c, ok := g.uuid2cell[uuid]
	return c, ok
}

// Has 报告实体是否在索引里。
func (g *ViewGrid) Has(uuid scenemap.UUID) bool {
	_, ok := g.uuid2cell[uuid]
	return ok
}

// Len 返回实体数量。
func (g *ViewGrid) Len() int { return len(g.uuid2cell) }

// EntitiesIn 返回这些视野格内出现的全部实体：去重 + 按 UUID 升序。
//
// 去重是必须的：九宫格的格子互不重叠，但"新旧九宫格并集"、
// 调用方传入的任意 cell 列表都可能重复，同一个实体会被算两次，
// 客户端就会收到两条一模一样的出现包。
//
// 排序同样是必须的：map 遍历随机序 ⇒ 每次广播顺序不同 ⇒ 客户端
// 按到达顺序做动画时会出现"同一个实体先出现后消失"的抖动；
// 单测断言也没法写稳定。
func (g *ViewGrid) EntitiesIn(cells []CellKey) []scenemap.UUID {
	if len(cells) == 0 {
		return nil
	}
	seen := make(map[scenemap.UUID]struct{}, 16)
	for _, c := range cells {
		for u := range g.cells[c] {
			seen[u] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]scenemap.UUID, 0, len(seen))
	for u := range seen {
		out = append(out, u)
	}
	sortUUIDs(out)
	return out
}

// CellEntities 返回单个视野格内的实体（排序）。快照构建用它。
func (g *ViewGrid) CellEntities(cell CellKey) []scenemap.UUID {
	bucket := g.cells[cell]
	if len(bucket) == 0 {
		return nil
	}
	out := make([]scenemap.UUID, 0, len(bucket))
	for u := range bucket {
		out = append(out, u)
	}
	sortUUIDs(out)
	return out
}

// Cells 返回有实体的视野格（按 X 再 Y 升序）。快照构建用它。
func (g *ViewGrid) Cells() []CellKey {
	if len(g.cells) == 0 {
		return nil
	}
	out := make([]CellKey, 0, len(g.cells))
	for c := range g.cells {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].X != out[j].X {
			return out[i].X < out[j].X
		}
		return out[i].Y < out[j].Y
	})
	return out
}

// Clear 清空索引（房间关房归还池时调用）。
//
// 直接重建 map 而不是逐条 delete：一局的实体可能上万，
// 逐条删是 O(n) 次 map 操作；重建是 O(1) 次分配 + 让 GC 收走旧表。
func (g *ViewGrid) Clear() {
	g.cells = make(map[CellKey]map[scenemap.UUID]struct{})
	g.uuid2cell = make(map[scenemap.UUID]CellKey)
}

// sortUUIDs 统一 UUID 升序，避免各处各写一份比较函数。
func sortUUIDs(in []scenemap.UUID) {
	sort.Slice(in, func(i, j int) bool { return in[i] < in[j] })
}

// itoa 是一个不引 strconv 的小工具（只在 String 里用）。
func itoa(v int32) string {
	if v == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	neg := v < 0
	var n int64 = int64(v)
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
