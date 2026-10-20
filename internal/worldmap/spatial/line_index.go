package spatial

import (
	"fmt"
	"sort"

	"game_study/internal/worldmap/domain"
)

// MarchLine 是一条行军线（折线）的索引记录（文档 12.5）。
//
// line_id = move_id：改速/召回会换 MoveID，旧线随旧 MoveID 一起失效，
// 因此不需要额外的线版本号。Points 是折线全部顶点（首点为起点），
// 段数 = len(Points)-1。
//
// Points 是索引自有的深拷贝：调用方的切片可能在 Upsert 之后被复用
// （比如从一块共享缓冲区反复填充再提交），如果索引只存底层数组的
// 别名，后续一次填充就会静默篡改已建立的行军线——这类"切片别名"
// bug 在 Go 里极其常见，且不 panic、只是数据错乱，必须从所有权上
// 杜绝。
type MarchLine struct {
	MoveID string
	Points []domain.Pos
	// MinX/MinY/MaxX/MaxY 是折线的包围盒（含端点的闭区间）。
	// 粗筛用：与视野矩形做 O(1) 区间重叠判断，先把绝大多数不可能
	// 可见的线挡掉，剩下的才做逐段精判。
	MinX, MinY, MaxX, MaxY int64
}

// LineIndex 是行军线索引：以 move_id 为键的折线段集合。
//
// 回答一个问题："给定视野矩形，哪些行军线可能进入玩家屏幕？"
// （文档 12.5：视野可与路线相交，即使起点和终点都不在视野也要显示。）
//
// 并发：SceneLoop/LineWorker 私有，非并发安全（文档 4.2）。
type LineIndex struct {
	lines map[string]*MarchLine
}

// NewLineIndex 构造空索引。
func NewLineIndex() *LineIndex {
	return &LineIndex{lines: make(map[string]*MarchLine)}
}

// Upsert 建立或替换一条行军线。
//
// 替换语义：同一 moveID 重复 Upsert 用新点集整体覆盖旧记录
// （改速后重新画线即此路径），不存在"追加顶点"的半更新状态。
//
// 校验：moveID 不能为空（它是线的身份与幂等键），pts 至少 2 个点
// （单点不构成折线；零长度段请传 [p, p]，包围盒即一个点，查询语义
// 仍正确）。
func (l *LineIndex) Upsert(moveID string, pts []domain.Pos) error {
	if moveID == "" {
		return fmt.Errorf("%w: move_id 不能为空", domain.ErrInvalidArgument)
	}
	if len(pts) < 2 {
		return fmt.Errorf("%w: 行军线 %s 至少需要 2 个点, got %d", domain.ErrInvalidArgument, moveID, len(pts))
	}
	// 深拷贝 + 顺带计算包围盒：一次遍历同时做两件事，避免先拷后算的
	// 第二次遍历。拷贝放在校验之后：非法入参直接拒绝，不做无谓分配。
	cp := make([]domain.Pos, len(pts))
	copy(cp, pts)
	minX, minY, maxX, maxY := cp[0].X, cp[0].Y, cp[0].X, cp[0].Y
	for _, p := range cp[1:] {
		if p.X < minX {
			minX = p.X
		}
		if p.X > maxX {
			maxX = p.X
		}
		if p.Y < minY {
			minY = p.Y
		}
		if p.Y > maxY {
			maxY = p.Y
		}
	}
	l.lines[moveID] = &MarchLine{MoveID: moveID, Points: cp, MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY}
	return nil
}

// Remove 删除行军线。不存在的 moveID 是 no-op：删除必须是幂等的，
// "到达删线"与"召回删线"可能对同一条线竞争执行，后到者不应报错。
func (l *LineIndex) Remove(moveID string) {
	delete(l.lines, moveID)
}

// QueryViewport 返回与视野矩形相交的行军线 move_id，按 id 排序
// （map 遍历随机，排序保证输出确定、测试可复现）。
//
// 两阶段过滤：
//  1. 包围盒粗筛：折线包围盒与视野矩形做闭区间重叠判断，O(1)。
//     包围盒是折线的超集，粗筛条件放款到"闭区间"绝不会漏掉任何
//     精判可通过的线——宁可多送几条去精判，不可漏。
//  2. 线段-矩形精判：逐段调用 segmentIntersectsRect。
//
// 为什么不能只按端点做 AOI：起点和终点都在视野外、但路线横穿视野
// 是常见情况（斜跨地图的行军、绕过视野中央障碍的行军），按端点
// 判定会让玩家看不到从自己家门口斜穿过去的敌军，是战斗信息缺失，
// 不是显示瑕疵。
//
// 视野边界取"含边界"（closed）：贴边可见比漏线好——漏线让部队凭空
// 消失，贴边多显一帧无害。
func (l *LineIndex) QueryViewport(minX, minY, maxX, maxY int64) []string {
	if minX > maxX || minY > maxY {
		return nil
	}
	var out []string
	for moveID, line := range l.lines {
		// 粗筛：包围盒闭区间与视野闭区间重叠。
		if line.MinX > maxX || minX > line.MaxX || line.MinY > maxY || minY > line.MaxY {
			continue
		}
		// 精判：任一段与矩形相交即可见。
		pts := line.Points
		for i := 0; i+1 < len(pts); i++ {
			if segmentIntersectsRect(pts[i].X, pts[i].Y, pts[i+1].X, pts[i+1].Y, minX, minY, maxX, maxY) {
				out = append(out, moveID)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// Len 返回索引中的行军线条数。
func (l *LineIndex) Len() int { return len(l.lines) }

// segmentIntersectsRect 判断闭线段 (x0,y0)-(x1,y1) 是否与闭矩形
// [minX,maxX]×[minY,maxY] 相交（含边界接触）。全程纯整数运算：
// 坐标是 int64 position units，浮点距离/斜率在 5e6 量级上虽然不会
// 立刻失真，但"斜率不存在"（竖线）与"斜率巨大"（近竖线）会让浮点
// 代码分裂出多个分支，而整数叉积对全部方向一致处理。
//
// 三步：
//  1. 端点在矩形内（含边上）→ 相交；
//  2. 分离轴粗拒：x/y 两个轴上矩形与线段的投影只要有一个不重叠，
//     就不可能相交（分离轴定理对轴对齐矩形的直接推论），O(1) 排除；
//     此时两端点必在矩形外，只剩"横穿"一种可能；
//  3. 与矩形四条边分别做线段-线段跨立实验（含端点接触与共线），
//     任一边相交即整体相交。
//
// 叉积量级：坐标差 ≤ 世界尺寸 5e6 units，乘积 ≤ 2.5e13，
// 差 ≤ 5e13，远低于 int64 上限；越界坐标是调用方 bug，不在本函数
// 防御范围内。
func segmentIntersectsRect(x0, y0, x1, y1, minX, minY, maxX, maxY int64) bool {
	// 1. 任一端点在矩形内（含边界）。
	if pointInClosedRect(x0, y0, minX, minY, maxX, maxY) ||
		pointInClosedRect(x1, y1, minX, minY, maxX, maxY) {
		return true
	}
	// 2. 分离轴粗拒：任一轴投影不重叠 → 不相交。
	if int64Max(x0, x1) < minX || int64Min(x0, x1) > maxX ||
		int64Max(y0, y1) < minY || int64Min(y0, y1) > maxY {
		return false
	}
	// 3. 横穿：线段与矩形四条边之一相交（边按顺时针给出）。
	return segmentsIntersect(x0, y0, x1, y1, minX, minY, maxX, minY) || // 下边
		segmentsIntersect(x0, y0, x1, y1, maxX, minY, maxX, maxY) || // 右边
		segmentsIntersect(x0, y0, x1, y1, maxX, maxY, minX, maxY) || // 上边
		segmentsIntersect(x0, y0, x1, y1, minX, maxY, minX, minY) // 左边
}

// pointInClosedRect 判断点是否在闭矩形内（含四条边）。
func pointInClosedRect(x, y, minX, minY, maxX, maxY int64) bool {
	return x >= minX && x <= maxX && y >= minY && y <= maxY
}

// segmentsIntersect 判断线段 AB 与 CD 是否相交（含端点接触与共线
// 重叠），即计算几何教材中的跨立实验。
//
// 原理：方向叉积 cross(o,a,b) 的符号表示"从 oa 到 ob 的旋向"。
//   - d1/d2 是 C、D 相对于直线 AB 的两侧方向；异号（或为零）说明
//     CD 段跨越 AB 所在直线；
//   - d3/d4 是 A、B 相对于直线 CD 的两侧方向；同样异号或为零说明
//     AB 跨越 CD 所在直线；
//   - 双向都跨越才真正相交；叉积为零时退化到"端点是公共点"，
//     用 onSegment 确认该端点确实落在对方线段上（共线但超出
//     线段范围的情况必须排除，否则平行共线的两条线段会被误判相交）。
func segmentsIntersect(ax, ay, bx, by, cx, cy, dx, dy int64) bool {
	d1 := cross(cx, cy, dx, dy, ax, ay)
	d2 := cross(cx, cy, dx, dy, bx, by)
	d3 := cross(ax, ay, bx, by, cx, cy)
	d4 := cross(ax, ay, bx, by, dx, dy)
	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
		((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}
	if d1 == 0 && onSegment(cx, cy, dx, dy, ax, ay) {
		return true
	}
	if d2 == 0 && onSegment(cx, cy, dx, dy, bx, by) {
		return true
	}
	if d3 == 0 && onSegment(ax, ay, bx, by, cx, cy) {
		return true
	}
	if d4 == 0 && onSegment(ax, ay, bx, by, dx, dy) {
		return true
	}
	return false
}

// cross 返回 (b-a)×(c-a)，即向量 ab 与 ac 的叉积的 z 分量。
// 符号 >0 表示 c 在 ab 左侧，<0 在右侧，=0 三点共线。
func cross(ax, ay, bx, by, cx, cy int64) int64 {
	return (bx-ax)*(cy-ay) - (by-ay)*(cx-ax)
}

// onSegment 在已知共线（叉积为 0）的前提下，判断点 (px,py) 是否
// 落在线段 ab 上（含端点）：只需检查坐标是否同时落在 ab 的两个
// 轴投影区间内。
func onSegment(ax, ay, bx, by, px, py int64) bool {
	return px >= int64Min(ax, bx) && px <= int64Max(ax, bx) &&
		py >= int64Min(ay, by) && py <= int64Max(ay, by)
}

func int64Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func int64Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
