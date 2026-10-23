package grid

import (
	"container/heap"
	"errors"

	"game_study/internal/scenemap"
)

// ErrNoPath 表示起点到终点不可达。
var ErrNoPath = errors.New("scenemap/grid: 无可达路径")

// 8 方向的步长代价（×1000，避免浮点）：直走 1000，斜走 1414（≈√2）。
const (
	straightCost int32 = 1000
	diagonalCost int32 = 1414
)

// AStar 在 8 邻域格图上寻找最短路径，返回不含起点的格子序列。
//
// 约束：
//   - 斜走禁止"穿角"：只有两个正交邻格都可走时，才允许斜穿，
//     否则怪物会从两面墙的缝隙里钻过去；
//   - maxNodes 限制搜索规模（默认 8000）：MGR 是单 Actor，一次寻路
//     扫全图会让整张图的 buff tick 都卡住。超限直接返回 ErrNoPath，
//     由 AI 退化为"原地/短距随机"，而不是把循环卡死；
//   - 启发函数用切比雪夫×straight：与 8 邻域的实际代价一致，
//     保证 A* 得到的是最优解（用欧氏×1000 会高估，变成贪心）。
//
// 返回的路径包含终点；起点等于终点时返回空切片。
func AStar(blocker *Blocker, from, to scenemap.GridPos, maxNodes int) ([]scenemap.GridPos, error) {
	if blocker == nil {
		return nil, scenemap.ErrInvalidArgument
	}
	if !blocker.size.Contains(from) || !blocker.size.Contains(to) {
		return nil, scenemap.ErrOutOfRange
	}
	if !blocker.Walkable(to) {
		return nil, scenemap.ErrBlocked
	}
	if from == to {
		return nil, nil
	}
	if maxNodes <= 0 {
		maxNodes = 8000
	}

	open := &nodeHeap{}
	heap.Init(open)
	start := &astarNode{pos: from, g: 0, f: heuristic(from, to)}
	heap.Push(open, start)

	// best 记录已发现的最小 g。用 map 而不是二维切片：地图可能是
	// 5000×5000，每次寻路分配 25M 的切片是不可接受的。
	best := map[scenemap.GridPos]int32{from: 0}
	// came 记录父节点，回溯路径用。
	came := make(map[scenemap.GridPos]scenemap.GridPos, 128)
	visited := 0

	for open.Len() > 0 {
		cur := heap.Pop(open).(*astarNode)
		if cur.pos == to {
			return reconstruct(came, from, to), nil
		}
		// 堆里可能有同一格的旧条目（g 更大），跳过。
		if g, ok := best[cur.pos]; ok && cur.g > g {
			continue
		}
		visited++
		if visited > maxNodes {
			// 超过预算：宁可放弃本次寻路，也不能让 MGR 卡住。
			return nil, ErrNoPath
		}
		for _, nb := range Neighbors8(cur.pos) {
			if !blocker.Walkable(nb) {
				continue
			}
			// 斜走穿角检查：两个正交邻格都必须可走。
			if nb.X != cur.pos.X && nb.Y != cur.pos.Y {
				if !blocker.Walkable(scenemap.GridPos{X: nb.X, Y: cur.pos.Y}) ||
					!blocker.Walkable(scenemap.GridPos{X: cur.pos.X, Y: nb.Y}) {
					continue
				}
			}
			step := straightCost
			if nb.X != cur.pos.X && nb.Y != cur.pos.Y {
				step = diagonalCost
			}
			ng := cur.g + step
			if old, ok := best[nb]; ok && ng >= old {
				continue
			}
			best[nb] = ng
			came[nb] = cur.pos
			heap.Push(open, &astarNode{pos: nb, g: ng, f: ng + heuristic(nb, to)})
		}
	}
	return nil, ErrNoPath
}

// Reachable 用 BFS 判断能否到达（不关心路径长度）。
//
// 与 AStar 分开的原因：AI 的"能否追击"只需要布尔值，BFS 不做堆操作，
// 在几百只怪每 tick 都判一次的场景下便宜得多。
func Reachable(blocker *Blocker, from, to scenemap.GridPos, maxNodes int) bool {
	if blocker == nil || !blocker.size.Contains(from) || !blocker.size.Contains(to) {
		return false
	}
	if !blocker.Walkable(to) {
		return false
	}
	if from == to {
		return true
	}
	if maxNodes <= 0 {
		maxNodes = 2000
	}
	seen := map[scenemap.GridPos]struct{}{from: {}}
	queue := []scenemap.GridPos{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, nb := range Neighbors8(cur) {
			if _, dup := seen[nb]; dup || !blocker.Walkable(nb) {
				continue
			}
			if nb == to {
				return true
			}
			seen[nb] = struct{}{}
			queue = append(queue, nb)
			if len(seen) > maxNodes {
				return false
			}
		}
	}
	return false
}

// StraightStep 返回朝目标迈一步的格（不寻路）。
//
// 用途：FIXED 态怪的"原地不动"、以及 A* 失败后的退化移动。
// 只走 8 方向之一，且要求目标格可走；不可走就原地不动（返回 from）。
func StraightStep(blocker *Blocker, from, to scenemap.GridPos) scenemap.GridPos {
	d := DirOf(from, to)
	if d == scenemap.DirNone {
		return from
	}
	dx, dy := dirDelta(d)
	next := scenemap.GridPos{X: from.X + dx, Y: from.Y + dy}
	if !blocker.Walkable(next) {
		return from
	}
	return next
}

// dirDelta 是 8 方向的整数步长。
func dirDelta(d scenemap.Dir) (int32, int32) {
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

// heuristic 是 A* 的启发函数：切比雪夫距离 × 直走代价。
func heuristic(a, b scenemap.GridPos) int32 {
	return Chebyshev(a, b) * straightCost
}

// reconstruct 回溯路径（不含起点，含终点）。
func reconstruct(came map[scenemap.GridPos]scenemap.GridPos, from, to scenemap.GridPos) []scenemap.GridPos {
	var rev []scenemap.GridPos
	cur := to
	for cur != from {
		rev = append(rev, cur)
		prev, ok := came[cur]
		if !ok {
			return nil // 数据不一致（理论上不会发生）
		}
		cur = prev
	}
	// 反转
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	return rev
}

// astarNode 是开放列表元素。
type astarNode struct {
	pos scenemap.GridPos
	g   int32
	f   int32
	idx int // heap 内部下标
}

// nodeHeap 是 container/heap 用的最小堆。
type nodeHeap []*astarNode

func (h nodeHeap) Len() int { return len(h) }

// Less 按 f 升序；f 相同按坐标排序，保证同代价节点顺序可复现
// （map 遍历顺序随机，若只按 f 比较，两次寻路可能给出不同路径）。
func (h nodeHeap) Less(i, j int) bool {
	if h[i].f != h[j].f {
		return h[i].f < h[j].f
	}
	if h[i].pos.X != h[j].pos.X {
		return h[i].pos.X < h[j].pos.X
	}
	return h[i].pos.Y < h[j].pos.Y
}

func (h nodeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *nodeHeap) Push(x any) {
	n := x.(*astarNode)
	n.idx = len(*h)
	*h = append(*h, n)
}

func (h *nodeHeap) Pop() any {
	old := *h
	n := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return n
}
