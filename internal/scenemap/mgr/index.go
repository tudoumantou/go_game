package mgr

import (
	"fmt"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
)

// Index 是场景的格子索引：回答"这一格有谁 / 这个人在哪一格"。
//
// # 并发契约（红线）
//
// **所有方法只允许 MGR 的 goroutine 调用**。内部不持锁、不上原子操作，
// 与 State 遵循同一条单写者规则。理由：
//   - 索引是热路径（每次移动都要查/改），锁竞争会直接吃掉单 Actor 的吞吐；
//   - 更重要是"设计约束"：加锁会让 AOI/玩家 Actor 顺手读索引变得合法，
//     而索引随时在变，读到一半的数据会引发极难查的错位。
//   - 跨 Actor 需要视野数据时，走 AOI 的视野快照，而不是来读这里。
//
// 六个 map 的分工：
//
//	uuid2pos        UUID → 所在格（中心格）
//	pos2uuids       格 → 该格中心上的 UUID（含玩家，玩家之间可以重叠）
//	pos2players     格 → 该格上的玩家数量（统计用，也用于"怪物不叠玩家"）
//	blockCount      格 → 占位计数（玩家 1、怪物按体积；用于怪物落点判空）
//	uuid2dynBlocks  UUID → 它占用的全部格（体积怪的 footprint）
//	dynBlock2uuids  格 → 占用该格的体积怪（反查，用于"这格被谁堵住"）
//
// # 玩家可重叠、怪物不可重叠（规格取舍）
//
// 规格说"实体不重叠"，但那是目标状态而不是入场规则：
//   - 玩家进图时目标格可能已经有玩家（多人同点回城、出生点扎堆），
//     此时**允许重叠**——客户端会做挤开表现，服务端强行拒收只会让
//     玩家卡在加载界面。代价是两个玩家共享一格，技能范围判定要用
//     像素而不是格子（这是 AOI/战斗侧的事）。
//   - 怪物则**不允许**叠在任何人身上：怪物重叠会造成"打不到/穿模/卡位"，
//     且 AI 的目标格会被同格的其他怪反复抢占。所以 Enter 对怪物做
//     "占位失败则沿切比雪夫环向外找邻格"，找不到返回 ErrBlocked。
//   - 无论哪种，blockCount 都如实记录占用（玩家的占用也计数），
//     这样"怪物不落玩家身上"这一条才成立。
type Index struct {
	size grid.Size

	uuid2pos       map[scenemap.UUID]scenemap.GridPos
	pos2uuids      map[scenemap.GridPos][]scenemap.UUID
	pos2players    map[scenemap.GridPos]int
	blockCount     map[scenemap.GridPos]int
	uuid2dynBlocks map[scenemap.UUID][]scenemap.GridPos
	dynBlock2uuids map[scenemap.GridPos][]scenemap.UUID
	// meta 记录每个实体的类型与体积：Move/Leave 时需要据此回收占位。
	// 不放进 State.Users/NPES：Index 要能独立被单元测试构造，不依赖业务结构。
	meta map[scenemap.UUID]indexMeta
}

type indexMeta struct {
	kind   scenemap.EntityKind
	radius int32
}

// NewIndex 创建空索引。size 用于体积 footprint 的裁剪与越界判定。
func NewIndex(size grid.Size) *Index {
	return &Index{
		size:           size,
		uuid2pos:       make(map[scenemap.UUID]scenemap.GridPos),
		pos2uuids:      make(map[scenemap.GridPos][]scenemap.UUID),
		pos2players:    make(map[scenemap.GridPos]int),
		blockCount:     make(map[scenemap.GridPos]int),
		uuid2dynBlocks: make(map[scenemap.UUID][]scenemap.GridPos),
		dynBlock2uuids: make(map[scenemap.GridPos][]scenemap.UUID),
		meta:           make(map[scenemap.UUID]indexMeta),
	}
}

// EnterOption 是入索引的参数。
type EnterOption struct {
	// Kind 实体类型。决定 pos2players 计数与 Move 的占位回收方式。
	Kind scenemap.EntityKind
	// Radius 体积半径：0=1 格；r>0 占 (2r+1)² 格并写入动态阻挡。
	Radius int32
	// AllowOverlap true=允许与其他实体同格（玩家）。
	// false=必须先找到空位，否则 ErrBlocked（怪物）。
	AllowOverlap bool
	// MaxSearchR 找空位时向外搜索的最大切比雪夫半径。
	// <=0 时取默认 3（再远就说明这张图挤爆了，应该让调用方决定怎么办）。
	MaxSearchR int32
}

// Enter 把实体放进索引，返回**实际**落点。
//
// 注意返回值：当 AllowOverlap=false 且目标格被占时，会沿 Ring 向外找
// 第一个可放置的格并返回它；调用方必须用返回值更新自己的状态，而不是
// 继续用请求里的 pos，否则 State 与 Index 立刻不一致。
func (ix *Index) Enter(uuid scenemap.UUID, pos scenemap.GridPos, opt EnterOption) (scenemap.GridPos, error) {
	if !uuid.Valid() {
		return pos, fmt.Errorf("%w: uuid 为 0", scenemap.ErrInvalidArgument)
	}
	if !ix.size.Contains(pos) {
		return pos, fmt.Errorf("%w: %s 不在地图内", scenemap.ErrOutOfRange, pos)
	}
	if _, dup := ix.uuid2pos[uuid]; dup {
		return pos, fmt.Errorf("%w: uuid=%s 已在索引中", scenemap.ErrAlreadyExists, uuid)
	}

	radius := opt.Radius
	if radius < 0 {
		radius = 0
	}
	want := pos

	if !opt.AllowOverlap {
		maxR := opt.MaxSearchR
		if maxR <= 0 {
			maxR = 3
		}
		found := false
		// 从内向外逐环搜索。grid.Ring 的顺序固定（上→下→左→右），
		// 所以同样的地图状态一定得到同样的落点，可复现。
		for r := int32(0); r <= maxR && !found; r++ {
			for _, cand := range grid.Ring(want, r) {
				if !ix.size.Contains(cand) {
					continue
				}
				if ix.canPlace(cand, radius) {
					pos = cand
					found = true
					break
				}
			}
		}
		if !found {
			return want, fmt.Errorf("%w: %s 附近 %d 格内无空位", scenemap.ErrBlocked, want, maxR)
		}
	}

	ix.place(uuid, pos, indexMeta{kind: opt.Kind, radius: radius})
	return pos, nil
}

// Leave 把实体移出索引。返回它原来的位置与是否存在。
func (ix *Index) Leave(uuid scenemap.UUID) (scenemap.GridPos, bool) {
	pos, ok := ix.uuid2pos[uuid]
	if !ok {
		return scenemap.GridPos{}, false
	}
	ix.unplace(uuid, pos)
	return pos, true
}

// Move 更新实体所在格，返回"格子是否真的变了"。
//
// **同格微调不迁移索引**（规格 11.7 的核心不变量）：客户端每秒会发多次
// 像素微调，绝大多数落在同一格。如果每次都先删后插，pos2uuids 的切片
// 会频繁分配、blockCount 会抖动，而结果完全一样。所以 old == newPos
// 时直接返回 false，调用方据此跳过 EntitySync 与触发器。
func (ix *Index) Move(uuid scenemap.UUID, newPos scenemap.GridPos) (bool, error) {
	old, ok := ix.uuid2pos[uuid]
	if !ok {
		return false, fmt.Errorf("%w: uuid=%s 不在索引中", scenemap.ErrNotFound, uuid)
	}
	if old == newPos {
		return false, nil
	}
	if !ix.size.Contains(newPos) {
		return false, fmt.Errorf("%w: %s 不在地图内", scenemap.ErrOutOfRange, newPos)
	}
	m := ix.meta[uuid]
	ix.unplace(uuid, old)
	ix.place(uuid, newPos, m)
	return true, nil
}

// At 返回实体所在格。
func (ix *Index) At(uuid scenemap.UUID) (scenemap.GridPos, bool) {
	pos, ok := ix.uuid2pos[uuid]
	return pos, ok
}

// EntitiesAt 返回该格中心上的全部实体（副本，调用方可安全遍历）。
//
// 只包含"中心落在这一格"的实体；体积怪占据的周边格请用 BlockCount /
// BlockedFor 判断。顺序是入索引顺序（稳定、可复现）。
func (ix *Index) EntitiesAt(pos scenemap.GridPos) []scenemap.UUID {
	src := ix.pos2uuids[pos]
	if len(src) == 0 {
		return nil
	}
	out := make([]scenemap.UUID, len(src))
	copy(out, src)
	return out
}

// PlayersAt 返回该格上的玩家数量。
func (ix *Index) PlayersAt(pos scenemap.GridPos) int { return ix.pos2players[pos] }

// BlockCount 返回该格的占位计数（玩家 + 怪物体积）。
//
// 用途：怪物落点判空、机关占位判定。**玩家移动不应该用它**——玩家
// 之间允许重叠，用 BlockCount 会把"两个人站一起"误判成"不可走"。
func (ix *Index) BlockCount(pos scenemap.GridPos) int { return ix.blockCount[pos] }

// CanPlace 报告"以 pos 为中心、半径 radius 的体积"能否放下（不考虑自身）。
func (ix *Index) CanPlace(pos scenemap.GridPos, radius int32) bool {
	if !ix.size.Contains(pos) {
		return false
	}
	return ix.canPlace(pos, radius)
}

// BlockedFor 报告 pos 是否被 uuid 之外的**阻挡型**实体占用。
//
// 与 BlockCount 的区别：玩家之间允许重叠，所以玩家移动只看"有没有怪"。
// 这是"玩家可重叠、怪物不可重叠"规则的另一半。
func (ix *Index) BlockedFor(uuid scenemap.UUID, pos scenemap.GridPos) bool {
	for _, other := range ix.pos2uuids[pos] {
		if other == uuid {
			continue
		}
		if ix.meta[other].kind == scenemap.EntityKindPlayer {
			continue
		}
		return true
	}
	// 体积怪：它的 footprint 可能覆盖 pos，而中心不在这里。
	for _, other := range ix.dynBlock2uuids[pos] {
		if other != uuid {
			return true
		}
	}
	return false
}

// Len 返回索引中的实体数量（监控/测试用）。
func (ix *Index) Len() int { return len(ix.uuid2pos) }

// ---- 内部实现 ----

func (ix *Index) canPlace(pos scenemap.GridPos, radius int32) bool {
	if radius <= 0 {
		return ix.blockCount[pos] == 0
	}
	for _, c := range ix.footprint(pos, radius) {
		if ix.blockCount[c] > 0 {
			return false
		}
	}
	return true
}

func (ix *Index) place(uuid scenemap.UUID, pos scenemap.GridPos, m indexMeta) {
	ix.uuid2pos[uuid] = pos
	ix.meta[uuid] = m
	ix.pos2uuids[pos] = append(ix.pos2uuids[pos], uuid)
	if m.kind == scenemap.EntityKindPlayer {
		ix.pos2players[pos]++
	}

	cells := ix.footprint(pos, m.radius)
	for _, c := range cells {
		ix.blockCount[c]++
	}
	// 只有体积怪登记动态阻挡。玩家虽然也计数，但不进 dynBlock 反查表：
	// 否则"玩家挡住怪物走位"会变成永久阻挡，玩家一多就把图锁死。
	if m.radius > 0 {
		ix.uuid2dynBlocks[uuid] = cells
		for _, c := range cells {
			ix.dynBlock2uuids[c] = append(ix.dynBlock2uuids[c], uuid)
		}
	}
}

func (ix *Index) unplace(uuid scenemap.UUID, pos scenemap.GridPos) {
	m := ix.meta[uuid]

	// 1) 从中心格列表移除
	if list := ix.pos2uuids[pos]; len(list) > 0 {
		ix.pos2uuids[pos] = removeUUID(list, uuid)
		if len(ix.pos2uuids[pos]) == 0 {
			// 删空键：长局地图里"每格一个空切片"也是内存，而且会让
			// 监控里的 len(pos2uuids) 失去意义。
			delete(ix.pos2uuids, pos)
		}
	}
	if m.kind == scenemap.EntityKindPlayer {
		if n := ix.pos2players[pos]; n <= 1 {
			delete(ix.pos2players, pos)
		} else {
			ix.pos2players[pos] = n - 1
		}
	}

	// 2) 回收占位计数
	for _, c := range ix.footprint(pos, m.radius) {
		if n := ix.blockCount[c]; n <= 1 {
			delete(ix.blockCount, c)
		} else {
			ix.blockCount[c] = n - 1
		}
	}

	// 3) 回收体积怪的反查表
	if cells, ok := ix.uuid2dynBlocks[uuid]; ok {
		for _, c := range cells {
			if list := ix.dynBlock2uuids[c]; len(list) > 0 {
				ix.dynBlock2uuids[c] = removeUUID(list, uuid)
				if len(ix.dynBlock2uuids[c]) == 0 {
					delete(ix.dynBlock2uuids, c)
				}
			}
		}
		delete(ix.uuid2dynBlocks, uuid)
	}

	delete(ix.uuid2pos, uuid)
	delete(ix.meta, uuid)
}

// footprint 返回以 center 为中心、半径 r 的方形占格（裁剪到地图内）。
//
// 复用 grid.Square + Clip：边界判断只能有一个实现，否则"索引认为在格内、
// 阻挡器认为在格内"迟早漂移。r<=0 时只返回中心格。
func (ix *Index) footprint(center scenemap.GridPos, r int32) []scenemap.GridPos {
	if r <= 0 {
		return []scenemap.GridPos{center}
	}
	rect := grid.Square(center, r).Clip(ix.size)
	out := make([]scenemap.GridPos, 0, int(rect.Width()*rect.Height()))
	for y := rect.MinY; y < rect.MaxY; y++ {
		for x := rect.MinX; x < rect.MaxX; x++ {
			out = append(out, scenemap.GridPos{X: x, Y: y})
		}
	}
	if len(out) == 0 {
		// 中心在地图内时矩形至少包含中心格，这里只是兜底。
		out = append(out, center)
	}
	return out
}

// removeUUID 从切片里删掉一个 uuid（保持顺序，实体数量是个位数，线性足够）。
func removeUUID(list []scenemap.UUID, uuid scenemap.UUID) []scenemap.UUID {
	for i, v := range list {
		if v == uuid {
			copy(list[i:], list[i+1:])
			return list[:len(list)-1]
		}
	}
	return list
}
