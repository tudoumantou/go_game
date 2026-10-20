// internal/game/scene/aoi.go —— 九宫格 AOI 索引与视野差集（R9.2）
//
// 数据结构：grid（格键 → 实体 ID 集合）+ entity.cell（实体当前格键）。
// 视野 = 自身所在格周围 3x3；进出视野以「实体集合差集」为单位判定，
// 理由见 package 子句下方注释与 docs/R9-场景AOI设计.md §5。
// 本文件所有方法都只被场景 Actor 的分片 goroutine 调用，无锁。
package scene

// 九宫格 AOI 索引与视野差集。
//
// 核心原则：**差集以「实体集合」为单位，不是以「格坐标集合」为单位**。
//
// 反例：A 在格 (5,5)、B 在格 (6,5)，两者相邻可见。A 移到 (6,5) 时，
// 旧 3x3 与新 3x3 的「格坐标」差集不为空，但 B 始终在 A 的视野里——
// 按格坐标做差集会误发一对 Leave/Appear，客户端出现实体闪烁。

// cellKeyOf 格坐标 → 64 位键。格坐标恒为非负（坐标已钳制到地图内）。
func cellKeyOf(cx, cy int32) int64 {
	return int64(cx)<<32 | int64(cy)
}

// cellOf 坐标 → 当前格键。Cell 启动期校验为正，整数除法不会除零。
func (a *Actor) cellOf(x, y int32) int64 {
	return cellKeyOf(x/a.cfg.Cell, y/a.cfg.Cell)
}

// viewSet 返回 (x,y) 周围 3x3 格内的实体 ID 集合（含调用者自己）。
//
// 越界的邻居格直接跳过：地图边缘的视野自然变小，不需要虚拟格。
// 含自己是有意的：进入回执与差集比较都能少写一个特例分支，由调用方自行跳过。
// 空格子读到 nil map，range nil 是空转——不必先判键存在。
func (a *Actor) viewSet(x, y int32) map[int64]bool {
	cx := x / a.cfg.Cell
	cy := y / a.cfg.Cell
	out := make(map[int64]bool, 16)
	for dx := int32(-1); dx <= 1; dx++ {
		for dy := int32(-1); dy <= 1; dy++ {
			nx, ny := cx+dx, cy+dy
			if nx < 0 || ny < 0 {
				continue
			}
			for id := range a.grid[cellKeyOf(nx, ny)] {
				out[id] = true
			}
		}
	}
	return out
}

// gridAdd 把实体放进它当前坐标对应的格。
//
// 同时记住格键 e.cell：gridRemove 凭它 O(1) 摘除，不必全表扫描。
func (a *Actor) gridAdd(e *entity) {
	k := a.cellOf(e.x, e.y)
	set := a.grid[k]
	if set == nil {
		set = make(map[int64]struct{}, 8)
		a.grid[k] = set
	}
	set[e.id] = struct{}{}
	e.cell = k
}

// gridRemove 从实体当前格移除，并顺手回收空格子（避免地图跑久了留下大量空 map）。
func (a *Actor) gridRemove(e *entity) {
	set := a.grid[e.cell]
	if set == nil {
		return
	}
	delete(set, e.id)
	// 空格子立即从索引删除：长跑场景不给 grid 留只增不减的空 map
	if len(set) == 0 {
		delete(a.grid, e.cell)
	}
}

// gridMove 跨格时更新索引；同格返回 false。
func (a *Actor) gridMove(e *entity) bool {
	if k := a.cellOf(e.x, e.y); k != e.cell {
		a.gridRemove(e)
		a.gridAdd(e)
		return true
	}
	return false
}

// applyViewDelta 比较移动前后的视野，推送四类消息。
//
//	新增可见：双方各收到一条 Appear
//	离开可见：双方各收到一条 Leave
//	仍在视野：只把移动者的新坐标推给视野内其他人（移动者自己靠回执更新）
//
// 不变量：同一对实体在一次移动中最多各收到一条 Appear 或一条 Leave，
// 不会既 Appear 又 Leave（两个差集互斥）。
func (a *Actor) applyViewDelta(e *entity, oldView, newView map[int64]bool) {
	// 第一遍：newView − oldView = 新进入视野的实体。
	// 视野对称（R9 §5 不变量 2）：A 看到 B ⇒ B 也看到 A，所以双向各推一条 Appear。
	for id := range newView {
		if id == e.id || oldView[id] {
			continue
		}
		other := a.ents[id]
		if other == nil {
			continue
		}
		a.pushBody(e, appearNotice(other))
		a.pushBody(other, appearNotice(e))
	}
	// 第二遍：oldView − newView = 离开视野的实体，同样双向 Leave（对称）。
	for id := range oldView {
		if id == e.id || newView[id] {
			continue
		}
		other := a.ents[id]
		if other == nil {
			continue
		}
		a.pushBody(e, leaveNotice(id))
		a.pushBody(other, leaveNotice(e.id))
	}
	// 第三遍：交集 = 仍在视野的实体，只把移动者的新坐标推给他们；
	// 移动者自己由 replyMove 的回执更新位置，不会收到自己的 Move。
	for id := range newView {
		if id == e.id || !oldView[id] {
			continue
		}
		if other := a.ents[id]; other != nil {
			a.pushBody(other, moveNotice(e))
		}
	}
}
