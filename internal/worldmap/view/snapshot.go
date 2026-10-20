package view

import (
	"sort"

	"game_study/internal/worldmap/domain"
	"game_study/pkg/pb/internalv2"
)

// Rect 是格坐标矩形，左闭右开：[MinX, MaxX) × [MinY, MaxY)。
//
// 为什么统一用"格"而不是 position units：视野、AOI 块、场景边界天然
// 以格为单位，左闭右开与格边界对齐，不会出现"x=1000 units 算左格还是
// 右格"的抖动。对象位置在内部仍保存 units，但入桶/过滤前会被截断成格。
//
// 值类型：Rect 只有四个 int64，按值传递比指针安全，也不用担心调用方
// 在另一个 goroutine 改写。
type Rect struct {
	MinX int64
	MinY int64
	MaxX int64
	MaxY int64
}

// Width 返回宽度（格）。空矩形返回 0 或负数，调用方应先用 Empty 判断。
func (r Rect) Width() int64 { return r.MaxX - r.MinX }

// Height 返回高度（格）。
func (r Rect) Height() int64 { return r.MaxY - r.MinY }

// Empty 报告矩形是否为空。左闭右开语义下 min==max 已经不含任何格。
func (r Rect) Empty() bool { return r.MinX >= r.MaxX || r.MinY >= r.MaxY }

// ContainsCell 判断格 (x,y) 是否在矩形内（左闭右开）。
func (r Rect) ContainsCell(x, y int64) bool {
	return x >= r.MinX && x < r.MaxX && y >= r.MinY && y < r.MaxY
}

// ClampToWorld 把矩形裁剪到世界边界内。世界外部分直接丢弃（文档 12.1）。
func (r Rect) ClampToWorld(topo domain.Topology) Rect {
	if r.MinX < 0 {
		r.MinX = 0
	}
	if r.MinY < 0 {
		r.MinY = 0
	}
	if r.MaxX > topo.WidthCells {
		r.MaxX = topo.WidthCells
	}
	if r.MaxY > topo.HeightCells {
		r.MaxY = topo.HeightCells
	}
	return r
}

// Clamp 按文档 12.1 裁剪视野：先裁到世界内，再按左上角保留最大
// MaxViewWidth×MaxViewHeight 的区域。maxW/maxH<=0 表示不做尺寸限制。
//
// 为什么选择"裁剪"而不是"返回错误"：视野过大是客户端/网络问题，
// 直接断开流会让玩家彻底看不到地图；裁到上限后仍能正常游戏，只是
// 看不到屏幕外的内容。非法空矩形（完全在世界外）由调用方拒绝。
func (r Rect) Clamp(topo domain.Topology, maxW, maxH int64) Rect {
	r = r.ClampToWorld(topo)
	if r.Empty() {
		return r
	}
	if maxW > 0 && r.Width() > maxW {
		r.MaxX = r.MinX + maxW
	}
	if maxH > 0 && r.Height() > maxH {
		r.MaxY = r.MinY + maxH
	}
	return r
}

// 块网格：每个场景内部固定 4×4 个子块。
//
// 为什么块号必须能只由 topo 推导（而不是读 pkg/config.AOIBlockCols）：
// BlockOf/BlocksInRect 是纯函数，SceneView 的块索引也需要一个稳定、
// 与配置热更新无关的键。MapConfig.AOIBlockCols=4 描述的是覆盖整个
// 世界的广播块网格（默认 4×4，每块 1250 格），而视图层只需要场景内
// 可寻址的桶——两者粒度不同，用途也不同。若 M2 要求统一，应把块尺寸
// 放进 Config 由调用方传入，而不是让 view 反向 import config。
const (
	blockColsPerScene = 4
	blockRowsPerScene = 4
	blocksPerScene    = blockColsPerScene * blockRowsPerScene
)

// blockCellSize 返回单个子块的边长（格），向上取整保证覆盖。
// 拓扑只保证世界能被场景整除，没有保证场景能被 4 整除；
// 用 ceil 后最后一块可能略窄，但不会漏格。
func blockCellSize(total int64, parts int) int64 {
	if total <= 0 || parts <= 0 {
		return 1
	}
	return (total + int64(parts) - 1) / int64(parts)
}

// BlockOf 返回格 (x,y) 所属的块号；坐标非法返回 0。
//
// 编号方式（全局唯一、1-based）：
//
//	sceneIndex = sy*sceneCols + sx
//	blockID    = sceneIndex*blocksPerScene + by*blockCols + bx + 1
//
// 0 是无效块号，与 domain.SceneID 的 0 值语义一致。
func BlockOf(topo domain.Topology, x, y int64) int32 {
	if !topo.InBoundsCells(x, y) {
		return 0
	}
	sw := topo.SceneWidthCells()
	sh := topo.SceneHeightCells()
	if sw <= 0 || sh <= 0 {
		return 0
	}
	sx := x / sw
	sy := y / sh
	bw := blockCellSize(sw, blockColsPerScene)
	bh := blockCellSize(sh, blockRowsPerScene)
	bx := (x % sw) / bw
	by := (y % sh) / bh
	// ceil 的副作用：最后一块可能比 bw 窄，x%sw 恰好落在块数之外
	// 的情况理论上不会出现（x%sw < sw），但 cfg 极端时仍夹一下。
	if bx >= blockColsPerScene {
		bx = blockColsPerScene - 1
	}
	if by >= blockRowsPerScene {
		by = blockRowsPerScene - 1
	}
	sceneIndex := sy*int64(topo.SceneCols) + sx
	return int32(sceneIndex*blocksPerScene + by*blockColsPerScene + bx + 1)
}

// BlocksInRect 返回矩形覆盖的块号（升序去重）。
//
// 先裁到世界边界：视野可以跨场景、也可以伸出世界，块号只对世界内
// 的格有定义。跨场景时逐场景计算局部块区间，而不是简单地在全局块
// 坐标上画一个 range——块号里编了 sceneIndex，跨场景的全局矩形在
// 编号空间里并不连续。
//
// 输出排序：map 遍历在 Go 里是随机序，排序后 Flush 的事件顺序、
// 测试断言、日志 diff 都稳定可复现。
func BlocksInRect(topo domain.Topology, r Rect) []int32 {
	r = r.ClampToWorld(topo)
	if r.Empty() {
		return nil
	}
	sw := topo.SceneWidthCells()
	sh := topo.SceneHeightCells()
	if sw <= 0 || sh <= 0 {
		return nil
	}
	bw := blockCellSize(sw, blockColsPerScene)
	bh := blockCellSize(sh, blockRowsPerScene)

	sx0 := r.MinX / sw
	sx1 := (r.MaxX - 1) / sw
	sy0 := r.MinY / sh
	sy1 := (r.MaxY - 1) / sh

	seen := make(map[int32]struct{})
	for sy := sy0; sy <= sy1; sy++ {
		for sx := sx0; sx <= sx1; sx++ {
			sceneMinX := sx * sw
			sceneMinY := sy * sh
			// 矩形与当前场景的交集（局部坐标，0-based）。
			lx0 := maxInt64(r.MinX, sceneMinX) - sceneMinX
			lx1 := minInt64(r.MaxX, sceneMinX+sw) - 1 - sceneMinX
			ly0 := maxInt64(r.MinY, sceneMinY) - sceneMinY
			ly1 := minInt64(r.MaxY, sceneMinY+sh) - 1 - sceneMinY
			if lx0 > lx1 || ly0 > ly1 {
				continue
			}
			bx0 := lx0 / bw
			bx1 := lx1 / bw
			by0 := ly0 / bh
			by1 := ly1 / bh
			sceneIndex := sy*int64(topo.SceneCols) + sx
			for by := by0; by <= by1; by++ {
				for bx := bx0; bx <= bx1; bx++ {
					id := int32(sceneIndex*blocksPerScene + by*blockColsPerScene + bx + 1)
					seen[id] = struct{}{}
				}
			}
		}
	}
	out := make([]int32, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// SceneSnapshot 是某一场景视图在某一时刻的只读副本。
//
// Blocks 按块组织，Entry 里的 Upsert 已深拷贝：调用方拿到后可以自由
// 读取/序列化，不会和 SceneView 的后续 Flush 产生数据竞争。
type SceneSnapshot struct {
	SceneID  domain.SceneID
	Revision uint64
	Blocks   map[int32]map[domain.ObjectKey]ObjectEntry
}

// LoadView 按块取出视野内的对象，转成 ObjectUpsert（文档 12.3 的全量部分）。
//
// 入参 sceneView 必须已经完成懒加载（Subscribe/Snapshot 会保证）。
// 只做读取，返回深拷贝；结果按 ObjectKey 排序，避免 map 随机序
// 导致同一视野两次拉取顺序不同（客户端 diff/单测都会因此抖动）。
func LoadView(sceneView *SceneView, rect Rect) []*internalv2.ObjectUpsert {
	if sceneView == nil {
		return nil
	}
	sceneView.mu.Lock()
	defer sceneView.mu.Unlock()
	return sceneView.loadViewLocked(rect)
}

// loadViewLocked 是 LoadView 的持锁版本，Flush 内部可直接复用。
func (sv *SceneView) loadViewLocked(rect Rect) []*internalv2.ObjectUpsert {
	entries := sv.collectRectLocked(rect)
	if len(entries) == 0 {
		return nil
	}
	keys := make([]domain.ObjectKey, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sortObjectKeys(keys)
	out := make([]*internalv2.ObjectUpsert, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneUpsert(entries[k].Upsert))
	}
	return out
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// sortObjectKeys 按 (WorldID, ID) 排序，消除 map 遍历随机序。
// 与 spatial 包的同名逻辑保持一致，避免两个包输出顺序不同。
func sortObjectKeys(keys []domain.ObjectKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].WorldID != keys[j].WorldID {
			return keys[i].WorldID < keys[j].WorldID
		}
		return keys[i].ID < keys[j].ID
	})
}
