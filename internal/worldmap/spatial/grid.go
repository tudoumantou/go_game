// Package spatial 是 SLG 大地图子系统的空间索引层（文档第 5 章、9.1、12.5）。
//
// 架构职责：本包只提供"纯计算 + 内存索引"，不依赖 MySQL/Redis/Protobuf，
// 也不反向依赖 config/gamedata 包——所有与配置相关的判定（坐标是否合法、
// 某格是否阻挡）都以回调或已校验参数的形式由调用方注入，避免 spatial 变成
// 第二个配置中心。
//
// 四个部分各司其职：
//
//	SceneGrid      按场景组织的对象索引：回答"这个矩形里有哪些对象"、
//	              "某对象在哪个场景"，是 AOI 粗筛与跨场景移交的查找基础。
//	OccupancyIndex 静态占位（主堡/资源点 1×1 格）的内存预检，
//	              最终权威是 SQL map_occupancy 唯一键（文档 5.3）。
//	path.go        直线行军段的长度/时间推导与 Bresenham 阻挡检测（文档 9.1/9.2）。
//	LineIndex      行军线（折线）索引，回答"视野内有哪些行军线"（文档 12.5）。
//
// 并发模型（文档 4.2）：SceneGrid / OccupancyIndex / LineIndex 都是
// SceneLoop 私有的可变状态，非并发安全，只允许对应 OwnerScene 的 SceneLoop
// 单 goroutine 访问。TxWorkerPool、AOI 投影等其他 goroutine 只能拿到
// 值快照（Entry、Snapshot、QueryViewport 的返回值都必须是拷贝）。
// 因此本包刻意不引入 sync.Mutex：加锁会暗示"并发使用是被支持的"，
// 而正确的用法是根本不共享——跨 goroutine 传递一律深拷贝。
// path.go 中的函数是无状态纯函数，可并发调用。
package spatial

import (
	"fmt"
	"sort"

	"game_study/internal/worldmap/domain"
)

// CellEntry 是 SceneGrid 内部为每个对象保存的索引记录。
//
// 为什么字段全部导出：Entry 快照与 CellEntry 是同一组字段的两种视图，
// 导出后可以用一条赋值语句完成快照转换，不需要为每个字段手写映射。
// 注意 CellEntry 只被 *SceneGrid 以指针形式持有（map[ObjectKey]*CellEntry），
// 任何拿到 CellEntry 指针的代码都不得跨 goroutine 使用或长期缓存——
// Put/Delete 会原地修改它。
type CellEntry struct {
	// Kind 对象种类（城池/部队/资源点）。
	Kind domain.ObjectKind
	// Pos 当前权威位置（position units，定点）。
	Pos domain.Pos
	// Scene 该位置推导出的物理场景（domain.Topology.SceneAt(pos)），
	// 与 byScene 分桶一致。SceneOf 直接读它，不需要再算一次除法。
	Scene domain.SceneID
	// Deleted 墓碑标记。Delete 先置位再摘索引：本 tick 内仍持有该记录
	// 指针的投影/计划代码能识别"此对象已删"，而不是把已删对象当成
	// 从未存在。摘索引之后 Get 返回 false，与 ErrObjectNotFound 语义一致。
	Deleted bool
}

// Entry 是 SceneGrid 对外返回的值快照（深拷贝语义）。
//
// 为什么不能直接把 *CellEntry 交出去：调用方（尤其是跨 goroutine 的
// 投影代码）拿到指针后就能改索引内部状态，破坏单写者模型；
// 返回值类型只含标量字段，复制即深拷贝，天然安全。
type Entry struct {
	Key     domain.ObjectKey
	Kind    domain.ObjectKind
	Pos     domain.Pos
	Scene   domain.SceneID
	Deleted bool
}

// SceneGrid 是按场景分桶的对象空间索引。
//
// 数据结构（双索引共享同一份记录）：
//
//	byKey   全局索引 map[ObjectKey]*CellEntry，支撑 O(1) 的
//	         Get/Delete/SceneOf/Len，删除时不需要遍历所有场景桶。
//	byScene 分场景索引 map[SceneID]map[ObjectKey]*CellEntry，
//	         支撑 QueryRect/EntriesInScene 只遍历相关场景。
//
// 两个 map 持有相同的 *CellEntry 指针——这是 Go 里"一份规范记录 + 多个
// 索引视图"的常用手法，省内存且更新一处即两处可见。安全性来自单写者
// 约定：只有 OwnerScene 的 SceneLoop 会调用 Put/Delete 修改记录。
//
// 并发：非并发安全（见文件头与文档 4.2），禁止加锁后共享使用。
type SceneGrid struct {
	topo    domain.Topology
	byKey   map[domain.ObjectKey]*CellEntry
	byScene map[domain.SceneID]map[domain.ObjectKey]*CellEntry
}

// NewSceneGrid 用已校验的拓扑构造空索引。topo 应在启动时通过
// domain.Topology.Validate 校验（文档 17.1）；此处只保存引用——
// Topology 是纯值类型且运行期不可变（热更新需停世界迁移，16.5），
// 不需要深拷贝。
func NewSceneGrid(topo domain.Topology) *SceneGrid {
	return &SceneGrid{
		topo:    topo,
		byKey:   make(map[domain.ObjectKey]*CellEntry),
		byScene: make(map[domain.SceneID]map[domain.ObjectKey]*CellEntry),
	}
}

// Put 插入或更新一个对象。
//
// scene 必须等于 topo.SceneAt(pos)：不一致说明调用方把 OwnerScene
// （当前唯一写入者，文档 5.2）与 PhysicalScene（按位置推导）混用了，
// 属于调用方 bug，直接返回 ErrInvalidArgument 而不是静默纠正——
// 静默纠会让两类归属的混淆扩散到持久层。
//
// 已存在的 key 执行原地更新（位置变化即换场景桶）：部队每 tick 推进
// 都走这条路，Delete+Put 会让索引出现中间态。
func (g *SceneGrid) Put(key domain.ObjectKey, kind domain.ObjectKind, scene domain.SceneID, pos domain.Pos) error {
	// kind 零值非法（domain 约定 ObjectKindUnspecified 不是业务种类），
	// 放进去会让 AOI 与冲突判定拿到无意义的种类。
	if !kind.Valid() {
		return fmt.Errorf("%w: 对象 %s 种类非法: %d", domain.ErrInvalidArgument, key, int32(kind))
	}
	want := g.topo.SceneAt(pos)
	if want == 0 {
		return fmt.Errorf("%w: 对象 %s 位置越界: (%d,%d)", domain.ErrInvalidArgument, key, pos.X, pos.Y)
	}
	if scene != want {
		return fmt.Errorf("%w: 对象 %s 位置 (%d,%d) 推导场景 %d, 传入场景 %d（调用方把 OwnerScene 与 PhysicalScene 混用?）",
			domain.ErrInvalidArgument, key, pos.X, pos.Y, want, scene)
	}
	if old, ok := g.byKey[key]; ok {
		// 原位更新：先从旧场景桶摘掉（跨场景移动时新旧不同），
		// 再挂到新桶。byKey 中的指针不动，SceneOf 等读者看到一致状态。
		if old.Scene != scene {
			g.removeFromScene(old.Scene, key)
			g.addToScene(scene, key, old)
		}
		old.Kind = kind
		old.Pos = pos
		old.Scene = scene
		old.Deleted = false
		return nil
	}
	e := &CellEntry{Kind: kind, Pos: pos, Scene: scene}
	g.byKey[key] = e
	g.addToScene(scene, key, e)
	return nil
}

// Get 返回对象的快照。不存在（或已删除）时 ok=false。
func (g *SceneGrid) Get(key domain.ObjectKey) (Entry, bool) {
	e, ok := g.byKey[key]
	if !ok {
		return Entry{}, false
	}
	return snapshot(key, e), true
}

// Delete 删除对象（幂等：删除不存在的 key 是 no-op）。
//
// 先置墓碑再摘索引：本 tick 内持有该 *CellEntry 的代码能通过 Deleted
// 识别"已删除"，不会把尸体当成活对象继续仲裁。摘索引后外部不可达，
// 记录由 GC 回收。为什么删除不需要扫描场景桶：byKey 里存了 Scene，
// 直接定位那一个桶即可，代价 O(1)。
func (g *SceneGrid) Delete(key domain.ObjectKey) {
	e, ok := g.byKey[key]
	if !ok {
		return
	}
	e.Deleted = true
	g.removeFromScene(e.Scene, key)
	delete(g.byKey, key)
}

// SceneOf 返回对象当前所在物理场景；不存在返回 0（无效场景号，
// 与 domain 约定一致：0 表示"尚未归属任何场景"）。
func (g *SceneGrid) SceneOf(key domain.ObjectKey) domain.SceneID {
	if e, ok := g.byKey[key]; ok {
		return e.Scene
	}
	return 0
}

// QueryRect 查询格坐标矩形 [minX,maxX)×[minY,maxY)（左闭右开）内的对象。
//
// "按场景分块遍历，不扫全世界"：先把矩形换算成场景编号区间
// [sx0,sx1]×[sy0,sy1]，只遍历这些场景桶。5000×5000 格世界有 25 个
// 场景桶，一次跨场景 AOI 查询通常只碰 2~4 个桶，代价与桶内对象数
// 成正比，而不是与全世界对象数成正比。
//
// 为什么用格坐标而不是 units：调用方（AOI 广播块、富饶块、占领查询）
// 天然持有格坐标矩形，且左闭右开语义与格边界对齐，不会出现
// "x=1000 units 到底算左格还是右格"的边界抖动。
//
// 返回值按 (WorldID, ID) 排序：map 遍历顺序在 Go 里是随机的，
// 排序后输出确定，测试可以直接比较，日志也可 diff。
func (g *SceneGrid) QueryRect(minX, minY, maxX, maxY int64) []domain.ObjectKey {
	// 空矩形（左闭右开，min==max 时不含任何格）。
	if minX >= maxX || minY >= maxY {
		return nil
	}
	sw := g.topo.SceneWidthCells()
	sh := g.topo.SceneHeightCells()
	cols := int64(g.topo.SceneCols)
	rows := int64(g.topo.SceneRows)

	// 矩形覆盖的场景区间。maxX-1 是因为右开：最后一列是 maxX-1。
	// Go 整数除法向零取整，负数会得到 0 或负值，因此下面统一 clamp，
	// 不能指望除法自己产生合法区间。
	sx0 := clampSceneAxis(minX/sw, cols)
	sx1 := clampSceneAxis((maxX-1)/sw, cols)
	sy0 := clampSceneAxis(minY/sh, rows)
	sy1 := clampSceneAxis((maxY-1)/sh, rows)
	if sx0 > sx1 || sy0 > sy1 {
		// 矩形完全在世界左侧/下侧之外（clamp 后区间为空）。
		return nil
	}

	var out []domain.ObjectKey
	for sy := sy0; sy <= sy1; sy++ {
		for sx := sx0; sx <= sx1; sx++ {
			scene := domain.SceneID(sy*cols + sx + 1)
			bucket := g.byScene[scene]
			if len(bucket) == 0 {
				continue
			}
			for key, e := range bucket {
				// 对象位置换算回格坐标。Put 已保证 pos 非负，
				// 除法即向下取整，与左闭右开的格归属一致。
				cx := e.Pos.X / g.topo.PositionUnitsCell
				cy := e.Pos.Y / g.topo.PositionUnitsCell
				if cx >= minX && cx < maxX && cy >= minY && cy < maxY {
					out = append(out, key)
				}
			}
		}
	}
	sortObjectKeys(out)
	return out
}

// EntriesInScene 返回某场景内全部对象的快照，按 (WorldID, ID) 排序。
// 非法场景号返回 nil——查询接口不返回 error，非法入参按"空结果"处理，
// 调用方若需要区分"场景错"与"场景空"，应先自行校验 topo.MaxSceneID()。
func (g *SceneGrid) EntriesInScene(scene domain.SceneID) []Entry {
	bucket := g.byScene[scene]
	if len(bucket) == 0 {
		return nil
	}
	out := make([]Entry, 0, len(bucket))
	for key, e := range bucket {
		out = append(out, snapshot(key, e))
	}
	// 与 QueryRect 相同：排序消除 map 随机遍历顺序，保证输出确定。
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key.WorldID != out[j].Key.WorldID {
			return out[i].Key.WorldID < out[j].Key.WorldID
		}
		return out[i].Key.ID < out[j].Key.ID
	})
	return out
}

// Len 返回索引中的对象总数。
func (g *SceneGrid) Len() int { return len(g.byKey) }

// addToScene 把记录挂入场景桶（桶不存在时惰性创建）。
func (g *SceneGrid) addToScene(scene domain.SceneID, key domain.ObjectKey, e *CellEntry) {
	bucket := g.byScene[scene]
	if bucket == nil {
		bucket = make(map[domain.ObjectKey]*CellEntry)
		g.byScene[scene] = bucket
	}
	bucket[key] = e
}

// removeFromScene 从场景桶摘除记录，桶空时删除桶本身——
// 长期不清理空桶会让 byScene 退化成"25 个空 map + 已迁移走的脏键"，
// 遍历时平白消耗CPU。
func (g *SceneGrid) removeFromScene(scene domain.SceneID, key domain.ObjectKey) {
	bucket := g.byScene[scene]
	if bucket == nil {
		return
	}
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(g.byScene, scene)
	}
}

// snapshot 把内部记录转为对外值快照。Entry 只含标量字段，
// 结构体赋值即完成深拷贝，不存在"共享底层 map/切片"的伪快照问题
// （对比 Troop.Clone：那里有 map 字段，必须逐字段复制）。
func snapshot(key domain.ObjectKey, e *CellEntry) Entry {
	return Entry{Key: key, Kind: e.Kind, Pos: e.Pos, Scene: e.Scene, Deleted: e.Deleted}
}

// clampSceneAxis 把场景轴坐标夹到 [0, n-1]。矩形可以整体在世界外
// （minX 为负、maxX 超过世界宽），夹紧后若区间反转，由调用方判空。
func clampSceneAxis(v, n int64) int64 {
	if v < 0 {
		return 0
	}
	if v >= n {
		return n - 1
	}
	return v
}

// sortObjectKeys 按 (WorldID, ID) 排序对象键。ObjectKey 只含两个
// uint64，可直接比较——Go 的结构体键可比较性（所有字段可比较）正是
// byKey 能用 map[ObjectKey] 的前提。
func sortObjectKeys(keys []domain.ObjectKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].WorldID != keys[j].WorldID {
			return keys[i].WorldID < keys[j].WorldID
		}
		return keys[i].ID < keys[j].ID
	})
}
