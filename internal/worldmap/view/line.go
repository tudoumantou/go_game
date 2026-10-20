package view

import (
	"hash/fnv"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/spatial"
	"game_study/pkg/pb/internalv2"
)

// LineKind 是行军线在投影里的种类值（文档 12.5）。
//
// 为什么是 4 而不是复用 domain.ObjectKind：domain 明确写了
// "LINE 是投影不是业务对象，不在此列"。proto 里 ObjectUpsert.kind
// 是 int32，客户端据 4 识别"这是一条线"；后续若新增 LineView 消息，
// 这里的值保持兼容（客户端按 kind 分支渲染）。
const LineKind int32 = 4

// lineIDTag 用来把合成的线 ID 与业务对象 ID 区分开。
//
// 业务 ID 来自 SQL 自增，实际远小于 2^63；把线 ID 的最高位置 1，
// 保证两者空间不重叠。即使 FNV 碰撞，也不会和真实对象撞键——
// 碰撞只可能发生在两条线之间，届时后到的线会覆盖前一条的渲染，
// 属于可接受的 MVP 退化（真正修复要等 proto 增加字符串 line_id）。
const lineIDTag uint64 = 1 << 63

// LineSet 是 view 层对 spatial.LineIndex 的薄封装。
//
// 为什么需要封装：spatial.LineIndex 只暴露 QueryViewport(moveID 列表)，
// 不暴露顶点查询，而本任务不允许修改 spatial。视图层在注册线时同时
// 保存一份顶点深拷贝，就能在保持"用 LineIndex 做粗筛+精判"的同时
// 拿到渲染所需的折线。两份数据在同一个 Upsert/Remove 里同步更新，
// 不会出现"索引里有线、顶点表里没有"的中间态。
//
// 并发：线索引可能由 LineWorker 更新、SceneView 的 Flush 读取，
// 因此这里加 RWMutex。与 spatial 只给单 goroutine 使用的约定不同，
// 本结构是跨 goroutine 的投影侧缓存，必须自己保护。
type LineSet struct {
	mu  sync.RWMutex
	idx *spatial.LineIndex
	pts map[string][]domain.Pos
}

// NewLineSet 构造空的线集合。
func NewLineSet() *LineSet {
	return &LineSet{
		idx: spatial.NewLineIndex(),
		pts: make(map[string][]domain.Pos),
	}
}

// Upsert 建立/替换一条线。move_id 是线的身份（改速/召回会换 MoveID）。
func (s *LineSet) Upsert(moveID string, points []domain.Pos) error {
	// 先校验/写索引再写顶点表：两步都在锁内，读侧不会看到
	// "索引有、顶点没有"的中间态（QueryViewport 与 points 各持一次锁，
	// 但 Flush 只用 move_id 做展示，短暂缺顶点会被跳过，不会发坏数据）。
	cp := append([]domain.Pos(nil), points...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.idx.Upsert(moveID, cp); err != nil {
		return err
	}
	s.pts[moveID] = cp
	return nil
}

// Remove 删除一条线（幂等）。
func (s *LineSet) Remove(moveID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idx.Remove(moveID)
	delete(s.pts, moveID)
}

// QueryViewport 直接复用 spatial.LineIndex 的两阶段过滤：
// 包围盒粗筛 + 线段-矩形精判，起点终点都在视野外但穿过视野的线
// 不会被漏掉（文档 12.5）。
func (s *LineSet) QueryViewport(minX, minY, maxX, maxY int64) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idx.QueryViewport(minX, minY, maxX, maxY)
}

// points 返回顶点深拷贝。拷贝是为了让调用方（BuildLineEvents）在锁外
// 安全使用，同时防止误改索引内部状态。
func (s *LineSet) points(moveID string) ([]domain.Pos, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pts, ok := s.pts[moveID]
	if !ok {
		return nil, false
	}
	return append([]domain.Pos(nil), pts...), true
}

// BuildLineEvents 把穿越视野的行军线转成 ObjectUpsert（文档 12.5）。
//
// 视野 Rect 是格坐标，而 LineIndex 的顶点是 position units，这里先
// 换算再查询。边界取闭区间（max 也换算进去）：贴边多显示一帧无害，
// 漏线会让部队凭空消失。
//
// proto 没有 Line 消息，MVP 用 ObjectUpsert 承载：
//   - Kind = LineKind(4)，ObjectId = 由 move_id 哈希得到的稳定合成 ID；
//   - X/Y 与 Troop.X/Y 存起点；
//   - 折线顶点放在 Troop.Cargo：ItemId = 顶点序号，Count = pack(x,y)
//     （两个坐标各占 32 位，世界最大 5e6 units 远小于 2^32）。
//
// 这是明确的权宜编码：Cargo 本是货物字段，客户端需要按约定解码。
// M3 应在 proto 增加 string line_id + repeated Point，届时删除本编码；
// 在此之前，服务端至少保证同一 move_id 的 ObjectId 稳定，客户端可以
// 用它做 upsert/remove 的键。
func BuildLineEvents(sv *SceneView, rect Rect, lines *LineSet) []*internalv2.ObjectUpsert {
	if sv == nil || lines == nil || rect.Empty() {
		return nil
	}
	units := sv.topo.PositionUnitsCell
	if units <= 0 {
		return nil
	}
	ids := lines.QueryViewport(
		rect.MinX*units, rect.MinY*units,
		rect.MaxX*units, rect.MaxY*units,
	)
	if len(ids) == 0 {
		return nil
	}
	out := make([]*internalv2.ObjectUpsert, 0, len(ids))
	for _, moveID := range ids {
		pts, ok := lines.points(moveID)
		if !ok || len(pts) < 2 {
			continue
		}
		up := lineUpsert(moveID, pts)
		if up == nil {
			continue
		}
		if limit := sv.cfg.PayloadLimitBytes; limit > 0 && proto.Size(up) > limit {
			up = truncateLine(up, limit)
			if up == nil {
				// 连两个顶点都放不下：配置的 payload 上限过小，
				// 宁可不发也不发一条会被客户端判为非法的线。
				continue
			}
		}
		out = append(out, up)
	}
	// map/索引输出可能乱序，按 ObjectId 排序保证同一视野事件顺序稳定。
	sort.Slice(out, func(i, j int) bool { return out[i].ObjectId < out[j].ObjectId })
	return out
}

// lineUpsert 把一条折线编码成 ObjectUpsert（MVP 简化，见 BuildLineEvents）。
func lineUpsert(moveID string, pts []domain.Pos) *internalv2.ObjectUpsert {
	id := lineObjectID(moveID)
	cargo := make([]*internalv2.CargoEntry, 0, len(pts))
	for i, p := range pts {
		cargo = append(cargo, &internalv2.CargoEntry{
			ItemId: int32(i),
			Count:  int64(packPoint(p)),
		})
	}
	start := pts[0]
	return &internalv2.ObjectUpsert{
		ObjectId: id,
		Kind:     LineKind,
		X:        start.X,
		Y:        start.Y,
		Troop: &internalv2.TroopView{
			TroopId: id,
			Phase:   int32(domain.TroopPhaseMarching),
			X:       start.X,
			Y:       start.Y,
			Cargo:   cargo,
		},
	}
}

// lineObjectID 由 move_id 生成稳定的合成 ID（FNV-1a + 高位标记）。
//
// 稳定性很重要：两次 Flush 对同一条线必须得到同一个 ObjectId，
// 否则订阅者会把"同一条线"看成删除+新增，客户端来回闪。
func lineObjectID(moveID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(moveID))
	id := h.Sum64() | lineIDTag
	if id == 0 {
		return lineIDTag
	}
	return id
}

// packPoint 把两个坐标压进一个 uint64。
//
// 允许负坐标：折线起点可能在场景外（甚至世界外），uint32 截断后
// 两个补码拼接仍可无损还原成 int32。世界坐标上限 5e6，远小于 2^31。
func packPoint(p domain.Pos) uint64 {
	return uint64(uint32(p.X))<<32 | uint64(uint32(p.Y))
}

// truncateLine 按顶点数截断超限的线事件，保留起点和至少两个顶点。
// 截断后的几何不完整，但事件仍合法；完整折线需要 M3 的分块协议。
func truncateLine(up *internalv2.ObjectUpsert, limit int) *internalv2.ObjectUpsert {
	for up.Troop != nil && len(up.Troop.Cargo) > 2 && proto.Size(up) > limit {
		up.Troop.Cargo = up.Troop.Cargo[:len(up.Troop.Cargo)-1]
	}
	if proto.Size(up) > limit {
		return nil
	}
	return up
}
