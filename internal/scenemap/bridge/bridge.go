// Package bridge 把 mgr 的 AOIHandle 接到 aoi.AOI 上。
//
// 为什么要单独一个包：mgr 定义 AOIHandle 接口（依赖倒置），aoi 是具体实现，
// 两边的实体描述与广播范围枚举是各自演化的（mgr 用格 + 像素两份坐标，
// aoi 只认像素；mgr 的 ScopeAround 是格半径，aoi 是像素半径）。
// 让任一方 import 另一方都会成环，所以翻译层放在这里，装配时注入。
package bridge

import (
	"context"
	"fmt"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/aoi"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// AOIAdapter 是 mgr.AOIHandle 的实现。
type AOIAdapter struct {
	// A 是真实的 AOI Actor。
	A *aoi.AOI
	// Geo 用于"格 ↔ 像素"换算（ScopeAround 的半径换算）。
	Geo grid.Geometry
}

// NewAOIAdapter 构造适配器。A 为 nil 时返回 nil——调用方据此退化为
// mgr.NoopAOI（单机测试不需要视野）。
func NewAOIAdapter(a *aoi.AOI, geo grid.Geometry) *AOIAdapter {
	if a == nil {
		return nil
	}
	return &AOIAdapter{A: a, Geo: geo}
}

// 编译期断言：适配器必须满足 MGR 的依赖接口。
var _ mgr.AOIHandle = (*AOIAdapter)(nil)

// Enter 把 MGR 的实体描述翻译成 AOI 的视图实体。
//
// 像素坐标优先级：
//  1. 用 e.Pixel（MGR 进图/移动时已经把权威格换算成像素）；
//  2. 只有 Pixel 恰好是 (0,0) 且 GridPos 不是原点时才回退到格中心——
//     (0,0) 本身是合法位置（地图左上角），所以这个回退只在
//     "MGR 忘了填 Pixel"时兜底，并在日志口径上应视为缺陷。
func (x *AOIAdapter) Enter(ctx context.Context, e mgr.AOIEntity) error {
	px, py := e.Pixel.X, e.Pixel.Y
	if px == 0 && py == 0 && (e.GridPos.X != 0 || e.GridPos.Y != 0) {
		c := x.Geo.CellCenter(e.GridPos)
		px, py = c.X, c.Y
	}
	return x.A.Enter(ctx, aoi.Entity{
		UUID:       e.UUID,
		Kind:       e.Kind,
		PX:         px,
		PY:         py,
		Dir:        e.Dir,
		State:      e.State,
		MasterUUID: e.MasterUUID,
	})
}

// Sync 直接透传：两边都是"像素 + 朝向 + 状态位"。
func (x *AOIAdapter) Sync(ctx context.Context, uuid scenemap.UUID, px, py int32, dir scenemap.Dir, state int32) error {
	return x.A.Sync(ctx, uuid, px, py, dir, state)
}

// Leave 直接透传。
func (x *AOIAdapter) Leave(ctx context.Context, uuid scenemap.UUID) error {
	return x.A.Leave(ctx, uuid)
}

// Broadcast 做范围枚举翻译。
//
// 半径换算：mgr 的 Radius 单位是行走格，AOI 的圆形范围用像素。
// 用 CellW（而不是 CellH）是等距地图的近似——等距投影下圆形本来就
// 会被压扁，客户端表现层自己会做椭圆修正；这里只要"大致覆盖"。
func (x *AOIAdapter) Broadcast(msg any, scope mgr.BroadcastScope) error {
	switch scope.Kind {
	case mgr.ScopeAll:
		return x.A.Broadcast(msg, aoi.MapScope())
	case mgr.ScopeAround:
		c := x.Geo.CellCenter(scope.Center)
		r := scope.Radius * x.Geo.CellW
		if r <= 0 {
			r = x.Geo.CellW
		}
		return x.A.Broadcast(msg, aoi.CircleScope(c.X, c.Y, r))
	case mgr.ScopeUUIDs:
		return x.A.Broadcast(msg, aoi.ToScope(scope.UUIDs...))
	default:
		// 未知范围：返回错误而不是退化成全图。全图广播一条技能特效
		// 在 500 人的主城就是一次 500 倍放大，宁可漏发也不能误发。
		return fmt.Errorf("%w: 未知广播范围 %d", scenemap.ErrInvalidArgument, scope.Kind)
	}
}
