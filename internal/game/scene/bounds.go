// internal/game/scene/bounds.go —— 地图边界钳制与坐标合法性（场景坐标的唯一入口）
package scene

// Clamp 把坐标钳制到 [0, max) 区间。
//
// 用钳制而不是「越界拒绝」：客户端撞墙时如果收到失败，会卡在原地反复重试；
// 钳制到最近的合法点则能自然贴边滑动。
func Clamp(v, max int32) int32 {
	// max<=0 只可能是配置非法（manager.fromConfig 已在启动期拦截）：
	// 这里是运行时兜底，保证返回值恒在 [0, max)，不会把负坐标漏进网格索引
	// （负格键会与正格键在 cellKeyOf 的位拼装里互相污染）。
	if max <= 0 {
		return 0
	}
	if v < 0 {
		return 0
	}
	if v >= max {
		return max - 1
	}
	return v
}

// InBounds 坐标是否在地图内。
//
// 供测试与配置校验使用；热路径（Enter/Move）一律走 Clamp——「先校正再
// 使用」比「校验后拒绝」对客户端更友好（见 Clamp 注释）。
func InBounds(x, y, width, height int32) bool {
	return x >= 0 && x < width && y >= 0 && y < height
}
