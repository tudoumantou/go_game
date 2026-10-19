package scene

// Clamp 把坐标钳制到 [0, max) 区间。
//
// 用钳制而不是「越界拒绝」：客户端撞墙时如果收到失败，会卡在原地反复重试；
// 钳制到最近的合法点则能自然贴边滑动。
func Clamp(v, max int32) int32 {
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
func InBounds(x, y, width, height int32) bool {
	return x >= 0 && x < width && y >= 0 && y < height
}
