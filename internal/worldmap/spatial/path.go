package spatial

import (
	"fmt"
	"math"

	"game_study/internal/worldmap/domain"
)

// maxInt64Sqrt 是 floor(sqrt(MaxInt64))。dx、dy 各自超过它时，
// 平方一定溢出 int64，必须在乘法前拒绝，不能先乘再检查。
const maxInt64Sqrt int64 = 3037000499

// BuildMove 构造一段直线行军（文档 9.1/9.2）。
//
// 公式：
//
//	len       = ceil(sqrt(dx²+dy²))          // units，向上取整
//	duration  = ceil(len/speed) 秒           // 不能少算一毫秒
//	end_at_ms = start_at_ms + duration*1000
//
// 三个"为什么"：
//  1. 长度向上取整：保证速度为正时零长度段不出现、非零段至少 1 unit，
//     否则 ceil(1/1000)=0 会让一步之隔的移动瞬间完成。
//  2. 零长度段直接完成：from==to 时 len=0、duration=0、end==start，
//     PositionAt 对零时长段直接返回 To，不需要特例分支，也不会除零。
//  3. 速度必须为正：零/负速度说明配置或状态错误，返回错误而不是
//     截断成"立即到达"——静默截断会把配置 bug 伪装成业务结果。
//
// inBounds 是调用方注入的合法谓词（通常是 topo.InBounds）：spatial
// 不持有 Topology、不反向依赖 config，边界规则由上层传入，
// 保持本包"纯计算"的边界。
func BuildMove(from, to domain.Pos, speedUnitsPerSec int64, startAtMS int64, moveID string, inBounds func(domain.Pos) bool) (*domain.MoveSegment, error) {
	if inBounds == nil {
		return nil, fmt.Errorf("%w: inBounds 谓词不能为 nil（调用方必须提供拓扑边界）", domain.ErrInvalidArgument)
	}
	if speedUnitsPerSec <= 0 {
		return nil, fmt.Errorf("%w: 速度必须为正, got %d units/s", domain.ErrInvalidArgument, speedUnitsPerSec)
	}
	if !inBounds(from) || !inBounds(to) {
		return nil, fmt.Errorf("%w: 起终点必须合法: from=(%d,%d) to=(%d,%d)",
			domain.ErrInvalidArgument, from.X, from.Y, to.X, to.Y)
	}
	segLen, err := SegmentLengthUnits(from, to)
	if err != nil {
		return nil, fmt.Errorf("move %s 段长计算失败: %w", moveID, err)
	}
	// 向上取整到秒：CeilDiv 已做正数与溢出检查。
	sec, err := domain.CeilDiv(segLen, speedUnitsPerSec)
	if err != nil {
		return nil, fmt.Errorf("move %s 时长计算失败: %w", moveID, err)
	}
	if sec > math.MaxInt64/1000 {
		return nil, fmt.Errorf("%w: move %s 时长溢出: %d 秒", domain.ErrOverflow, moveID, sec)
	}
	return &domain.MoveSegment{
		MoveID:           moveID,
		From:             from,
		To:               to,
		StartAtMS:        startAtMS,
		EndAtMS:          startAtMS + sec*1000,
		SpeedUnitsPerSec: speedUnitsPerSec,
		// Path 为空：第一版只有直线路径，多段 A* 是后续扩展
		// （文档 9.1：后续 A* 在工作池计算，结果带请求版本）。
		SegmentIndex: 0,
	}, nil
}

// SegmentLengthUnits 返回 from→to 的直线段长度（units），即
// ceil(sqrt(dx²+dy²))。
//
// 为什么显式做溢出检查而不用 domain.SafeMulDiv(dx,dx,1)：SafeMulDiv 的
// 溢出判定只覆盖"高位非零且低位为零"的情形，对 2^64 量级的乘积会漏判；
// 而这里需要的检查很直白——|dx|、|dy| 各自不超过 sqrt(MaxInt64)，
// 且平方和不超过 MaxInt64，两行比较即可完全覆盖，比套一个语义不完全
// 匹配的通用函数更可靠。平方根本身用 domain.Isqrt（牛顿邻域修正，
// 避免 float64 在 2^53 附近的 ±1 偏差），再向上取整。
func SegmentLengthUnits(from, to domain.Pos) (int64, error) {
	dx := to.X - from.X
	if dx < 0 {
		dx = -dx
	}
	dy := to.Y - from.Y
	if dy < 0 {
		dy = -dy
	}
	if dx > maxInt64Sqrt {
		return 0, fmt.Errorf("%w: Δx=%d 平方溢出 int64", domain.ErrOverflow, dx)
	}
	if dy > maxInt64Sqrt {
		return 0, fmt.Errorf("%w: Δy=%d 平方溢出 int64", domain.ErrOverflow, dy)
	}
	sq := dx * dx
	if sq > math.MaxInt64-dy*dy {
		return 0, fmt.Errorf("%w: dx²+dy² 溢出 int64 (dx=%d, dy=%d)", domain.ErrOverflow, dx, dy)
	}
	sum := sq + dy*dy
	r, err := domain.Isqrt(sum)
	if err != nil {
		return 0, fmt.Errorf("段长平方根计算失败: %w", err)
	}
	// 向上取整：完全平方数不 +1，其余 +1。
	if r*r < sum {
		r++
	}
	return r, nil
}

// RaycastBlocked 用整数 Bresenham 直线遍历 from→to 之间的所有格，
// 返回第一个被阻挡格的左下角坐标（units）。
//
// 为什么用整数 Bresenham 而不是浮点采样（比如每隔 0.5 格取样判断
// blocked(x,y)）：
//   - 浮点采样步长是猜的：步长大了会漏格——一条斜线从两格之间的
//     缝隙"溜过去"，缝隙里的墙检不到，玩家穿墙；步长小了采样数
//     随距离线性膨胀，5000 格距离要采一万个点。
//   - 浮点坐标转格坐标靠 floor，在格边界附近 ±1 抖动，同一条线段
//     两次计算可能命中不同格集合，破坏"相同输入→相同结果"的幂等性。
//   - Bresenham 全程整数运算，给出的是 8-连通的格序列，恰好覆盖
//     "线段真正经过的格"的上界集合，不漏格、结果确定、无分配。
//
// 起始格与终止格都检测：墙可能就贴在起终点上（贴着主堡出兵、
// 目标格被资源点占），只测中间格会让这两种情况穿墙。
//
// blocked 回调在格坐标上判定阻挡（由调用方从静态地形/占位数据提供）。
// nil 回调视为无阻挡。from/to 的合法性由调用方保证（BuildMove 已用
// inBounds 校验）；本函数只要求坐标量级在世界范围内（≤5e6 units），
// 中间运算 err=dx-dy、e2=2*err 均以格为单位，远离 int64 上限。
func RaycastBlocked(from, to domain.Pos, blocked func(x, y int64) bool) (blockedAt domain.Pos, ok bool) {
	if blocked == nil {
		return domain.Pos{}, false
	}
	x0, y0 := posToCells(from.X, from.Y)
	x1, y1 := posToCells(to.X, to.Y)

	var hit bool
	walkBresenham(x0, y0, x1, y1, func(x, y int64) bool {
		if blocked(x, y) {
			blockedAt = domain.Pos{X: x * domain.DefaultPositionUnitsPerCell, Y: y * domain.DefaultPositionUnitsPerCell}
			hit = true
			return false // 找到即停：第一个阻挡点就是回执需要的落点
		}
		return true
	})
	return blockedAt, hit
}

// walkBresenham 是标准整数 Bresenham：从 (x0,y0) 到 (x1,y1) 逐格调用
// visit，visit 返回 false 时提前结束。
//
// 算法要点：err=dx-dy 是"还欠多少 y"的误差项，每步先在 x/y 中选
// 误差更亏的一个前进，e2=2*err 与 -dy、dx 比较决定是否修正另一轴。
// 对任意符号的坐标都正确（dx/dy 取绝对值，sx/sy 记方向），因此
// from/to 谁大谁小无所谓，射线是双向对称的。
//
// visit 的调用顺序包含起点与终点：先 visit 当前格，再判断是否到达，
// 所以 (x0,y0) 与 (x1,y1) 都会被执行到——RaycastBlocked 依赖这一点
// 检测贴脸的墙。
func walkBresenham(x0, y0, x1, y1 int64, visit func(x, y int64) bool) {
	dx := x1 - x0
	if dx < 0 {
		dx = -dx
	}
	dy := y1 - y0
	if dy < 0 {
		dy = -dy
	}
	var sx, sy int64 = 1, 1
	if x0 > x1 {
		sx = -1
	}
	if y0 > y1 {
		sy = -1
	}
	err := dx - dy
	for {
		if !visit(x0, y0) {
			return
		}
		if x0 == x1 && y0 == y1 {
			return
		}
		e2 := 2 * err
		if e2 > -dy {
			err -= dy
			x0 += sx
		}
		if e2 < dx {
			err += dx
			y0 += sy
		}
	}
}

// posToCells 把定点坐标换算为格坐标（向下取整）。
//
// 为什么不用 a/b 直接除：Go 整数除法向零取整，负坐标会归到错误的格
// （-1 units 应属于第 -1 格，向零除得 0）。合法调用不会传负坐标，
// 但射线反转、调试工具、未来跨边界补算都可能出现，三行 floor 逻辑
// 买一个"坐标→格"的单一真相，避免每个调用方各自踩坑。
func posToCells(x, y int64) (cx, cy int64) {
	return floorDivInt64(x, domain.DefaultPositionUnitsPerCell), floorDivInt64(y, domain.DefaultPositionUnitsPerCell)
}

// floorDivInt64 返回 floor(a/b)（b 为正）。
func floorDivInt64(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// AdvanceAlong 薄封装 domain.MoveSegment.PositionAt：取 move 在 nowMS
// 时刻的权威位置。
//
// 存在意义：把"位置推导"收敛到 spatial 包内，调用方（SceneLoop tick、
// 到达仲裁）不需要记住该方法在指针接收者上、也不需要各自处理
// nil segment——nil 是"该部队没有移动段"的合法状态，返回
// ErrInvalidArgument 让调用方按业务分支处理，而不是 panic。
func AdvanceAlong(m *domain.MoveSegment, nowMS int64) (domain.Pos, error) {
	if m == nil {
		return domain.Pos{}, fmt.Errorf("%w: MoveSegment 为 nil", domain.ErrInvalidArgument)
	}
	return m.PositionAt(nowMS)
}
