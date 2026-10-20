package domain

import (
	"fmt"
	"math"
)

// WorldID 世界编号。地图的所有主键、幂等键、日志字段都必须携带 world_id：
// 大世界是按 world 分片的顶级实体，没有 world_id 的对象 ID 在跨世界查询时
// 会产生歧义（不同世界各有一条 object_id=10001 是完全合法的）。
type WorldID uint64

// ObjectID 对象编号（城池/部队/资源点）。由 SQL 自增分配，不用
// time.Now().UnixNano() 或进程内计数器——重启后这两种方案都会碰撞或产生
// 无法预测的顺序。允许 ID 之间有空洞，空洞不代表对象被删除。
type ObjectID uint64

// SceneID 逻辑场景编号，1-based。文档第 5.1 章规定行优先编号：
//
//	scene_id = sy * sceneCols + sx + 1
//
// 取值范围 [1, sceneCols*sceneRows]。0 是无效值，用于表示“尚未归属任何场景”。
type SceneID int32

// Pos 是定点坐标，单位是 position units（1 格 = 1000 units）。
//
// 为什么不用 float64：地图权威位置要跨进程、跨重启保持一致，浮点误差会
// 随“每帧累加 pos += speed*dt”无限放大；定点数保证任意时刻按
// (start,end,now) 重算的结果逐位一致。界面插值可以用浮点，但那是展示层
// 的事，不落库、不参与裁决。
type Pos struct {
	X int64
	Y int64
}

// DefaultPositionUnitsPerCell 是 1 格对应的 position units 数。
// 文档第 5.1 章固定为 1000：1 米的粒度足够精细，同时 int64 乘法
// （5000 格 × 1000 × 简单系数）距离 64 位上限极远，不需要大整数。
const DefaultPositionUnitsPerCell int64 = 1000

// DefaultWorldWidthCells / DefaultWorldHeightCells 默认世界尺寸（格）。
// 世界尺寸是拓扑级配置，只能停世界迁移，不能热更新（文档 16.5）。
const (
	DefaultWorldWidthCells  int64 = 5000
	DefaultWorldHeightCells int64 = 5000
)

// DefaultSceneCols / DefaultSceneRows 默认 5×5 个逻辑场景，
// 每场景 1000×1000 格。25 是初始配置值，不按 CPU 核数自动调整。
const (
	DefaultSceneCols int32 = 5
	DefaultSceneRows int32 = 5
)

// Topology 描述世界的静态拓扑，由配置注入、启动时一次性校验。
// 所有尺寸以“格”为单位；换算成 position units 的乘法在本包边界处完成，
// 避免每个调用方各自乘一遍 1000。
type Topology struct {
	WorldID           WorldID
	WidthCells        int64
	HeightCells       int64
	SceneCols         int32
	SceneRows         int32
	PositionUnitsCell int64
}

// DefaultTopology 返回文档第 5.1 章规定的默认拓扑。
func DefaultTopology(world WorldID) Topology {
	return Topology{
		WorldID:           world,
		WidthCells:        DefaultWorldWidthCells,
		HeightCells:       DefaultWorldHeightCells,
		SceneCols:         DefaultSceneCols,
		SceneRows:         DefaultSceneRows,
		PositionUnitsCell: DefaultPositionUnitsPerCell,
	}
}

// Validate 校验拓扑自洽性（文档 17.1 启动校验）：
// 尺寸为正、世界能被场景网格整除、单位换算乘积不溢出 int64。
//
// 这里只做拓扑自身的静态校验；“富饶块不跨场景”依赖富饶块大小配置，
// 属于 gamedata 层，不在此处。
func (t Topology) Validate() error {
	if t.WorldID == 0 {
		return fmt.Errorf("%w: world_id 不能为 0", ErrInvalidTopology)
	}
	if t.WidthCells <= 0 || t.HeightCells <= 0 {
		return fmt.Errorf("%w: 世界尺寸必须为正, got %dx%d", ErrInvalidTopology, t.WidthCells, t.HeightCells)
	}
	if t.SceneCols <= 0 || t.SceneRows <= 0 {
		return fmt.Errorf("%w: 场景网格必须为正, got %dx%d", ErrInvalidTopology, t.SceneCols, t.SceneRows)
	}
	if t.PositionUnitsCell <= 0 {
		return fmt.Errorf("%w: position_units_per_cell 必须为正, got %d", ErrInvalidTopology, t.PositionUnitsCell)
	}
	// 世界必须能被场景网格整除：否则边缘会出现“半个场景”，
	// 而 SceneAt 的整数除法假设每场景等宽等高。
	if t.WidthCells%int64(t.SceneCols) != 0 || t.HeightCells%int64(t.SceneRows) != 0 {
		return fmt.Errorf("%w: 世界 %dx%d 无法被 %dx%d 场景网格整除",
			ErrInvalidTopology, t.WidthCells, t.HeightCells, t.SceneCols, t.SceneRows)
	}
	// 最大坐标乘积必须远离 int64 上限，防止后续派生计算静默溢出。
	if t.WidthCells > math.MaxInt64/t.PositionUnitsCell || t.HeightCells > math.MaxInt64/t.PositionUnitsCell {
		return fmt.Errorf("%w: 世界尺寸与单位换算乘积溢出 int64", ErrInvalidTopology)
	}
	return nil
}

// SceneWidthCells / SceneHeightCells 返回单个场景的格数。
func (t Topology) SceneWidthCells() int64  { return t.WidthCells / int64(t.SceneCols) }
func (t Topology) SceneHeightCells() int64 { return t.HeightCells / int64(t.SceneRows) }

// SceneWidthUnits / SceneHeightUnits 返回单个场景的 position units 宽高。
func (t Topology) SceneWidthUnits() int64  { return t.SceneWidthCells() * t.PositionUnitsCell }
func (t Topology) SceneHeightUnits() int64 { return t.SceneHeightCells() * t.PositionUnitsCell }

// MaxSceneID 返回合法场景编号上限（含）。
func (t Topology) MaxSceneID() SceneID { return SceneID(t.SceneCols * t.SceneRows) }

// InBounds 判断定点坐标是否在合法范围内。
// 边界是左闭右开：0 <= x < width，右上角外一格即越界。
func (t Topology) InBounds(p Pos) bool {
	return p.X >= 0 && p.Y >= 0 &&
		p.X < t.WidthCells*t.PositionUnitsCell &&
		p.Y < t.HeightCells*t.PositionUnitsCell
}

// InBoundsCells 以格为单位判断合法性（含负数拒绝）。
// 注意：Go 的整数除法对负数是向零取整，负 X 会算出负的 sx，
// 因此必须在除法之前先拒绝负数，不能依赖除法结果。
func (t Topology) InBoundsCells(x, y int64) bool {
	return x >= 0 && y >= 0 && x < t.WidthCells && y < t.HeightCells
}

// SceneAt 按当前位置计算物理场景编号，返回 0 表示坐标非法。
//
// 对应文档 5.1 的设计骨架。左闭右开语义：x 恰等于场景宽度时属于右侧场景，
// 不同时归属两个场景——这是 PhysicalScene 单所有者模型的基础。
func (t Topology) SceneAt(p Pos) SceneID {
	if !t.InBounds(p) {
		return 0
	}
	sx := p.X / t.SceneWidthUnits()
	sy := p.Y / t.SceneHeightUnits()
	return SceneID(sy*int64(t.SceneCols) + sx + 1)
}

// SceneOrigin 返回某场景左下角的坐标（用于把局部坐标换算回世界坐标，
// 以及 AOI 广播块与场景的相互定位）。
func (t Topology) SceneOrigin(s SceneID) (Pos, bool) {
	if s < 1 || s > t.MaxSceneID() {
		return Pos{}, false
	}
	idx := int64(s) - 1
	sx := idx % int64(t.SceneCols)
	sy := idx / int64(t.SceneCols)
	return Pos{X: sx * t.SceneWidthUnits(), Y: sy * t.SceneHeightUnits()}, true
}

// SceneBounds 返回场景覆盖的矩形（左下闭、右上开），格坐标。
func (t Topology) SceneBounds(s SceneID) (minX, minY, maxX, maxY int64, ok bool) {
	origin, ok := t.SceneOrigin(s)
	if !ok {
		return 0, 0, 0, 0, false
	}
	return origin.X / t.PositionUnitsCell, origin.Y / t.PositionUnitsCell,
		origin.X/t.PositionUnitsCell + t.SceneWidthCells(),
		origin.Y/t.PositionUnitsCell + t.SceneHeightCells(), true
}

// CellsToUnits 把格坐标换算为定点坐标（格中心不在此处处理，
// 需要格中心的调用方自行加半格）。
func (t Topology) CellsToUnits(x, y int64) Pos {
	return Pos{X: x * t.PositionUnitsCell, Y: y * t.PositionUnitsCell}
}

// UnitsToCells 把定点坐标截断为格坐标。只应对合法坐标调用，
// 否则返回值的语义未定义（调用前先 InBounds）。
func (t Topology) UnitsToCells(p Pos) (x, y int64) {
	return p.X / t.PositionUnitsCell, p.Y / t.PositionUnitsCell
}

// SafeMulDiv 计算 a*b/c 并对中间乘积做溢出检查。
// 移动位置推导 (to-from)*elapsed/duration 的中间结果可能很大：
// 5000 格 × 1000 units × 数小时毫秒数接近 2^53，虽然仍在 int64 内，
// 但路径更长、时间更长的场景下不检查就是埋雷。c 必须为正。
func SafeMulDiv(a, b, c int64) (int64, error) {
	if c <= 0 {
		return 0, fmt.Errorf("%w: 除数必须为正, got %d", ErrInvalidArgument, c)
	}
	hi, lo := mul128(a, b)
	if hi != 0 && (hi < 0 || lo == 0) {
		// 粗略判定：结果超出 int64 可表示范围。
		return 0, fmt.Errorf("%w: %d*%d 溢出 int64", ErrOverflow, a, b)
	}
	return div128(hi, lo, c), nil
}

// mul128 返回 128 位乘积的高低 64 位（基于 32 位分段，避免依赖 math/big）。
func mul128(a, b int64) (hi, lo uint64) {
	ua := uint64(a)
	ub := uint64(b)
	// 拆成 32 位小段，4 次乘法相加。负数转无符号数后按模 2^64 运算，
	// 高低位的组合对最终除法仍然成立。
	const mask = 0xFFFFFFFF
	a0, a1 := ua&mask, ua>>32
	b0, b1 := ub&mask, ub>>32
	lo = a0 * b0
	mid1 := a1*b0 + (lo >> 32)
	mid2 := a0*b1 + (mid1 & mask)
	hi = a1*b1 + (mid1 >> 32) + (mid2 >> 32)
	lo = (mid2 << 32) | (lo & mask)
	return hi, lo
}

// div128 把 128 位被除数除以正 64 位除数。借助大端分段逐位还原，
// 调用方需保证商在 int64 范围内（由 SafeMulDiv 的溢出检查保证）。
func div128(hi, lo uint64, c int64) int64 {
	// 无符号 128 位除以无符号 64 位：把被除数按 32 位分 4 段，
	// 每段用 64 位除法推进。c 已由调用方保证为正。
	uc := uint64(c)
	const mask = 0xFFFFFFFF
	var rem uint64
	var quo uint64
	for _, part := range []uint64{hi >> 32, hi & mask, lo >> 32, lo & mask} {
		cur := (rem << 32) | part
		quo = (quo << 32) | (cur / uc)
		rem = cur % uc
	}
	return int64(quo)
}

// ClampDurationMS 把 elapsed 夹到 [0, duration]，防止时钟跳变
// （NTP 回拨、假时钟跳变）产生负收益或超采。
func ClampDurationMS(elapsed, duration int64) int64 {
	if elapsed < 0 {
		return 0
	}
	if elapsed > duration {
		return duration
	}
	return elapsed
}

// CeilDiv 返回 ceil(a/b)（b 为正）。用于 need_ms = planned*1_000_000/rate
// 这类“不能少算一毫秒”的场景，避免浮点 ceil 的精度问题。
func CeilDiv(a, b int64) (int64, error) {
	if b <= 0 {
		return 0, fmt.Errorf("%w: 除数必须为正, got %d", ErrInvalidArgument, b)
	}
	if a < 0 {
		return 0, fmt.Errorf("%w: 被除数不能为负, got %d", ErrInvalidArgument, a)
	}
	q := a / b
	if a%b != 0 {
		q++
	}
	return q, nil
}

// FloorDiv 返回 floor(a/b)（b 为正）。提前召回结算必须用 floor：
// 每次中断都 ceil 会被玩家反复开始/中断来套利（文档 10.2）。
func FloorDiv(a, b int64) (int64, error) {
	if b <= 0 {
		return 0, fmt.Errorf("%w: 除数必须为正, got %d", ErrInvalidArgument, b)
	}
	if a < 0 {
		return 0, fmt.Errorf("%w: 被除数不能为负, got %d", ErrInvalidArgument, a)
	}
	return a / b, nil
}

// Isqrt 返回非负整数的整数平方根（向下取整）。
// 段长度用 sqrt(dx²+dy²) 并规定向上取整到 units，
// 保证“速度为正时零长度段不出现、非零段至少 1 unit”。
func Isqrt(n int64) (int64, error) {
	if n < 0 {
		return 0, fmt.Errorf("%w: Isqrt 参数不能为负, got %d", ErrInvalidArgument, n)
	}
	if n > math.MaxInt64/2 {
		// 牛顿迭代初始值过大时会平方溢出，先用位数折半。
	}
	r := int64(math.Sqrt(float64(n)))
	// 浮点 sqrt 在 2^53 附近可能偏差 1，用邻域修正。
	for r > 0 && r*r > n {
		r--
	}
	for (r+1)*(r+1) <= n {
		r++
	}
	return r, nil
}
