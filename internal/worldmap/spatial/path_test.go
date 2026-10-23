package spatial

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

// TestSegmentLengthUnits 覆盖文档 9.2 的段长公式：
// ceil(sqrt(dx²+dy²))，方向无关，完全平方数不 +1，溢出提前拒绝。
func TestSegmentLengthUnits(t *testing.T) {
	tests := []struct {
		name    string
		from    domain.Pos
		to      domain.Pos
		want    int64
		wantErr bool
	}{
		{"零长度", domain.Pos{}, domain.Pos{}, 0, false},
		{"345勾股", domain.Pos{}, domain.Pos{X: 3000, Y: 4000}, 5000, false},
		{"ceil:sqrt(2e6)=1414.21→1415", domain.Pos{}, domain.Pos{X: 1000, Y: 1000}, 1415, false},
		{"反向对称", domain.Pos{X: 3000, Y: 4000}, domain.Pos{}, 5000, false},
		{"负方向单轴", domain.Pos{}, domain.Pos{X: 0, Y: -3000}, 3000, false},
		{"完全平方不+1", domain.Pos{}, domain.Pos{X: 3000, Y: 0}, 3000, false},
		{"完全平方+1", domain.Pos{}, domain.Pos{X: 3001, Y: 0}, 3001, false},
		{"Δx平方溢出", domain.Pos{X: -4_000_000_000, Y: 0}, domain.Pos{X: 4_000_000_000, Y: 0}, 0, true},
		{"dx²+dy²求和溢出", domain.Pos{}, domain.Pos{X: 3_037_000_000, Y: 3_037_000_000}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SegmentLengthUnits(tt.from, tt.to)
			if tt.wantErr {
				require.Error(t, err)
				require.False(t, !errors.Is(err, domain.ErrOverflow), fmt.Sprintf("错误应包装 ErrOverflow, got %v", err))
				return
			}
			require.NoError(t, err, "意外错误")
			require.EqualValues(t, tt.want, got)
		})
	}
}

// TestBuildMove 覆盖文档 9.1/9.2 的行军段构造：
// 时长 = ceil(len/speed)*1000ms、零长度段直接完成、零/负速度拒绝、
// 起终点越界拒绝、inBounds 谓词缺失拒绝。
func TestBuildMove(t *testing.T) {
	topo := testTopo()
	tests := []struct {
		name     string
		from     domain.Pos
		to       domain.Pos
		speed    int64
		startAt  int64
		inBounds func(domain.Pos) bool
		wantErr  bool
		wantDur  int64 // EndAtMS - StartAtMS
	}{
		{
			name: "正常:3-4-5段", from: domain.Pos{}, to: domain.Pos{X: 3000, Y: 4000},
			speed: 1000, startAt: 10_000, inBounds: topo.InBounds, wantDur: 5000,
		},
		{
			name: "时长向上取整:len=1415 speed=1000→2s", from: domain.Pos{}, to: domain.Pos{X: 1000, Y: 1000},
			speed: 1000, startAt: 0, inBounds: topo.InBounds, wantDur: 2000,
		},
		{
			name: "零长度段直接完成", from: domain.Pos{X: 1000, Y: 2000}, to: domain.Pos{X: 1000, Y: 2000},
			speed: 500, startAt: 5000, inBounds: topo.InBounds, wantDur: 0,
		},
		{
			name: "零速度拒绝", from: domain.Pos{}, to: domain.Pos{X: 3000, Y: 0},
			speed: 0, startAt: 0, inBounds: topo.InBounds, wantErr: true,
		},
		{
			name: "负速度拒绝", from: domain.Pos{}, to: domain.Pos{X: 3000, Y: 0},
			speed: -100, startAt: 0, inBounds: topo.InBounds, wantErr: true,
		},
		{
			name: "起点越界拒绝", from: domain.Pos{X: -1, Y: 0}, to: domain.Pos{X: 3000, Y: 0},
			speed: 1000, startAt: 0, inBounds: topo.InBounds, wantErr: true,
		},
		{
			name: "终点越界拒绝(右上边界外)", from: domain.Pos{}, to: domain.Pos{X: 5_000_000, Y: 0},
			speed: 1000, startAt: 0, inBounds: topo.InBounds, wantErr: true,
		},
		{
			name: "边界坐标合法", from: domain.Pos{}, to: domain.Pos{X: 4_999_999, Y: 4_999_999},
			speed: 1000, startAt: 0, inBounds: topo.InBounds, wantDur: 7_072_000, // len=ceil(sqrt(2)*4999999)=7071068 units → 7072s
		},
		{
			name: "inBounds为nil拒绝", from: domain.Pos{}, to: domain.Pos{X: 1000, Y: 0},
			speed: 1000, startAt: 0, inBounds: nil, wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := BuildMove(tt.from, tt.to, tt.speed, tt.startAt, "move-1", tt.inBounds)
			if tt.wantErr {
				require.Error(t, err)
				require.False(t, !errors.Is(err, domain.ErrInvalidArgument), fmt.Sprintf("错误应包装 ErrInvalidArgument, got %v", err))
				return
			}
			require.NoError(t, err, "意外错误")
			if m.MoveID != "move-1" || m.From != tt.from || m.To != tt.to ||
				m.StartAtMS != tt.startAt || m.SpeedUnitsPerSec != tt.speed {
				t.Fatalf("字段回填错误: %+v", m)
			}
			got := m.EndAtMS - m.StartAtMS
			require.EqualValues(t, tt.wantDur, got, fmt.Sprintf("duration = %d, want %d (EndAtMS=%d)", got, tt.wantDur, m.EndAtMS))
			require.Nil(t, m.Path, fmt.Sprintf("第一版直线路径 Path 应为空、SegmentIndex 为 0: %+v", m))
			require.EqualValues(t, 0, m.SegmentIndex)
			// 零长度段：PositionAt 直接给终点。
			if tt.wantDur == 0 {
				pos, err := m.PositionAt(tt.startAt)
				require.NoError(t, err, fmt.Sprintf("零长度段 PositionAt = %v,%v want %v,nil", pos, err, tt.to))
				require.EqualValues(t, tt.to, pos)
			}
		})
	}
}

// TestAdvanceAlongTrajectory 覆盖位置推导（文档 9.2）：
// elapsed 夹紧、绝对时间重算一致、nil 段报错。
func TestAdvanceAlongTrajectory(t *testing.T) {
	topo := testTopo()
	m, err := BuildMove(domain.Pos{}, domain.Pos{X: 3000, Y: 4000}, 1000, 10_000, "m1", topo.InBounds)
	require.NoError(t, err, "BuildMove")
	// len=5000 units, speed=1000 → 5s → [10000,15000]。
	tests := []struct {
		name string
		now  int64
		want domain.Pos
	}{
		{"起点前(夹紧到From)", 0, domain.Pos{}},
		{"起点时刻", 10_000, domain.Pos{}},
		{"中点", 12_500, domain.Pos{X: 1500, Y: 2000}},
		{"终点时刻", 15_000, domain.Pos{X: 3000, Y: 4000}},
		{"终点后(夹紧到To)", 99_999, domain.Pos{X: 3000, Y: 4000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AdvanceAlong(m, tt.now)
			require.NoError(t, err, "AdvanceAlong 错误")
			require.EqualValues(t, tt.want, got)
		})
	}
	// 绝对时间重算一致性：任意时刻两次调用结果相同（不累加误差）。
	p1, _ := AdvanceAlong(m, 12_345)
	p2, _ := AdvanceAlong(m, 12_345)
	require.EqualValues(t, p2, p1)
	// nil 段：报错而不是 panic。
	_, err = AdvanceAlong(nil, 100)
	require.False(t, err == nil || !errors.Is(err, domain.ErrInvalidArgument), fmt.Sprintf("nil 段应返回 ErrInvalidArgument, got %v", err))
}

// TestRaycastBlocked 覆盖 Bresenham 不漏格：45 度、近水平、近垂直、
// 单格、跨场景边界、反向对称；起始格与终止格都检测。
func TestRaycastBlocked(t *testing.T) {
	// blockAt 构造"只有指定格被挡"的回调。
	blockAt := func(cx, cy int64) func(x, y int64) bool {
		return func(x, y int64) bool { return x == cx && y == cy }
	}
	tests := []struct {
		name    string
		from    domain.Pos
		to      domain.Pos
		blocked func(x, y int64) bool
		wantOK  bool
		wantAt  domain.Pos
	}{
		{
			name: "无阻挡", from: domain.Pos{}, to: domain.Pos{X: 10_000, Y: 10_000},
			blocked: func(int64, int64) bool { return false }, wantOK: false,
		},
		{
			name: "45度:中间格(5,5)被挡", from: domain.Pos{}, to: domain.Pos{X: 10_000, Y: 10_000},
			blocked: blockAt(5, 5), wantOK: true, wantAt: domain.Pos{X: 5000, Y: 5000},
		},
		{
			name: "起始格被挡", from: domain.Pos{X: 1000, Y: 1000}, to: domain.Pos{X: 5000, Y: 5000},
			blocked: blockAt(1, 1), wantOK: true, wantAt: domain.Pos{X: 1000, Y: 1000},
		},
		{
			name: "终止格被挡", from: domain.Pos{X: 1000, Y: 1000}, to: domain.Pos{X: 5000, Y: 5000},
			blocked: blockAt(5, 5), wantOK: true, wantAt: domain.Pos{X: 5000, Y: 5000},
		},
		{
			// 格坐标 (0,0)→(20,2)：2500 units = 2.5 格 → 第 2 行。
			name: "近水平:格(15,1)被挡", from: domain.Pos{}, to: domain.Pos{X: 20_000, Y: 2500},
			blocked: blockAt(15, 1), wantOK: true, wantAt: domain.Pos{X: 15_000, Y: 1000},
		},
		{
			name: "近水平:格(10,2)不在线上不误报", from: domain.Pos{}, to: domain.Pos{X: 20_000, Y: 2500},
			blocked: blockAt(10, 2), wantOK: false,
		},
		{
			name: "近垂直:格(1,10)被挡", from: domain.Pos{}, to: domain.Pos{X: 2000, Y: 20_000},
			blocked: blockAt(1, 10), wantOK: true, wantAt: domain.Pos{X: 1000, Y: 10_000},
		},
		{
			name: "单格:起点格被挡", from: domain.Pos{}, to: domain.Pos{X: 500, Y: 500},
			blocked: blockAt(0, 0), wantOK: true, wantAt: domain.Pos{},
		},
		{
			name: "单格:邻格不误报", from: domain.Pos{}, to: domain.Pos{X: 500, Y: 500},
			blocked: blockAt(1, 0), wantOK: false,
		},
		{
			name: "跨场景边界:左格(999,0)被挡", from: domain.Pos{X: 999_500, Y: 0}, to: domain.Pos{X: 1_000_500, Y: 0},
			blocked: blockAt(999, 0), wantOK: true, wantAt: domain.Pos{X: 999_000, Y: 0},
		},
		{
			name: "跨场景边界:右格(1000,0)被挡", from: domain.Pos{X: 999_500, Y: 0}, to: domain.Pos{X: 1_000_500, Y: 0},
			blocked: blockAt(1000, 0), wantOK: true, wantAt: domain.Pos{X: 1_000_000, Y: 0},
		},
		{
			name: "跨场景边界:不邻格(998,0)不误报", from: domain.Pos{X: 999_500, Y: 0}, to: domain.Pos{X: 1_000_500, Y: 0},
			blocked: blockAt(998, 0), wantOK: false,
		},
		{
			name: "反向对称:(5,5)被挡", from: domain.Pos{X: 10_000, Y: 10_000}, to: domain.Pos{},
			blocked: blockAt(5, 5), wantOK: true, wantAt: domain.Pos{X: 5000, Y: 5000},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			at, ok := RaycastBlocked(tt.from, tt.to, tt.blocked)
			require.EqualValues(t, tt.wantOK, ok)
			require.False(t, ok && at != tt.wantAt, fmt.Sprintf("blockedAt = %v, want %v", at, tt.wantAt))
			require.False(t, !ok && at != (domain.Pos{}), fmt.Sprintf("未命中时应返回零值, got %v", at))
		})
	}
}

// walkCell 记录一次射线遍历到的格（测试本地类型）。
type walkCell struct{ x, y int64 }

// recordWalker 返回"记录全部遍历格且永不命中"的回调。
func recordWalker(visited *[]walkCell) func(x, y int64) bool {
	return func(x, y int64) bool {
		*visited = append(*visited, walkCell{x, y})
		return false
	}
}

// TestBresenhamWalksEveryCell 直接断言遍历序列：用"永不命中"的回调
// 收集 Bresenham 实际走过的每一格，与数学期望逐格比对。
// 这是"不漏格"的最强形式——不是抽检几个格，而是全集比对。
func TestBresenhamWalksEveryCell(t *testing.T) {
	t.Run("45度对角线", func(t *testing.T) {
		var got []walkCell
		_, ok := RaycastBlocked(domain.Pos{}, domain.Pos{X: 10_000, Y: 10_000}, recordWalker(&got))
		require.False(t, ok, "永假回调不应命中")
		want := make([]walkCell, 0, 11)
		for i := int64(0); i <= 10; i++ {
			want = append(want, walkCell{i, i})
		}
		require.False(t, !reflect.DeepEqual(got, want), fmt.Sprintf("遍历序列 = %v, want %v", got, want))
	})

	t.Run("近水平(0,0)→(20,2):每格都在", func(t *testing.T) {
		var got []walkCell
		// 2500 units = 2.5 格 → 终点格 (20,2)。
		RaycastBlocked(domain.Pos{}, domain.Pos{X: 20_000, Y: 2500}, recordWalker(&got))
		// 直线 y=x/10 的最近格行：x∈[0,6) 时 y<0.6 → 第 0 行；
		// x∈[6,16) 时 y∈[0.6,1.6) → 离第 1 行更近 → 第 1 行；
		// x∈[16,20] 时 y≥1.6 → 第 2 行。三行都不得漏。
		var want []walkCell
		for x := int64(0); x <= 5; x++ {
			want = append(want, walkCell{x, 0})
		}
		for x := int64(6); x <= 15; x++ {
			want = append(want, walkCell{x, 1})
		}
		for x := int64(16); x <= 20; x++ {
			want = append(want, walkCell{x, 2})
		}
		require.False(t, !reflect.DeepEqual(got, want), fmt.Sprintf("遍历序列 = %v, want %v", got, want))
	})

	t.Run("近垂直(0,0)→(2,20):每格都在", func(t *testing.T) {
		var got []walkCell
		RaycastBlocked(domain.Pos{}, domain.Pos{X: 2000, Y: 20_000}, recordWalker(&got))
		// 直线 x=y/10：y∈[0,6) 在第 0 列，y∈[6,16) 在第 1 列，
		// y∈[16,20] 在第 2 列。
		var want []walkCell
		for y := int64(0); y <= 5; y++ {
			want = append(want, walkCell{0, y})
		}
		for y := int64(6); y <= 15; y++ {
			want = append(want, walkCell{1, y})
		}
		for y := int64(16); y <= 20; y++ {
			want = append(want, walkCell{2, y})
		}
		require.False(t, !reflect.DeepEqual(got, want), fmt.Sprintf("遍历序列 = %v, want %v", got, want))
	})

	t.Run("单格只走一格", func(t *testing.T) {
		var got []walkCell
		RaycastBlocked(domain.Pos{X: 100, Y: 100}, domain.Pos{X: 900, Y: 900}, recordWalker(&got))
		want := []walkCell{{0, 0}}
		require.False(t, !reflect.DeepEqual(got, want), fmt.Sprintf("遍历序列 = %v, want %v", got, want))
	})
}

// TestRaycastBlockedNilCallback 覆盖 nil 阻挡源：视为无阻挡，
// 不 panic（调用方可能还没加载静态地形数据）。
func TestRaycastBlockedNilCallback(t *testing.T) {
	at, ok := RaycastBlocked(domain.Pos{}, domain.Pos{X: 10_000, Y: 10_000}, nil)
	require.False(t, ok || at != (domain.Pos{}), fmt.Sprintf("nil blocked 应返回 (零值,false), got %v,%v", at, ok))
}
