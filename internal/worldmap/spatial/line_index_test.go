package spatial

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

// TestLineIndexUpsertRemove 覆盖行军线的基本生命周期：建立/替换/删除、
// 非法入参拒绝、点集深拷贝（防切片别名）。
func TestLineIndexUpsertRemove(t *testing.T) {
	li := NewLineIndex()
	line1 := []domain.Pos{{X: 0, Y: 0}, {X: 1000, Y: 0}}
	line2 := []domain.Pos{{X: 0, Y: 500}, {X: 0, Y: 1500}}
	err := li.Upsert("m1", line1)
	require.NoError(t, err, fmt.Sprintf("Upsert m1: %v", err))
	err = li.Upsert("m2", line2)
	require.NoError(t, err, fmt.Sprintf("Upsert m2: %v", err))
	require.EqualValues(t, 2, li.Len())

	// 同 moveID 替换（改速后重画线）。
	line1b := []domain.Pos{{X: 0, Y: 0}, {X: 2000, Y: 2000}}
	err = li.Upsert("m1", line1b)
	require.NoError(t, err, fmt.Sprintf("Upsert 替换: %v", err))
	require.EqualValues(t, 2, li.Len())
	// 替换生效：旧线段不可见、新线段可见。
	got := li.QueryViewport(900, -10, 1100, 10)
	require.Empty(t, got, fmt.Sprintf("旧线段应已被替换, got %v", got))
	got = li.QueryViewport(900, 900, 1100, 1100)
	require.Len(t, got, 1, fmt.Sprintf("新线段应可见, got %v", got))
	require.EqualValues(t, "m1", got[0])

	// 深拷贝：调用方改底层数组不影响索引。
	// 若索引只存了切片别名，line2 会被改成 (0,500)-(9999,9999)，
	// 下面这个贴住原端点 (0,1500) 的视野就再也看不见它。
	line2[1] = domain.Pos{X: 9999, Y: 9999}
	got = li.QueryViewport(-10, 1400, 10, 1600)
	require.Len(t, got, 1, fmt.Sprintf("索引应持有自有拷贝, got %v", got))
	require.EqualValues(t, "m2", got[0])

	// Remove：幂等。
	li.Remove("m1")
	li.Remove("m1")
	require.EqualValues(t, 1, li.Len())
	got = li.QueryViewport(0, 0, 10_000, 10_000)
	require.Len(t, got, 1, fmt.Sprintf("删除后不应再可见, got %v", got))
	require.EqualValues(t, "m2", got[0])

	// 非法入参。
	err = li.Upsert("", line1)
	require.False(t, !errors.Is(err, domain.ErrInvalidArgument), fmt.Sprintf("空 move_id 应拒绝, got %v", err))
	err = li.Upsert("m3", []domain.Pos{{X: 1, Y: 1}})
	require.False(t, !errors.Is(err, domain.ErrInvalidArgument), fmt.Sprintf("单点应拒绝, got %v", err))
	err = li.Upsert("m3", nil)
	require.False(t, !errors.Is(err, domain.ErrInvalidArgument), fmt.Sprintf("nil 点集应拒绝, got %v", err))
	require.EqualValues(t, 1, li.Len())

	// 零长度段 [p,p] 合法：包围盒即一个点，查询语义正确。
	err = li.Upsert("m4", []domain.Pos{{X: 500, Y: 500}, {X: 500, Y: 500}})
	require.NoError(t, err, fmt.Sprintf("零长度段应合法: %v", err))
	got = li.QueryViewport(499, 499, 501, 501)
	require.Len(t, got, 1, fmt.Sprintf("零长度段点应可见, got %v", got))
	require.EqualValues(t, "m4", got[0])
}

// TestQueryViewportEndpointsOutsideButCrossing 覆盖文档 12.5/19.1 的
// 核心用例：起点和终点都在视野外，但路线穿过视野——必须可见。
// 只按端点做 AOI 会漏掉这种"斜穿家门"的敌军。
func TestQueryViewportEndpointsOutsideButCrossing(t *testing.T) {
	li := NewLineIndex()
	// 横穿：从视野左侧外到右侧外。
	err := li.Upsert("cross-h", []domain.Pos{{X: 0, Y: 5}, {X: 300, Y: 5}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	got := li.QueryViewport(100, 0, 110, 10)
	require.Len(t, got, 1, fmt.Sprintf("两端在视野外但横穿视野应可见, got %v", got))
	require.EqualValues(t, "cross-h", got[0])

	// 斜穿对角：从左上外到右下外，穿过视野中央。
	err = li.Upsert("cross-d", []domain.Pos{{X: 0, Y: 0}, {X: 200, Y: 200}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	got = li.QueryViewport(100, 100, 110, 110)
	require.Len(t, got, 1, fmt.Sprintf("斜穿视野应可见(cross-h 在 y=5, 不在此视野), got %v", got))
	require.EqualValues(t, "cross-d", got[0])

	// 多段折线：只有第 2 段穿过视野（第 1 段在远处）。
	err = li.Upsert("cross-poly", []domain.Pos{{X: 0, Y: 0}, {X: 1000, Y: 0}, {X: 1000, Y: 1000}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	got = li.QueryViewport(995, 500, 1005, 510)
	require.Len(t, got, 1, fmt.Sprintf("折线第2段穿视野应可见, got %v", got))
	require.EqualValues(t, "cross-poly", got[0])
}

// TestQueryViewportBboxCoarseFilter 覆盖包围盒粗筛：粗筛必须放款到
// 不漏（本例验证粗筛放行后由精判拒绝），同时完全不相交的线不得出现。
func TestQueryViewportBboxCoarseFilter(t *testing.T) {
	li := NewLineIndex()
	// L 形折线：包围盒 [0,1000]×[0,1000] 覆盖视野，但线段本身
	// （先竖后横）不碰视野 [500,510]×[500,510]——粗筛会放行它，
	// 精判必须拒绝。若精判也错放，说明相交判定过于宽松。
	err := li.Upsert("L", []domain.Pos{{X: 0, Y: 0}, {X: 0, Y: 1000}, {X: 1000, Y: 1000}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	got := li.QueryViewport(500, 500, 510, 510)
	require.Len(t, got, 0)

	// 完全在视野左侧：粗筛直接排除。
	err = li.Upsert("left", []domain.Pos{{X: 0, Y: 0}, {X: 10, Y: 10}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	got = li.QueryViewport(100, 0, 200, 10)
	require.Len(t, got, 0)

	// 粗筛不漏的正面验证：长线包围盒与视野重叠且线段确实穿过。
	err = li.Upsert("far", []domain.Pos{{X: 0, Y: 5}, {X: 5000, Y: 5}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	got = li.QueryViewport(4000, 0, 4010, 10)
	require.Len(t, got, 1, fmt.Sprintf("远处横穿线应可见, got %v", got))
	require.EqualValues(t, "far", got[0])

	// 空视野（零面积）返回空。
	got = li.QueryViewport(100, 100, 100, 100)
	require.Empty(t, got, fmt.Sprintf("零面积视野应返回空, got %v", got))
	got = li.QueryViewport(200, 100, 100, 100)
	require.Empty(t, got, fmt.Sprintf("反向视野应返回空, got %v", got))
}

// TestQueryViewportTouchingEdge 覆盖贴边用例：线段贴着视野边界时
// 按"含边界"可见（漏线比多显一帧危险）。
func TestQueryViewportTouchingEdge(t *testing.T) {
	li := NewLineIndex()
	// 终点恰在视野左边界 x=100。
	err := li.Upsert("touch-left", []domain.Pos{{X: 99, Y: 5}, {X: 100, Y: 5}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	// 差一格，终点 x=99 在视野外。
	err = li.Upsert("outside-left", []domain.Pos{{X: 98, Y: 5}, {X: 99, Y: 5}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	// 竖线贴视野上边 y=10。
	err = li.Upsert("touch-top", []domain.Pos{{X: 105, Y: 5}, {X: 105, Y: 10}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	// 竖线贴视野下边 y=0。
	err = li.Upsert("touch-bottom", []domain.Pos{{X: 105, Y: 0}, {X: 105, Y: 5}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))
	// 竖线贴视野右边 x=110。
	err = li.Upsert("touch-right", []domain.Pos{{X: 110, Y: 2}, {X: 110, Y: 8}})
	require.NoError(t, err, fmt.Sprintf("Upsert: %v", err))

	got := li.QueryViewport(100, 0, 110, 10)
	want := []string{"touch-bottom", "touch-left", "touch-right", "touch-top"}
	require.False(t, !reflect.DeepEqual(got, want), fmt.Sprintf("QueryViewport = %v, want %v", got, want))

	// 含边界语义：outside-left 终点 x=99 贴视野右边界 99 时可见。
	got = li.QueryViewport(0, 0, 99, 10)
	want = []string{"outside-left", "touch-left"}
	require.False(t, !reflect.DeepEqual(got, want), fmt.Sprintf("贴右边界应可见: got %v, want %v", got, want))
	// 视野右边界缩到 97：两条线都完全在右侧外，不可见。
	got = li.QueryViewport(0, 0, 97, 10)
	require.Empty(t, got, fmt.Sprintf("视野[0,97]不应看见 x>=98 的线, got %v", got))
}

// TestSegmentIntersectsRect 直接对精判函数做表驱动覆盖：端点在矩形内、
// 横穿四条边、贴角、退化单点、共线不重叠。
func TestSegmentIntersectsRect(t *testing.T) {
	rect := []int64{100, 100, 200, 200} // minX,minY,maxX,maxY
	tests := []struct {
		name string
		x0   int64
		y0   int64
		x1   int64
		y1   int64
		want bool
	}{
		{"端点在矩形内", 150, 150, 250, 150, true},
		{"起点在矩形内", 150, 150, 999, 999, true},
		{"终点在矩形内", -50, -50, 150, 150, true},
		{"横穿左边", 50, 150, 250, 150, true},
		{"横穿右边", 150, 50, 150, 250, true},
		{"横穿上边", 150, 250, 250, 50, true},
		{"横穿下边", 250, 250, 50, 50, true},
		{"斜穿对角", 50, 50, 250, 250, true},
		{"贴矩形角", 50, 50, 100, 100, true},
		{"完全在左下外", 0, 0, 50, 50, false},
		{"完全在右上外", 250, 250, 400, 400, false},
		{"y方向完全分离", 150, 300, 160, 400, false},
		{"x方向完全分离", 300, 150, 400, 160, false},
		{"共线不重叠(右边延长)", 300, 150, 400, 150, false},
		{"共线重叠(穿过)", 150, 150, 250, 150, true},
		{"退化单点在矩形内", 150, 150, 150, 150, true},
		{"退化单点在矩形外", 300, 300, 300, 300, false},
		{"退化单点贴边", 100, 100, 100, 100, true},
		{"近垂直长距离斜穿", 149, -1000, 151, 1000, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := segmentIntersectsRect(tt.x0, tt.y0, tt.x1, tt.y1, rect[0], rect[1], rect[2], rect[3])
			if got != tt.want {
				t.Fatalf("segmentIntersectsRect(%d,%d → %d,%d, rect=%v) = %v, want %v",
					tt.x0, tt.y0, tt.x1, tt.y1, rect, got, tt.want)
			}
		})
	}
}

// TestQueryViewportDeterministic 覆盖输出确定性：map 遍历随机，
// 返回值必须排序，重复调用结果一致。
func TestQueryViewportDeterministic(t *testing.T) {
	li := NewLineIndex()
	for _, id := range []string{"m3", "m1", "m2"} {
		err := li.Upsert(id, []domain.Pos{{X: 0, Y: 0}, {X: 500, Y: 500}})
		require.NoError(t, err, fmt.Sprintf("Upsert %s: %v", id, err))
	}
	want := []string{"m1", "m2", "m3"}
	for i := 0; i < 20; i++ { // 多次调用对抗 map 随机序
		got := li.QueryViewport(0, 0, 600, 600)
		require.False(t, !reflect.DeepEqual(got, want), fmt.Sprintf("第 %d 次 QueryViewport = %v, want %v", i, got, want))
	}
}
