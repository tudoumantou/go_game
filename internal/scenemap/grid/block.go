package grid

import (
	"fmt"

	"game_study/internal/scenemap"
)

// Blocker 是阻挡查询器：静态矩阵 + 动态阻挡两层。
//
// 静态阻挡来自场景生成器产出的地图矩阵（块值 1 = 不可走）；
// 动态阻挡来自怪物体积（占多格）与临时障碍。两者分离存储，
// 因为生命周期完全不同：静态只在地图加载时写，动态随怪物生死增删。
//
// 并发契约：**只允许 MGR 的 goroutine 写**。AOI 不需要查阻挡
// （它只管视野），玩家 Actor 的移动校验读的是 MGR 发布的只读快照。
// 因此本结构内部不加锁——加了反而掩盖"谁在写"的设计问题。
type Blocker struct {
	size Size
	// static 一维展开的矩阵，1=不可走。用 []byte 而不是 [][]byte：
	// 一次分配、缓存友好，且避免了"每行长度不一致"的隐式错误。
	static []byte
	// dynamic 动态阻挡计数。用计数而不是布尔：两只怪的体积可能重叠，
	// 一只死了不能把另一只的阻挡也清掉。
	dynamic map[scenemap.GridPos]int
}

// NewBlocker 创建阻挡器。matrix 长度必须等于 width*height。
func NewBlocker(size Size, matrix []byte) (*Blocker, error) {
	if !size.Valid() {
		return nil, fmt.Errorf("%w: 地图尺寸非法 %dx%d", scenemap.ErrInvalidArgument, size.Width, size.Height)
	}
	want := int(size.Width) * int(size.Height)
	if len(matrix) != want {
		return nil, fmt.Errorf("%w: 阻挡矩阵长度 %d != %dx%d=%d",
			scenemap.ErrInvalidArgument, len(matrix), size.Width, size.Height, want)
	}
	// 复制一份：调用方的切片可能复用，地图数据被改是极难查的 bug
	cp := make([]byte, len(matrix))
	copy(cp, matrix)
	return &Blocker{size: size, static: cp, dynamic: make(map[scenemap.GridPos]int)}, nil
}

// NewBlockerFromRows 用字符画初始化："#"=阻挡，"."=可走，其他字符按可走处理。
// 只用于测试与示例，生产用 NewBlocker 喂真实矩阵。
func NewBlockerFromRows(rows []string) (*Blocker, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("%w: 空地图", scenemap.ErrInvalidArgument)
	}
	h := int32(len(rows))
	w := int32(len(rows[0]))
	matrix := make([]byte, 0, int(w*h))
	for y, row := range rows {
		if int32(len(row)) != w {
			return nil, fmt.Errorf("%w: 第 %d 行长度 %d != %d", scenemap.ErrInvalidArgument, y, len(row), w)
		}
		for _, ch := range row {
			if ch == '#' {
				matrix = append(matrix, 1)
			} else {
				matrix = append(matrix, 0)
			}
		}
	}
	return NewBlocker(Size{Width: w, Height: h}, matrix)
}

// Size 返回地图尺寸。
func (b *Blocker) Size() Size { return b.size }

// StaticBlocked 报告静态矩阵是否阻挡。越界视为阻挡（图外不可走）。
func (b *Blocker) StaticBlocked(p scenemap.GridPos) bool {
	if !b.size.Contains(p) {
		return true
	}
	return b.static[int(p.Y)*int(b.size.Width)+int(p.X)] != 0
}

// DynamicBlocked 报告是否有动态阻挡。
func (b *Blocker) DynamicBlocked(p scenemap.GridPos) bool { return b.dynamic[p] > 0 }

// Walkable 报告格是否可走（静态与动态都通过）。
// 越界一律不可走——这是"负数坐标不能穿墙"的最后一道防线。
func (b *Blocker) Walkable(p scenemap.GridPos) bool {
	return !b.StaticBlocked(p) && !b.DynamicBlocked(p)
}

// SetStatic 修改静态格（场景机关打开/关闭通道时用）。
// 只允许 MGR 调用。
func (b *Blocker) SetStatic(p scenemap.GridPos, blocked bool) {
	if !b.size.Contains(p) {
		return
	}
	idx := int(p.Y)*int(b.size.Width) + int(p.X)
	if blocked {
		b.static[idx] = 1
	} else {
		b.static[idx] = 0
	}
}

// AddDynamic 给格加一层动态阻挡（怪物出生/召出）。
func (b *Blocker) AddDynamic(p scenemap.GridPos) {
	b.dynamic[p]++
}

// RemoveDynamic 移除一层动态阻挡（怪物死亡）。计数归零时删除键，
// 避免长局地图里 map 无限增长（每格一条空记录也是内存）。
func (b *Blocker) RemoveDynamic(p scenemap.GridPos) {
	n, ok := b.dynamic[p]
	if !ok {
		return
	}
	if n <= 1 {
		delete(b.dynamic, p)
		return
	}
	b.dynamic[p] = n - 1
}

// DynamicCount 返回当前动态阻挡格数（监控用）。
func (b *Blocker) DynamicCount() int { return len(b.dynamic) }

// Footprint 返回以 center 为中心、半径 r 的方形占格（怪物体积）。
//
// 第一版怪物都是 1×1（r=0）；大型怪用 r>=1，占 (2r+1)² 格。
// 只返回地图内的格：边缘怪的体积不应把矩阵写成越界。
func (b *Blocker) Footprint(center scenemap.GridPos, r int32) []scenemap.GridPos {
	if r <= 0 {
		return []scenemap.GridPos{center}
	}
	out := make([]scenemap.GridPos, 0, (2*r+1)*(2*r+1))
	for y := center.Y - r; y <= center.Y+r; y++ {
		for x := center.X - r; x <= center.X+r; x++ {
			p := scenemap.GridPos{X: x, Y: y}
			if b.size.Contains(p) {
				out = append(out, p)
			}
		}
	}
	return out
}
