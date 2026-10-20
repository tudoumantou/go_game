package spatial

import (
	"errors"
	"reflect"
	"testing"

	"game_study/internal/worldmap/domain"
)

// testTopo 返回文档 5.1 的默认拓扑：5000×5000 格、5×5 场景、
// 每场景 1000×1000 格、1 格 = 1000 units。
func testTopo() domain.Topology { return domain.DefaultTopology(1) }

// key 快速构造跨世界对象键。
func key(id domain.ObjectID) domain.ObjectKey { return domain.ObjectKey{WorldID: 1, ID: id} }

// TestSceneAtConsistency 覆盖文档 19.1 的"世界四角、负数、右上边界"：
// SceneAt 必须左闭右开、负坐标与右上界外一律返回 0（无效场景号），
// 绝不返回看似合法的负数或越界场景号。
func TestSceneAtConsistency(t *testing.T) {
	topo := testTopo()
	const (
		wu = 1000 * domain.DefaultPositionUnitsPerCell // 单场景宽（units）
		hu = 1000 * domain.DefaultPositionUnitsPerCell // 单场景高（units）
		w  = domain.DefaultWorldWidthCells * domain.DefaultPositionUnitsPerCell
		h  = domain.DefaultWorldHeightCells * domain.DefaultPositionUnitsPerCell
	)
	tests := []struct {
		name string
		pos  domain.Pos
		want domain.SceneID
	}{
		{"左下角(0,0)", domain.Pos{X: 0, Y: 0}, 1},
		{"右下角(width-1,0)", domain.Pos{X: w - 1, Y: 0}, 5},
		{"左上角(0,height-1)", domain.Pos{X: 0, Y: h - 1}, 21},
		{"右上角(width-1,height-1)", domain.Pos{X: w - 1, Y: h - 1}, 25},
		{"负数x", domain.Pos{X: -1, Y: 0}, 0},
		{"负数y", domain.Pos{X: 0, Y: -1}, 0},
		{"双负", domain.Pos{X: -1000, Y: -1000}, 0},
		{"右上边界外x=width", domain.Pos{X: w, Y: 0}, 0},
		{"右上边界外y=height", domain.Pos{X: 0, Y: h}, 0},
		{"x恰为场景1右边界归右侧场景", domain.Pos{X: wu, Y: 0}, 2},
		{"y恰为场景行边界归上行", domain.Pos{X: 0, Y: hu}, 6},
		{"两轴同时跨界", domain.Pos{X: wu, Y: hu}, 7},
		{"场景内一格", domain.Pos{X: wu - 1, Y: hu - 1}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topo.SceneAt(tt.pos); got != tt.want {
				t.Fatalf("SceneAt(%v) = %d, want %d", tt.pos, got, tt.want)
			}
		})
	}
}

// TestPutSceneMismatch 覆盖文档 5.2/5.3：scene 必须与位置推导的
// PhysicalScene 一致；不一致是调用方把 OwnerScene 与 PhysicalScene
// 混用的 bug，必须报错而不是静默接受。越界坐标同样拒绝。
func TestPutSceneMismatch(t *testing.T) {
	g := NewSceneGrid(testTopo())
	tests := []struct {
		name    string
		key     domain.ObjectKey
		kind    domain.ObjectKind
		scene   domain.SceneID
		pos     domain.Pos
		wantErr bool
	}{
		{
			name: "合法:场景与位置一致", key: key(1), kind: domain.ObjectKindCity,
			scene: 1, pos: domain.Pos{X: 100, Y: 200}, wantErr: false,
		},
		{
			name: "合法:场景2", key: key(2), kind: domain.ObjectKindResource,
			scene: 2, pos: domain.Pos{X: 1_000_000 + 500, Y: 100}, wantErr: false,
		},
		{
			name: "不一致:传入OwnerScene但位置在别处", key: key(3), kind: domain.ObjectKindCity,
			scene: 7, pos: domain.Pos{X: 100, Y: 100}, wantErr: true,
		},
		{
			name: "不一致:差一个场景", key: key(4), kind: domain.ObjectKindCity,
			scene: 1, pos: domain.Pos{X: 1_000_000, Y: 0}, wantErr: true,
		},
		{
			name: "越界:负坐标", key: key(5), kind: domain.ObjectKindCity,
			scene: 1, pos: domain.Pos{X: -1, Y: 0}, wantErr: true,
		},
		{
			name: "越界:右上边界外", key: key(6), kind: domain.ObjectKindCity,
			scene: 25, pos: domain.Pos{X: 5_000_000, Y: 0}, wantErr: true,
		},
		{
			name: "种类零值非法", key: key(7), kind: domain.ObjectKindUnspecified,
			scene: 1, pos: domain.Pos{X: 0, Y: 0}, wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := g.Put(tt.key, tt.kind, tt.scene, tt.pos)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Put 期望报错但成功")
				}
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Fatalf("Put 错误应包装 ErrInvalidArgument, got %v", err)
				}
				// 失败的 Put 不得污染索引。
				if _, ok := g.Get(tt.key); ok {
					t.Fatalf("失败的 Put 不应写入索引")
				}
				return
			}
			if err != nil {
				t.Fatalf("Put 意外错误: %v", err)
			}
		})
	}
	// 合法写入的 2 个对象应在索引中。
	if g.Len() != 2 {
		t.Fatalf("Len = %d, want 2", g.Len())
	}
}

// TestQueryRectNoMissNoDup 覆盖"矩形查询不漏不重"与场景分块遍历：
// 对象分布在不同场景，查询矩形跨越场景边界时两端对象都必须出现
// （不漏），且任何对象不出现两次（不重）。
func TestQueryRectNoMissNoDup(t *testing.T) {
	g := NewSceneGrid(testTopo())
	// 放置表：(cellX,cellY) → 对象。覆盖四角与场景边界两侧。
	places := []struct {
		id domain.ObjectID
		cx int64
		cy int64
	}{
		{1, 0, 0},       // 场景 1
		{2, 999, 999},   // 场景 1 右上格（边界左侧）
		{3, 1000, 1000}, // 场景 7 左下格（边界右侧，左闭右开）
		{4, 4999, 4999}, // 场景 25 右上角
		{5, 2500, 2500}, // 场景 14 中央
		{6, 4999, 0},    // 场景 5 右下角
		{7, 0, 4999},    // 场景 21 左上角
		{8, 1999, 3999}, // 场景 20（x 在场景 2 右缘、y 在场景 4 底缘）
	}
	for _, p := range places {
		pos := domain.Pos{X: p.cx * 1000, Y: p.cy * 1000}
		if err := g.Put(key(p.id), domain.ObjectKindCity, testTopo().SceneAt(pos), pos); err != nil {
			t.Fatalf("Put(%d) 失败: %v", p.id, err)
		}
	}
	tests := []struct {
		name    string
		minX    int64
		minY    int64
		maxX    int64
		maxY    int64
		wantIDs []uint64
	}{
		{
			name: "跨场景边界矩形(995..1005)", minX: 995, minY: 995, maxX: 1005, maxY: 1005,
			wantIDs: []uint64{2, 3},
		},
		{
			name: "场景1全矩形", minX: 0, minY: 0, maxX: 1000, maxY: 1000,
			wantIDs: []uint64{1, 2},
		},
		{
			name: "空矩形(min==max,左闭右开)", minX: 100, minY: 100, maxX: 100, maxY: 100,
			wantIDs: nil,
		},
		{
			name: "反向矩形(min>max)", minX: 500, minY: 500, maxX: 100, maxY: 100,
			wantIDs: nil,
		},
		{
			name: "左侧界外(-100..10)", minX: -100, minY: -100, maxX: 10, maxY: 10,
			wantIDs: []uint64{1},
		},
		{
			name: "完全界外", minX: 6000, minY: 6000, maxX: 7000, maxY: 7000,
			wantIDs: nil,
		},
		{
			name: "覆盖两个分离场景的大矩形", minX: 0, minY: 0, maxX: 1000, maxY: 5000,
			wantIDs: []uint64{1, 2, 7},
		},
		{
			name: "右下角", minX: 4999, minY: 0, maxX: 5000, maxY: 1,
			wantIDs: []uint64{6},
		},
		{
			name: "世界全矩形(不漏)", minX: 0, minY: 0, maxX: 5000, maxY: 5000,
			wantIDs: []uint64{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := g.QueryRect(tt.minX, tt.minY, tt.maxX, tt.maxY)
			gotIDs := make([]uint64, 0, len(got))
			for _, k := range got {
				gotIDs = append(gotIDs, uint64(k.ID))
			}
			// 空结果用长度比较（nil 与空切片在 DeepEqual 下不等，
			// 这里只关心"有哪些 ID"）。
			if len(gotIDs) != len(tt.wantIDs) ||
				(len(gotIDs) > 0 && !reflect.DeepEqual(gotIDs, tt.wantIDs)) {
				t.Fatalf("QueryRect(%d,%d,%d,%d) = %v, want %v",
					tt.minX, tt.minY, tt.maxX, tt.maxY, gotIDs, tt.wantIDs)
			}
		})
	}
}

// TestGridLifecycle 覆盖 Get/Delete/SceneOf/Len/EntriesInScene 与
// "同 key 跨场景移动"的原地更新路径。
func TestGridLifecycle(t *testing.T) {
	topo := testTopo()
	g := NewSceneGrid(topo)

	posA := domain.Pos{X: 100, Y: 100}             // 场景 1
	posB := domain.Pos{X: 1_000_000 + 100, Y: 100} // 场景 2
	posC := domain.Pos{X: 200, Y: 200}             // 场景 1

	if err := g.Put(key(1), domain.ObjectKindCity, 1, posA); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := g.Put(key(2), domain.ObjectKindResource, 2, posB); err != nil {
		t.Fatalf("Put B: %v", err)
	}
	if err := g.Put(key(3), domain.ObjectKindTroop, 1, posC); err != nil {
		t.Fatalf("Put C: %v", err)
	}
	if g.Len() != 3 {
		t.Fatalf("Len = %d, want 3", g.Len())
	}

	// Get 返回值字段完整。
	e, ok := g.Get(key(1))
	if !ok {
		t.Fatalf("Get(1) 应存在")
	}
	want := Entry{Key: key(1), Kind: domain.ObjectKindCity, Pos: posA, Scene: 1}
	if e != want {
		t.Fatalf("Get(1) = %+v, want %+v", e, want)
	}

	// SceneOf。
	if got := g.SceneOf(key(1)); got != 1 {
		t.Fatalf("SceneOf(1) = %d, want 1", got)
	}
	if got := g.SceneOf(key(2)); got != 2 {
		t.Fatalf("SceneOf(2) = %d, want 2", got)
	}
	if got := g.SceneOf(key(999)); got != 0 {
		t.Fatalf("SceneOf(不存在) = %d, want 0", got)
	}

	// EntriesInScene 场景 1 应有 2 个对象，按 ID 排序。
	entries := g.EntriesInScene(1)
	if len(entries) != 2 || entries[0].Key.ID != 1 || entries[1].Key.ID != 3 {
		t.Fatalf("EntriesInScene(1) = %+v", entries)
	}
	// 非法场景号返回空。
	if got := g.EntriesInScene(0); got != nil {
		t.Fatalf("EntriesInScene(0) = %+v, want nil", got)
	}
	if got := g.EntriesInScene(26); got != nil {
		t.Fatalf("EntriesInScene(26) = %+v, want nil", got)
	}

	// 跨场景移动：key(1) 从场景 1 移到场景 2。
	posA2 := domain.Pos{X: 1_000_000 + 300, Y: 400}
	if err := g.Put(key(1), domain.ObjectKindCity, 2, posA2); err != nil {
		t.Fatalf("Put 移动: %v", err)
	}
	if got := g.SceneOf(key(1)); got != 2 {
		t.Fatalf("移动后 SceneOf(1) = %d, want 2", got)
	}
	if got := g.QueryRect(0, 0, 1000, 1000); len(got) != 1 || got[0].ID != 3 {
		t.Fatalf("移动后场景1应只剩对象3, got %v", got)
	}
	if got := g.QueryRect(1000, 0, 2000, 1000); len(got) != 2 {
		t.Fatalf("移动后场景2应有2个对象, got %v", got)
	}
	if g.Len() != 3 {
		t.Fatalf("移动不应改变总数: Len = %d", g.Len())
	}

	// Delete：之后 Get/SceneOf 查不到，QueryRect 不含。
	g.Delete(key(1))
	if _, ok := g.Get(key(1)); ok {
		t.Fatalf("Delete 后 Get 应不存在")
	}
	if got := g.SceneOf(key(1)); got != 0 {
		t.Fatalf("Delete 后 SceneOf = %d, want 0", got)
	}
	if g.Len() != 2 {
		t.Fatalf("Delete 后 Len = %d, want 2", g.Len())
	}
	// 删除不存在的 key：幂等 no-op，不 panic。
	g.Delete(key(999))
	if g.Len() != 2 {
		t.Fatalf("重复删除后 Len = %d, want 2", g.Len())
	}

	// 删除后同 key 可重新 Put（复活）。
	if err := g.Put(key(1), domain.ObjectKindCity, 1, posA); err != nil {
		t.Fatalf("复活 Put: %v", err)
	}
	if e, ok := g.Get(key(1)); !ok || e.Deleted {
		t.Fatalf("复活后 Get = %+v ok=%v", e, ok)
	}
}

// TestQueryRectDoesNotScanWorld 验证"按场景分块遍历"的局部性：
// 在远端场景放入对象后，查询局部矩形不应看到它，且对象数与矩形
// 所在场景相关而不是全世界（行为等价于只遍历了相关场景桶）。
func TestQueryRectDoesNotScanWorld(t *testing.T) {
	g := NewSceneGrid(testTopo())
	// 场景 25 放 3 个对象。
	for i, id := range []domain.ObjectID{101, 102, 103} {
		pos := domain.Pos{X: (4000 + int64(i)*100) * 1000, Y: (4000 + int64(i)*100) * 1000}
		if err := g.Put(key(id), domain.ObjectKindCity, 25, pos); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// 场景 1 放 1 个对象。
	if err := g.Put(key(1), domain.ObjectKindCity, 1, domain.Pos{X: 500, Y: 500}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got := g.QueryRect(0, 0, 100, 100)
	if len(got) != 1 || got[0].ID != 1 {
		t.Fatalf("局部查询应只见场景1对象, got %v", got)
	}
	// byScene 桶数 = 2（两个非空场景），证明对象按场景隔离。
	if len(g.byScene) != 2 {
		t.Fatalf("byScene 桶数 = %d, want 2", len(g.byScene))
	}
}
