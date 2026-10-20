package spatial

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"game_study/internal/worldmap/domain"
)

// occKey 快速构造占位键。
func occKey(cx, cy int64) OccupancyKey { return OccupancyKey{CellX: cx, CellY: cy, Layer: 0} }

// TestClaimAndOccupant 覆盖文档 5.3 双占失败：第二个人必须拿到
// ErrPositionOccupied，且错误信息里带占用者信息（客户端要靠它
// 决定换点还是攻击）。
func TestClaimAndOccupant(t *testing.T) {
	idx := NewOccupancyIndex()
	k := occKey(10, 20)
	if err := idx.Claim(k, key(100)); err != nil {
		t.Fatalf("首次 Claim 应成功: %v", err)
	}
	obj, ok := idx.Occupant(k)
	if !ok || obj != key(100) {
		t.Fatalf("Occupant = %v,%v want %v,true", obj, ok, key(100))
	}

	// 不同对象双占：必须失败。
	err := idx.Claim(k, key(200))
	if err == nil {
		t.Fatalf("双占应失败")
	}
	if !errors.Is(err, domain.ErrPositionOccupied) {
		t.Fatalf("双占错误应包装 ErrPositionOccupied, got %v", err)
	}
	if !strings.Contains(err.Error(), "100") {
		t.Fatalf("错误信息应含占用者 100, got %v", err)
	}

	// 同一对象重复 Claim：幂等成功（命令重试不能永远失败）。
	if err := idx.Claim(k, key(100)); err != nil {
		t.Fatalf("同对象重复 Claim 应幂等成功: %v", err)
	}
	// 占用者未被覆盖。
	if obj, _ := idx.Occupant(k); obj != key(100) {
		t.Fatalf("重复 Claim 后占用者 = %v, want 100", obj)
	}
	if idx.Len() != 1 {
		t.Fatalf("Len = %d, want 1", idx.Len())
	}

	// 空闲格查询。
	if _, ok := idx.Occupant(occKey(11, 20)); ok {
		t.Fatalf("未占用格 Occupant 应 false")
	}
}

// TestReleaseRules 覆盖"只能释放自己占的格"与重复释放：
// A 释放 B 的占位必须失败（否则 B 的事务以为格还空着，两个建筑叠一起）；
// 释放空闲格说明计划层有 bug，也要显式报错。
func TestReleaseRules(t *testing.T) {
	idx := NewOccupancyIndex()
	k := occKey(5, 5)
	if err := idx.Claim(k, key(100)); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// 他人释放：失败且不真释放。
	err := idx.Release(k, key(200))
	if err == nil {
		t.Fatalf("释放他人占位应失败")
	}
	if !errors.Is(err, domain.ErrNotOwner) {
		t.Fatalf("错误应包装 ErrNotOwner, got %v", err)
	}
	if obj, ok := idx.Occupant(k); !ok || obj != key(100) {
		t.Fatalf("失败的释放不得动占位: occupant=%v ok=%v", obj, ok)
	}

	// 释放未占用格：失败。
	err = idx.Release(occKey(6, 6), key(100))
	if err == nil {
		t.Fatalf("释放空格应失败")
	}
	if !errors.Is(err, domain.ErrObjectNotFound) {
		t.Fatalf("错误应包装 ErrObjectNotFound, got %v", err)
	}

	// 本人释放：成功，之后其他人可以claim。
	if err := idx.Release(k, key(100)); err != nil {
		t.Fatalf("本人释放应成功: %v", err)
	}
	if _, ok := idx.Occupant(k); ok {
		t.Fatalf("释放后格应空闲")
	}
	if err := idx.Claim(k, key(200)); err != nil {
		t.Fatalf("释放后其他对象应可占用: %v", err)
	}

	// 重复释放：失败（双重释放是计划层 bug，不静默）。
	if err := idx.Release(k, key(100)); err == nil {
		t.Fatalf("重复释放应失败")
	}
}

// TestObjectsOnReverseIndex 覆盖删除对象时的反查释放：
// ObjectsOn 必须返回对象占的全部格（含多 layer），逐个释放后
// 反索引无残留。
func TestObjectsOnReverseIndex(t *testing.T) {
	idx := NewOccupancyIndex()
	obj := key(100)
	keys := []OccupancyKey{
		occKey(1, 1),
		{CellX: 1, CellY: 1, Layer: 2},  // 同格不同 layer
		{CellX: 2, CellY: 3, Layer: 1},  // 不同格不同 layer
		{CellX: -7, CellY: 4, Layer: 0}, // 负数格坐标（索引层不做范围校验）
	}
	for _, k := range keys {
		if err := idx.Claim(k, obj); err != nil {
			t.Fatalf("Claim %+v: %v", k, err)
		}
	}

	got := idx.ObjectsOn(obj)
	if len(got) != len(keys) {
		t.Fatalf("ObjectsOn = %+v, want %d 个键", got, len(keys))
	}
	// 按键排序确定：Layer 优先，然后 CellX/CellY。
	want := []OccupancyKey{
		{CellX: -7, CellY: 4, Layer: 0},
		occKey(1, 1),
		{CellX: 2, CellY: 3, Layer: 1},
		{CellX: 1, CellY: 1, Layer: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ObjectsOn = %+v, want %+v", got, want)
	}

	// 其他对象的格不出现在反查里。
	if got := idx.ObjectsOn(key(999)); got != nil {
		t.Fatalf("无占位对象的 ObjectsOn = %+v, want nil", got)
	}

	// 模拟对象删除：反查全部格并逐个释放。
	for _, k := range idx.ObjectsOn(obj) {
		if err := idx.Release(k, obj); err != nil {
			t.Fatalf("Release %+v: %v", k, err)
		}
	}
	if idx.Len() != 0 {
		t.Fatalf("释放后 Len = %d, want 0", idx.Len())
	}
	if got := idx.ObjectsOn(obj); got != nil {
		t.Fatalf("反索引应无残留: %+v", got)
	}
	// Snapshot 也为空：证明正反索引一致。
	if got := idx.Snapshot(); len(got) != 0 {
		t.Fatalf("Snapshot 应为空, got %+v", got)
	}
}

// TestSnapshotDeepCopy 覆盖快照深拷贝语义：改快照不影响索引，
// 索引后续变更也不影响已取快照。
func TestSnapshotDeepCopy(t *testing.T) {
	idx := NewOccupancyIndex()
	if err := idx.Claim(occKey(1, 1), key(100)); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	snap := idx.Snapshot()
	if len(snap) != 1 || snap[occKey(1, 1)] != key(100) {
		t.Fatalf("Snapshot = %+v", snap)
	}

	// 改快照：删除/新增条目都不影响索引。
	delete(snap, occKey(1, 1))
	snap[occKey(9, 9)] = key(999)
	if _, ok := idx.Occupant(occKey(1, 1)); !ok {
		t.Fatalf("改快照不应影响索引: 格(1,1) 应仍被占用")
	}
	if _, ok := idx.Occupant(occKey(9, 9)); ok {
		t.Fatalf("改快照不应影响索引: 格(9,9) 应空闲")
	}

	// 索引后续变更不影响已取快照（快照是当时的独立拷贝）：
	// snap 此刻只剩测试自己插入的假条目 {(9,9):999}。
	if err := idx.Claim(occKey(2, 2), key(200)); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(snap) != 1 {
		t.Fatalf("旧快照不应看到新占位, got %+v", snap)
	}
	if _, ok := snap[occKey(2, 2)]; ok {
		t.Fatalf("旧快照不应包含新占的格")
	}
	if idx.Len() != 2 {
		t.Fatalf("索引 Len = %d, want 2", idx.Len())
	}
}

// TestLayerSeparation 覆盖 layer 维度：同格不同 layer 互不冲突，
// 与 SQL 唯一键 (world,cell_x,cell_y,layer) 的语义一致。
func TestLayerSeparation(t *testing.T) {
	idx := NewOccupancyIndex()
	base := OccupancyKey{CellX: 3, CellY: 4}
	if err := idx.Claim(base, key(100)); err != nil {
		t.Fatalf("layer0 Claim: %v", err)
	}
	l1 := base
	l1.Layer = 1
	if err := idx.Claim(l1, key(200)); err != nil {
		t.Fatalf("layer1 应可占用: %v", err)
	}
	l2 := base
	l2.Layer = 2
	if err := idx.Claim(l2, key(300)); err != nil {
		t.Fatalf("layer2 应可占用: %v", err)
	}
	if idx.Len() != 3 {
		t.Fatalf("Len = %d, want 3", idx.Len())
	}
}
