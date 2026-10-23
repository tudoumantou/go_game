package spatial

import (
	"reflect"
	"strings"
	"testing"

	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

// occKey 快速构造占位键。
func occKey(cx, cy int64) OccupancyKey { return OccupancyKey{CellX: cx, CellY: cy, Layer: 0} }

// TestClaimAndOccupant 覆盖文档 5.3 双占失败：第二个人必须拿到
// ErrPositionOccupied，且错误信息里带占用者信息（客户端要靠它
// 决定换点还是攻击）。
func TestClaimAndOccupant(t *testing.T) {
	idx := NewOccupancyIndex()
	k := occKey(10, 20)
	err1 := idx.Claim(k, key(100))
	require.NoError(t, err1, "首次 Claim 应成功: %v", err1)
	obj, ok := idx.Occupant(k)
	require.True(t, ok, "Occupant = %v,%v want %v,true", obj, ok, key(100))
	require.EqualValues(t, key(100), obj, "Occupant = %v,%v want %v,true", obj, ok, key(100))

	// 不同对象双占：必须失败。
	err := idx.Claim(k, key(200))
	require.Error(t, err, "双占应失败")
	require.ErrorIs(t, err, domain.ErrPositionOccupied, "双占错误应包装 ErrPositionOccupied, got %v", err)
	require.True(t, strings.Contains(err.Error(), "100"), "错误信息应含占用者 100, got %v", err)

	// 同一对象重复 Claim：幂等成功（命令重试不能永远失败）。
	err2 := idx.Claim(k, key(100))
	require.NoError(t, err2, "同对象重复 Claim 应幂等成功: %v", err2)
	// 占用者未被覆盖。
	obj3, _ := idx.Occupant(k)
	require.EqualValues(t, key(100), obj3, "重复 Claim 后占用者 = %v, want 100", obj3)
	require.EqualValues(t, 1, idx.Len())

	// 空闲格查询。
	_, ok4 := idx.Occupant(occKey(11, 20))
	require.False(t, ok4, "未占用格 Occupant 应 false")
}

// TestReleaseRules 覆盖"只能释放自己占的格"与重复释放：
// A 释放 B 的占位必须失败（否则 B 的事务以为格还空着，两个建筑叠一起）；
// 释放空闲格说明计划层有 bug，也要显式报错。
func TestReleaseRules(t *testing.T) {
	idx := NewOccupancyIndex()
	k := occKey(5, 5)
	err5 := idx.Claim(k, key(100))
	require.NoError(t, err5, "Claim: %v", err5)

	// 他人释放：失败且不真释放。
	err := idx.Release(k, key(200))
	require.Error(t, err, "释放他人占位应失败")
	require.ErrorIs(t, err, domain.ErrNotOwner, "错误应包装 ErrNotOwner, got %v", err)
	obj6, ok7 := idx.Occupant(k)
	require.True(t, ok7, "失败的释放不得动占位: occupant=%v ok=%v", obj6, ok7)
	require.EqualValues(t, key(100), obj6, "失败的释放不得动占位: occupant=%v ok=%v", obj6, ok7)

	// 释放未占用格：失败。
	err = idx.Release(occKey(6, 6), key(100))
	require.Error(t, err, "释放空格应失败")
	require.ErrorIs(t, err, domain.ErrObjectNotFound, "错误应包装 ErrObjectNotFound, got %v", err)

	// 本人释放：成功，之后其他人可以claim。
	err8 := idx.Release(k, key(100))
	require.NoError(t, err8, "本人释放应成功: %v", err8)
	_, ok9 := idx.Occupant(k)
	require.False(t, ok9, "释放后格应空闲")
	err10 := idx.Claim(k, key(200))
	require.NoError(t, err10, "释放后其他对象应可占用: %v", err10)

	// 重复释放：失败（双重释放是计划层 bug，不静默）。
	err11 := idx.Release(k, key(100))
	require.Error(t, err11, "重复释放应失败")
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
		err12 := idx.Claim(k, obj)
		require.NoError(t, err12, "Claim %+v: %v", k, err12)
	}

	got := idx.ObjectsOn(obj)
	require.EqualValues(t, len(keys), len(got))
	// 按键排序确定：Layer 优先，然后 CellX/CellY。
	want := []OccupancyKey{
		{CellX: -7, CellY: 4, Layer: 0},
		occKey(1, 1),
		{CellX: 2, CellY: 3, Layer: 1},
		{CellX: 1, CellY: 1, Layer: 2},
	}
	require.True(t, reflect.DeepEqual(got, want), "ObjectsOn = %+v, want %+v", got, want)

	// 其他对象的格不出现在反查里。
	got13 := idx.ObjectsOn(key(999))
	require.Nil(t, got13, "无占位对象的 ObjectsOn = %+v, want nil", got13)

	// 模拟对象删除：反查全部格并逐个释放。
	for _, k := range idx.ObjectsOn(obj) {
		err14 := idx.Release(k, obj)
		require.NoError(t, err14, "Release %+v: %v", k, err14)
	}
	require.EqualValues(t, 0, idx.Len())
	got15 := idx.ObjectsOn(obj)
	require.Nil(t, got15, "反索引应无残留: %+v", got15)
	// Snapshot 也为空：证明正反索引一致。
	got16 := idx.Snapshot()
	require.EqualValues(t, 0, len(got16), "Snapshot 应为空, got %+v", got16)
}

// TestSnapshotDeepCopy 覆盖快照深拷贝语义：改快照不影响索引，
// 索引后续变更也不影响已取快照。
func TestSnapshotDeepCopy(t *testing.T) {
	idx := NewOccupancyIndex()
	err17 := idx.Claim(occKey(1, 1), key(100))
	require.NoError(t, err17, "Claim: %v", err17)
	snap := idx.Snapshot()
	require.EqualValues(t, 1, len(snap), "Snapshot = %+v", snap)
	require.EqualValues(t, key(100), snap[occKey(1, 1)], "Snapshot = %+v", snap)

	// 改快照：删除/新增条目都不影响索引。
	delete(snap, occKey(1, 1))
	snap[occKey(9, 9)] = key(999)
	_, ok18 := idx.Occupant(occKey(1, 1))
	require.True(t, ok18, "改快照不应影响索引: 格(1,1) 应仍被占用")
	_, ok19 := idx.Occupant(occKey(9, 9))
	require.False(t, ok19, "改快照不应影响索引: 格(9,9) 应空闲")

	// 索引后续变更不影响已取快照（快照是当时的独立拷贝）：
	// snap 此刻只剩测试自己插入的假条目 {(9,9):999}。
	err20 := idx.Claim(occKey(2, 2), key(200))
	require.NoError(t, err20, "Claim: %v", err20)
	require.Len(t, snap, 1)
	_, ok21 := snap[occKey(2, 2)]
	require.False(t, ok21, "旧快照不应包含新占的格")
	require.EqualValues(t, 2, idx.Len())
}

// TestLayerSeparation 覆盖 layer 维度：同格不同 layer 互不冲突，
// 与 SQL 唯一键 (world,cell_x,cell_y,layer) 的语义一致。
func TestLayerSeparation(t *testing.T) {
	idx := NewOccupancyIndex()
	base := OccupancyKey{CellX: 3, CellY: 4}
	err22 := idx.Claim(base, key(100))
	require.NoError(t, err22, "layer0 Claim: %v", err22)
	l1 := base
	l1.Layer = 1
	err23 := idx.Claim(l1, key(200))
	require.NoError(t, err23, "layer1 应可占用: %v", err23)
	l2 := base
	l2.Layer = 2
	err24 := idx.Claim(l2, key(300))
	require.NoError(t, err24, "layer2 应可占用: %v", err24)
	require.EqualValues(t, 3, idx.Len())
}
