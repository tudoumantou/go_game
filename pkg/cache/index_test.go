// index_test.go —— 归属索引的语义与边界
package cache

import (
	"context"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTestIndex(t *testing.T) *Index {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewIndex(rdb, "partner")
}

// TestIndexAddAndQuery 覆盖索引的三个用途：判存在、反查归属、按归属者取列表。
func TestIndexAddAndQuery(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	require.NoError(t, idx.Add(ctx, 10086, 2001))
	require.NoError(t, idx.Add(ctx, 10086, 2002))
	require.NoError(t, idx.Add(ctx, 10087, 2003))

	ok, err := idx.Exists(ctx, 2001)
	require.NoError(t, err)
	require.True(t, ok)

	owner, err := idx.Owner(ctx, 2003)
	require.NoError(t, err)
	require.EqualValues(t, 10087, owner, "反查归属：ZSCORE 的 score 就是归属者")

	ids, err := idx.Members(ctx, 10086)
	require.NoError(t, err)
	require.Equal(t, []int64{2001, 2002}, ids)

	n, err := idx.CountByOwner(ctx, 10086)
	require.NoError(t, err)
	require.EqualValues(t, 2, n)

	total, err := idx.Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, total)
}

// TestIndexMembersNumericOrder 验证列表按数值升序，而不是 Redis 的字典序。
//
// ZRANGEBYSCORE 对同 score 的成员按字典序返回："1001" < "999"，
// 直接用会让「伙伴列表」的顺序看起来很随机。
func TestIndexMembersNumericOrder(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	require.NoError(t, idx.Add(ctx, 7, 1001))
	require.NoError(t, idx.Add(ctx, 7, 999))
	require.NoError(t, idx.Add(ctx, 7, 20))

	ids, err := idx.Members(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, []int64{20, 999, 1001}, ids)
}

// TestIndexRemoveWithoutOwner 验证「删除不需要知道归属者」。
//
// 这是选 ZSET 的直接收益：Hash + 位置编号的方案里，删一个实体必须先
// 查到它属于谁才能定位 field；这里一个 ZREM 就够。
func TestIndexRemoveWithoutOwner(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	require.NoError(t, idx.Add(ctx, 10086, 2001))
	require.NoError(t, idx.Add(ctx, 10086, 2002))
	require.NoError(t, idx.Remove(ctx, 2001))

	ok, err := idx.Exists(ctx, 2001)
	require.NoError(t, err)
	require.False(t, ok, "删除后必须判不存在")

	ids, err := idx.Members(ctx, 10086)
	require.NoError(t, err)
	require.Equal(t, []int64{2002}, ids, "列表里也必须消失")
}

// TestIndexMoveOwner 验证转移归属：同一个 member 再 ZADD 会覆盖 score。
func TestIndexMoveOwner(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	require.NoError(t, idx.Add(ctx, 10086, 2001))
	require.NoError(t, idx.Add(ctx, 10087, 2001)) // 转移

	owner, err := idx.Owner(ctx, 2001)
	require.NoError(t, err)
	require.EqualValues(t, 10087, owner)

	old, err := idx.Members(ctx, 10086)
	require.NoError(t, err)
	require.Empty(t, old, "旧归属者的列表里不能还留着")

	cur, err := idx.Members(ctx, 10087)
	require.NoError(t, err)
	require.Equal(t, []int64{2001}, cur)
}

// TestIndexMissing 验证未索引时的返回值语义。
//
// 孤儿行（没有归属者）也走这条：ZSCORE 返回 nil 即「不存在」。
func TestIndexMissing(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	_, err := idx.Owner(ctx, 9999)
	require.ErrorIs(t, err, ErrNotIndexed)

	ok, err := idx.Exists(ctx, 9999)
	require.NoError(t, err)
	require.False(t, ok)

	ids, err := idx.Members(ctx, 9999)
	require.NoError(t, err)
	require.Empty(t, ids)
}

// TestIndexConcurrentAddDoesNotLose 验证并发新增不丢。
//
// 这是相对「Hash + 位置编号 + 字符串列表」的核心改进：那套是
// 「HGET 列表 → 追加 → HSET 覆盖」，两个入口并发（玩家操作 + GM 发放）
// 后写直接盖掉前写；ZADD 本身原子，不存在这个窗口。
func TestIndexConcurrentAddDoesNotLose(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			require.NoError(t, idx.Add(ctx, 10086, 3000+id))
		}(int64(i))
	}
	wg.Wait()

	ids, err := idx.Members(ctx, 10086)
	require.NoError(t, err)
	require.Len(t, ids, n, "并发新增不得丢失")
}

// TestIndexBuildSwapsAtomically 验证全量重建走临时 key + RENAME。
func TestIndexBuildSwapsAtomically(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	// 上一轮残留
	require.NoError(t, idx.Add(ctx, 1, 100))

	// 分两批供稿，模拟启动时的分页读取
	require.NoError(t, idx.BuildBegin(ctx))
	_, err := idx.BuildAppend(ctx, []Row{{ID: 2001, Owner: 10086}, {ID: 2002, Owner: 10086}})
	require.NoError(t, err)
	_, err = idx.BuildAppend(ctx, []Row{{ID: 2003, Owner: 10087}})
	require.NoError(t, err)

	// 切换前旧索引仍然可读（业务不能看到「半空」的索引）
	ok, err := idx.Exists(ctx, 100)
	require.NoError(t, err)
	require.True(t, ok, "构建期间必须读旧索引")

	n, err := idx.BuildCommit(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)

	// 切换后旧数据消失、新数据就位
	ok, err = idx.Exists(ctx, 100)
	require.NoError(t, err)
	require.False(t, ok, "切换后旧索引必须消失")

	ids, err := idx.Members(ctx, 10086)
	require.NoError(t, err)
	require.Equal(t, []int64{2001, 2002}, ids)
}

// TestIndexBuildEmptyTable 验证空表重建不报错（RENAME 空 key 的坑）。
func TestIndexBuildEmptyTable(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	require.NoError(t, idx.Add(ctx, 1, 100))
	n, err := idx.Build(ctx, nil)
	require.NoError(t, err, "空表重建不该因 RENAME 不存在 key 而失败")
	require.Zero(t, n)

	total, err := idx.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, total, "空表重建后正式 key 也应为空")
}

// TestIndexRejectsInvalidIDs 验证非法入参被拦。
func TestIndexRejectsInvalidIDs(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)

	require.Error(t, idx.Add(ctx, 0, 1), "归属者 0 非法")
	require.Error(t, idx.Add(ctx, 1, 0), "实体 0 非法")

	require.NoError(t, idx.Add(ctx, 1, 2))
	require.NoError(t, idx.Remove(ctx, 2))

	_, err := idx.Owner(ctx, 2)
	require.ErrorIs(t, err, ErrNotIndexed)
}

// TestIndexNilRedis 验证未启用缓存时所有方法安全退化。
func TestIndexNilRedis(t *testing.T) {
	ctx := context.Background()
	idx := NewIndex(nil, "partner")

	require.NoError(t, idx.Add(ctx, 1, 2))
	require.NoError(t, idx.Remove(ctx, 2))

	ok, err := idx.Exists(ctx, 2)
	require.NoError(t, err)
	require.False(t, ok)

	ids, err := idx.Members(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, ids)

	n, err := idx.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
}
