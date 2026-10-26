package writebehind

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newDropTestStore 造一个挂在 miniredis 上的 Store。
func newDropTestStore(t *testing.T) (*Store, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStore(rdb), rdb
}

// TestDropRemovesSnapshotAndQueueKeepsLease 验证同步落库后的缓存处置。
//
// 三件事必须同时成立：
//   - 快照消失：否则玩家重登读到充值前的余额；
//   - 队列成员消失：否则落库进程每轮都白扫一条已同步落库的记录；
//   - 租约保留：顶号保护依赖 epoch，删掉会让旧 Actor 重新获得写权限。
func TestDropRemovesSnapshotAndQueueKeepsLease(t *testing.T) {
	ctx := context.Background()
	store, _ := newDropTestStore(t)

	snapshot := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	_, err := store.Seed(ctx, snapshot, 0)
	require.NoError(t, err)
	token, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	_, err = store.Commit(ctx, 7, snapshot, snapshot.FieldNames(), nil, token)
	require.NoError(t, err)

	ids, err := store.Pending(ctx, Partition(7), 10)
	require.NoError(t, err)
	require.Len(t, ids, 1, "前置条件：提交后应有脏任务")

	require.NoError(t, store.Drop(ctx, 7))

	_, err = store.Load(ctx, 7)
	require.ErrorIs(t, err, ErrMiss, "快照必须消失，否则重登读到旧值")

	ids, err = store.Pending(ctx, Partition(7), 10)
	require.NoError(t, err)
	require.Empty(t, ids, "队列成员必须一起清掉")

	// 租约仍在：token 继续递增（若 Drop 误删了 epoch，这里会从 1 重来）。
	// 这一点很关键——epoch 被删等于把顶号保护一起清掉，旧 Actor 会重新
	// 获得写权限。
	again, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	require.EqualValues(t, token+1, again, "Drop 不能清掉 epoch 租约")
}

// TestFlushBatchSkipsDroppedSnapshot 验证「快照已被同步落库删除」时，
// 落库进程跳过而不是让整批失败。
//
// 这是同步（A 类）与异步通道共存的前提：残留队列成员如果让批次回滚，
// 同一批里的其他玩家会被无限拖住，积压再也追不上。
func TestFlushBatchSkipsDroppedSnapshot(t *testing.T) {
	ctx := context.Background()
	store, rdb := newDropTestStore(t)

	raw, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer raw.Close()
	worker := NewWorker(store, raw, 100)

	// 只往队列塞 id、不 Seed 快照——这就是 Drop 之后留下的形态。
	require.NoError(t, rdb.ZAdd(ctx, store.dirtyKey(Partition(7)),
		redis.Z{Score: 1, Member: "7"}).Err())

	n, err := worker.FlushBatch(ctx, Partition(7))
	require.NoError(t, err, "缺快照不该让整批失败")
	require.Zero(t, n)
	require.NoError(t, mock.ExpectationsWereMet(), "不该对数据库发起任何语句")

	ids, err := store.Pending(ctx, Partition(7), 10)
	require.NoError(t, err)
	require.Empty(t, ids, "残留成员必须被清掉，否则每轮都白扫一次")
}
