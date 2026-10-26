package writebehind

import (
	"context"
	"testing"

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

// TestSyncWritesReplica 副本同步：内容与版本都更新，供跨进程读与崩溃恢复。
func TestSyncWritesReplica(t *testing.T) {
	ctx := context.Background()
	store, _ := newDropTestStore(t)

	snapshot := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	_, err := store.Seed(ctx, snapshot, 0)
	require.NoError(t, err)
	token, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)

	snapshot.Items[1001] = 20
	version, err := store.Sync(ctx, 7, snapshot, snapshot.FieldNames(), nil, token)
	require.NoError(t, err)
	require.EqualValues(t, 1, version)

	record, err := store.Load(ctx, 7)
	require.NoError(t, err)
	require.EqualValues(t, 20, record.State.Items[1001], "副本必须更新")
	require.EqualValues(t, 1, record.Version)
}

// TestSyncRejectsStaleLease 副本同步受租约保护。
//
// 顶号之后旧 Actor 的 Sync 必须被拒——否则它会把旧节点的内存状态写进副本，
// 而新节点刚刚写过同一份数据。
func TestSyncRejectsStaleLease(t *testing.T) {
	ctx := context.Background()
	store, _ := newDropTestStore(t)

	snapshot := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	_, err := store.Seed(ctx, snapshot, 0)
	require.NoError(t, err)
	stale, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	_, err = store.AcquireLease(ctx, 7) // 新会话接管，旧令牌失效
	require.NoError(t, err)

	_, err = store.Sync(ctx, 7, snapshot, snapshot.FieldNames(), nil, stale)
	require.ErrorIs(t, err, ErrConflict)
}

// TestDropRemovesSnapshotKeepsLease 同步落库后的缓存处置。
//
// 两件事必须同时成立：
//   - 快照消失：否则玩家重登读到充值前的余额；
//   - 租约保留：顶号保护依赖 epoch，删掉会让旧 Actor 重新获得写权限。
func TestDropRemovesSnapshotKeepsLease(t *testing.T) {
	ctx := context.Background()
	store, _ := newDropTestStore(t)

	snapshot := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	_, err := store.Seed(ctx, snapshot, 0)
	require.NoError(t, err)
	token, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)

	require.NoError(t, store.Drop(ctx, 7))

	_, err = store.Load(ctx, 7)
	require.ErrorIs(t, err, ErrMiss, "快照必须消失，否则重登读到旧值")

	// 租约仍在：token 继续递增（若 Drop 误删了 epoch，这里会从 1 重来）。
	// 这一点很关键——epoch 被删等于把顶号保护一起清掉，旧 Actor 会重新
	// 获得写权限。
	again, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	require.EqualValues(t, token+1, again, "Drop 不能清掉 epoch 租约")

	// Drop 之后 Seed 必须能写进去（这正是「副本落后于库」时重建的前提）。
	_, err = store.Seed(ctx, snapshot, 3)
	require.NoError(t, err, "Drop 之后应允许重新 Seed")
	restored, err := store.Load(ctx, 7)
	require.NoError(t, err)
	require.EqualValues(t, 3, restored.Version)
}
