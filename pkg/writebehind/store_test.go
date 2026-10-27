package writebehind

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSyncVersionAndLeaseFencing(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	store := NewStore(rdb)
	snapshot := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	initial, err := store.Seed(ctx, snapshot, 0)
	require.NoError(t, err)
	require.EqualValues(t, 0, initial.Version)

	// 提交凭会话令牌（token），不是数据版本。
	token, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	require.EqualValues(t, 1, token, "首次获取令牌应从 1 开始")

	snapshot.Items[1001] = 110
	first, err := store.Sync(ctx, snapshot.PlayerID, snapshot, snapshot.FieldNames(), nil, token)
	require.NoError(t, err, fmt.Sprintf("sync %d %v", first, err))
	require.EqualValues(t, 1, first)

	// 新会话接管：epoch+1，旧令牌立即失效——这是「防双 Actor 同时写」的核心。
	newToken, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	require.Greater(t, newToken, token)
	_, err = store.Sync(ctx, snapshot.PlayerID, snapshot, snapshot.FieldNames(), nil, token)
	require.ErrorIs(t, err, ErrConflict, "被接管后的旧令牌仍可写")

	snapshot.Items[1001] = 109
	_, err = store.Sync(ctx, snapshot.PlayerID, snapshot, snapshot.FieldNames(), nil, newToken)
	require.NoError(t, err)

	// 新进程通过相同 Redis 恢复全部热数据。
	restored, err := NewStore(rdb).Load(ctx, 7)
	require.NoError(t, err, fmt.Sprintf("restore: %+v %v", restored, err))
	require.EqualValues(t, 2, restored.Version)
	require.EqualValues(t, 109, restored.State.Items[1001])
}

func TestCorruptCacheDoesNotBecomeColdMiss(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	s := NewStore(rdb)
	ctx := context.Background()
	_, err := s.Load(ctx, 8)
	require.False(t, !errors.Is(err, ErrMiss))
	_, err = s.Seed(ctx, Snapshot{PlayerID: 8}, 0)
	require.NoError(t, err)
	// 破坏一个分字段（旧版是破坏整份 data；现在每个字段独立，破坏 items 即可）。
	rdb.HSet(ctx, s.stateKey(8), "items", "{broken")
	_, err = s.Load(ctx, 8)
	require.False(t, err == nil || errors.Is(err, ErrMiss), fmt.Sprintf("corrupt cache hidden: %v", err))
}

// TestLoadManyPipeline 批量读：命中返回完整快照，未命中不出现在结果里。
//
// 这条路径专门服务「跨玩家只读」（好友列表）：50 个 id 必须一次往返拿完，
// 且不能因为某个 id 不存在就整批失败。
func TestLoadManyPipeline(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	store := NewStore(rdb)

	// 两个有快照的玩家 + 一个没有的
	for _, id := range []int64{7, 8} {
		_, err := store.Seed(ctx, Snapshot{
			PlayerID: id,
			Profile:  Profile{Name: fmt.Sprintf("p%d", id), Level: int32(id)},
			Items:    map[int32]int64{1001: id},
		}, 1)
		require.NoError(t, err)
	}

	recs, err := store.LoadMany(ctx, []int64{7, 8, 9, 0})
	require.NoError(t, err)
	require.Len(t, recs, 2, "只有 7/8 有快照，9 与 0 不应出现")
	require.Equal(t, "p7", recs[7].State.Profile.Name)
	require.EqualValues(t, 7, recs[7].State.Items[1001])
	require.Equal(t, "p8", recs[8].State.Profile.Name)

	// 空入参直接返回，不产生 Redis 调用
	empty, err := store.LoadMany(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
}
