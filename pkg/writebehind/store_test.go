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

func TestCacheCommitAndVersionedAcknowledgement(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	store := NewStore(rdb)
	snapshot := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	initial, err := store.Seed(ctx, snapshot, 0)
	require.NoError(t, err)
	require.EqualValues(t, 0, initial.Version)
	ids, err := store.Pending(ctx, Partition(7), 10)
	require.NoError(t, err, fmt.Sprintf("seed became dirty: %v %v", ids, err))
	require.Empty(t, ids)
	// R2.5：提交凭会话令牌（token），不再是数据版本。
	token, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	require.EqualValues(t, 1, token, "首次获取令牌应从 1 开始")

	snapshot.Items[1001] = 110
	first, err := store.Commit(ctx, snapshot.PlayerID, snapshot, snapshot.FieldNames(), nil, token)
	require.NoError(t, err, fmt.Sprintf("commit %d %v", first, err))
	require.EqualValues(t, 1, first)
	ids, err = store.Pending(ctx, Partition(7), 10)
	require.NoError(t, err, fmt.Sprintf("lost dirty work: %v %v", ids, err))
	require.Len(t, ids, 1)
	require.EqualValues(t, 7, ids[0])

	// 新会话接管：epoch+1，旧令牌立即失效——这是「防双 Actor 同时写」的核心。
	newToken, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	require.Greater(t, newToken, token)
	_, err = store.Commit(ctx, snapshot.PlayerID, snapshot, snapshot.FieldNames(), nil, token)
	require.False(t, !errors.Is(err, ErrConflict), fmt.Sprintf("被接管后的旧令牌仍可写: %v", err))
	snapshot.Items[1001] = 109
	_, err = store.Commit(ctx, snapshot.PlayerID, snapshot, snapshot.FieldNames(), nil, newToken)
	require.NoError(t, err)
	err = store.Ack(ctx, 7, 1)
	require.NoError(t, err)
	ids, _ = store.Pending(ctx, Partition(7), 10)
	require.Len(t, ids, 1, "old acknowledgement lost new work")
	// 新进程通过相同 Redis 恢复全部热数据。
	restored, err := NewStore(rdb).Load(ctx, 7)
	require.NoError(t, err, fmt.Sprintf("restore: %+v %v", restored, err))
	require.EqualValues(t, 2, restored.Version)
	require.EqualValues(t, 109, restored.State.Items[1001])
	err = store.Ack(ctx, 7, 2)
	require.NoError(t, err)
	ids, _ = store.Pending(ctx, Partition(7), 10)
	require.Len(t, ids, 0, "ack did not clear work")
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

func TestInvalidQueueTypeDoesNotPartiallyUpdateSnapshot(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	s := NewStore(rdb)
	ctx := context.Background()
	state := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	_, err := s.Seed(ctx, state, 0)
	require.NoError(t, err)
	rdb.Set(ctx, s.dirtyKey(Partition(7)), "wrong type", 0)
	state.Items[1001] = 110
	_, err = s.Commit(ctx, state.PlayerID, state, state.FieldNames(), nil, 0)
	require.Error(t, err, "wrong queue type accepted")
	record, err := s.Load(ctx, 7)
	require.NoError(t, err, fmt.Sprintf("partial Lua update: %+v %v", record, err))
	require.EqualValues(t, 0, record.Version)
	require.EqualValues(t, 10, record.State.Items[1001])
}
