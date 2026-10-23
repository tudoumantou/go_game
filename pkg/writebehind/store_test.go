package writebehind

import (
	"context"
	"errors"
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
	if initial.Version != 0 {
		t.Fatal(initial.Version)
	}
	ids, err := store.Pending(ctx, Partition(7), 10)
	if err != nil || len(ids) != 0 {
		t.Fatalf("seed became dirty: %v %v", ids, err)
	}
	snapshot.Items[1001] = 110
	first, err := store.Commit(ctx, snapshot, 0)
	if err != nil || first != 1 {
		t.Fatalf("commit %d %v", first, err)
	}
	ids, err = store.Pending(ctx, Partition(7), 10)
	if err != nil || len(ids) != 1 || ids[0] != 7 {
		t.Fatalf("lost dirty work: %v %v", ids, err)
	}
	if _, err := store.Commit(ctx, snapshot, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update accepted: %v", err)
	}
	snapshot.Items[1001] = 109
	if _, err := store.Commit(ctx, snapshot, 1); err != nil {
		t.Fatal(err)
	}
	if err := store.Ack(ctx, 7, 1); err != nil {
		t.Fatal(err)
	}
	ids, _ = store.Pending(ctx, Partition(7), 10)
	if len(ids) != 1 {
		t.Fatal("old acknowledgement lost new work")
	}
	// 新进程通过相同 Redis 恢复全部热数据。
	restored, err := NewStore(rdb).Load(ctx, 7)
	if err != nil || restored.Version != 2 || restored.State.Items[1001] != 109 {
		t.Fatalf("restore: %+v %v", restored, err)
	}
	if err := store.Ack(ctx, 7, 2); err != nil {
		t.Fatal(err)
	}
	ids, _ = store.Pending(ctx, Partition(7), 10)
	if len(ids) != 0 {
		t.Fatal("ack did not clear work")
	}
}

func TestCorruptCacheDoesNotBecomeColdMiss(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	s := NewStore(rdb)
	ctx := context.Background()
	if _, err := s.Load(ctx, 8); !errors.Is(err, ErrMiss) {
		t.Fatal(err)
	}
	if _, err := s.Seed(ctx, Snapshot{PlayerID: 8}, 0); err != nil {
		t.Fatal(err)
	}
	rdb.HSet(ctx, s.stateKey(8), "data", "broken")
	if _, err := s.Load(ctx, 8); err == nil || errors.Is(err, ErrMiss) {
		t.Fatalf("corrupt cache hidden: %v", err)
	}
}

func TestInvalidQueueTypeDoesNotPartiallyUpdateSnapshot(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	s := NewStore(rdb)
	ctx := context.Background()
	state := Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}
	if _, err := s.Seed(ctx, state, 0); err != nil {
		t.Fatal(err)
	}
	rdb.Set(ctx, s.dirtyKey(Partition(7)), "wrong type", 0)
	state.Items[1001] = 110
	if _, err := s.Commit(ctx, state, 0); err == nil {
		t.Fatal("wrong queue type accepted")
	}
	record, err := s.Load(ctx, 7)
	if err != nil || record.Version != 0 || record.State.Items[1001] != 10 {
		t.Fatalf("partial Lua update: %+v %v", record, err)
	}
}
