package flush

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

// TestNextTickAligned 唤醒时刻必须落在「interval 边界 + 分片偏移」，不能是「启动后 + interval」。
func TestNextTickAligned(t *testing.T) {
	now := time.Now()

	for _, offset := range []time.Duration{0, 6 * time.Second, 54 * time.Second} {
		next := nextTick(time.Minute, offset)
		require.False(t, !next.After(now), fmt.Sprintf("offset %s: next tick %s is not in the future", offset, next))
		d := next.Sub(now)
		require.False(t, d > time.Minute+offset, fmt.Sprintf("offset %s: next tick too far away: %s", offset, d))
		// 秒数必须精确等于偏移（10 片均匀铺满一分钟）
		got := next.Second()
		require.EqualValues(t, int(offset/time.Second), got, fmt.Sprintf("offset %s: next tick second = %d", offset, got))
		require.EqualValues(t, 0, next.Nanosecond())
	}
}

// TestNextTickFollowsInterval 短周期下必须按 interval 对齐，不能写死 1 分钟。
//
// 回归：曾硬编码 time.Minute，测试环境 interval=2s 时落库要等 60s，
// 表现为「改了数据但库里迟迟不变」。
func TestNextTickFollowsInterval(t *testing.T) {
	now := time.Now()
	next := nextTick(2*time.Second, 0)
	d := next.Sub(now)
	require.False(t, d > 2*time.Second+100*time.Millisecond, fmt.Sprintf("next tick with 2s interval should be within 2s, got %s", d))
	require.EqualValues(t, 0, next.Nanosecond())
}

// TestMarkDeleteFlush 墓碑在 flush 时被合批执行。
func TestMarkDeleteFlush(t *testing.T) {
	var (
		mu     sync.Mutex
		tables []string
		ids    []int64
	)
	f := New(Config{
		ShardCount: 1,
		Interval:   time.Minute,
		DeleteFn: func(_ context.Context, table string, batch []int64) error {
			mu.Lock()
			defer mu.Unlock()
			tables = append(tables, table)
			ids = append(ids, batch...)
			return nil
		},
	}, func(context.Context, int64) error { return nil }, log.New("test"))

	f.MarkDelete("mail", 1)
	f.MarkDelete("mail", 2)
	f.MarkDelete("team_member", 3)

	got := f.PendingDeletes()
	require.EqualValues(t, 3, got, fmt.Sprintf("pending deletes = %d, want 3", got))

	err := f.shards[0].flush(context.Background())
	require.NoError(t, err, fmt.Sprintf("flush: %v", err))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, ids, 3)
	count := map[string]int{}
	for _, tb := range tables {
		count[tb]++
	}
	require.EqualValues(t, 1, count["mail"], fmt.Sprintf("expect one batch per table, got %v", count))
	require.EqualValues(t, 1, count["team_member"])
	require.EqualValues(t, 0, f.PendingDeletes())
}

// TestMarkDeleteFailureKeepsTombstone 删除失败必须回填，下轮重试。
func TestMarkDeleteFailureKeepsTombstone(t *testing.T) {
	f := New(Config{
		ShardCount: 1,
		Interval:   time.Minute,
		DeleteFn: func(context.Context, string, []int64) error {
			return errors.New("db down")
		},
	}, func(context.Context, int64) error { return nil }, log.New("test"))

	f.MarkDelete("mail", 9)

	err := f.shards[0].flush(context.Background())
	require.Error(t, err, "expect flush error")
	got := f.PendingDeletes()
	require.EqualValues(t, 1, got, fmt.Sprintf("tombstone should be kept for retry, got %d", got))
}

// TestMarkDeleteWithoutFn 未注入删除器时丢弃并计数，不能无限堆积。
func TestMarkDeleteWithoutFn(t *testing.T) {
	f := New(Config{ShardCount: 1, Interval: time.Minute},
		func(context.Context, int64) error { return nil }, log.New("test"))

	f.MarkDelete("mail", 1)
	err := f.shards[0].flush(context.Background())
	require.NoError(t, err, fmt.Sprintf("flush should not fail without deleter: %v", err))
	got := f.PendingDeletes()
	require.EqualValues(t, 0, got, fmt.Sprintf("dropped tombstones should be cleared, got %d", got))
}

// TestDeleteBeforeUpsert 墓碑必须先于 upsert 执行。
//
// 顺序反了会出现「先写回旧值、再删掉」的中间态，玩家数据在库里短暂复活。
func TestDeleteBeforeUpsert(t *testing.T) {
	var order []string

	f := New(Config{
		ShardCount: 1,
		Interval:   time.Minute,
		DeleteFn: func(context.Context, string, []int64) error {
			order = append(order, "delete")
			return nil
		},
	}, func(context.Context, int64) error {
		order = append(order, "upsert")
		return nil
	}, log.New("test"))

	f.MarkDelete("mail", 1)
	f.MarkDirty(2)

	err := f.shards[0].flush(context.Background())
	require.NoError(t, err, fmt.Sprintf("flush: %v", err))
	require.Len(t, order, 2, fmt.Sprintf("order = %v, want [delete upsert]", order))
	require.EqualValues(t, "delete", order[0])
	require.EqualValues(t, "upsert", order[1])
}

// TestChunkIDs id 列表按批大小切分。
func TestChunkIDs(t *testing.T) {
	ids := []int64{1, 2, 3, 4, 5}
	batches := chunkIDs(ids, 2)
	require.Len(t, batches, 3)
	require.Len(t, batches[0], 2, fmt.Sprintf("batch sizes = %d/%d", len(batches[0]), len(batches[2])))
	require.Len(t, batches[2], 1)
	require.Len(t, chunkIDs(nil, 2), 0, "empty ids should produce no batches")
}

// TestShardIndexStable 同一 playerID 永远落同一分片（否则单实体写入无序）。
func TestShardIndexStable(t *testing.T) {
	first := ShardIndex(12345, 10)
	for i := 0; i < 100; i++ {
		got := ShardIndex(12345, 10)
		require.EqualValues(t, first, got, fmt.Sprintf("shard index unstable: %d != %d", got, first))
	}
	require.False(t, first < 0 || first >= 10, fmt.Sprintf("shard index out of range: %d", first))
}

// TestFlushAllDrainsAllBatches 关服落库必须排空全部批次，不能只落第一批。
//
// 回归背景：FlushAll 曾对每个分片只调一次 flush，脏数据超过 batch_rows 时
// 其余玩家被静默留在内存里，进程退出即丢失。
func TestFlushAllDrainsAllBatches(t *testing.T) {
	var mu sync.Mutex
	persisted := make(map[int64]int)
	f := New(Config{ShardCount: 1, Interval: time.Minute, BatchRows: 2}, func(_ context.Context, id int64) error {
		mu.Lock()
		persisted[id]++
		mu.Unlock()
		return nil
	}, log.New("test"))

	for i := int64(1); i <= 5; i++ {
		f.MarkDirty(i)
	}
	err := f.FlushAll(context.Background())
	require.NoError(t, err, fmt.Sprintf("flush all: %v", err))
	require.Len(t, persisted, 5)
	got := f.shards[0].dirtyCount()
	require.EqualValues(t, 0, got, fmt.Sprintf("dirty left after FlushAll: %d", got))
}

// TestPersistFailureKeepsDirty 保存失败必须保留脏标记，下一轮重试；
// FlushAll 不能让「持续失败」变成死循环。
func TestPersistFailureKeepsDirty(t *testing.T) {
	f := New(Config{ShardCount: 1, Interval: time.Minute, BatchRows: 2}, func(_ context.Context, _ int64) error {
		return errors.New("db down")
	}, log.New("test"))

	f.MarkDirty(1)
	err := f.FlushAll(context.Background())
	require.Error(t, err, "FlushAll must report persistence failure")
	got := f.shards[0].dirtyCount()
	require.EqualValues(t, 1, got, fmt.Sprintf("failed player must stay dirty, got %d", got))
}
