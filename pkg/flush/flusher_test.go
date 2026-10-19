package flush

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"game_study/pkg/log"
)

// TestNextTickAligned 唤醒时刻必须落在「interval 边界 + 分片偏移」，不能是「启动后 + interval」。
func TestNextTickAligned(t *testing.T) {
	now := time.Now()

	for _, offset := range []time.Duration{0, 6 * time.Second, 54 * time.Second} {
		next := nextTick(time.Minute, offset)
		if !next.After(now) {
			t.Fatalf("offset %s: next tick %s is not in the future", offset, next)
		}
		if d := next.Sub(now); d > time.Minute+offset {
			t.Fatalf("offset %s: next tick too far away: %s", offset, d)
		}
		// 秒数必须精确等于偏移（10 片均匀铺满一分钟）
		if got := next.Second(); got != int(offset/time.Second) {
			t.Fatalf("offset %s: next tick second = %d", offset, got)
		}
		if next.Nanosecond() != 0 {
			t.Fatalf("offset %s: next tick has sub-second component: %s", offset, next)
		}
	}
}

// TestNextTickFollowsInterval 短周期下必须按 interval 对齐，不能写死 1 分钟。
//
// 回归：曾硬编码 time.Minute，测试环境 interval=2s 时落库要等 60s，
// 表现为「改了数据但库里迟迟不变」。
func TestNextTickFollowsInterval(t *testing.T) {
	now := time.Now()
	next := nextTick(2*time.Second, 0)
	if d := next.Sub(now); d > 2*time.Second+100*time.Millisecond {
		t.Fatalf("next tick with 2s interval should be within 2s, got %s", d)
	}
	if next.Nanosecond() != 0 {
		t.Fatalf("next tick should align to interval boundary, got %s", next)
	}
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

	if got := f.PendingDeletes(); got != 3 {
		t.Fatalf("pending deletes = %d, want 3", got)
	}

	if err := f.shards[0].flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(ids) != 3 {
		t.Fatalf("deleted %d ids, want 3", len(ids))
	}
	count := map[string]int{}
	for _, tb := range tables {
		count[tb]++
	}
	if count["mail"] != 1 || count["team_member"] != 1 {
		t.Fatalf("expect one batch per table, got %v", count)
	}
	if f.PendingDeletes() != 0 {
		t.Fatalf("pending deletes should be cleared, got %d", f.PendingDeletes())
	}
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

	if err := f.shards[0].flush(context.Background()); err == nil {
		t.Fatal("expect flush error")
	}
	if got := f.PendingDeletes(); got != 1 {
		t.Fatalf("tombstone should be kept for retry, got %d", got)
	}
}

// TestMarkDeleteWithoutFn 未注入删除器时丢弃并计数，不能无限堆积。
func TestMarkDeleteWithoutFn(t *testing.T) {
	f := New(Config{ShardCount: 1, Interval: time.Minute},
		func(context.Context, int64) error { return nil }, log.New("test"))

	f.MarkDelete("mail", 1)
	if err := f.shards[0].flush(context.Background()); err != nil {
		t.Fatalf("flush should not fail without deleter: %v", err)
	}
	if got := f.PendingDeletes(); got != 0 {
		t.Fatalf("dropped tombstones should be cleared, got %d", got)
	}
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

	if err := f.shards[0].flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if len(order) != 2 || order[0] != "delete" || order[1] != "upsert" {
		t.Fatalf("order = %v, want [delete upsert]", order)
	}
}

// TestChunkIDs id 列表按批大小切分。
func TestChunkIDs(t *testing.T) {
	ids := []int64{1, 2, 3, 4, 5}
	batches := chunkIDs(ids, 2)
	if len(batches) != 3 {
		t.Fatalf("batches = %d, want 3", len(batches))
	}
	if len(batches[0]) != 2 || len(batches[2]) != 1 {
		t.Fatalf("batch sizes = %d/%d", len(batches[0]), len(batches[2]))
	}
	if len(chunkIDs(nil, 2)) != 0 {
		t.Fatal("empty ids should produce no batches")
	}
}

// TestShardIndexStable 同一 playerID 永远落同一分片（否则单实体写入无序）。
func TestShardIndexStable(t *testing.T) {
	first := ShardIndex(12345, 10)
	for i := 0; i < 100; i++ {
		if got := ShardIndex(12345, 10); got != first {
			t.Fatalf("shard index unstable: %d != %d", got, first)
		}
	}
	if first < 0 || first >= 10 {
		t.Fatalf("shard index out of range: %d", first)
	}
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
	if err := f.FlushAll(context.Background()); err != nil {
		t.Fatalf("flush all: %v", err)
	}
	if len(persisted) != 5 {
		t.Fatalf("persisted %d players, want 5", len(persisted))
	}
	if got := f.shards[0].dirtyCount(); got != 0 {
		t.Fatalf("dirty left after FlushAll: %d", got)
	}
}

// TestPersistFailureKeepsDirty 保存失败必须保留脏标记，下一轮重试；
// FlushAll 不能让「持续失败」变成死循环。
func TestPersistFailureKeepsDirty(t *testing.T) {
	f := New(Config{ShardCount: 1, Interval: time.Minute, BatchRows: 2}, func(_ context.Context, _ int64) error {
		return errors.New("db down")
	}, log.New("test"))

	f.MarkDirty(1)
	if err := f.FlushAll(context.Background()); err == nil {
		t.Fatal("FlushAll must report persistence failure")
	}
	if got := f.shards[0].dirtyCount(); got != 1 {
		t.Fatalf("failed player must stay dirty, got %d", got)
	}
}
