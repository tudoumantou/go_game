// internal/center/stats/stats_test.go —— 统计聚合测试（miniredis）
package stats

import (
	"context"
	"testing"
	"time"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTestService(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, log.New("test", log.WithLevel("error"))), mr
}

func TestWriteAndQuery(t *testing.T) {
	s, _ := newTestService(t)
	now := time.Now().Unix()

	for i := 0; i < 5; i++ {
		s.Write(&internalv2.FeatureReport{
			GroupId: "group-01",
			Feature: "arena_match",
			Metrics: map[string]int64{"matches": 1},
			Ts:      now,
		})
	}

	list := s.Query("group-01", "arena_match", now-600, now)
	if len(list) != 1 {
		t.Fatalf("expect 1 series, got %d", len(list))
	}
	if got := list[0].Metrics["total"]; got != 5 {
		t.Fatalf("expect total 5, got %d", got)
	}
	if list[0].Feature != "arena_match" {
		t.Fatalf("feature mismatch: %s", list[0].Feature)
	}
}

func TestQueryFilterByGroupAndFeature(t *testing.T) {
	s, _ := newTestService(t)
	now := time.Now().Unix()

	s.Write(&internalv2.FeatureReport{GroupId: "g1", Feature: "a", Metrics: map[string]int64{"v": 1}, Ts: now})
	s.Write(&internalv2.FeatureReport{GroupId: "g1", Feature: "b", Metrics: map[string]int64{"v": 2}, Ts: now})
	s.Write(&internalv2.FeatureReport{GroupId: "g2", Feature: "a", Metrics: map[string]int64{"v": 3}, Ts: now})

	all := s.Query("", "", now-60, now)
	if len(all) != 3 {
		t.Fatalf("expect 3 series, got %d", len(all))
	}

	g1 := s.Query("g1", "", now-60, now)
	if len(g1) != 2 {
		t.Fatalf("expect 2 series in g1, got %d", len(g1))
	}

	a := s.Query("", "a", now-60, now)
	if len(a) != 2 {
		t.Fatalf("expect 2 series for feature a, got %d", len(a))
	}

	one := s.Query("g2", "a", now-60, now)
	if len(one) != 1 || one[0].Metrics["total"] != 3 {
		t.Fatalf("expect g2/a total 3, got %+v", one)
	}
}

func TestWindowRollover(t *testing.T) {
	s, _ := newTestService(t)

	// 1h 窗口：当前时间片写入 10
	now := time.Now()
	s.Write(&internalv2.FeatureReport{
		GroupId: "g1", Feature: "f", Metrics: map[string]int64{"v": 10}, Ts: now.Unix(),
	})
	// 60 分钟前的写入应落在不同时间片，不计入当前 1h 窗口
	old := now.Add(-70 * time.Minute)
	s.Write(&internalv2.FeatureReport{
		GroupId: "g1", Feature: "f", Metrics: map[string]int64{"v": 99}, Ts: old.Unix(),
	})

	list := s.Query("g1", "f", now.Add(-time.Hour).Unix(), now.Unix())
	if len(list) != 1 {
		t.Fatalf("expect 1 series, got %d", len(list))
	}
	if got := list[0].Metrics["total"]; got != 10 {
		t.Fatalf("1h window should only count recent writes (10), got %d", got)
	}
}

func TestSeriesCountAndFeatures(t *testing.T) {
	s, _ := newTestService(t)
	now := time.Now().Unix()

	if s.SeriesCount() != 0 {
		t.Fatalf("initial series should be 0, got %d", s.SeriesCount())
	}

	s.Write(&internalv2.FeatureReport{GroupId: "g1", Feature: "a", Metrics: map[string]int64{"v": 1}, Ts: now})
	s.Write(&internalv2.FeatureReport{GroupId: "g1", Feature: "b", Metrics: map[string]int64{"v": 1}, Ts: now})

	if s.SeriesCount() != 2 {
		t.Fatalf("expect 2 series, got %d", s.SeriesCount())
	}
	feats := s.Features()
	if len(feats) != 2 || feats[0] != "a" || feats[1] != "b" {
		t.Fatalf("features mismatch: %v", feats)
	}
}

func TestSnapshotWritesRedis(t *testing.T) {
	s, mr := newTestService(t)
	now := time.Now().Unix()

	s.Write(&internalv2.FeatureReport{
		GroupId: "g1", Feature: "arena", Metrics: map[string]int64{"v": 7}, Ts: now,
	})

	if err := s.Snapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// 快照按实例写一个字段，字段值是该实例各窗口的 JSON（R8.4：
	// 多实例各写各的字段，读取时求和，不再互相覆盖）。
	key := "stats:g1:arena"
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	vals, err := rdb.HGetAll(context.Background(), key).Result()
	require.NoError(t, err, "hgetall")
	if len(vals) != 1 {
		t.Fatalf("expect 1 instance field, got %d: %v", len(vals), vals)
	}
	merged := s.MergedMetrics(context.Background(), "g1", "arena")
	for _, w := range []string{Window1m, Window5m, Window1h, Window1d} {
		if _, ok := merged[w]; !ok {
			t.Fatalf("snapshot missing window %s: %v", w, merged)
		}
	}
	if merged[Window1h] != 7 {
		t.Fatalf("1h snapshot: want 7, got %d", merged[Window1h])
	}
}

func TestSnapshotEmptyNoError(t *testing.T) {
	s, _ := newTestService(t)
	if err := s.Snapshot(context.Background()); err != nil {
		t.Fatalf("empty snapshot should not error: %v", err)
	}
}

// TestSnapshotMergedAcrossInstances 多实例快照求和而不是互相覆盖（R8.4）。
func TestSnapshotMergedAcrossInstances(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	a := New(rdb, log.New("test", log.WithLevel("error")))
	a.SetInstanceID("center-a")
	b := New(rdb, log.New("test", log.WithLevel("error")))
	b.SetInstanceID("center-b")

	rep := func() *internalv2.FeatureReport {
		return &internalv2.FeatureReport{
			GroupId: "group-01",
			Feature: "battle",
			Metrics: map[string]int64{"count": 5},
			Ts:      time.Now().Unix(),
		}
	}
	a.Write(rep())
	b.Write(rep())

	ctx := context.Background()
	if err := a.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot a: %v", err)
	}
	if err := b.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot b: %v", err)
	}

	if n := a.InstanceCount(ctx, "group-01", "battle"); n != 2 {
		t.Fatalf("instance fields = %d, want 2（覆盖会退化成 1）", n)
	}
	merged := a.MergedMetrics(ctx, "group-01", "battle")
	// 两个实例各上报 5，合并后必须是 10。
	if merged[Window1h] != 10 {
		t.Fatalf("merged 1h = %d, want 10", merged[Window1h])
	}
}
