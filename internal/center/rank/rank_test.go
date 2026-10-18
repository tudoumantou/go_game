// internal/center/rank/rank_test.go —— 排行榜测试（miniredis，无需真实 Redis）
package rank

import (
	"context"
	"testing"

	"game_study/pkg/log"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	internalv2 "game_study/pkg/pb/internalv2"
)

func newTestService(t *testing.T) (*Service, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, "s1", log.New("test", log.WithLevel("error"))), mr
}

func write(s *Service, node string, playerID int64, score int64) {
	s.Write(&internalv2.RankReport{
		NodeId:   node,
		RankType: "arena_score",
		PlayerId: playerID,
		Score:    score,
	})
}

func TestWriteAndTopN(t *testing.T) {
	s, _ := newTestService(t)

	write(s, "game-01", 1001, 100)
	write(s, "game-01", 1002, 300)
	write(s, "game-02", 1003, 200)

	top := s.TopN("arena_score", "", 10)
	if len(top) != 3 {
		t.Fatalf("expect 3 entries, got %d", len(top))
	}
	// 按分数降序：1002(300) > 1003(200) > 1001(100)
	want := []int64{1002, 1003, 1001}
	for i, e := range top {
		if e.PlayerId != want[i] {
			t.Fatalf("rank %d: want player %d, got %d", i+1, want[i], e.PlayerId)
		}
		if e.Rank != int64(i+1) {
			t.Fatalf("rank no mismatch at %d: %d", i, e.Rank)
		}
	}
	if top[0].Score != 300 {
		t.Fatalf("top score: want 300, got %d", top[0].Score)
	}
}

func TestWriteAccumulates(t *testing.T) {
	s, _ := newTestService(t)

	// 同一玩家三次上报增量 → 累加（ZINCRBY 语义）
	write(s, "game-01", 1001, 30)
	write(s, "game-01", 1001, 30)
	write(s, "game-01", 1001, -10)

	r, score := s.RankOf("arena_score", "", 1001)
	if r != 1 {
		t.Fatalf("expect rank 1, got %d", r)
	}
	if score != 50 {
		t.Fatalf("expect accumulated 50 (30+30-10), got %d", score)
	}
}

func TestRankOfNotFound(t *testing.T) {
	s, _ := newTestService(t)
	write(s, "game-01", 1001, 100)

	r, score := s.RankOf("arena_score", "", 9999)
	if r != 0 || score != 0 {
		t.Fatalf("missing player should be rank 0 score 0, got %d/%d", r, score)
	}
}

func TestAround(t *testing.T) {
	s, _ := newTestService(t)
	for i := int64(1); i <= 10; i++ {
		write(s, "game-01", 1000+i, i*10)
	}

	// 取 1005 周边（应含自己和相邻名次）
	list := s.Around("arena_score", "", 1005, 5)
	if len(list) == 0 {
		t.Fatal("around should return entries")
	}

	found := false
	for _, e := range list {
		if e.PlayerId == 1005 {
			found = true
		}
	}
	if !found {
		t.Fatalf("around must include the target player, got %+v", list)
	}
	// 名次连续递增
	for i := 1; i < len(list); i++ {
		if list[i].Rank != list[i-1].Rank+1 {
			t.Fatalf("ranks should be consecutive: %d then %d", list[i-1].Rank, list[i].Rank)
		}
	}
}

func TestSeasonIsolation(t *testing.T) {
	s, _ := newTestService(t)

	write(s, "game-01", 1001, 100)
	s.Write(&internalv2.RankReport{
		NodeId: "game-01", RankType: "arena_score", PlayerId: 1002, Score: 500, Season: "s2",
	})

	// s1（默认赛季）只有 1001
	s1Top := s.TopN("arena_score", "s1", 10)
	if len(s1Top) != 1 || s1Top[0].PlayerId != 1001 {
		t.Fatalf("s1 should only contain player 1001, got %+v", s1Top)
	}
	// s2 只有 1002
	s2Top := s.TopN("arena_score", "s2", 10)
	if len(s2Top) != 1 || s2Top[0].PlayerId != 1002 {
		t.Fatalf("s2 should only contain player 1002, got %+v", s2Top)
	}
}

func TestArchiveAndReset(t *testing.T) {
	s, mr := newTestService(t)
	write(s, "game-01", 1001, 100)
	write(s, "game-01", 1002, 200)

	var archived []*internalv2.RankEntry
	err := s.Archive(context.Background(), "arena_score", "s1", 100, func(entries []*internalv2.RankEntry) error {
		archived = entries
		return nil
	})
	if err != nil {
		t.Fatalf("archive: %v", err)
	}
	if len(archived) != 2 || archived[0].PlayerId != 1002 {
		t.Fatalf("archive should be rank-ordered, got %+v", archived)
	}

	// 归档后清空
	if err := s.Reset(context.Background(), "arena_score", "s1"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if top := s.TopN("arena_score", "s1", 10); len(top) != 0 {
		t.Fatalf("after reset, top should be empty, got %d", len(top))
	}
	// key 应被删除
	if mr.Exists(Key(ScopeGlobal, "s1", "arena_score")) {
		t.Fatal("zset key should be deleted after reset")
	}
}

func TestKeyAndMember(t *testing.T) {
	if got := Key(ScopeGlobal, "s1", "arena"); got != "rank:global:s1:arena" {
		t.Fatalf("global key mismatch: %s", got)
	}
	if got := Key("group:7", "s1", "arena"); got != "rank:group:7:s1:arena" {
		t.Fatalf("group key mismatch: %s", got)
	}
	if got := Member(123, "game-01"); got != "123:game-01" {
		t.Fatalf("member mismatch: %s", got)
	}
	if got := Member(123, ""); got != "123:unknown" {
		t.Fatalf("member with empty server mismatch: %s", got)
	}

	pid, srv := parseMember("456:game-02")
	if pid != 456 || srv != "game-02" {
		t.Fatalf("parseMember mismatch: %d %s", pid, srv)
	}
}
