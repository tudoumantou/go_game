// internal/center/rank/rank_test.go —— 排行榜测试（miniredis，无需真实 Redis）
package rank

import (
	"context"
	"fmt"
	"testing"

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
	require.Len(t, top, 3)
	// 按分数降序：1002(300) > 1003(200) > 1001(100)
	want := []int64{1002, 1003, 1001}
	for i, e := range top {
		require.EqualValues(t, want[i], e.PlayerId)
		require.EqualValues(t, int64(i+1), e.Rank)
	}
	require.EqualValues(t, 300, top[0].Score)
}

func TestWriteAccumulates(t *testing.T) {
	s, _ := newTestService(t)

	// 同一玩家三次上报增量 → 累加（ZINCRBY 语义）
	write(s, "game-01", 1001, 30)
	write(s, "game-01", 1001, 30)
	write(s, "game-01", 1001, -10)

	r, score := s.RankOf("arena_score", "", 1001)
	require.EqualValues(t, 1, r)
	require.EqualValues(t, 50, score)
}

func TestRankOfNotFound(t *testing.T) {
	s, _ := newTestService(t)
	write(s, "game-01", 1001, 100)

	r, score := s.RankOf("arena_score", "", 9999)
	require.EqualValues(t, 0, r, fmt.Sprintf("missing player should be rank 0 score 0, got %d/%d", r, score))
	require.EqualValues(t, 0, score)
}

func TestAround(t *testing.T) {
	s, _ := newTestService(t)
	for i := int64(1); i <= 10; i++ {
		write(s, "game-01", 1000+i, i*10)
	}

	// 取 1005 周边（应含自己和相邻名次）
	list := s.Around("arena_score", "", 1005, 5)
	require.NotEmpty(t, list, "around should return entries")

	found := false
	for _, e := range list {
		if e.PlayerId == 1005 {
			found = true
		}
	}
	require.True(t, found)
	// 名次连续递增
	for i := 1; i < len(list); i++ {
		require.EqualValues(t, list[i-1].Rank+1, list[i].Rank)
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
	require.Len(t, s1Top, 1, fmt.Sprintf("s1 should only contain player 1001, got %+v", s1Top))
	require.EqualValues(t, 1001, s1Top[0].PlayerId)
	// s2 只有 1002
	s2Top := s.TopN("arena_score", "s2", 10)
	require.Len(t, s2Top, 1, fmt.Sprintf("s2 should only contain player 1002, got %+v", s2Top))
	require.EqualValues(t, 1002, s2Top[0].PlayerId)
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
	require.NoError(t, err, "archive")
	require.Len(t, archived, 2, fmt.Sprintf("archive should be rank-ordered, got %+v", archived))
	require.EqualValues(t, 1002, archived[0].PlayerId)

	// 归档后清空
	err = s.Reset(context.Background(), "arena_score", "s1")
	require.NoError(t, err, fmt.Sprintf("reset: %v", err))
	top := s.TopN("arena_score", "s1", 10)
	require.Empty(t, top, fmt.Sprintf("after reset, top should be empty, got %d", len(top)))
	// key 应被删除
	require.False(t, mr.Exists(Key(ScopeGlobal, "s1", "arena_score")), "zset key should be deleted after reset")
}

func TestKeyAndMember(t *testing.T) {
	got := Key(ScopeGlobal, "s1", "arena")
	require.EqualValues(t, "rank:global:s1:arena", got, fmt.Sprintf("global key mismatch: %s", got))
	got = Key("group:7", "s1", "arena")
	require.EqualValues(t, "rank:group:7:s1:arena", got, fmt.Sprintf("group key mismatch: %s", got))
	got = Member(123, "game-01")
	require.EqualValues(t, "123:game-01", got, fmt.Sprintf("member mismatch: %s", got))
	got = Member(123, "")
	require.EqualValues(t, "123:unknown", got, fmt.Sprintf("member with empty server mismatch: %s", got))

	pid, srv := parseMember("456:game-02")
	require.EqualValues(t, 456, pid, fmt.Sprintf("parseMember mismatch: %d %s", pid, srv))
	require.EqualValues(t, "game-02", srv)
}

// TestWriteDedupByBizID 同一业务键重投只加一次分（R8.2）。
func TestWriteDedupByBizID(t *testing.T) {
	s, _ := newTestService(t)
	rep := &internalv2.RankReport{
		NodeId:   "game-1",
		RankType: "arena_score",
		PlayerId: 5001,
		Score:    30,
		BizId:    "battle:9001:5001",
	}
	s.Write(rep)
	s.Write(rep) // 重投
	s.Write(rep) // 再重投

	_, score := s.RankOf("arena_score", "s1", 5001)
	require.EqualValues(t, 30, score, fmt.Sprintf("score = %d, want 30（重投不能重复加分）", score))

	// 换一个业务键（新的一场结算）应当继续累加。
	rep.BizId = "battle:9002:5001"
	s.Write(rep)
	_, score = s.RankOf("arena_score", "s1", 5001)
	require.EqualValues(t, 60, score, fmt.Sprintf("score = %d, want 60", score))
}

// TestWriteWithoutBizIDAccumulates 无业务键的上报保持「每次都累加」的旧语义。
func TestWriteWithoutBizIDAccumulates(t *testing.T) {
	s, _ := newTestService(t)
	write(s, "game-1", 5002, 10)
	write(s, "game-1", 5002, 10)
	_, score := s.RankOf("arena_score", "s1", 5002)
	require.EqualValues(t, 20, score, fmt.Sprintf("score = %d, want 20", score))
}

// TestQueryTopMineAround 一次查询同时给出 Top-N、我的名次与周边。
func TestQueryTopMineAround(t *testing.T) {
	s, _ := newTestService(t)
	write(s, "game-1", 1, 100)
	write(s, "game-1", 2, 200)
	write(s, "game-1", 3, 300)

	resp := s.Query("arena_score", "", 2, 10, 0)
	require.EqualValues(t, "s1", resp.Season)
	require.Len(t, resp.Top, 3, fmt.Sprintf("top = %+v", resp.Top))
	require.EqualValues(t, 3, resp.Top[0].PlayerId)
	require.EqualValues(t, 1, resp.Top[0].Rank)
	require.False(t, !resp.OnBoard || resp.MyRank != 2 || resp.MyScore != 200, fmt.Sprintf("mine = on_board:%v rank:%d score:%d", resp.OnBoard, resp.MyRank, resp.MyScore))
	require.NotEmpty(t, resp.Around, "around must contain the player and neighbours")
	require.NotEqualValues(t, 0, resp.UpdatedAt, "updated_at should be set after a write")
}

// TestQueryNotOnBoard 未上榜时 on_board=false、不给周边（没有「周边」可言）。
func TestQueryNotOnBoard(t *testing.T) {
	s, _ := newTestService(t)
	write(s, "game-1", 1, 100)

	resp := s.Query("arena_score", "", 999, 10, 0)
	require.False(t, resp.OnBoard || resp.MyRank != 0 || resp.MyScore != 0, fmt.Sprintf("unexpected mine: %+v", resp))
	require.Len(t, resp.Around, 0)
	require.Len(t, resp.Top, 1)
}

// TestQueryTopNCapped 客户端协议上限 100，避免一次拉全服。
func TestQueryTopNCapped(t *testing.T) {
	s, _ := newTestService(t)
	for i := int64(1); i <= 5; i++ {
		write(s, "game-1", i, i*10)
	}
	got := len(s.Query("arena_score", "", 1, 1000, 0).Top)
	require.EqualValues(t, 5, got, fmt.Sprintf("top = %d, want 5（只有 5 条数据）", got))
}
