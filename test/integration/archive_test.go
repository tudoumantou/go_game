//go:build integration

// test/integration/archive_test.go —— 赛季归档与统计落库（R8.3 / R8.4 验收）
//
// 这组测试守的是「错了会悄悄丢数据」的路径，而不是接口通不通：
//
//	R8.3  - 重复归档：done 是终态，重跑不写第二份历史
//	      - 断点恢复：running（进程在归档中途被杀）重启后重跑，UPSERT 不翻倍
//	      - 归档失败：**不切赛季**——切了旧榜 key 就没人引用了，未归档的数据
//	        会随赛季切换永久丢失
//	R8.4  - 同一窗口并发写入只留一行（唯一键 + UPSERT 覆盖语义）
//
// 时钟不注入：archive 的 now 是私有字段，同包才能改。这里改用「把
// center_season.started_at 推到 31 天前」来模拟到期——反而更贴近真实
// 事故（赛季起点是库里的持久状态，不是进程内存）。
package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"game_study/internal/center/archive"
	"game_study/internal/center/rank"
	"game_study/internal/center/stats"
	"game_study/pkg/db"
	"game_study/pkg/log"
	internalv2 "game_study/pkg/pb/internalv2"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const (
	itSeason   = "s1"
	itRankType = "arena_score"
)

func itLogger() *log.Logger { return log.New("archive-it", log.WithLevel("error")) }

// newArchiveService 组装被测服务（producer 为 nil → 跳过发奖，只验归档）。
func newArchiveService(database *db.DB, rdb *redis.Client, statsEnabled bool) (*archive.Service, *rank.Service) {
	rankSvc := rank.New(rdb, itSeason, itLogger())
	statsSvc := stats.New(rdb, itLogger())
	return archive.New(database, rankSvc, statsSvc, nil, archive.Config{
		SeasonDays:   30,
		RankTypes:    []string{itRankType},
		TopN:         100,
		GroupID:      "group-01",
		StatsEnabled: statsEnabled,
	}, itLogger()), rankSvc
}

// seedRank 往指定赛季的全局榜写 n 条（分数 100, 99, ... 保证名次确定）。
func seedRank(t *testing.T, rdb *redis.Client, season string, n int) {
	t.Helper()
	ctx := context.Background()
	key := rank.Key(rank.ScopeGlobal, season, itRankType)
	for i := 1; i <= n; i++ {
		member := rank.Member(int64(1000+i), "game-01")
		require.NoError(t, rdb.ZAdd(ctx, key, redis.Z{
			Score:  float64(100 - i),
			Member: member,
		}).Err())
	}
}

// seedSeason 写赛季起点；startedAt 给 31 天前即「已到期」。
func seedSeason(t *testing.T, database *db.DB, season string, daysAgo int) {
	t.Helper()
	started := time.Now().Add(-time.Duration(daysAgo) * 24 * time.Hour).Unix()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO center_season (id, season, started_at, updated_at) VALUES (1, ?, ?, ?)
		ON DUPLICATE KEY UPDATE season = VALUES(season), started_at = VALUES(started_at)`,
		season, started, started)
	require.NoError(t, err, "seed center_season")
}

func readSeason(t *testing.T, database *db.DB) string {
	t.Helper()
	var season string
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT season FROM center_season WHERE id = 1`).Scan(&season))
	return season
}

func readTask(t *testing.T, database *db.DB, id string) (status, rows int32) {
	t.Helper()
	err := database.QueryRowContext(context.Background(),
		`SELECT status, archived_rows FROM rank_archive_task WHERE id = ?`, id).Scan(&status, &rows)
	require.NoError(t, err, "read task %s", id)
	return status, rows
}

func countHistory(t *testing.T, database *db.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM rank_history`).Scan(&n))
	return n
}

// TestArchiveRotatesAndPersists 主路径：到期 → 归档 → 切赛季。
func TestArchiveRotatesAndPersists(t *testing.T) {
	database, rdb, _ := startStorage(t)
	svc, rankSvc := newArchiveService(database, rdb, false)

	seedRank(t, rdb, itSeason, 3)
	seedSeason(t, database, itSeason, 31)

	require.NoError(t, svc.Tick(context.Background()))

	// 历史落库：3 行，赛季号是数字 1（rank_history.season 是 INT）
	var rows, seasonNo int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT COUNT(*), MIN(season) FROM rank_history`).Scan(&rows, &seasonNo))
	require.Equal(t, 3, rows, "三条榜单记录都应进历史")
	require.Equal(t, 1, seasonNo, "s1 应映射成数字 1")

	// 名次按分数降序：100 分是第 1 名
	var first int64
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT player_id FROM rank_history WHERE rank_no = 1`).Scan(&first))
	require.EqualValues(t, 1001, first)

	// 断点标 done，并记下写入行数
	status, written := readTask(t, database, itSeason+":"+itRankType)
	require.EqualValues(t, archive.StatusDone, status)
	require.EqualValues(t, 3, written)

	// 赛季切换：库里与内存都要变
	require.Equal(t, "s2", readSeason(t, database))
	require.Equal(t, "s2", rankSvc.Season(), "内存赛季也要跟着切，否则下轮又归档一次")
}

// TestArchiveSkipsDoneTask 重复归档：done 是终态，历史不会被写第二份。
//
// 手法：跑完一次后再把赛季「退回」s1 并重新到期。这样 archiveOne 会
// 真的被调到、并且看到 done——而不是走「未到期直接返回」那条捷径。
func TestArchiveSkipsDoneTask(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()

	svc, _ := newArchiveService(database, rdb, false)
	seedRank(t, rdb, itSeason, 3)
	seedSeason(t, database, itSeason, 31)
	require.NoError(t, svc.Tick(ctx))
	before := countHistory(t, database)
	require.Equal(t, 3, before)

	// 退回 s1：模拟「已经归档过，但因为某种原因赛季状态被重置」
	seedSeason(t, database, itSeason, 31)
	svc2, rankSvc2 := newArchiveService(database, rdb, false)
	require.NoError(t, svc2.Tick(ctx))

	require.Equal(t, before, countHistory(t, database),
		"done 态必须直接跳过——重跑写第二份历史会污染赛季榜单")
	require.Equal(t, "s2", readSeason(t, database), "跳过归档但赛季仍应推进")
	require.Equal(t, "s2", rankSvc2.Season())
}

// TestArchiveResumesRunningTask 断点恢复：running（归档中途被杀）→ 重跑成功。
//
// 关键断言是「行数不翻倍」：rank_history 的唯一键
// (group_id, rank_type, season, player_id) + UPSERT 保证重跑幂等。
func TestArchiveResumesRunningTask(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()

	svc, _ := newArchiveService(database, rdb, false)
	seedRank(t, rdb, itSeason, 3)
	seedSeason(t, database, itSeason, 31)
	require.NoError(t, svc.Tick(ctx))

	// 模拟「上次归档写了一半就崩了」：任务停在 running，历史只剩一行
	_, err := database.ExecContext(ctx,
		`UPDATE rank_archive_task SET status = ? WHERE id = ?`,
		archive.StatusRunning, itSeason+":"+itRankType)
	require.NoError(t, err)
	_, err = database.ExecContext(ctx,
		`DELETE FROM rank_history WHERE rank_no > 1`)
	require.NoError(t, err)
	require.Equal(t, 1, countHistory(t, database))

	seedSeason(t, database, itSeason, 31)
	svc2, _ := newArchiveService(database, rdb, false)
	require.NoError(t, svc2.Tick(ctx))

	require.Equal(t, 3, countHistory(t, database),
		"running 态应重跑并补全缺失的名次，且已存在的行不重复")
	status, written := readTask(t, database, itSeason+":"+itRankType)
	require.EqualValues(t, archive.StatusDone, status)
	require.EqualValues(t, 3, written)
}

// TestArchiveFailureKeepsSeason 归档失败时绝不切赛季。
//
// 这是 R8.3 最要紧的一条：切了赛季，旧榜的 Redis key 就再没人引用，
// 未归档的数据会随 key 过期永久丢失。所以顺序必须是「先归档完，再切」。
//
// 造失败用关 Redis：archiveOne 写完 running 断点后调 rank.Archive 会失败。
func TestArchiveFailureKeepsSeason(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()

	svc, rankSvc := newArchiveService(database, rdb, false)
	seedRank(t, rdb, itSeason, 3)
	seedSeason(t, database, itSeason, 31)

	// Redis 不可用 → 读不到榜单 → 归档失败
	require.NoError(t, rdb.Close())

	err := svc.Tick(ctx)
	require.Error(t, err, "归档失败必须向上报错，让 leader 下一轮重试")

	require.Equal(t, itSeason, readSeason(t, database),
		"归档未成功时不能切赛季——切了就再也没机会归档这一期")
	require.Equal(t, itSeason, rankSvc.Season())

	// 断点应停在 running：下轮据此重跑，而不是当成「没跑过」重新展开
	status, _ := readTask(t, database, itSeason+":"+itRankType)
	require.EqualValues(t, archive.StatusRunning, status,
		"失败后应留下 running 断点，供重启续跑")
}

// TestFeatureStatsWindowOverwrite 同一窗口只留一行（R8.4「避免互相覆盖」）。
//
// 两个实例各报一份快照 → 合并值 30；连写两次（补跑/重跑）仍是一行，
// 值不累加成 60——线上表现是「GM 看到的统计值莫名翻倍」。
func TestFeatureStatsWindowOverwrite(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()

	// 赛季未到期：让 Tick 只走 flushStats，不触发归档
	seedSeason(t, database, itSeason, 0)

	const feature = "kill"
	// 两个中心服实例各写一份快照（field = 实例，value = 各窗口 JSON）
	require.NoError(t, rdb.HSet(ctx, "stats:group-01:"+feature, "inst-a", `{"1m":10}`).Err())
	require.NoError(t, rdb.HSet(ctx, "stats:group-01:"+feature, "inst-b", `{"1m":20}`).Err())

	// 注册 feature（Features() 返回的是内存里登记过的名字）
	statsSvc := stats.New(rdb, itLogger())
	statsSvc.Write(&internalv2.FeatureReport{Feature: feature, GroupId: "group-01",
		Ts: time.Now().Unix(), Metrics: map[string]int64{"value": 0}})

	// 用同一个 statsSvc 组装服务，保证 Features() 里有 kill
	rankSvc := rank.New(rdb, itSeason, itLogger())
	svc := archive.New(database, rankSvc, statsSvc, nil, archive.Config{
		SeasonDays: 30, RankTypes: []string{itRankType}, TopN: 100,
		GroupID: "group-01", StatsEnabled: true,
	}, itLogger())

	require.NoError(t, svc.Tick(ctx))
	require.NoError(t, svc.Tick(ctx), "重跑同一窗口不应报错")

	var rows int
	var value int64
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*), MAX(value) FROM feature_stats WHERE feature = ?`, feature).
		Scan(&rows, &value))
	require.Equal(t, 1, rows, "同一 (group, feature, window_start) 只能有一行")
	require.EqualValues(t, 30, value, "两个实例的量应合并（10+20），而不是互相覆盖")
}

// TestFeatureStatsConcurrentUpsert 多中心服并发写同一窗口，只留一行不报错。
//
// 真实部署里每个中心服都会独立 flushStats，它们的 window_start 对齐后完全
// 相同。这条测的是表的唯一键 + ON DUPLICATE KEY 在并发下也成立。
func TestFeatureStatsConcurrentUpsert(t *testing.T) {
	database, _, _ := startStorage(t)
	ctx := context.Background()

	const feature = "concurrent"
	start := time.Now().Truncate(time.Minute)
	end := start.Add(time.Minute)

	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = database.ExecContext(ctx, `
				INSERT INTO feature_stats (group_id, feature, window_start, window_end, value, updated_at)
				VALUES (?, ?, ?, ?, ?, NOW())
				ON DUPLICATE KEY UPDATE window_end = VALUES(window_end), value = VALUES(value)`,
				0, feature, start, end, int64(i+1))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "并发写第 %d 个失败", i)
	}

	var rows int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM feature_stats WHERE feature = ?`, feature).Scan(&rows))
	require.Equal(t, 1, rows, "并发写同一窗口不能产生多行")
}
