//go:build integration

// test/integration/owner_index_test.go —— 归属索引全链路（缓存架构设计 §3/§4）
//
// 用真实 MySQL + Redis 走完「一对多」的完整生命周期：
//   - 启动构建：keyset 分页取材 → ZSET → ready（含归属隔离）；
//   - 登录展开：Members 取 id → LoadBatch 批量加载 + 回填数据表；
//   - 读时自愈：删行漏清索引 → LoadBatch 报 missing → Remove 摘掉；
//   - 定时对账：漏清索引没被自愈掉 → Reconcile 数量不一致 → 重建；
//   - 整表预热：preload_all 的 Preheat 把全表搬进实体 hash。
package integration

import (
	"context"
	"testing"

	"game_study/pkg/cache"

	"github.com/stretchr/testify/require"
)

// TestOwnerIndexFullLoop 归属索引从构建到自愈的完整链路。
func TestOwnerIndexFullLoop(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()

	// 造一个玩家 + 三封自己的邮件 + 一封别人的（验证归属隔离）
	res, err := database.ExecContext(ctx,
		`INSERT INTO player (account_id, name) VALUES (?, ?)`, 9200, "owner_idx_role")
	require.NoError(t, err, "insert player")
	playerID, err := res.LastInsertId()
	require.NoError(t, err)

	otherID := playerID + 999
	for _, title := range []string{"m1", "m2", "m3"} {
		_, err := database.ExecContext(ctx,
			`INSERT INTO mail (player_id, title, content) VALUES (?, ?, ?)`,
			playerID, title, "body of "+title)
		require.NoError(t, err, "insert mail "+title)
	}
	_, err = database.ExecContext(ctx,
		`INSERT INTO mail (player_id, title) VALUES (?, ?)`, otherID, "other")
	require.NoError(t, err, "insert other mail")

	oi := cache.NewOwnerIndex(rdb, database.DB, "mail", "player_id", "id")

	// ① 启动构建：取材 → ZSET → ready
	n, err := oi.BuildOne(ctx)
	require.NoError(t, err, "build")
	require.EqualValues(t, 4, n, "四封邮件全部入索引")
	require.True(t, oi.Ready(ctx), "ready 应置位")

	// ② 归属隔离：按玩家取列表，互不串台
	ids, err := oi.Members(ctx, playerID)
	require.NoError(t, err)
	require.Len(t, ids, 3, "只有自己的三封")
	other, err := oi.Members(ctx, otherID)
	require.NoError(t, err)
	require.Len(t, other, 1)

	// ③ 登录展开：批量加载 + 回填数据表（真实 Redis）
	rows, missing, err := oi.LoadBatch(ctx, ids)
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Empty(t, missing)
	title, err := rdb.HGet(ctx, cache.EntityKey("mail", ids[0]), "title").Result()
	require.NoError(t, err, "回填后数据表可读")
	require.Equal(t, "m1", title, "回填的字段与库中一致")

	// ④ 读时自愈：模拟「删行漏清索引」——直接从库里删一封，缓存 hash 也清掉
	doomed := ids[1]
	_, err = database.ExecContext(ctx, `DELETE FROM mail WHERE id = ?`, doomed)
	require.NoError(t, err, "delete mail")
	require.NoError(t, rdb.Del(ctx, cache.EntityKey("mail", doomed)).Err(), "drop entity hash")

	rows, missing, err = oi.LoadBatch(ctx, ids)
	require.NoError(t, err)
	require.Len(t, rows, 2, "库中没有的行不进结果")
	require.Equal(t, []int64{doomed}, missing, "两处都没有 → missing（自愈输入）")

	// 自愈由调用方（登录展开）执行：摘掉残留
	require.NoError(t, oi.Index().Remove(ctx, missing...))
	exists, err := oi.Index().Exists(ctx, doomed)
	require.NoError(t, err)
	require.False(t, exists, "残留 id 应被摘掉")

	// 自愈后数量一致，Reconcile 不应触发重建
	rebuilt, err := oi.Reconcile(ctx)
	require.NoError(t, err)
	require.False(t, rebuilt, "自愈后数量一致，不需要重建")

	// ⑤ 定时对账路径：再删一封且不自愈 → 数量不一致 → 重建
	doomed2 := ids[2]
	_, err = database.ExecContext(ctx, `DELETE FROM mail WHERE id = ?`, doomed2)
	require.NoError(t, err, "delete second mail")

	rebuilt, err = oi.Reconcile(ctx)
	require.NoError(t, err)
	require.True(t, rebuilt, "数量不一致应触发重建")

	after, err := oi.Members(ctx, playerID)
	require.NoError(t, err)
	require.Len(t, after, 1, "重建后索引与库一致（只剩 m1）")
	require.True(t, oi.Ready(ctx), "重建后 ready 重新置位")

	// ⑥ 整表预热：preload_all 的 Preheat 把全表搬进实体 hash
	_, err = database.ExecContext(ctx,
		`INSERT INTO guild (name, leader_id, notice) VALUES (?, ?, ?)`,
		"owner_idx_guild", playerID, "hello")
	require.NoError(t, err, "insert guild")

	it := cache.NewIDTable(rdb, database.DB)
	preheated, err := it.Preheat(ctx, cache.TableMeta{Name: "guild", PK: "id"})
	require.NoError(t, err, "preheat")
	require.EqualValues(t, 1, preheated)

	var guildID int64
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT id FROM guild WHERE name = 'owner_idx_guild'`).Scan(&guildID))
	name, err := rdb.HGet(ctx, cache.EntityKey("guild", guildID), "name").Result()
	require.NoError(t, err, "预热后实体 hash 可读")
	require.Equal(t, "owner_idx_guild", name)

	// 再插一行，第二次预热应把它补上（幂等且增量可见）
	_, err = database.ExecContext(ctx,
		`INSERT INTO guild (name, leader_id) VALUES (?, ?)`, "owner_idx_guild2", playerID)
	require.NoError(t, err, "insert second guild")
	preheated, err = it.Preheat(ctx, cache.TableMeta{Name: "guild", PK: "id"})
	require.NoError(t, err)
	require.EqualValues(t, 2, preheated, "第二次预热补上新增行")

	var guild2 int64
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT id FROM guild WHERE name = 'owner_idx_guild2'`).Scan(&guild2))
	name2, err := rdb.HGet(ctx, cache.EntityKey("guild", guild2), "name").Result()
	require.NoError(t, err)
	require.Equal(t, "owner_idx_guild2", name2, "新增行应在第二次预热后可见")
}
