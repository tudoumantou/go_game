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
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module/mail"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/cache"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	gamev1 "game_study/pkg/pb/game/v1"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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

// TestMailIndexThroughModule 「索引有真实调用方」的验收点。
//
// 前一条测试直接操作 ZSET，证明的是「索引层自己能跑」；这一条走真实模块，
// 证明的是「业务真的在用它」：
//
//	mail.Deliver（Kafka 消费路径）→ 落库 + ZADD
//	  → oi.Members 走 ZSET 拿到 id（ready 已置位，不打库）
//	  → oi.Expand 展开出完整行（缓存未命中时的回源 + 回填）
//
// 顺带验一件事：时间列经 EntityRow 的字符串化之后还能还原。这是 mail
// 列表显示「发送时间」的前提，也是缓存层用 map[string]string 装动态列的
// 必然代价——不显式格式化就会变成 "2026-10-26 22:00:00 +0800 CST"，
// 谁都解析不了。
func TestMailIndexThroughModule(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()

	res, err := database.ExecContext(ctx,
		`INSERT INTO player (account_id, name) VALUES (?, ?)`, 9300, "mail_idx_role")
	require.NoError(t, err, "insert player")
	playerID, err := res.LastInsertId()
	require.NoError(t, err)

	oi := cache.NewOwnerIndex(rdb, database.DB, "mail", "player_id", "id")
	m := mail.New(mail.NewRepo(database), log.New("it-mail", log.WithLevel("error")))
	m.SetOwnerIndex(oi)

	// 模拟「索引已就绪」：真实环境里由 BuildOne 置位，这里只要 ZADD 生效。
	require.NoError(t, rdb.Set(ctx, cache.OwnerIndexReadyKey("mail"), "1", 0).Err())

	id1, err := m.Deliver(ctx, &pkgmq.MailMsg{PlayerID: playerID, Title: "first"})
	require.NoError(t, err, "deliver 1")
	id2, err := m.Deliver(ctx, &pkgmq.MailMsg{PlayerID: playerID, Title: "second"})
	require.NoError(t, err, "deliver 2")
	require.NotEqual(t, id1, id2)

	// ① 索引里能按玩家捞到这两封（走 ZSET，不打库）
	ids, err := oi.Members(ctx, playerID)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{id1, id2}, ids, "投递后索引里应有两封")

	// ② 展开：缓存未命中 → 一条 IN 回源 → 回填
	rows, removed, err := oi.Expand(ctx, playerID)
	require.NoError(t, err)
	require.Empty(t, removed, "刚投递的邮件不该被当成残留")
	require.Len(t, rows, 2)

	byID := make(map[int64]cache.EntityRow, len(rows))
	for _, r := range rows {
		byID[r.ID] = r
	}
	require.Equal(t, "first", byID[id1].Fields["title"])
	require.Equal(t, "second", byID[id2].Fields["title"])

	// ③ 时间列：RFC3339，且往返后与「直接查库读到的时刻」完全相同。
	//
	// 断言对象刻意选 DB 的原值而不是 time.Now()：这里要验的是**缓存往返无损**，
	// 而不是 DB 时区对不对。两者是两件事——DSN 没配 loc 时，驱动会把库里的
	// 时间当本地时区解析，SQL 路径与缓存路径会一起偏（同一个偏差，不是缓存
	// 引入的）。拿 time.Now() 比会把配置问题算到缓存头上。
	raw := byID[id1].Fields["created_at"]
	require.NotEmpty(t, raw, "created_at 必须被缓存")
	got, err := time.Parse(time.RFC3339, raw)
	require.NoError(t, err, "created_at 应是 RFC3339，实际 %q", raw)

	var fromDB time.Time
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT created_at FROM mail WHERE id = ?`, id1).Scan(&fromDB))
	require.True(t, fromDB.Equal(got),
		"缓存往返不能改变时刻：db=%v cache=%v（若差 8 小时，检查 DSN 的 loc 参数）", fromDB, got)

	// ④ 行还在但字段变了：DropEntity 后重新展开必须拿到库里的新值。
	//
	// 这是「领取邮件附件」的模型——status 由 0 变 2。不删缓存的话下次
	// 列表读到的是领取前的状态，而缓存条目没有 TTL，不会自己恢复。
	_, err = database.ExecContext(ctx, `UPDATE mail SET status = 2 WHERE id = ?`, id1)
	require.NoError(t, err)
	require.NoError(t, oi.DropEntity(ctx, id1))

	refreshed, _, err := oi.Expand(ctx, playerID)
	require.NoError(t, err)
	byID = make(map[int64]cache.EntityRow, len(refreshed))
	for _, r := range refreshed {
		byID[r.ID] = r
	}
	require.Equal(t, "2", byID[id1].Fields["status"],
		"Drop 之后必须回源拿到最新 status（0→2）")

	// 索引没被一起删掉：邮件还在，只是内容刷新了
	stillIndexed, err := oi.Members(ctx, playerID)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{id1, id2}, stillIndexed,
		"DropEntity 只丢缓存行，不能摘索引")

	// ⑤ 回填生效：清掉 ready 后 Members 回落库仍一致，说明数据表与库同源
	require.NoError(t, rdb.Del(ctx, cache.OwnerIndexReadyKey("mail")).Err())
	fallback, err := oi.Members(ctx, playerID)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{id1, id2}, fallback, "回落库结果应与索引一致")

	// ⑥ 删除：走真实模块（事务 + 归属校验），库 / 索引 / 缓存三处一起清。
	// 这是 RemoveEntity 的唯一真实调用方，必须验证「三处同时清」而不是只有库。
	require.NoError(t, rdb.Set(ctx, cache.OwnerIndexReadyKey("mail"), "1", 0).Err())
	st := moduletest.NewState(playerID)
	rec := moduletest.NewRecorder()
	mctx := moduletest.NewContext(st, rec)
	delRaw, err := moduletest.Call(m, mctx, rec, codec.MsgMailDeleteReq, codec.MsgMailDeleteResp,
		&gamev1.MailDeleteReq{MailId: id2})
	require.NoError(t, err, "call mail delete")
	delResp := &gamev1.MailDeleteResp{}
	require.NoError(t, proto.Unmarshal(delRaw, delResp))
	require.EqualValues(t, int32(errcode.OK), delResp.Code, "无附件应允许删除")

	var left int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mail WHERE id = ?`, id2).Scan(&left))
	require.Zero(t, left, "库里必须删掉")

	after, err := oi.Members(ctx, playerID)
	require.NoError(t, err)
	require.Equal(t, []int64{id1}, after, "索引必须摘掉")
	require.Zero(t, rdb.Exists(ctx, cache.EntityKey("mail", id2)).Val(), "缓存行必须清掉")
}
