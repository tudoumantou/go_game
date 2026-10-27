//go:build integration

// test/integration/p10_p11_storage_test.go —— P10 数据层两条主线
//
// P10（12.5）ID 表与回源：
//   - 启动构建：keyset 分页扫库 → Redis Set，置 ready；
//   - 存在性判断：ready 后走 Redis（不打库），未 ready 回落 MySQL；
//   - 创建/删除同步：创角后集合里有新 id，物理删除后集合里消失；
//   - 回源：idtab 有、实体 hash 无 → 读库 + 回填 Redis。
//
// P11（12.4.4 / 12.4.5）落库优化：
//   - 道具从「先删后插全量」改为「只写变更项 + 合批 UPSERT」；
//   - 数量归零的道具走 DELETE 而不是写 0 行；
//   - 墓碑队列合批删除。
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/log"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// startStorage 起 MySQL + Redis 并初始化 schema，返回 DB、Redis 客户端与 DSN。
func startStorage(t *testing.T) (*db.DB, *redis.Client, string) {
	t.Helper()

	// 容器与 Redis 由 TestMain 起一次共享；这里只取本用例的独立库。
	dsn := freshEnv(t)

	database, err := db.New(db.Config{DSN: dsn, MaxOpen: 10, MaxIdle: 5})
	require.NoError(t, err, "connect mysql")
	t.Cleanup(func() { _ = database.Close() })

	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	t.Cleanup(func() { _ = rdb.Close() })

	return database, rdb, dsn
}

// TestP10IDTable 构建 → 判断 → 同步 → 回源全链路。
func TestP10IDTable(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()

	// 造两个角色（直接插库，绕过 service 以便控制 id）
	for i := 0; i < 3; i++ {
		if _, err := database.ExecContext(ctx,
			`INSERT INTO player (account_id, name, level, exp, coin, scene_id) VALUES (?, ?, 1, 0, 0, 0)`,
			9000+i, "idtab_role_"+string(rune('a'+i))); err != nil {
			t.Fatalf("insert player %d: %v", i, err)
		}
	}

	it := cache.NewIDTable(rdb, database.DB)
	meta := cache.TableMeta{Name: "player", PK: "id"}

	// ① 构建
	n, err := it.BuildOne(ctx, meta)
	require.NoError(t, err, "build")
	require.False(t, n < 3, fmt.Sprintf("built %d ids, want >= 3", n))
	require.False(t, !it.Ready(ctx, "player"), "ready flag not set after build")

	// ② 存在性：ready 后不再打库
	var playerID int64
	if err := database.QueryRowContext(ctx,
		`SELECT id FROM player WHERE name = 'idtab_role_a'`).Scan(&playerID); err != nil {
		t.Fatalf("query player id: %v", err)
	}
	exists, err := it.Exists(ctx, "player", playerID)
	require.NoError(t, err, fmt.Sprintf("exists(%d) = %v, %v", playerID, exists, err))
	require.True(t, exists)
	exists, err = it.Exists(ctx, "player", 99999999)
	require.False(t, err != nil || exists, fmt.Sprintf("exists(nonexistent) = %v, %v", exists, err))

	// ③ 创建同步：新 id 登记后立即可见
	if _, err := database.ExecContext(ctx,
		`INSERT INTO player (account_id, name, level, exp, coin, scene_id) VALUES (9991, 'idtab_role_new', 1, 0, 0, 0)`); err != nil {
		t.Fatalf("insert new player: %v", err)
	}
	var newID int64
	if err := database.QueryRowContext(ctx,
		`SELECT id FROM player WHERE name = 'idtab_role_new'`).Scan(&newID); err != nil {
		t.Fatalf("query new id: %v", err)
	}
	err = it.Add(ctx, "player", newID)
	require.NoError(t, err, fmt.Sprintf("add: %v", err))
	ok, _ := it.Exists(ctx, "player", newID)
	require.True(t, ok, "newly added id should exist")

	// ④ 删除同步
	_, err = database.ExecContext(ctx, `DELETE FROM player WHERE id = ?`, newID)
	require.NoError(t, err, fmt.Sprintf("delete player: %v", err))
	err = it.Remove(ctx, "player", newID)
	require.NoError(t, err, fmt.Sprintf("remove: %v", err))
	ok, _ = it.Exists(ctx, "player", newID)
	require.False(t, ok, "removed id should not exist")

	// ⑤ 对账：集合与表行数一致 → 不重建
	rebuilt, err := it.Reconcile(ctx, meta)
	require.NoError(t, err, "reconcile")
	require.False(t, rebuilt, "should not rebuild when counts match")

	// ⑥ 人为制造不一致 → 对账触发重建
	err = rdb.SRem(ctx, cache.IDTabKey("player"), playerID).Err()
	require.NoError(t, err, fmt.Sprintf("srem: %v", err))
	rebuilt, err = it.Reconcile(ctx, meta)
	require.NoError(t, err, "reconcile 2")
	require.True(t, rebuilt, "expect rebuild after manual removal")
	ok, _ = it.Exists(ctx, "player", playerID)
	require.True(t, ok, "id should be back after rebuild")

	// ⑦ 回源：清掉实体 hash，LoadEntity 应从库里读并回填
	key := cache.EntityKey("player", playerID)
	_ = rdb.Del(ctx, key).Err()
	row, err := it.LoadEntity(ctx, meta, playerID)
	require.NoError(t, err, "load entity")
	require.EqualValues(t, "idtab_role_a", row["name"])
	n, _ = rdb.HLen(ctx, key).Result()
	require.NotEqualValues(t, 0, n, "entity hash should be filled back")

	// ⑧ 不存在的 id 直接返回 ErrNotFound（不打库）
	_, err = it.LoadEntity(ctx, meta, 99999999)
	require.EqualValues(t, cache.ErrNotFound, err, fmt.Sprintf("err = %v, want ErrNotFound", err))
}

// TestP11IncrementalFlush 道具增量合批落库。
func TestP11IncrementalFlush(t *testing.T) {
	database, _, _ := startStorage(t)
	ctx := context.Background()

	// 造一个角色 + 两格初始道具
	res, err := database.ExecContext(ctx,
		`INSERT INTO player (account_id, name, level, exp, coin, scene_id) VALUES (7001, 'p11_role', 1, 0, 0, 0)`)
	require.NoError(t, err, "insert player")
	playerID, _ := res.LastInsertId()
	if _, err := database.ExecContext(ctx,
		`INSERT INTO player_item (player_id, item_id, count) VALUES (?, 1001, 10), (?, 1002, 20)`,
		playerID, playerID); err != nil {
		t.Fatalf("seed items: %v", err)
	}

	repo := player.NewRepository(database)

	// ① 加载：SeedItem 不标脏 → 直接保存不应产生任何道具写
	state, err := repo.Load(ctx, playerID)
	require.NoError(t, err, "load")
	require.EqualValues(t, 10, state.GetItemCount(1001), fmt.Sprintf("loaded items = %d/%d", state.GetItemCount(1001), state.GetItemCount(1002)))
	require.EqualValues(t, 20, state.GetItemCount(1002))
	dirty := state.DirtyItems()
	require.Empty(t, dirty, fmt.Sprintf("fresh load should have no dirty items, got %v", dirty))

	err = repo.Save(ctx, state)
	require.NoError(t, err, fmt.Sprintf("save unchanged: %v", err))
	n := countItemRows(t, database, playerID)
	require.EqualValues(t, 2, n, fmt.Sprintf("row count after no-op save = %d, want 2", n))

	// ② 只写变更项：改 1001，1002 不动
	state.AddItem(1001, 5)
	dirty = state.DirtyItems()
	require.Len(t, dirty, 1, fmt.Sprintf("dirty items = %v, want {1001:15}", dirty))
	require.EqualValues(t, 15, dirty[1001])
	err = repo.Save(ctx, state)
	require.NoError(t, err, fmt.Sprintf("save increment: %v", err))
	got := itemCount(t, database, playerID, 1001)
	require.EqualValues(t, 15, got, fmt.Sprintf("item 1001 = %d, want 15", got))
	// 1002 未被重写（值不变）
	got = itemCount(t, database, playerID, 1002)
	require.EqualValues(t, 20, got, fmt.Sprintf("item 1002 = %d, want 20", got))
	// 保存后脏标记清空
	dirty = state.DirtyItems()
	require.Empty(t, dirty, fmt.Sprintf("dirty should be cleared after save, got %v", dirty))

	// ③ 合批 UPSERT 不产生重复行（依赖 uk_player_item 唯一键）
	err = repo.Save(ctx, state)
	require.NoError(t, err, fmt.Sprintf("save again: %v", err))
	state.AddItem(1001, 1)
	err = repo.Save(ctx, state)
	require.NoError(t, err, fmt.Sprintf("save third: %v", err))
	n = countItemRows(t, database, playerID)
	require.EqualValues(t, 2, n, fmt.Sprintf("row count = %d, want 2 (upsert must not duplicate)", n))

	// ④ 数量归零走 DELETE
	require.False(t, !state.RemoveItem(1002, 20), "remove 1002 should succeed")
	err = repo.Save(ctx, state)
	require.NoError(t, err, fmt.Sprintf("save zero: %v", err))
	rows := countItemRows(t, database, playerID)
	require.EqualValues(t, 1, rows, fmt.Sprintf("row count after zeroing = %d, want 1", rows))

	// ⑤ 墓碑合批删除
	deleter := flush.NewSQLDeleter(database.DB)
	f := flush.New(flush.Config{
		ShardCount: 2,
		Interval:   time.Minute,
		DeleteFn:   deleter.DeleteBatch,
	}, func(context.Context, int64) error { return nil }, log.New("test"))

	// 注意：MarkDelete 的 id 是**表主键**（player_item.id），不是 player_id
	var itemRowID int64
	if err := database.QueryRowContext(ctx,
		`SELECT id FROM player_item WHERE player_id = ?`, playerID).Scan(&itemRowID); err != nil {
		t.Fatalf("query item row id: %v", err)
	}
	f.MarkDelete("player_item", itemRowID)
	err = f.FlushAll(ctx)
	require.NoError(t, err, fmt.Sprintf("flush tombstone: %v", err))
	rows = countItemRows(t, database, playerID)
	require.EqualValues(t, 0, rows, fmt.Sprintf("row count after tombstone = %d, want 0", rows))
	require.EqualValues(t, 0, f.PendingDeletes())
}

// countItemRows 读某玩家的道具行数（验证没有重复行）。
func countItemRows(t *testing.T, database *db.DB, playerID int64) int {
	t.Helper()
	var n int
	if err := database.QueryRow(
		`SELECT COUNT(*) FROM player_item WHERE player_id = ?`, playerID).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}
