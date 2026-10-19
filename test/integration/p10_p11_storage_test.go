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
	"testing"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/log"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// startStorage 起 MySQL + Redis 并初始化 schema，返回 DB、Redis 客户端与 DSN。
func startStorage(t *testing.T) (*db.DB, *redis.Client, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	mysqlC, err := mysql.Run(ctx, "mysql:8.0",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: map[string]string{
					"MYSQL_ROOT_PASSWORD": "root",
					"MYSQL_DATABASE":      "game_study",
					"MYSQL_USER":          "game",
					"MYSQL_PASSWORD":      "game_study",
				},
			},
		}),
	)
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	t.Cleanup(func() { _ = mysqlC.Terminate(context.Background()) })

	redisC, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = redisC.Terminate(context.Background()) })

	dsn, err := mysqlC.ConnectionString(ctx, "parseTime=true", "multiStatements=true", "loc=Local")
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}
	initSchema(t, dsn)

	database, err := db.New(db.Config{DSN: dsn, MaxOpen: 10, MaxIdle: 5})
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	host, _ := redisC.Host(ctx)
	port, _ := redisC.MappedPort(ctx, "6379/tcp")
	rdb := redis.NewClient(&redis.Options{Addr: host + ":" + port.Port()})
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
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if n < 3 {
		t.Fatalf("built %d ids, want >= 3", n)
	}
	if !it.Ready(ctx, "player") {
		t.Fatal("ready flag not set after build")
	}

	// ② 存在性：ready 后不再打库
	var playerID int64
	if err := database.QueryRowContext(ctx,
		`SELECT id FROM player WHERE name = 'idtab_role_a'`).Scan(&playerID); err != nil {
		t.Fatalf("query player id: %v", err)
	}
	exists, err := it.Exists(ctx, "player", playerID)
	if err != nil || !exists {
		t.Fatalf("exists(%d) = %v, %v", playerID, exists, err)
	}
	exists, err = it.Exists(ctx, "player", 99999999)
	if err != nil || exists {
		t.Fatalf("exists(nonexistent) = %v, %v", exists, err)
	}

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
	if err := it.Add(ctx, "player", newID); err != nil {
		t.Fatalf("add: %v", err)
	}
	if ok, _ := it.Exists(ctx, "player", newID); !ok {
		t.Fatal("newly added id should exist")
	}

	// ④ 删除同步
	if _, err := database.ExecContext(ctx, `DELETE FROM player WHERE id = ?`, newID); err != nil {
		t.Fatalf("delete player: %v", err)
	}
	if err := it.Remove(ctx, "player", newID); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if ok, _ := it.Exists(ctx, "player", newID); ok {
		t.Fatal("removed id should not exist")
	}

	// ⑤ 对账：集合与表行数一致 → 不重建
	rebuilt, err := it.Reconcile(ctx, meta)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if rebuilt {
		t.Fatal("should not rebuild when counts match")
	}

	// ⑥ 人为制造不一致 → 对账触发重建
	if err := rdb.SRem(ctx, cache.IDTabKey("player"), playerID).Err(); err != nil {
		t.Fatalf("srem: %v", err)
	}
	rebuilt, err = it.Reconcile(ctx, meta)
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if !rebuilt {
		t.Fatal("expect rebuild after manual removal")
	}
	if ok, _ := it.Exists(ctx, "player", playerID); !ok {
		t.Fatal("id should be back after rebuild")
	}

	// ⑦ 回源：清掉实体 hash，LoadEntity 应从库里读并回填
	key := cache.EntityKey("player", playerID)
	_ = rdb.Del(ctx, key).Err()
	row, err := it.LoadEntity(ctx, meta, playerID)
	if err != nil {
		t.Fatalf("load entity: %v", err)
	}
	if row["name"] != "idtab_role_a" {
		t.Fatalf("row name = %q", row["name"])
	}
	if n, _ := rdb.HLen(ctx, key).Result(); n == 0 {
		t.Fatal("entity hash should be filled back")
	}

	// ⑧ 不存在的 id 直接返回 ErrNotFound（不打库）
	if _, err := it.LoadEntity(ctx, meta, 99999999); err != cache.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestP11IncrementalFlush 道具增量合批落库。
func TestP11IncrementalFlush(t *testing.T) {
	database, _, _ := startStorage(t)
	ctx := context.Background()

	// 造一个角色 + 两格初始道具
	res, err := database.ExecContext(ctx,
		`INSERT INTO player (account_id, name, level, exp, coin, scene_id) VALUES (7001, 'p11_role', 1, 0, 0, 0)`)
	if err != nil {
		t.Fatalf("insert player: %v", err)
	}
	playerID, _ := res.LastInsertId()
	if _, err := database.ExecContext(ctx,
		`INSERT INTO player_item (player_id, item_id, count) VALUES (?, 1001, 10), (?, 1002, 20)`,
		playerID, playerID); err != nil {
		t.Fatalf("seed items: %v", err)
	}

	repo := player.NewRepository(database)

	// ① 加载：SeedItem 不标脏 → 直接保存不应产生任何道具写
	state, err := repo.Load(ctx, playerID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if state.GetItemCount(1001) != 10 || state.GetItemCount(1002) != 20 {
		t.Fatalf("loaded items = %d/%d", state.GetItemCount(1001), state.GetItemCount(1002))
	}
	if dirty := state.DirtyItems(); len(dirty) != 0 {
		t.Fatalf("fresh load should have no dirty items, got %v", dirty)
	}

	if err := repo.Save(ctx, state); err != nil {
		t.Fatalf("save unchanged: %v", err)
	}
	if n := countItemRows(t, database, playerID); n != 2 {
		t.Fatalf("row count after no-op save = %d, want 2", n)
	}

	// ② 只写变更项：改 1001，1002 不动
	state.AddItem(1001, 5)
	if dirty := state.DirtyItems(); len(dirty) != 1 || dirty[1001] != 15 {
		t.Fatalf("dirty items = %v, want {1001:15}", dirty)
	}
	if err := repo.Save(ctx, state); err != nil {
		t.Fatalf("save increment: %v", err)
	}
	if got := itemCount(t, database, playerID, 1001); got != 15 {
		t.Fatalf("item 1001 = %d, want 15", got)
	}
	// 1002 未被重写（值不变）
	if got := itemCount(t, database, playerID, 1002); got != 20 {
		t.Fatalf("item 1002 = %d, want 20", got)
	}
	// 保存后脏标记清空
	if dirty := state.DirtyItems(); len(dirty) != 0 {
		t.Fatalf("dirty should be cleared after save, got %v", dirty)
	}

	// ③ 合批 UPSERT 不产生重复行（依赖 uk_player_item 唯一键）
	if err := repo.Save(ctx, state); err != nil {
		t.Fatalf("save again: %v", err)
	}
	state.AddItem(1001, 1)
	if err := repo.Save(ctx, state); err != nil {
		t.Fatalf("save third: %v", err)
	}
	if n := countItemRows(t, database, playerID); n != 2 {
		t.Fatalf("row count = %d, want 2 (upsert must not duplicate)", n)
	}

	// ④ 数量归零走 DELETE
	if !state.RemoveItem(1002, 20) {
		t.Fatal("remove 1002 should succeed")
	}
	if err := repo.Save(ctx, state); err != nil {
		t.Fatalf("save zero: %v", err)
	}
	if got := countItemRows(t, database, playerID); got != 1 {
		t.Fatalf("row count after zeroing = %d, want 1", got)
	}

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
	if err := f.FlushAll(ctx); err != nil {
		t.Fatalf("flush tombstone: %v", err)
	}
	if got := countItemRows(t, database, playerID); got != 0 {
		t.Fatalf("row count after tombstone = %d, want 0", got)
	}
	if f.PendingDeletes() != 0 {
		t.Fatalf("tombstone queue should be drained, got %d", f.PendingDeletes())
	}
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
