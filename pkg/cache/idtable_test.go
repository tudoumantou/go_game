package cache

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newTestIDTable 构造「miniredis + sqlmock」环境。
func newTestIDTable(t *testing.T) (*IDTable, *miniredis.Miniredis, sqlmock.Sqlmock, func()) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err, "sqlmock")

	it := NewIDTable(rdb, sqlDB)
	cleanup := func() {
		_ = rdb.Close()
		_ = sqlDB.Close()
	}
	return it, mr, mock, cleanup
}

// TestBuildOneKeysetPaging 构建必须用 keyset 分页，不能 OFFSET（大表深翻页退化）。
func TestBuildOneKeysetPaging(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()
	it.SetPageSize(2)

	meta := TableMeta{Name: "player", PK: "id"}

	// 第 1 页：2 条（满页，继续）
	mock.ExpectQuery(`SELECT id FROM player WHERE id > \? ORDER BY id LIMIT \?`).
		WithArgs(int64(0), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))
	// 第 2 页：1 条（不满页，结束）
	mock.ExpectQuery(`SELECT id FROM player WHERE id > \? ORDER BY id LIMIT \?`).
		WithArgs(int64(2), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(5))

	n, err := it.BuildOne(context.Background(), meta)
	require.NoError(t, err, "build")
	require.EqualValues(t, 3, n)

	// ready 置位 + 集合内容正确
	if !it.Ready(context.Background(), "player") {
		t.Fatal("ready flag not set")
	}
	for _, id := range []string{"1", "2", "5"} {
		if !mustMember(t, mr, IDTabKey("player"), id) {
			t.Fatalf("id %s missing in idtab", id)
		}
	}
	if mr.Exists(IDTabKey("player") + ":building") {
		t.Fatal("building key should be renamed away")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestBuildOneEmptyTable 空表也要成功置 ready。
//
// 回归：原先无条件 RENAME，空表时 building key 不存在 → "ERR no such key"，
// ready 永远置不上，每张空表启动时刷一条失败日志。
func TestBuildOneEmptyTable(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	meta := TableMeta{Name: "friend", PK: "id"}
	mock.ExpectQuery(`SELECT id FROM friend`).
		WithArgs(int64(0), int64(5000)).
		WillReturnRows(sqlmock.NewRows([]string{"id"})) // 0 行

	n, err := it.BuildOne(context.Background(), meta)
	require.NoError(t, err, "build empty table")
	require.EqualValues(t, 0, n)
	if !it.Ready(context.Background(), "friend") {
		t.Fatal("empty table should still be marked ready")
	}
	// 空集合：Exists 应返回 false 而不是报错
	if ok, err := it.Exists(context.Background(), "friend", 1); err != nil || ok {
		t.Fatalf("exists = %v, %v", ok, err)
	}
	_ = mr
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestBuildOneIdempotent 已 ready 的表重复构建应跳过（每次启动都会调用）。
func TestBuildOneIdempotent(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	meta := TableMeta{Name: "mail", PK: "id"}
	mock.ExpectQuery(`SELECT id FROM mail`).
		WithArgs(int64(0), int64(5000)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	if _, err := it.BuildOne(context.Background(), meta); err != nil {
		t.Fatalf("first build: %v", err)
	}

	// 第二次不再查库
	n, err := it.BuildOne(context.Background(), meta)
	require.NoError(t, err, "second build")
	require.EqualValues(t, 1, n)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	_ = mr
}

// TestExistsFallsBackWhenNotReady ready 未置位时必须回落 DB（保证正确性）。
func TestExistsFallsBackWhenNotReady(t *testing.T) {
	it, _, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT 1 FROM player WHERE id = \?`).
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	ok, err := it.Exists(context.Background(), "player", 42)
	require.NoError(t, err, "exists")
	require.True(t, ok, "expect exists via DB fallback")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestExistsHitsRedisWhenReady ready 后不应打库。
func TestExistsHitsRedisWhenReady(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	mustSet(t, mr, IDTabReadyKey("player"), "1")
	mustSAdd(t, mr, IDTabKey("player"), "42")

	ok, err := it.Exists(context.Background(), "player", 42)
	require.NoError(t, err, "exists")
	require.True(t, ok, "expect exists")

	// 不存在的 ID 也不打库（这是 idtab 的核心价值）
	ok, err = it.Exists(context.Background(), "player", 99)
	require.NoError(t, err, "exists miss")
	if ok {
		t.Fatal("expect not exists")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestExistsDBError 查库报错要透传，不能当成「不存在」。
func TestExistsDBError(t *testing.T) {
	it, _, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT 1 FROM player`).
		WillReturnError(errors.New("db down"))

	if _, err := it.Exists(context.Background(), "player", 1); err == nil {
		t.Fatal("expect error")
	}
}

// TestAddRemove 登记/注销只影响已 ready 的表。
func TestAddRemove(t *testing.T) {
	it, mr, _, cleanup := newTestIDTable(t)
	defer cleanup()

	ctx := context.Background()

	// 未 ready：Add 静默忽略（构建时会从库里扫到）
	if err := it.Add(ctx, "player", 1); err != nil {
		t.Fatalf("add before ready: %v", err)
	}
	if mustMember(t, mr, IDTabKey("player"), "1") {
		t.Fatal("should not add when not ready")
	}

	mustSet(t, mr, IDTabReadyKey("player"), "1")
	if err := it.Add(ctx, "player", 1); err != nil {
		t.Fatalf("add: %v", err)
	}
	if !mustMember(t, mr, IDTabKey("player"), "1") {
		t.Fatal("id 1 should exist")
	}

	// Remove 同时清实体 hash
	mr.HSet(EntityKey("player", 1), "name", "alice")
	if err := it.Remove(ctx, "player", 1); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if mustMember(t, mr, IDTabKey("player"), "1") {
		t.Fatal("id 1 should be removed")
	}
	if mr.Exists(EntityKey("player", 1)) {
		t.Fatal("entity hash should be deleted together")
	}
}

// TestReconcileRebuild 数量对不上时重建，一致时不动。
func TestReconcileRebuild(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()
	it.SetPageSize(100)

	meta := TableMeta{Name: "guild", PK: "id"}
	mustSet(t, mr, IDTabReadyKey("guild"), "1")
	mustSAdd(t, mr, IDTabKey("guild"), "1") // 缓存里 1 条

	// 库里实际 2 条 → 不一致
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM guild`).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(2))
	mock.ExpectQuery(`SELECT id FROM guild WHERE id > \? ORDER BY id LIMIT \?`).
		WithArgs(int64(0), int64(100)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))

	rebuilt, err := it.Reconcile(context.Background(), meta)
	require.NoError(t, err, "reconcile")
	require.True(t, rebuilt, "expect rebuild")
	if n, _ := it.Count(context.Background(), "guild"); n != 2 {
		t.Fatalf("count after rebuild = %d, want 2", n)
	}

	// 再核对一次：一致 → 不重建
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM guild`).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(2))
	rebuilt, err = it.Reconcile(context.Background(), meta)
	require.NoError(t, err, "reconcile 2")
	if rebuilt {
		t.Fatal("should not rebuild when counts match")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestLoadEntityCacheMissLoadsDB 三类判定矩阵的第三类：idtab 有、hash 无 → 回源 + 回填。
func TestLoadEntityCacheMissLoadsDB(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	meta := TableMeta{Name: "player", PK: "id"}
	mustSet(t, mr, IDTabReadyKey("player"), "1")
	mustSAdd(t, mr, IDTabKey("player"), "42")

	mock.ExpectQuery(`SELECT \* FROM player WHERE id = \?`).
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "level"}).
			AddRow(42, "alice", 7))

	row, err := it.LoadEntity(context.Background(), meta, 42)
	require.NoError(t, err, "load")
	if row["name"] != "alice" || row["level"] != "7" {
		t.Fatalf("row = %v", row)
	}

	// 已回填 Redis
	if got := mr.HGet(EntityKey("player", 42), "name"); got != "alice" {
		t.Fatalf("cache not filled, name = %q", got)
	}

	// 第二次读命中 Redis，不再查库
	row2, err := it.LoadEntity(context.Background(), meta, 42)
	require.NoError(t, err, "load 2")
	require.EqualValues(t, "alice", row2["name"])
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestLoadEntityNotExist idtab 无该 ID → 直接返回 ErrNotFound，不打库。
func TestLoadEntityNotExist(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	mustSet(t, mr, IDTabReadyKey("player"), "1")

	_, err := it.LoadEntity(context.Background(), TableMeta{Name: "player", PK: "id"}, 999)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestValidateIdent 动态拼 SQL 的标识符必须过白名单。
func TestValidateIdent(t *testing.T) {
	bad := []string{"player; DROP TABLE x", "player WHERE 1=1", "", "Player", "player-item"}
	for _, s := range bad {
		if err := validateIdent(s); err == nil {
			t.Fatalf("ident %q should be rejected", s)
		}
	}
	good := []string{"player", "player_attr", "mail_attachment2"}
	for _, s := range good {
		if err := validateIdent(s); err != nil {
			t.Fatalf("ident %q should be accepted: %v", s, err)
		}
	}
}

// TestCleanOrphans 孤儿 hash（idtab 无此 id）应被清理。
func TestCleanOrphans(t *testing.T) {
	it, mr, _, cleanup := newTestIDTable(t)
	defer cleanup()

	mustSet(t, mr, IDTabReadyKey("player"), "1")
	mustSAdd(t, mr, IDTabKey("player"), "1")
	mr.HSet(EntityKey("player", 1), "name", "alive")
	mr.HSet(EntityKey("player", 2), "name", "orphan")

	removed, err := it.CleanOrphans(context.Background(), "player", 1000)
	require.NoError(t, err, "clean")
	require.EqualValues(t, 1, removed)
	if !mr.Exists(EntityKey("player", 1)) {
		t.Fatal("live entity should be kept")
	}
	if mr.Exists(EntityKey("player", 2)) {
		t.Fatal("orphan entity should be deleted")
	}
}

// TestNoRedisDegrades 无 Redis 时所有方法降级为查库，不 panic。
func TestNoRedisDegrades(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err, "sqlmock")
	defer func() { _ = sqlDB.Close() }()

	it := NewIDTable(nil, sqlDB)
	ctx := context.Background()

	if it.Ready(ctx, "player") {
		t.Fatal("no redis → never ready")
	}
	if err := it.Add(ctx, "player", 1); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := it.Remove(ctx, "player", 1); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if n, err := it.Count(ctx, "player"); n != 0 || err != nil {
		t.Fatalf("count = %d, %v", n, err)
	}
	if n, err := it.BuildOne(ctx, TableMeta{Name: "player", PK: "id"}); n != 0 || err != nil {
		t.Fatalf("build = %d, %v", n, err)
	}

	mock.ExpectQuery(`SELECT 1 FROM player`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	if ok, err := it.Exists(ctx, "player", 1); err != nil || !ok {
		t.Fatalf("exists = %v, %v", ok, err)
	}
}

// mustMember miniredis 的 SIsMember 返回 (bool, error)，测试里包一层。
func mustMember(t *testing.T, mr *miniredis.Miniredis, key, member string) bool {
	t.Helper()
	ok, err := mr.SIsMember(key, member)
	if err != nil {
		// miniredis 在 key 不存在时报 ERR no such key（真实 Redis 返回 false, nil）
		if strings.Contains(err.Error(), "no such key") {
			return false
		}
		t.Fatalf("sismember %s %s: %v", key, member, err)
	}
	return ok
}

// mustSet miniredis 赋值（失败即中止测试）。
func mustSet(t *testing.T, mr *miniredis.Miniredis, key, val string) {
	t.Helper()
	if err := mr.Set(key, val); err != nil {
		t.Fatalf("set %s: %v", key, err)
	}
}

// mustSAdd miniredis 集合写入。
func mustSAdd(t *testing.T, mr *miniredis.Miniredis, key string, members ...string) {
	t.Helper()
	if _, err := mr.SAdd(key, members...); err != nil {
		t.Fatalf("sadd %s: %v", key, err)
	}
}
