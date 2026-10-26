package cache

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
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
	require.False(t, !it.Ready(context.Background(), "player"), "ready flag not set")
	for _, id := range []string{"1", "2", "5"} {
		require.False(t, !mustMember(t, mr, IDTabKey("player"), id), fmt.Sprintf("id %s missing in idtab", id))
	}
	require.False(t, mr.Exists(IDTabKey("player")+":building"), "building key should be renamed away")
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
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
	require.False(t, !it.Ready(context.Background(), "friend"), "empty table should still be marked ready")
	// 空集合：Exists 应返回 false 而不是报错
	ok, err := it.Exists(context.Background(), "friend", 1)
	require.False(t, err != nil || ok, fmt.Sprintf("exists = %v, %v", ok, err))
	_ = mr
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestBuildOneIdempotent 已 ready 的表重复构建应跳过（每次启动都会调用）。
func TestBuildOneIdempotent(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	meta := TableMeta{Name: "mail", PK: "id"}
	mock.ExpectQuery(`SELECT id FROM mail`).
		WithArgs(int64(0), int64(5000)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	_, err := it.BuildOne(context.Background(), meta)
	require.NoError(t, err, fmt.Sprintf("first build: %v", err))

	// 第二次不再查库
	n, err := it.BuildOne(context.Background(), meta)
	require.NoError(t, err, "second build")
	require.EqualValues(t, 1, n)
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
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
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
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
	require.False(t, ok, "expect not exists")

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestExistsDBError 查库报错要透传，不能当成「不存在」。
func TestExistsDBError(t *testing.T) {
	it, _, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	mock.ExpectQuery(`SELECT 1 FROM player`).
		WillReturnError(errors.New("db down"))

	_, err := it.Exists(context.Background(), "player", 1)
	require.Error(t, err, "expect error")
}

// TestAddRemove 登记/注销只影响已 ready 的表。
func TestAddRemove(t *testing.T) {
	it, mr, _, cleanup := newTestIDTable(t)
	defer cleanup()

	ctx := context.Background()

	// 未 ready：Add 静默忽略（构建时会从库里扫到）
	err := it.Add(ctx, "player", 1)
	require.NoError(t, err, fmt.Sprintf("add before ready: %v", err))
	require.False(t, mustMember(t, mr, IDTabKey("player"), "1"), "should not add when not ready")

	mustSet(t, mr, IDTabReadyKey("player"), "1")
	err = it.Add(ctx, "player", 1)
	require.NoError(t, err, fmt.Sprintf("add: %v", err))
	require.False(t, !mustMember(t, mr, IDTabKey("player"), "1"), "id 1 should exist")

	// Remove 同时清实体 hash
	mr.HSet(EntityKey("player", 1), "name", "alice")
	err = it.Remove(ctx, "player", 1)
	require.NoError(t, err, fmt.Sprintf("remove: %v", err))
	require.False(t, mustMember(t, mr, IDTabKey("player"), "1"), "id 1 should be removed")
	require.False(t, mr.Exists(EntityKey("player", 1)), "entity hash should be deleted together")
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
	n, _ := it.Count(context.Background(), "guild")
	require.EqualValues(t, 2, n, fmt.Sprintf("count after rebuild = %d, want 2", n))

	// 再核对一次：一致 → 不重建
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM guild`).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(2))
	rebuilt, err = it.Reconcile(context.Background(), meta)
	require.NoError(t, err, "reconcile 2")
	require.False(t, rebuilt, "should not rebuild when counts match")
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
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
	require.EqualValues(t, "alice", row["name"], fmt.Sprintf("row = %v", row))
	require.EqualValues(t, "7", row["level"])

	// 已回填 Redis
	got := mr.HGet(EntityKey("player", 42), "name")
	require.EqualValues(t, "alice", got, fmt.Sprintf("cache not filled, name = %q", got))

	// 第二次读命中 Redis，不再查库
	row2, err := it.LoadEntity(context.Background(), meta, 42)
	require.NoError(t, err, "load 2")
	require.EqualValues(t, "alice", row2["name"])
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestLoadEntityNotExist idtab 无该 ID → 直接返回 ErrNotFound，不打库。
func TestLoadEntityNotExist(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()

	mustSet(t, mr, IDTabReadyKey("player"), "1")

	_, err := it.LoadEntity(context.Background(), TableMeta{Name: "player", PK: "id"}, 999)
	require.False(t, !errors.Is(err, ErrNotFound), fmt.Sprintf("err = %v, want ErrNotFound", err))
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestValidateIdent 动态拼 SQL 的标识符必须过白名单。
func TestValidateIdent(t *testing.T) {
	bad := []string{"player; DROP TABLE x", "player WHERE 1=1", "", "Player", "player-item"}
	for _, s := range bad {
		err := validateIdent(s)
		require.Error(t, err, fmt.Sprintf("ident %q should be rejected", s))
	}
	good := []string{"player", "player_attr", "mail_attachment2"}
	for _, s := range good {
		err := validateIdent(s)
		require.NoError(t, err, fmt.Sprintf("ident %q should be accepted: %v", s, err))
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
	require.False(t, !mr.Exists(EntityKey("player", 1)), "live entity should be kept")
	require.False(t, mr.Exists(EntityKey("player", 2)), "orphan entity should be deleted")
}

// TestNoRedisDegrades 无 Redis 时所有方法降级为查库，不 panic。
func TestNoRedisDegrades(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err, "sqlmock")
	defer func() { _ = sqlDB.Close() }()

	it := NewIDTable(nil, sqlDB)
	ctx := context.Background()

	require.False(t, it.Ready(ctx, "player"), "no redis → never ready")
	err = it.Add(ctx, "player", 1)
	require.NoError(t, err, fmt.Sprintf("add: %v", err))
	err = it.Remove(ctx, "player", 1)
	require.NoError(t, err, fmt.Sprintf("remove: %v", err))
	n, err := it.Count(ctx, "player")
	require.EqualValues(t, 0, n, fmt.Sprintf("count = %d, %v", n, err))
	require.NoError(t, err)
	n, err = it.BuildOne(ctx, TableMeta{Name: "player", PK: "id"})
	require.EqualValues(t, 0, n, fmt.Sprintf("build = %d, %v", n, err))
	require.NoError(t, err)

	mock.ExpectQuery(`SELECT 1 FROM player`).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	ok, err := it.Exists(ctx, "player", 1)
	require.NoError(t, err, fmt.Sprintf("exists = %v, %v", ok, err))
	require.True(t, ok)
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
	err := mr.Set(key, val)
	require.NoError(t, err, fmt.Sprintf("set %s: %v", key, err))
}

// mustSAdd miniredis 集合写入。
func mustSAdd(t *testing.T, mr *miniredis.Miniredis, key string, members ...string) {
	t.Helper()
	_, err := mr.SAdd(key, members...)
	require.NoError(t, err, fmt.Sprintf("sadd %s: %v", key, err))
}

// guildRows 构造公会表的一页实体行（SELECT * 动态列）。
func guildRows(rows ...[]any) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"id", "name", "level"})
	for _, row := range rows {
		vals := make([]driver.Value, len(row))
		for i, v := range row {
			vals[i] = v
		}
		r.AddRow(vals...)
	}
	return r
}

// TestPreheatLoadsAllRows 整表预热：keyset 分页扫全表，逐页合批 HSET 回填。
func TestPreheatLoadsAllRows(t *testing.T) {
	it, mr, mock, cleanup := newTestIDTable(t)
	defer cleanup()
	it.SetPageSize(2) // 强制翻页

	mock.ExpectQuery("SELECT \\* FROM guild WHERE").WithArgs(0, 2).
		WillReturnRows(guildRows([]any{1, "g1", 5}, []any{2, "g2", 7}))
	mock.ExpectQuery("SELECT \\* FROM guild WHERE").WithArgs(2, 2).
		WillReturnRows(guildRows([]any{3, "g3", 9}))

	n, err := it.Preheat(context.Background(), TableMeta{Name: "guild", PK: "id"})
	require.NoError(t, err)
	require.EqualValues(t, 3, n, "预热行数")

	// 全部行进hash，字段原样（toString 归一）
	require.Equal(t, "g1", mr.HGet(EntityKey("guild", 1), "name"))
	require.Equal(t, "9", mr.HGet(EntityKey("guild", 3), "level"),
		"整型列经 toString 归一为字符串")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPreheatNilHandles 无双句柄时退化为空操作（单测/无 Redis 环境）。
func TestPreheatNilHandles(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	n, err := NewIDTable(nil, sqlDB).Preheat(context.Background(), TableMeta{Name: "guild", PK: "id"})
	require.NoError(t, err)
	require.EqualValues(t, 0, n)
}

// TestPreheatBadIdent 标识符白名单：非法表名在预热前报错。
func TestPreheatBadIdent(t *testing.T) {
	it, _, _, cleanup := newTestIDTable(t)
	defer cleanup()
	_, err := it.Preheat(context.Background(), TableMeta{Name: "guild; DROP TABLE player", PK: "id"})
	require.Error(t, err)
}
