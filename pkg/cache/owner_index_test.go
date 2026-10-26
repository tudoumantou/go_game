// owner_index_test.go —— 归属索引的构建、回落与批量加载
package cache

import (
	"context"
	"database/sql/driver"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newTestOwnerIndex 构造「miniredis + sqlmock」环境（表 mail，归属 player_id）。
func newTestOwnerIndex(t *testing.T, pageSize int64) (*OwnerIndex, *miniredis.Miniredis, sqlmock.Sqlmock, func()) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err, "sqlmock")

	oi := NewOwnerIndex(rdb, sqlDB, "mail", "player_id", "id")
	if pageSize > 0 {
		oi.SetPageSize(pageSize)
	}
	cleanup := func() {
		_ = rdb.Close()
		_ = sqlDB.Close()
	}
	return oi, mr, mock, cleanup
}

// indexRows 构造索引取材行（只有 queryRows 扫的两列：id, player_id）。
func indexRows(rows ...[]any) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"id", "player_id"})
	for _, row := range rows {
		vals := make([]driver.Value, len(row))
		for i, v := range row {
			vals[i] = v
		}
		r.AddRow(vals...)
	}
	return r
}

// mailRows 构造邮件表的一页实体行（SELECT * 动态列）。
func mailRows(rows ...[]any) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"id", "player_id", "title"})
	for _, row := range rows {
		vals := make([]driver.Value, len(row))
		for i, v := range row {
			vals[i] = v
		}
		r.AddRow(vals...)
	}
	return r
}

// TestOwnerIndexBuildOnePaging 构建走 keyset 分页（pageSize=2 强制翻页），
// 落点是临时 key + RENAME，ready 置位。
func TestOwnerIndexBuildOnePaging(t *testing.T) {
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 2)
	defer cleanup()

	// 三页：2 行 + 2 行 + 空（空页是循环终止条件）
	mock.ExpectQuery("SELECT id, player_id FROM mail").WithArgs(0, 2).
		WillReturnRows(indexRows([]any{1, 10086}, []any{2, 10086}))
	mock.ExpectQuery("SELECT id, player_id FROM mail").WithArgs(2, 2).
		WillReturnRows(indexRows([]any{3, 10087}, []any{4, 10086}))
	mock.ExpectQuery("SELECT id, player_id FROM mail").WithArgs(4, 2).
		WillReturnRows(indexRows())

	n, err := oi.BuildOne(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 4, n, "构建计数")

	// 临时 key 已被 RENAME 掉，正式 key 就位
	require.False(t, mr.Exists(OwnerIndexKey("mail")+":building"), "building key 应被切换走")
	members, err := mr.ZMembers(OwnerIndexKey("mail"))
	require.NoError(t, err)
	require.Len(t, members, 4)
	require.True(t, oi.Ready(context.Background()), "ready 应置位")

	// 无归属者的行不进索引：pageSize=2 下补一页含 player_id=0 的行
	oi2, mr2, mock2, cleanup2 := newTestOwnerIndex(t, 2)
	defer cleanup2()
	mock2.ExpectQuery("SELECT id, player_id FROM mail").WithArgs(0, 2).
		WillReturnRows(indexRows([]any{5, 0}, []any{6, 10086}))
	mock2.ExpectQuery("SELECT id, player_id FROM mail").WithArgs(6, 2).
		WillReturnRows(indexRows())
	n, err = oi2.BuildOne(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 1, n, "无归属者的行被剔除")
	require.True(t, mr2.Exists(OwnerIndexKey("mail")), "有效行仍要建索引")
	members, err = mr2.ZMembers(OwnerIndexKey("mail"))
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.True(t, oi2.Ready(context.Background()), "有空页也要置 ready")
}

// TestOwnerIndexBuildOneIdempotent 已 ready 的表重复构建零成本（不打库）。
func TestOwnerIndexBuildOneIdempotent(t *testing.T) {
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()

	mustSet(t, mr, OwnerIndexReadyKey("mail"), "1")
	require.NoError(t, oi.Index().Add(context.Background(), 10086, 101))
	require.NoError(t, oi.Index().Add(context.Background(), 10086, 102))

	n, err := oi.BuildOne(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 2, n, "已 ready 返回现有数量")
	require.NoError(t, mock.ExpectationsWereMet(), "不应产生任何 SQL")
}

// TestOwnerIndexMembersFallback ready 未置位 → 回落 MySQL；置位后走 Redis。
func TestOwnerIndexMembersFallback(t *testing.T) {
	ctx := context.Background()
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()

	// 未 ready：回落库（ORDER BY key 保证数值序由 SQL 侧给出）
	// 顺序由 SQL 的 ORDER BY key 保证（驱动不排序），mock 按有序返回
	mock.ExpectQuery("SELECT id FROM mail WHERE player_id =").WithArgs(10086).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(101).AddRow(102))
	ids, err := oi.Members(ctx, 10086)
	require.NoError(t, err)
	require.Equal(t, []int64{101, 102}, ids, "回落路径原序返回（排序在 SQL 侧）")

	// ready 后：走 Redis，且结果同样排序
	mustSet(t, mr, OwnerIndexReadyKey("mail"), "1")
	require.NoError(t, oi.Index().Add(ctx, 10086, 1001))
	require.NoError(t, oi.Index().Add(ctx, 10086, 999))
	ids, err = oi.Members(ctx, 10086)
	require.NoError(t, err)
	require.Equal(t, []int64{999, 1001}, ids, "字典序 1001 < 999，必须 Go 侧重排")

	// 非法归属者 id 直接报错，不发请求
	_, err = oi.Members(ctx, 0)
	require.Error(t, err, "owner id 非正必须报错")
}

// TestOwnerIndexLoadBatch hash 命中的直接用，未命中的一次 SQL 捞回并回填。
func TestOwnerIndexLoadBatch(t *testing.T) {
	ctx := context.Background()
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()

	// 101 在 Redis 有；102/103 没有 → 一次 IN 查询捞回
	mr.HSet(EntityKey("mail", 101), "id", "101", "title", "cached")
	mock.ExpectQuery("SELECT \\* FROM mail WHERE id IN").WithArgs(102, 103).
		WillReturnRows(mailRows([]any{102, 10086, "from-db"}))

	rows, missing, err := oi.LoadBatch(ctx, []int64{101, 102, 103})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, []int64{103}, missing, "两处都没有的 id 进 missing（调用方自愈）")

	// 按入参顺序返回
	require.EqualValues(t, 101, rows[0].ID)
	require.Equal(t, "cached", rows[0].Fields["title"])
	require.EqualValues(t, 102, rows[1].ID)
	require.Equal(t, "from-db", rows[1].Fields["title"])

	// 回填：102 的 hash 已写进 Redis（下次登录不再打库）
	require.True(t, mr.Exists(EntityKey("mail", 102)), "未命中行应回填数据表")
	require.False(t, mr.Exists(EntityKey("mail", 103)), "MySQL 也没有的行不回填")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestOwnerIndexLoadBatchRedisDown Redis 整体故障不报错：按全部未命中走 MySQL。
func TestOwnerIndexLoadBatchRedisDown(t *testing.T) {
	ctx := context.Background()
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()

	mock.ExpectQuery("SELECT \\* FROM mail WHERE id IN").WithArgs(101, 102).
		WillReturnRows(mailRows([]any{101, 10086, "a"}, []any{102, 10086, "b"}))

	mr.Close() // Redis 挂掉

	rows, missing, err := oi.LoadBatch(ctx, []int64{101, 102})
	require.NoError(t, err, "Redis 故障不升级为错误")
	require.Len(t, rows, 2)
	require.Empty(t, missing)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestOwnerIndexReconcile 数量不一致 → 清 ready 重建。
func TestOwnerIndexReconcile(t *testing.T) {
	ctx := context.Background()
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()

	mustSet(t, mr, OwnerIndexReadyKey("mail"), "1")
	require.NoError(t, oi.Index().Add(ctx, 10086, 101))
	require.NoError(t, oi.Index().Add(ctx, 10086, 102))

	// ZCARD=2 vs 库里有 3 行 → 重建（重建会重新取材）
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM mail").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).AddRow(3))
	mock.ExpectQuery("SELECT id, player_id FROM mail").WithArgs(0, 5000).
		WillReturnRows(indexRows([]any{101, 10086}, []any{102, 10086}, []any{103, 10087}))

	rebuilt, err := oi.Reconcile(ctx)
	require.NoError(t, err)
	require.True(t, rebuilt, "数量不一致应触发重建")

	n, err := oi.Index().Count(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 3, n, "重建后与库一致")
	require.True(t, oi.Ready(ctx), "重建后 ready 重新置位")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestOwnerIndexReconcileConsistent 数量一致 → 不重建、不打库。
func TestOwnerIndexReconcileConsistent(t *testing.T) {
	ctx := context.Background()
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()

	mustSet(t, mr, OwnerIndexReadyKey("mail"), "1")
	require.NoError(t, oi.Index().Add(ctx, 10086, 101))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM mail").WillReturnRows(
		sqlmock.NewRows([]string{"n"}).AddRow(1))

	rebuilt, err := oi.Reconcile(ctx)
	require.NoError(t, err)
	require.False(t, rebuilt)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestOwnerIndexValidate 标识符白名单：非法表名/列名在任何用法前报错，
// 不让动态 SQL 带着注入载体跑出去。
func TestOwnerIndexValidate(t *testing.T) {
	ctx := context.Background()
	bad := NewOwnerIndex(nil, nil, "mail; DROP TABLE player", "player_id", "id")

	_, err := bad.BuildOne(ctx)
	require.Error(t, err, "非法表名必须报错")

	_, err = bad.Members(ctx, 1)
	require.Error(t, err)

	_, _, err = bad.LoadBatch(ctx, []int64{1})
	require.Error(t, err)

	badCol := NewOwnerIndex(nil, nil, "mail", "player_id; --", "id")
	_, _, err = badCol.LoadBatch(ctx, []int64{1})
	require.Error(t, err, "非法归属列同样要拦")

	// 合法标识符 + 双 nil 句柄：退化不 panic
	ok := NewOwnerIndex(nil, nil, "mail", "player_id", "id")
	ids, err := ok.Members(ctx, 1)
	require.Error(t, err, "无 Redis 无 MySQL 时回落路径报错而不是返回空列表")
	require.Nil(t, ids)
	rows, missing, err := ok.LoadBatch(ctx, []int64{1})
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Equal(t, []int64{1}, missing, "两处都没有 → 全部 missing")
}

// TestOwnerIndexMembersFallbackWhenBreakerOpen 熔断打开后 Members 仍回落 MySQL。
//
// 这是熔断的核心承诺：故障确认后命令不再发出去，既有降级路径立刻生效——
// 而不是「等 5s 超时之后才降级」。
func TestOwnerIndexMembersFallbackWhenBreakerOpen(t *testing.T) {
	ctx := context.Background()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err, "sqlmock")
	t.Cleanup(func() { _ = sqlDB.Close() })

	b := newQuietBreaker(t, BreakerOptions{Threshold: 2, Cooldown: time.Minute})
	rdb.AddHook(b)

	oi := NewOwnerIndex(rdb, sqlDB, "mail", "player_id", "id")
	mustSet(t, mr, OwnerIndexReadyKey("mail"), "1")

	// 打穿熔断：Redis 宕机 + 两次失败
	mr.Close()
	_ = rdb.Get(ctx, "probe").Err()
	_ = rdb.Get(ctx, "probe").Err()
	require.Equal(t, BreakerOpen, b.State(), "应已熔断")

	// 熔断期间：Ready 拿不到，Members 回落 MySQL 且成功
	mock.ExpectQuery("SELECT id FROM mail WHERE player_id =").WithArgs(int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(101))
	ids, err := oi.Members(ctx, 7)
	require.NoError(t, err, "熔断不应让降级路径失败")
	require.Equal(t, []int64{101}, ids)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestExpandLoadsAndHeals 展开的主路径：命中缓存 + 残留自愈。
//
// Expand 是两个调用方（登录预加载、点开功能的按需读）的共享实现——
// 自愈放在这里而不是各调用方，是因为「两边各写一遍」迟早会漏掉一处，
// 而漏掉的表现是索引越积越脏且无人发现。
func TestExpandLoadsAndHeals(t *testing.T) {
	oi, mr, mock, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()
	ctx := context.Background()

	mustSet(t, mr, OwnerIndexReadyKey("mail"), "1")
	mustZAddMember(t, mr, 7, 101)
	mustZAddMember(t, mr, 7, 102)
	mustZAddMember(t, mr, 7, 103) // 库里已删、索引漏清

	// 101 在缓存里命中（0 次 SQL）；102、103 未命中 → 一条 IN；
	// 103 库里也没有 → 进 missing
	mr.HSet(EntityKey("mail", 101), "id", "101", "title", "cached")
	mock.ExpectQuery("SELECT \\* FROM mail WHERE id IN").WithArgs(102, 103).
		WillReturnRows(mailRows([]any{102, 7, "from-db"}))

	rows, removed, err := oi.Expand(ctx, 7)
	require.NoError(t, err)
	require.Len(t, rows, 2, "101 走缓存、102 回源；103 两边都没有")
	require.Equal(t, []int64{103}, removed, "残留 id 应被报出来")

	// 自愈真的执行了：ZSET 里不再有 103
	// （不用 ZScore 断言：miniredis 对「key 在、member 没有」返回 (0, nil)）
	members, err := mr.ZMembers(OwnerIndexKey("mail"))
	require.NoError(t, err)
	require.NotContains(t, members, "103", "残留索引应被摘掉")

	// 回填：102 被写进缓存，下次展开不再打库
	require.Equal(t, "from-db", mr.HGet(EntityKey("mail", 102), "title"))

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestExpandEmptyOwner 名下确实没有实体 → 空结果且不报错。
//
// 与「索引答不上来」严格区分：后者必须返回 error 让调用方回落 SQL，
// 否则数据缺失会被演变成「功能列表为空」。
func TestExpandEmptyOwner(t *testing.T) {
	oi, mr, _, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()

	mustSet(t, mr, OwnerIndexReadyKey("mail"), "1")
	rows, removed, err := oi.Expand(context.Background(), 7)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Empty(t, removed)
}

// TestAddRemoveEntity 运行时增删索引。
//
// 这两个是业务侧真正会调的入口（投递新邮件 / 删除邮件）：
// Add 需要归属者，Remove 不需要——ZSET 的 member 本身能定位。
func TestAddRemoveEntity(t *testing.T) {
	oi, mr, _, cleanup := newTestOwnerIndex(t, 0)
	defer cleanup()
	ctx := context.Background()

	require.NoError(t, oi.AddEntity(ctx, 7, 501))
	score, err := mr.ZScore(OwnerIndexKey("mail"), "501")
	require.NoError(t, err)
	require.EqualValues(t, 7, score)

	require.NoError(t, oi.RemoveEntity(ctx, 501))
	// 501 是唯一成员：删掉后整个 ZSET 随之消失（Redis 的空集合不留 key）
	require.False(t, mr.Exists(OwnerIndexKey("mail")), "删除后索引里不应再有")

	// 非法入参 fail-fast，不静默吞掉
	require.Error(t, oi.AddEntity(ctx, 0, 1))
	require.Error(t, oi.AddEntity(ctx, 1, 0))
	// 空列表是合法的 no-op（批量删除可能为空）
	require.NoError(t, oi.RemoveEntity(ctx))
}

// mustZAddMember 往归属索引里放一条成员。
func mustZAddMember(t *testing.T, mr *miniredis.Miniredis, score float64, member int64) {
	t.Helper()
	_, err := mr.ZAdd(OwnerIndexKey("mail"), score, strconv.FormatInt(member, 10))
	require.NoError(t, err)
}
