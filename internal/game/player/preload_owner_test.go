// preload_owner_test.go —— 登录归属展开（缓存架构设计 §4.2）回归测试
//
// 覆盖三条性质：
//   - 展开结果按表分组、按 id 升序，未命中行走 SQL 捞回并回填数据表；
//   - 索引残留（索引有、Redis 与 MySQL 都没有）在读时被摘掉（自愈）；
//   - 未注入索引时是空操作（没有 mappings 的部署行为不变）。
package player

import (
	"context"
	"testing"
	"time"

	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/log"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newOwnerPreloader 构造「miniredis + sqlmock + 一个 mail 归属索引」环境。
func newOwnerPreloader(t *testing.T) (*Preloader, *cache.OwnerIndex, *miniredis.Miniredis, sqlmock.Sqlmock) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err, "sqlmock")
	t.Cleanup(func() { _ = sqlDB.Close() })
	// 展开与三个基础查询并发执行，完成顺序不确定：关有序匹配。
	mock.MatchExpectationsInOrder(false)

	p := NewPreloader(&db.DB{DB: sqlDB}, log.New("test"), time.Second)
	oi := cache.NewOwnerIndex(rdb, sqlDB, "mail", "player_id", "id")
	p.SetOwnerIndexes(oi)

	// 索引就绪（生产里由启动构建置位；这里直接模拟「已建好」）
	require.NoError(t, mr.Set(cache.OwnerIndexReadyKey("mail"), "1"))
	return p, oi, mr, mock
}

// TestExpandOwnersLoadsAndBackfills hash 命中的直接用，未命中的一次 SQL 捞回并回填。
func TestExpandOwnersLoadsAndBackfills(t *testing.T) {
	ctx := context.Background()
	p, oi, mr, mock := newOwnerPreloader(t)

	require.NoError(t, oi.Index().Add(ctx, 7, 201))
	require.NoError(t, oi.Index().Add(ctx, 7, 202))
	// 201 的数据表里已有；202 没有 → 回源
	mr.HSet(cache.EntityKey("mail", 201), "id", "201", "title", "cached")
	mock.ExpectQuery("SELECT \\* FROM mail WHERE id IN").WithArgs(202).
		WillReturnRows(sqlmock.NewRows([]string{"id", "player_id", "title"}).
			AddRow(202, 7, "from-db"))

	rows, err := p.ExpandOwners(ctx, 7)
	require.NoError(t, err)

	got, ok := rows["mail"]
	require.True(t, ok, "mail 表应出现在展开结果里")
	require.Len(t, got, 2)
	require.EqualValues(t, 201, got[0].ID, "按 id 升序")
	require.Equal(t, "cached", got[0].Fields["title"])
	require.EqualValues(t, 202, got[1].ID)
	require.Equal(t, "from-db", got[1].Fields["title"])

	// 回填：202 的实体 hash 已写进 Redis，下次登录不再打库
	require.Equal(t, "from-db", mr.HGet(cache.EntityKey("mail", 202), "title"))

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestExpandOwnersSelfHealsStaleIndex 索引残留：索引有、两处数据都没有 →
// 读时摘掉，且不返回该行。
func TestExpandOwnersSelfHealsStaleIndex(t *testing.T) {
	ctx := context.Background()
	p, oi, mr, mock := newOwnerPreloader(t)

	require.NoError(t, oi.Index().Add(ctx, 7, 201))
	require.NoError(t, oi.Index().Add(ctx, 7, 202)) // 202 是残留
	mock.ExpectQuery("SELECT \\* FROM mail WHERE id IN").WithArgs(201, 202).
		WillReturnRows(sqlmock.NewRows([]string{"id", "player_id", "title"}).
			AddRow(201, 7, "alive"))

	rows, err := p.ExpandOwners(ctx, 7)
	require.NoError(t, err)
	require.Len(t, rows["mail"], 1, "残留行不进结果")
	require.EqualValues(t, 201, rows["mail"][0].ID)

	// 自愈：残留 id 已从 ZSET 摘掉（ZSCORE 判不存在）
	exists, err := oi.Index().Exists(ctx, 202)
	require.NoError(t, err)
	require.False(t, exists, "残留 id 应被自愈摘掉")
	exists, err = oi.Index().Exists(ctx, 201)
	require.NoError(t, err)
	require.True(t, exists, "有效 id 不受影响")

	members, err := mr.ZMembers(cache.OwnerIndexKey("mail"))
	require.NoError(t, err)
	require.Len(t, members, 1)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestExpandOwnersNoIndexes 未注入索引 → 空操作，不打任何存储。
func TestExpandOwnersNoIndexes(t *testing.T) {
	p, _, _, mock := newOwnerPreloader(t)
	p.SetOwnerIndexes() // 清空

	rows, err := p.ExpandOwners(context.Background(), 7)
	require.NoError(t, err)
	require.Nil(t, rows)
	require.NoError(t, mock.ExpectationsWereMet(), "不应产生任何 SQL")
}

// TestExpandOwnersFailsLoginWhenDBDown MySQL 答不上来 → 报错（登录失败），
// 而不是放行后展示空列表。
func TestExpandOwnersFailsLoginWhenDBDown(t *testing.T) {
	p, oi, _, _ := newOwnerPreloader(t)
	require.NoError(t, oi.Index().Add(context.Background(), 7, 201))

	// 不 mock 任何 SQL：查询直接报错
	_, err := p.ExpandOwners(context.Background(), 7)
	require.Error(t, err, "数据加载失败必须让登录失败")
}

// TestLoadIncludesOwnerRows 非缓存模式的 Load 把归属展开并进并行查询，
// 结果经 Prefetch.OwnerRows 暴露。
func TestLoadIncludesOwnerRows(t *testing.T) {
	p, oi, mr, mock := newOwnerPreloader(t)
	_ = mr // 索引与数据都从 mock 来，Redis 只在展开时被读到
	expectPlayerRows(mock, 7, time.Now())
	require.NoError(t, oi.Index().Add(context.Background(), 7, 301))
	mock.ExpectQuery("SELECT \\* FROM mail WHERE id IN").WithArgs(301).
		WillReturnRows(sqlmock.NewRows([]string{"id", "player_id", "title"}).
			AddRow(301, 7, "welcome"))

	state, pf, err := p.Load(context.Background(), 7)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Len(t, pf.OwnerRows("mail"), 1, "归属行应随 Prefetch 下发")
	require.Equal(t, "welcome", pf.OwnerRows("mail")[0].Fields["title"])
	// 未展开的表返回 nil（模块回落自行查库）
	require.Nil(t, pf.OwnerRows("guild"))

	require.NoError(t, mock.ExpectationsWereMet())
}
