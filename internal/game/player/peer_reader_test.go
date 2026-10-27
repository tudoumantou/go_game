// internal/game/player/peer_reader_test.go —— 跨玩家只读
//
// 盯三条语义：
//  1. 有快照时只读 Redis，不碰数据库（sqlmock 不设任何预期，一旦查库就失败）；
//  2. 没快照时回落库，且**不回填**（第二次读仍然查库）；
//  3. 批量读走 pipeline + 一条 IN，未命中的 id 不出现在结果里。
package player

import (
	"context"
	"testing"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/writebehind"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// newPeerRepo 起一个「缓存 + sqlmock」的仓储；cache 为 nil 时只走库。
func newPeerRepo(t *testing.T) (*Repository, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return NewRepository(&db.DB{DB: sqlDB}), mock
}

func seedSnapshot(t *testing.T, store *writebehind.Store, id int64, name string, level int32) {
	t.Helper()
	_, err := store.Seed(context.Background(), writebehind.Snapshot{
		PlayerID: id,
		Profile:  writebehind.Profile{Name: name, Level: level},
		Items:    map[int32]int64{1001: 3},
	}, 1)
	require.NoError(t, err)
}

// TestPeerViewHitsCache 命中时只用 Redis：sqlmock 无预期，查库即失败。
func TestPeerViewHitsCache(t *testing.T) {
	repo, _ := newPeerRepo(t)
	store := newTestStore(t)
	repo.SetCache(store)
	seedSnapshot(t, store, 7, "cached", 30)

	v, err := repo.View(context.Background(), 7)
	require.NoError(t, err)
	require.NotNil(t, v)
	require.Equal(t, "cached", v.Name)
	require.EqualValues(t, 30, v.Level)
}

// TestPeerViewFallsBackToDB 未命中回落库，且不回填（第二次仍查库）。
func TestPeerViewFallsBackToDB(t *testing.T) {
	repo, mock := newPeerRepo(t)
	store := newTestStore(t)
	repo.SetCache(store)
	now := time.Now()
	rows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at"}).
			AddRow(int64(8), int64(1), "fromdb", 12, 0, 0, 0, now, now)
	}
	mock.ExpectQuery(`FROM player WHERE id = \?`).WithArgs(int64(8)).WillReturnRows(rows())
	mock.ExpectQuery(`FROM player WHERE id = \?`).WithArgs(int64(8)).WillReturnRows(rows())

	for i := 0; i < 2; i++ {
		v, err := repo.View(context.Background(), 8)
		require.NoError(t, err)
		require.Equal(t, "fromdb", v.Name)
		require.EqualValues(t, 12, v.Level)
	}
	require.NoError(t, mock.ExpectationsWereMet())

	// 「不回填」的直接证据：第二次读之后快照仍然不存在
	_, err := store.Load(context.Background(), 8)
	require.ErrorIs(t, err, writebehind.ErrMiss, "只读路径不应回填 Redis")
}

// TestPeerViewMissingIsNil 玩家不存在返回 (nil, nil)，不是错误。
func TestPeerViewMissingIsNil(t *testing.T) {
	repo, mock := newPeerRepo(t)
	mock.ExpectQuery(`FROM player WHERE id = \?`).WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	v, err := repo.View(context.Background(), 9)
	require.NoError(t, err)
	require.Nil(t, v)
}

// TestPeerViewManyMixed 两个命中、一个回源：pipeline 一次 + IN 一条。
func TestPeerViewManyMixed(t *testing.T) {
	repo, mock := newPeerRepo(t)
	store := newTestStore(t)
	repo.SetCache(store)
	seedSnapshot(t, store, 7, "a", 10)
	seedSnapshot(t, store, 8, "b", 20)

	now := time.Now()
	mock.ExpectQuery(`FROM player WHERE id IN \(\?\)`).WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at"}).
			AddRow(int64(9), int64(1), "c", 30, 0, 0, 0, now, now))

	views, err := repo.ViewMany(context.Background(), []int64{7, 8, 9, 7, 0})
	require.NoError(t, err)
	require.Len(t, views, 3, "7/8 走缓存，9 回源，重复与非法 id 去重")
	require.Equal(t, "a", views[7].Name)
	require.Equal(t, "b", views[8].Name)
	require.Equal(t, "c", views[9].Name)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestPeerViewManyAllCached 全部命中时不产生任何 SQL。
func TestPeerViewManyAllCached(t *testing.T) {
	repo, _ := newPeerRepo(t)
	store := newTestStore(t)
	repo.SetCache(store)
	seedSnapshot(t, store, 7, "a", 10)
	seedSnapshot(t, store, 8, "b", 20)

	views, err := repo.ViewMany(context.Background(), []int64{7, 8})
	require.NoError(t, err)
	require.Len(t, views, 2)
}

// TestPeerViewWithoutCache 无缓存环境（测试/降级）直接查库。
func TestPeerViewWithoutCache(t *testing.T) {
	repo, mock := newPeerRepo(t)
	now := time.Now()
	mock.ExpectQuery(`FROM player WHERE id = \?`).WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at"}).
			AddRow(int64(5), int64(1), "nocache", 1, 0, 0, 0, now, now))

	v, err := repo.View(context.Background(), 5)
	require.NoError(t, err)
	require.Equal(t, "nocache", v.Name)
	require.NoError(t, mock.ExpectationsWereMet())
}
