// internal/game/player/preload_test.go —— 登录预加载回归测试
//
// 主要目的：锁死「非缓存模式 Load 必须返回」这一性质。历史 bug：
// wg.Add(5) 配 3 个 goroutine（删查询时漏改计数）→ Wait 永久阻塞，
// 且只在 Redis 缺席的降级路径上暴露，缓存权威模式（生产默认）不会触发。
// errgroup 改造后结构上不可能再出现，此测试防止将来有人改回手写 WaitGroup。
package player

import (
	"context"
	"testing"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/log"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

const preloadTestTimeout = 3 * time.Second

func newPreloadTestDB(t *testing.T) (*Preloader, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err, "sqlmock")
	t.Cleanup(func() { _ = sqlDB.Close() })
	// 三个查询并发执行，完成顺序不确定：关闭有序匹配，否则偶发失败。
	mock.MatchExpectationsInOrder(false)
	return NewPreloader(&db.DB{DB: sqlDB}, log.New("test"), time.Second), mock
}

func expectPlayerRows(mock sqlmock.Sqlmock, playerID int64, now time.Time) {
	mock.ExpectQuery("FROM player WHERE").WithArgs(playerID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at",
		}).AddRow(playerID, 1, "hero", 10, 100, 500, 0, now, now))
	mock.ExpectQuery("FROM player_attr WHERE").WithArgs(playerID).
		WillReturnRows(sqlmock.NewRows([]string{
			"player_id", "career", "hp", "mp", "atk", "def", "spd", "created_at", "updated_at",
		}).AddRow(playerID, 1, 100, 50, 10, 5, 8, now, now))
	mock.ExpectQuery("FROM player_item WHERE").WithArgs(playerID).
		WillReturnRows(sqlmock.NewRows([]string{"item_id", "count"}).AddRow(1001, 3))
}

// TestLoadReturns 预加载必须在一个有界时间内返回（防 WaitGroup 计数错配死锁）。
func TestLoadReturns(t *testing.T) {
	p, mock := newPreloadTestDB(t)
	expectPlayerRows(mock, 7, time.Now())

	type result struct {
		state *State
		pf    *Prefetch
		err   error
	}
	done := make(chan result, 1)
	go func() {
		state, pf, err := p.Load(context.Background(), 7)
		done <- result{state, pf, err}
	}()

	select {
	case got := <-done:
		require.NoError(t, got.err, "Load")
		require.NotNil(t, got.pf.Player, "player 未加载")
		require.Equal(t, "hero", got.pf.Player.Name)
		require.NotNil(t, got.pf.Attr, "attr 未加载")
		require.EqualValues(t, 100, got.pf.Attr.Hp)
		require.EqualValues(t, 3, got.pf.Items[1001], "items 未加载")
		require.Equal(t, "hero", got.state.Profile().Name, "state 组装错误")
		require.EqualValues(t, 10, got.state.Profile().Level)
	case <-time.After(preloadTestTimeout):
		t.Fatal("Load 未返回（疑似 WaitGroup 计数不配对导致死锁）")
	}

	require.NoError(t, mock.ExpectationsWereMet(), "SQL 预期未满足")
}

// TestLoadPlayerMissing player 行缺失 → ErrNoPlayer，且此时 attr/items 的
// 查询结果不影响结论（errgroup 返回第一个非 nil 错误）。
func TestLoadPlayerMissing(t *testing.T) {
	p, mock := newPreloadTestDB(t)
	mock.ExpectQuery("FROM player WHERE").WithArgs(int64(8)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at",
		})) // 空结果 = sql.ErrNoRows
	mock.ExpectQuery("FROM player_attr WHERE").WithArgs(int64(8)).
		WillReturnRows(sqlmock.NewRows([]string{
			"player_id", "career", "hp", "mp", "atk", "def", "spd", "created_at", "updated_at",
		}).AddRow(8, 1, 100, 50, 10, 5, 8, time.Now(), time.Now()))
	mock.ExpectQuery("FROM player_item WHERE").WithArgs(int64(8)).
		WillReturnRows(sqlmock.NewRows([]string{"item_id", "count"}))

	done := make(chan error, 1)
	go func() {
		_, _, err := p.Load(context.Background(), 8)
		done <- err
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrNoPlayer)
	case <-time.After(preloadTestTimeout):
		t.Fatal("Load 未返回（疑似死锁）")
	}
}
