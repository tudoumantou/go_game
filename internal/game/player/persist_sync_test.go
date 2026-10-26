package player

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/writebehind"
)

// newTestStore 造一个挂在 miniredis 上的缓存仓储，用于验证「缓存权威」
// 分支的判定，不需要真 Redis。
func newTestStore(t *testing.T) *writebehind.Store {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return writebehind.NewStore(rdb)
}

// TestSaveForceCommitsAssetsAndIdempotencyKeyTogether 验证 A 类同步落库的
// SQL 契约：资产、幂等键、版本推进在同一个事务里，版本号被读回。
//
// 语句顺序就是正确性本身。幂等键一旦拆到别的事务，「发了货但没记下
// 业务键」的窗口就会出现——Kafka 重投时幂等不命中，玩家白拿一份。
func TestSaveForceCommitsAssetsAndIdempotencyKeyTogether(t *testing.T) {
	raw, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer raw.Close()

	repo := NewRepository(&db.DB{DB: raw})
	state := NewState(7)
	state.SetProfile(module.Profile{Name: "n", Level: 2, Exp: 3, Coin: 4, SceneID: 5})
	state.AddItem(1001, 10)
	require.True(t, state.ClaimBiz("recharge:o-1"))

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE player SET").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_item").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT version FROM player_writebehind").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(11))
	mock.ExpectCommit()

	version, err := repo.SaveForce(context.Background(), state)
	require.NoError(t, err)
	require.EqualValues(t, 11, version,
		"版本必须读回：调用方要用它更新本地 CAS 期望值，否则下一次提交必冲突")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSaveForceFailureKeepsDirtyItems 验证落库失败时脏标记不被清掉。
//
// 清标记等于把「没写进去」当成「已经写完」：这一笔发货就再也不会重试，
// 玩家钱付了、货没了，而且没有任何报错。
func TestSaveForceFailureKeepsDirtyItems(t *testing.T) {
	raw, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer raw.Close()

	repo := NewRepository(&db.DB{DB: raw})
	state := NewState(7)
	state.SetProfile(module.Profile{Name: "n", Level: 2, Exp: 3, Coin: 4, SceneID: 5})
	state.AddItem(1001, 10)

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE player SET").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_item").WillReturnError(context.DeadlineExceeded)
	mock.ExpectRollback()

	_, err = repo.SaveForce(context.Background(), state)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, map[int32]int64{1001: 10}, state.DirtyItems(),
		"失败必须保留脏标记，下轮才能重试同一批数据")
}

// TestSaveRejectsDirectWriteInCacheMode 验证异步通道的旧约束没被破坏：
// 缓存权威模式下普通 Save 仍然拒绝直写，只有 SaveForce（A 类）例外。
func TestSaveRejectsDirectWriteInCacheMode(t *testing.T) {
	raw, _, err := sqlmock.New()
	require.NoError(t, err)
	defer raw.Close()

	repo := NewRepository(&db.DB{DB: raw})
	repo.SetCache(newTestStore(t))
	require.True(t, repo.CacheEnabled())

	err = repo.Save(context.Background(), NewState(7))
	require.Error(t, err, "缓存权威模式下普通 Save 必须拒绝直写")
}
