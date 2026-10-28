// internal/game/player/syncgrant_test.go —— A 类落库脱离 Actor 线程
package player

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"game_study/internal/game/module"
	"game_study/pkg/actor"
	"game_study/pkg/db"
	"game_study/pkg/log"
)

// expectSaveForce 一套完整的同步落库语句序列（与 persist_sync_test 一致）。
func expectSaveForce(mock sqlmock.Sqlmock, playerID, from, to int64) {
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE player SET").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_item").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("FROM player_writebehind WHERE player_id = \\?").
		WithArgs(playerID).
		WillReturnRows(sqlmock.NewRows([]string{"player_id", "version", "applied_grants"}).
			AddRow(playerID, from, nil))
	mock.ExpectExec("INSERT INTO player_writebehind").
		WithArgs(playerID, to, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

// 同步发货不在 Actor 线程里跑：grantSync 只克隆状态就返回，
// 落库结果以 grantDoneMsg 回投，由 Actor 自己收尾。
func TestGrantSyncOffloadsSaveToWorker(t *testing.T) {
	raw, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer raw.Close()

	a := NewActor(7, nil, log.New("test", log.WithLevel("error")), NewRepository(&db.DB{DB: raw}), nil, nil)
	a.state.SetProfile(module.Profile{Name: "n", Level: 2, Exp: 3})
	a.state.AddItem(1001, 10)
	require.True(t, a.state.ClaimBiz("recharge:o-1"))

	done := make(chan error, 1)
	msg := &GrantItemsMsg{Source: "recharge", BizID: "recharge:o-1", Sync: true, Done: done}

	posted := make(chan *grantDoneMsg, 1)
	a.self = &actor.PID{ID: "player:7"}
	a.WithPost(func(_ *actor.PID, m any) error {
		posted <- m.(*grantDoneMsg)
		return nil
	})

	expectSaveForce(mock, 7, 10, 11)

	a.grantSync(msg)
	require.True(t, a.syncBusy, "应立即置忙，且不阻塞调用方")

	select {
	case got := <-posted:
		require.NoError(t, got.err)
		require.EqualValues(t, 11, got.version)
		a.finishSyncGrant(got)
	case <-time.After(2 * time.Second):
		t.Fatal("落库结果没有回投")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("回执没有发出")
	}
	require.False(t, a.syncBusy)
	require.NoError(t, mock.ExpectationsWereMet())
}

// 同一玩家串行：在前一笔完成前，第二笔只排队，不产生第二次事务。
func TestGrantSyncSerializesPerPlayer(t *testing.T) {
	raw, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer raw.Close()

	a := NewActor(7, nil, log.New("test", log.WithLevel("error")), NewRepository(&db.DB{DB: raw}), nil, nil)
	a.self = &actor.PID{ID: "player:7"}
	a.post = func(*actor.PID, any) error { return nil }

	// 模拟「第一笔还在跑」。
	a.syncBusy = true
	a.grantSync(&GrantItemsMsg{Source: "recharge", BizID: "o-2", Sync: true})

	require.Len(t, a.syncQueue, 1, "第二笔必须排队而不是并发开事务")
	require.NoError(t, mock.ExpectationsWereMet(), "排队期间不应有任何 SQL")
}
