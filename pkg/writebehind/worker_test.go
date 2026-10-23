package writebehind

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSQLFenceSkipsOlderSnapshot(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO player_writebehind").WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT player_id, version FROM player_writebehind.*FOR UPDATE").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"player_id", "version"}).AddRow(7, 2))
	mock.ExpectCommit()
	worker := NewWorker(nil, database, 100)
	if err := worker.Persist(context.Background(), []Record{{Version: 1, State: Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 100}}}}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLFailureRollsBackFenceAndState(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT player_id, version FROM player_writebehind.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"player_id", "version"}).AddRow(7, 0))
	mock.ExpectExec("UPDATE player SET").WillReturnError(errors.New("database unavailable"))
	mock.ExpectRollback()
	if err := NewWorker(nil, database, 100).Persist(context.Background(), []Record{{Version: 1, State: Snapshot{PlayerID: 7}}}); err == nil {
		t.Fatal("failure lost")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestRunStaggersPartitionsAndDrains 验证错峰调度的两个不变量：
//  1. 错峰不跳分区：放在分区 3 的脏玩家（前 3 个分区的等待之后才轮到）
//     仍被完整落库并 Ack；
//  2. 空转不碰库：排空后的后续 tick 面对空队列不再产生任何 SQL。
func TestRunStaggersPartitionsAndDrains(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	store := NewStore(rdb)
	// 播种 version 0 再提交一次：版本 CAS 成功后玩家进入分区 3 的脏队列
	if _, err := store.Seed(ctx, Snapshot{PlayerID: 3, Items: map[int32]int64{}}, 0); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireLease(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	snap := Snapshot{PlayerID: 3, Items: map[int32]int64{}}
	if _, err := store.Commit(ctx, snap.PlayerID, snap, snap.FieldNames(), nil, lease); err != nil {
		t.Fatal(err)
	}

	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	// 单批单玩家的完整期望（顺序匹配）：版本栅栏 → 各表合批写 → 提交
	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT player_id, version FROM player_writebehind.*FOR UPDATE").
		WillReturnRows(sqlmock.NewRows([]string{"player_id", "version"}).AddRow(3, 0))
	mock.ExpectExec("UPDATE player SET").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE FROM player_item").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		// interval 20ms → stagger 2ms：分区 3 约在首个 tick 后 6ms 被处理
		done <- NewWorker(store, database, 100).Run(runCtx, 20*time.Millisecond, func(err error) {
			t.Errorf("unexpected flush error: %v", err)
		})
	}()

	// 轮询等待分区 3 落库并 Ack（初始必为非空，无「未处理即通过」竞态）
	deadline := time.Now().Add(3 * time.Second)
	for {
		ids, err := store.Pending(ctx, 3, 10)
		require.NoError(t, err)
		if len(ids) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("partition 3 not drained under stagger")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run should stop cleanly on cancel: %v", err)
	}
	// 全部期望恰好满足一次：证明排空后空转 tick 没有额外写库
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
