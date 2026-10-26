//go:build integration

// test/integration/r13_shutdown_test.go —— R1.3 关闭顺序：先排空在途，再确认落库
//
// 验证缓存权威模式下关服不丢最后一批：消息投递后不等处理完就进入关闭流程，
// 只要顺序是「Drain → 落库 → 停 Actor」，所有玩家状态都必须完整进 MySQL。
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/signin"
	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/codec"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/writebehind"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestR13ShutdownDrainsInflightBeforeFlush(t *testing.T) {
	database, rdb, _ := startStorage(t)
	ctx := context.Background()
	store := writebehind.NewStore(rdb)
	// batch=2：5 个玩家必须分 3 轮排空，顺带验证 FlushAll 的循环而不是只落一批。
	repo := player.NewRepository(database)
	repo.SetCache(store)
	logger := log.New("r13-test", log.WithLevel("error"))

	const n = 5
	ids := make([]int64, 0, n)
	defer func() {
		for _, id := range ids {
			rdb.Del(context.Background(), fmt.Sprintf("wb:{player-%d}:state:%d", writebehind.Partition(id), id))
			rdb.ZRem(context.Background(), fmt.Sprintf("wb:{player-%d}:dirty", writebehind.Partition(id)), id)
			database.Exec("DELETE FROM player_signin WHERE player_id=?", id)
			database.Exec("DELETE FROM player_writebehind WHERE player_id=?", id)
			database.Exec("DELETE FROM player_item WHERE player_id=?", id)
			database.Exec("DELETE FROM player_attr WHERE player_id=?", id)
			database.Exec("DELETE FROM player WHERE id=?", id)
		}
	}()
	for i := 0; i < n; i++ {
		id := int64(920000 + i)
		insertTestPlayer(t, database, id)
		ids = append(ids, id)
	}

	registry := module.NewRegistry()
	// 固定周三（北京时间），签到领取第 3 天奖励。
	signin.M.SetClock(func() time.Time {
		return time.Date(2026, 10, 21, 2, 0, 0, 0, time.UTC)
	})
	registry.Register(signin.M)

	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 256})
	sys.Start(ctx)

	replies := make(chan uint32, n*4)
	pids := make([]*actor.PID, 0, n)
	for _, id := range ids {
		a := player.NewActor(id, registry, logger, repo, nil,
			func(_ uint64, msgID uint32, _ []byte) error {
				replies <- msgID
				return nil
			})
		pid, err := sys.Spawn(fmt.Sprintf("r13:%d", id), a)
		require.NoError(t, err)
		err = sys.Send(pid, &player.BindMsg{PlayerID: id, SessionID: uint64(id)})
		require.NoError(t, err)
		pids = append(pids, pid)
	}

	// 投递后**不等待**处理完，直接进入关闭流程：这正是关服时的真实时序。
	for _, pid := range pids {
		err := sys.Send(pid, &player.PacketMsg{MsgID: codec.MsgSigninClaimReq})
		require.NoError(t, err)
	}

	closeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// ① 排空在途（等价于 Server.Shutdown 的 drain-actors 步骤）
	err := sys.Drain(closeCtx)
	require.NoError(t, err, fmt.Sprintf("drain: %v", err))
	// ② 落库（等价于 flush-all 步骤）。内存权威下数据源是 Actor 内存，
	// 这里对每个玩家发一次 SaveMsg，与 Server.FlushAll → persistPlayer
	// 走的是同一条路径。
	for _, pid := range pids {
		done := make(chan error, 1)
		require.NoError(t, sys.Send(pid, &player.SaveMsg{Done: done}), "send save")
		select {
		case saveErr := <-done:
			require.NoError(t, saveErr, "save must succeed during shutdown")
		case <-closeCtx.Done():
			t.Fatal("save timeout during shutdown")
		}
	}
	// ③ 停 Actor
	sys.Stop()

	// 每个玩家都必须有完整签到行，不能只落下前两批。
	for _, id := range ids {
		var days, rewards string
		if err := database.QueryRow(
			`SELECT claimed_days,claimed_rewards FROM player_signin WHERE player_id=?`, id,
		).Scan(&days, &rewards); err != nil {
			t.Fatalf("player %d not persisted: %v", id, err)
		}
		require.EqualValues(t, "[3]", days, fmt.Sprintf("player %d: days=%q rewards=%q", id, days, rewards))
		require.EqualValues(t, `{"1001":300}`, rewards)
	}
}

// TestR13FlushAllKeepsDirtyOnDatabaseFailure 落库失败时不能 Ack：
// 脏任务必须留在 Redis，供下次启动或独立 flusher 重试。
func TestR13FlushAllKeepsDirtyOnDatabaseFailure(t *testing.T) {
	database, rdb, dsn := startStorage(t)
	ctx := context.Background()
	store := writebehind.NewStore(rdb)
	repo := player.NewRepository(database)
	repo.SetCache(store)
	logger := log.New("r13-test", log.WithLevel("error"))

	id := int64(920100)
	insertTestPlayer(t, database, id)
	defer func() {
		rdb.Del(context.Background(), fmt.Sprintf("wb:{player-%d}:state:%d", writebehind.Partition(id), id))
		rdb.ZRem(context.Background(), fmt.Sprintf("wb:{player-%d}:dirty", writebehind.Partition(id)), id)
	}()

	registry := module.NewRegistry()
	signin.M.SetClock(func() time.Time {
		return time.Date(2026, 10, 21, 2, 0, 0, 0, time.UTC)
	})
	registry.Register(signin.M)
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 64})
	sys.Start(ctx)

	a := player.NewActor(id, registry, logger, repo, nil,
		func(uint64, uint32, []byte) error { return nil })
	pid, err := sys.Spawn(fmt.Sprintf("r13b:%d", id), a)
	require.NoError(t, err)
	err = sys.Send(pid, &player.BindMsg{PlayerID: id, SessionID: uint64(id)})
	require.NoError(t, err)
	err = sys.Send(pid, &player.PacketMsg{MsgID: codec.MsgSigninClaimReq})
	require.NoError(t, err)
	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = sys.Drain(drainCtx)
	require.NoError(t, err, fmt.Sprintf("drain: %v", err))
	sys.Stop()

	// 数据库不可用：排空必须失败，且脏任务仍在。
	//
	// 内存权威下 Sync 不再入队（落库由 flusher 从内存走），这里手动补一条
	// 队列成员，专门验证 writebehind 通道（兼容路径）的失败语义仍然正确。
	_, err = rdb.ZAdd(ctx, fmt.Sprintf("wb:{player-%d}:dirty", writebehind.Partition(id)),
		redis.Z{Score: 1, Member: fmt.Sprintf("%d", id)}).Result()
	require.NoError(t, err, "enqueue for writebehind path")
	err = database.Close()
	require.NoError(t, err, fmt.Sprintf("close db: %v", err))
	worker := writebehind.NewWorker(store, database.DB, 10)
	err = worker.FlushAll(ctx)
	require.Error(t, err, "flush must fail when MySQL is unavailable")
	pending, err := store.Pending(ctx, writebehind.Partition(id), 100)
	require.NoError(t, err, fmt.Sprintf("dirty task lost after failed flush: %v %v", pending, err))
	require.Len(t, pending, 1)

	// 恢复连接后同一脏任务可以落库。
	reopened, err := db.New(db.Config{DSN: dsn, MaxOpen: 5, MaxIdle: 2})
	require.NoError(t, err, "reconnect")
	defer func() {
		database.Exec("DELETE FROM player_signin WHERE player_id=?", id)
		database.Exec("DELETE FROM player_writebehind WHERE player_id=?", id)
		database.Exec("DELETE FROM player_item WHERE player_id=?", id)
		database.Exec("DELETE FROM player_attr WHERE player_id=?", id)
		database.Exec("DELETE FROM player WHERE id=?", id)
		reopened.Close()
	}()
	worker2 := writebehind.NewWorker(store, reopened.DB, 10)
	err = worker2.FlushAll(ctx)
	require.NoError(t, err, fmt.Sprintf("flush after recovery: %v", err))
	var days string
	err = reopened.QueryRow(`SELECT claimed_days FROM player_signin WHERE player_id=?`, id).Scan(&days)
	require.NoError(t, err, fmt.Sprintf("signin row: %v", err))
	require.EqualValues(t, "[3]", days)
	pending, err = store.Pending(ctx, writebehind.Partition(id), 100)
	require.NoError(t, err, fmt.Sprintf("dirty queue not drained: %v %v", pending, err))
	require.Empty(t, pending)
}
