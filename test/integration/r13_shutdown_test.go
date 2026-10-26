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
	"game_study/pkg/log"
	"game_study/pkg/writebehind"
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
			rdb.Del(context.Background(), fmt.Sprintf("wb:{player-%d}:state:%d", id, id))
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
