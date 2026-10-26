//go:build integration

// test/integration/signin_test.go —— 单表签到全链路（缓存权威 + Write-Behind，见 docs/signin.md）
//
// 不起真实服务进程：直接装配 actor.System + 模块 Registry + Repository/Worker，
// 用可控时钟把「今天」固定为周三，覆盖四条路径：
//  1. 在线签到只更新副本（SQL 立即查应为空）；
//  2. 落库从 Actor 内存走（发 SaveMsg 触发，与 flusher 同一条路径）；
//  3. Actor 重启：Redis 副本仍在 → 从副本恢复已领状态；
//  4. Redis 丢失 → FlatPersister 走生成代码从 SQL 冷启动恢复。
//
// 另外验证角色隔离与重复领取幂等。
package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/signin"
	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/codec"
	"game_study/pkg/db"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/writebehind"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestSigninActorFlow 验证单表签到的完整链路：在线只写缓存、独立进程合批落库、
// Redis 丢失后从 SQL 冷启动恢复，且重复领取不重复发奖。
func TestSigninActorFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	database, err := db.New(db.Config{DSN: testDSN, MaxOpen: 5})
	require.NoError(t, err)
	defer database.Close()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	defer rdb.Close()
	ids := []int64{88021, 88031} // 相同持久化分片，验证多角色真正合批。
	defer func() {
		for _, id := range ids {
			rdb.Del(context.Background(), fmt.Sprintf("wb:{player-%d}:state:%d", writebehind.Partition(id), id))
			rdb.ZRem(context.Background(), fmt.Sprintf("wb:{player-%d}:dirty", writebehind.Partition(id)), id)
		}
		for _, table := range []string{"player_signin", "player_writebehind", "player_item", "player_attr", "player"} {
			key := "player_id"
			if table == "player" {
				key = "id"
			}
			database.Exec("DELETE FROM " + table + " WHERE " + key + " IN (88021,88031)")
		}
	}()
	for _, id := range ids {
		_, err := database.Exec(`INSERT INTO player (id,account_id,name) VALUES (?,?,?)`, id, 99, fmt.Sprintf("signin_%d", id))
		require.NoError(t, err)
	}
	store := writebehind.NewStore(rdb)
	repo := player.NewRepository(database)
	repo.SetCache(store)
	logger := log.New("signin-test", log.WithLevel("error"))
	var clock atomic.Int64
	// 2026-10-21 10:00 北京时间，周三。
	clock.Store(time.Date(2026, 10, 21, 2, 0, 0, 0, time.UTC).Unix())
	registry := module.NewRegistry()
	signin.M.SetClock(func() time.Time { return time.Unix(clock.Load(), 0) })
	registry.Register(signin.M)
	type response struct {
		id   uint32
		body []byte
	}
	responses := make(chan response, 100)
	var sys *actor.System
	start := func() { sys = actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 128}); sys.Start(ctx) }
	start()
	defer func() { sys.Stop() }()
	spawn := func(id int64) *actor.PID {
		a := player.NewActor(id, registry, logger, repo, nil, func(_ uint64, id uint32, body []byte) error { responses <- response{id, body}; return nil })
		pid, err := sys.Spawn(fmt.Sprintf("signin:%d", id), a)
		require.NoError(t, err)
		err = sys.Send(pid, &player.BindMsg{PlayerID: id, SessionID: uint64(id)})
		require.NoError(t, err)
		return pid
	}
	p1, p2 := spawn(ids[0]), spawn(ids[1])
	call := func(pid *actor.PID, id uint32, out proto.Message) {
		t.Helper()
		err := sys.Send(pid, &player.PacketMsg{MsgID: id})
		require.NoError(t, err)
		for {
			select {
			case response := <-responses:
				if response.id == id+1 {
					err := proto.Unmarshal(response.body, out)
					require.NoError(t, err)
					return
				}
			case <-time.After(10 * time.Second):
				t.Fatal("actor timeout")
			}
		}
	}
	claim := func(pid *actor.PID) *gamev1.SigninClaimResp {
		t.Helper()
		out := &gamev1.SigninClaimResp{}
		call(pid, codec.MsgSigninClaimReq, out)
		require.EqualValues(t, 0, out.Code)
		return out
	}
	sqlRow := func(id int64) (days, rewards string) {
		t.Helper()
		err := database.QueryRow(`SELECT claimed_days,claimed_rewards FROM player_signin WHERE player_id=?`, id).Scan(&days, &rewards)
		if err != nil {
			return "", ""
		}
		return days, rewards
	}
	info := &gamev1.SigninInfoResp{}
	call(p1, codec.MsgSigninInfoReq, info)
	require.False(t, info.Code != 0 || info.Today != 3 || info.ClaimedToday)
	first := claim(p1)
	require.False(t, first.Already || first.Day != 3 || first.ClaimedRewards[1001] != 300, "claim must grant day 3")
	_, rewards := sqlRow(ids[0])
	require.EqualValues(t, "", rewards, "claim wrote SQL synchronously")
	got := claim(p1)
	require.False(t, !got.Already || got.ClaimedRewards[1001] != 300, "duplicate claim")
	got = claim(p2)
	require.False(t, got.Already || got.ClaimedRewards[1001] != 300, "role isolation")
	// 内存权威：落库从 Actor 内存走（生产环境由 pkg/flush 的分片 worker
	// 定时触发同一条路径）。这里对两个角色各发一次 SaveMsg，等价一轮落库。
	for _, pid := range []*actor.PID{p1, p2} {
		done := make(chan error, 1)
		require.NoError(t, sys.Send(pid, &player.SaveMsg{Done: done}), "send save")
		select {
		case err := <-done:
			require.NoError(t, err, "save must succeed")
		case <-time.After(10 * time.Second):
			t.Fatal("save timeout")
		}
	}
	for _, id := range ids {
		days, rewards := sqlRow(id)
		require.EqualValues(t, "[3]", days, fmt.Sprintf("row %d: days=%q rewards=%q", id, days, rewards))
		require.EqualValues(t, `{"1001":300}`, rewards)
	}
	// Actor 重启：Redis 仍在，从缓存恢复已领状态。
	sys.Stop()
	start()
	p1 = spawn(ids[0])
	got = claim(p1)
	require.False(t, !got.Already || got.ClaimedRewards[1001] != 300, "restart from cache")
	// Redis 丢失：FlatPersister 走生成代码从 SQL 冷启动恢复。
	//
	// 必须把 state 与脏队列一起清掉：只删 state 会留下「快照没了、脏标记还在」
	// 的不一致状态，Store 会以「脏队列里还有比 MySQL 更新的快照」为由拒绝
	// seed（见 seedScript）。真实场景里 Redis 整体丢失，两者是一起没的。
	sys.Stop()
	part := writebehind.Partition(ids[0])
	err = rdb.Del(ctx, fmt.Sprintf("wb:{player-%d}:state:%d", part, ids[0])).Err()
	require.NoError(t, err)
	_, err = rdb.ZRem(ctx, fmt.Sprintf("wb:{player-%d}:dirty", part), ids[0]).Result()
	require.NoError(t, err)
	start()
	p1 = spawn(ids[0])
	got = claim(p1)
	require.False(t, !got.Already || got.ClaimedRewards[1001] != 300, "cold recovery")
}
