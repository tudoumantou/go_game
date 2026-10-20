//go:build integration

// test/integration/signin_test.go —— 单表签到全链路（缓存权威 + Write-Behind，见 docs/signin.md）
//
// 不起真实服务进程：直接装配 actor.System + 模块 Registry + Repository/Worker，
// 用可控时钟把「今天」固定为周三，覆盖四条路径：
//  1. 在线签到只写缓存（SQL 立即查应为空）；
//  2. 同分片多角色由 worker 合批落库（一次 FlushBatch 落两行）；
//  3. Actor 重启：Redis 仍在 → 从缓存恢复已领状态；
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
	"game_study/pkg/db"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/writebehind"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// TestSigninActorFlow 验证单表签到的完整链路：在线只写缓存、独立进程合批落库、
// Redis 丢失后从 SQL 冷启动恢复，且重复领取不重复发奖。
func TestSigninActorFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	database, err := db.New(db.Config{DSN: testDSN, MaxOpen: 5})
	if err != nil {
		t.Fatal(err)
	}
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
		if _, err := database.Exec(`INSERT INTO player (id,account_id,name) VALUES (?,?,?)`, id, 99, fmt.Sprintf("signin_%d", id)); err != nil {
			t.Fatal(err)
		}
	}
	store := writebehind.NewStore(rdb)
	worker := writebehind.NewWorker(store, database.DB, 100)
	repo := player.NewRepository(database)
	repo.SetCache(store)
	logger := log.New("signin-test", log.WithLevel("error"))
	var clock atomic.Int64
	// 2026-10-21 10:00 北京时间，周三。
	clock.Store(time.Date(2026, 10, 21, 2, 0, 0, 0, time.UTC).Unix())
	registry := module.NewRegistry()
	registry.Register(signin.New(logger, func() time.Time { return time.Unix(clock.Load(), 0) }))
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
		if err != nil {
			t.Fatal(err)
		}
		if err := sys.Send(pid, &player.BindMsg{PlayerID: id, SessionID: uint64(id)}); err != nil {
			t.Fatal(err)
		}
		return pid
	}
	p1, p2 := spawn(ids[0]), spawn(ids[1])
	call := func(pid *actor.PID, id uint32, out proto.Message) {
		t.Helper()
		if err := sys.Send(pid, &player.PacketMsg{MsgID: id}); err != nil {
			t.Fatal(err)
		}
		for {
			select {
			case response := <-responses:
				if response.id == id+1 {
					if err := proto.Unmarshal(response.body, out); err != nil {
						t.Fatal(err)
					}
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
		call(pid, signin.MsgClaimReq, out)
		if out.Code != 0 {
			t.Fatal(out)
		}
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
	call(p1, signin.MsgInfoReq, info)
	if info.Code != 0 || info.Today != 3 || info.ClaimedToday {
		t.Fatal(info)
	}
	if first := claim(p1); first.Already || first.Day != 3 || first.ClaimedRewards[1001] != 300 {
		t.Fatal("claim must grant day 3", first)
	}
	if _, rewards := sqlRow(ids[0]); rewards != "" {
		t.Fatal("claim wrote SQL synchronously", rewards)
	}
	if got := claim(p1); !got.Already || got.ClaimedRewards[1001] != 300 {
		t.Fatal("duplicate claim", got)
	}
	if got := claim(p2); got.Already || got.ClaimedRewards[1001] != 300 {
		t.Fatal("role isolation", got)
	}
	if n, err := worker.FlushBatch(ctx, writebehind.Partition(ids[0])); err != nil || n != 2 {
		t.Fatalf("batch flush: %d %v", n, err)
	}
	for _, id := range ids {
		if days, rewards := sqlRow(id); days != "[3]" || rewards != `{"1001":300}` {
			t.Fatalf("row %d: days=%q rewards=%q", id, days, rewards)
		}
	}
	// Actor 重启：Redis 仍在，从缓存恢复已领状态。
	sys.Stop()
	start()
	p1 = spawn(ids[0])
	if got := claim(p1); !got.Already || got.ClaimedRewards[1001] != 300 {
		t.Fatal("restart from cache", got)
	}
	// Redis 丢失：FlatPersister 走生成代码从 SQL 冷启动恢复。
	sys.Stop()
	if err := rdb.Del(ctx, fmt.Sprintf("wb:{player-%d}:state:%d", writebehind.Partition(ids[0]), ids[0])).Err(); err != nil {
		t.Fatal(err)
	}
	start()
	p1 = spawn(ids[0])
	if got := claim(p1); !got.Already || got.ClaimedRewards[1001] != 300 {
		t.Fatal("cold recovery", got)
	}
}
