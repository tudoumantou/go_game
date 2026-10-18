//go:build integration

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

// TestSigninActorFlow 使用真实 Redis/MySQL，验证在线业务不写 SQL，独立进程定时合批。
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
	ids := []int64{88001, 88011} // 相同持久化分片，验证多角色真正合批。
	defer func() {
		for _, id := range ids {
			rdb.Del(context.Background(), fmt.Sprintf("wb:{player-%d}:state:%d", writebehind.Partition(id), id))
			rdb.ZRem(context.Background(), fmt.Sprintf("wb:{player-%d}:dirty", writebehind.Partition(id)), id)
		}
		for _, table := range []string{"signin_claim", "player_signin", "player_writebehind", "player_item", "player_attr", "player"} {
			key := "player_id"
			if table == "player" {
				key = "id"
			}
			database.Exec("DELETE FROM " + table + " WHERE " + key + " IN (88001,88011)")
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
	base := time.Date(2026, 10, 18, 15, 59, 59, 0, time.UTC)
	clock.Store(base.Unix())
	registry := module.NewRegistry()
	registry.Register(signin.New(signin.NewRepo(), logger, func() time.Time { return time.Unix(clock.Load(), 0) }))
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
	cachedCount := func(id int64, item int32) int64 {
		t.Helper()
		record, err := store.Load(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return record.State.Items[item]
	}
	sqlCount := func(id int64, item int32) int64 {
		t.Helper()
		var count int64
		if err := database.QueryRow(`SELECT COALESCE(SUM(count),0) FROM player_item WHERE player_id=? AND item_id=?`, id, item).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	info := &gamev1.SigninInfoResp{}
	call(p1, 6601, info)
	if info.Code != 0 || info.State.Today != 1 || info.State.SignedToday {
		t.Fatal(info)
	}
	if first := claim(p1); first.Already || cachedCount(ids[0], 1001) != 100 || sqlCount(ids[0], 1001) != 0 {
		t.Fatal("claim must update cache only", first)
	}
	var records int
	if err := database.QueryRow(`SELECT COUNT(*) FROM signin_claim WHERE player_id=?`, ids[0]).Scan(&records); err != nil || records != 0 {
		t.Fatal("claim wrote SQL synchronously", records, err)
	}
	if got := claim(p1); !got.Already || cachedCount(ids[0], 1001) != 100 {
		t.Fatal(got)
	}
	if got := claim(p2); got.Already || cachedCount(ids[1], 1001) != 100 {
		t.Fatal(got)
	}
	old, err := store.Load(ctx, ids[0])
	if err != nil {
		t.Fatal(err)
	}
	n, err := worker.FlushBatch(ctx, writebehind.Partition(ids[0]))
	if err != nil || n != 2 || sqlCount(ids[0], 1001) != 100 || sqlCount(ids[1], 1001) != 100 {
		t.Fatalf("multi-player batch: %d %v", n, err)
	}
	var itemUID int64
	if err := database.QueryRow(`SELECT id FROM player_item WHERE player_id=? AND item_id=1001`, ids[0]).Scan(&itemUID); err != nil {
		t.Fatal(err)
	}
	expires := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := database.Exec(`UPDATE player_item SET expire_at=? WHERE player_id=? AND item_id=1001`, expires, ids[0]); err != nil {
		t.Fatal(err)
	}
	clock.Store(base.Add(time.Second).Unix())
	if got := claim(p1); got.State.Today != 2 || cachedCount(ids[0], 1001) != 300 || sqlCount(ids[0], 1001) != 100 {
		t.Fatal("midnight", got)
	}
	// SQL 尚未追平，Actor 重启仍恢复 Redis 中的已领状态与奖励。
	sys.Stop()
	start()
	p1 = spawn(ids[0])
	p2 = spawn(ids[1])
	if got := claim(p1); !got.Already || cachedCount(ids[0], 1001) != 300 {
		t.Fatal("restart before flush", got)
	}
	clock.Store(base.Add(48*time.Hour + time.Second).Unix())
	if got := claim(p1); got.State.Today != 4 || !got.State.Days[2].Missed {
		t.Fatal("missed", got)
	}
	clock.Store(base.Add(5*24*time.Hour + time.Second).Unix())
	if got := claim(p1); got.State.Today != 7 || cachedCount(ids[0], 1002) != 1 {
		t.Fatal("day7", got)
	}
	clock.Store(base.Add(6*24*time.Hour + time.Second).Unix())
	if got := claim(p1); got.State.Today != 1 || cachedCount(ids[0], 1001) != 1500 {
		t.Fatal("new cycle", got)
	}
	// 模拟 SQL 暂不可写，客户端成功与 Redis 状态不依赖落库成功。
	if _, err := database.Exec(`ALTER TABLE player_item ADD CONSTRAINT signin_test_limit CHECK (player_id <> 88001 OR count <= 100)`); err != nil {
		t.Fatal(err)
	}
	defer func() { database.Exec(`ALTER TABLE player_item DROP CHECK signin_test_limit`) }()
	if _, err := worker.FlushBatch(ctx, writebehind.Partition(ids[0])); err == nil {
		t.Fatal("expected SQL failure")
	}
	if sqlCount(ids[0], 1001) != 100 || cachedCount(ids[0], 1001) != 1500 {
		t.Fatal("failed flush altered authoritative state")
	}
	pending, err := store.Pending(ctx, writebehind.Partition(ids[0]), 100)
	if err != nil || len(pending) != 1 {
		t.Fatal("failure lost dirty work", pending, err)
	}
	if _, err := database.Exec(`ALTER TABLE player_item DROP CHECK signin_test_limit`); err != nil {
		t.Fatal(err)
	}
	// 独立 worker 只在定时器到期后处理，游戏服不触发 SQL 写入。
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(workerCtx, 20*time.Millisecond, nil) }()
	defer func() { stopWorker(); <-workerDone }()
	deadline := time.Now().Add(5 * time.Second)
	for sqlCount(ids[0], 1001) != 1500 {
		if time.Now().After(deadline) {
			t.Fatal("timer did not flush")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// 较老 worker 延迟到达或提交后重投，不能覆盖已保存的新状态。
	if err := worker.Persist(ctx, []writebehind.Record{old}); err != nil {
		t.Fatal(err)
	}
	if sqlCount(ids[0], 1001) != 1500 {
		t.Fatal("older batch overwrote newer state")
	}
	var currentUID int64
	var currentExpiry time.Time
	if err := database.QueryRow(`SELECT id,expire_at FROM player_item WHERE player_id=? AND item_id=1001`, ids[0]).Scan(&currentUID, &currentExpiry); err != nil || currentUID != itemUID || !currentExpiry.Equal(expires) {
		t.Fatal("flush replaced item metadata", currentUID, currentExpiry, err)
	}
	if err := database.QueryRow(`SELECT COUNT(*) FROM signin_claim WHERE player_id=?`, ids[0]).Scan(&records); err != nil || records != 5 {
		t.Fatal(records, err)
	}
	if got := claim(p2); got.Already || cachedCount(ids[1], 1001) != 200 {
		t.Fatal("role isolation", got)
	}
	for i := 0; i < 10; i++ {
		if got := claim(p1); !got.Already {
			t.Fatal("duplicate granted")
		}
	}
	// 发奖业务键随合批事务入库，Redis 真正丢失后从 SQL 恢复也能防重。
	grant := func(pid *actor.PID) {
		t.Helper()
		done := make(chan error, 1)
		if err := sys.Send(pid, &player.GrantItemsMsg{Source: "recharge", BizID: "integration-order-1", Items: []player.GrantItem{{ItemID: 1001, Count: 5}}, Done: done}); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("grant timeout")
		}
	}
	grant(p1)
	deadline = time.Now().Add(5 * time.Second)
	for {
		pending, err := store.Pending(ctx, writebehind.Partition(ids[0]), 100)
		if err != nil {
			t.Fatal(err)
		}
		if sqlCount(ids[0], 1001) == 1505 && len(pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("receipt did not flush")
		}
		time.Sleep(10 * time.Millisecond)
	}
	sys.Stop()
	if err := rdb.Del(ctx, fmt.Sprintf("wb:{player-%d}:state:%d", writebehind.Partition(ids[0]), ids[0])).Err(); err != nil {
		t.Fatal(err)
	}
	start()
	p1 = spawn(ids[0])
	grant(p1)
	if cachedCount(ids[0], 1001) != 1505 {
		t.Fatal("cold recovery lost grant receipt")
	}
}
