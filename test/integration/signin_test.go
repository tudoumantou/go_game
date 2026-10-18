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
	"google.golang.org/protobuf/proto"
)

// TestSigninActorFlow 使用真实 MySQL 与实际玩家 Actor 验证事务、跨日、重启与幂等。
func TestSigninActorFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// 复用本包 TestMain 已启动的真实 MySQL，无需再启动一套容器。
	database, err := db.New(db.Config{DSN: testDSN, MaxOpen: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	defer func() {
		for _, table := range []string{"signin_claim", "player_signin", "player_item", "player_attr", "player"} {
			key := "player_id"
			if table == "player" {
				key = "id"
			}
			_, _ = database.Exec("DELETE FROM " + table + " WHERE " + key + " IN (88001,88002)")
		}
	}()
	for _, id := range []int64{88001, 88002} {
		if _, err := database.Exec(`INSERT INTO player (id, account_id, name) VALUES (?, ?, ?)`, id, 99, fmt.Sprintf("signin_%d", id)); err != nil {
			t.Fatal(err)
		}
	}
	logger := log.New("signin-test", log.WithLevel("error"))
	var clock atomic.Int64
	base := time.Date(2026, 10, 18, 15, 59, 59, 0, time.UTC)
	clock.Store(base.Unix())
	repo := player.NewRepository(database)
	registry := module.NewRegistry()
	registry.Register(signin.New(signin.NewRepo(database), logger, func() time.Time { return time.Unix(clock.Load(), 0) }))
	type response struct {
		id   uint32
		body []byte
	}
	responses := make(chan response, 100)
	var sys *actor.System
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
	start := func() { sys = actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 128}); sys.Start(ctx) }
	start()
	defer func() { sys.Stop() }()
	p1, p2 := spawn(88001), spawn(88002)
	call := func(pid *actor.PID, id uint32, out proto.Message) {
		t.Helper()
		if err := sys.Send(pid, &player.PacketMsg{MsgID: id}); err != nil {
			t.Fatal(err)
		}
		for {
			select {
			case resp := <-responses:
				if resp.id == id+1 {
					if err := proto.Unmarshal(resp.body, out); err != nil {
						t.Fatal(err)
					}
					return
				}
			case <-time.After(10 * time.Second):
				t.Fatal("actor response timeout")
			}
		}
	}
	claim := func(pid *actor.PID) *gamev1.SigninClaimResp {
		t.Helper()
		out := &gamev1.SigninClaimResp{}
		call(pid, signin.MsgClaimReq, out)
		if out.Code != 0 {
			t.Fatalf("claim: %v", out)
		}
		return out
	}
	count := func(id int64, item int32) int64 {
		t.Helper()
		var n int64
		if err := database.QueryRow(`SELECT count FROM player_item WHERE player_id=? AND item_id=?`, id, item).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	info := &gamev1.SigninInfoResp{}
	call(p1, signin.MsgInfoReq, info)
	if info.Code != 0 || info.State.Today != 1 || info.State.SignedToday {
		t.Fatalf("initial: %v", info)
	}
	if got := claim(p1); got.Already || got.State.Today != 1 || count(88001, 1001) != 100 {
		t.Fatalf("first: %v", got)
	}
	if got := claim(p1); !got.Already || count(88001, 1001) != 100 {
		t.Fatalf("duplicate: %v", got)
	}
	// 同账号另一角色独立签到。
	if got := claim(p2); got.Already || count(88002, 1001) != 100 {
		t.Fatalf("second role: %v", got)
	}
	// 相差一秒即跨过北京时间零点，不能按24小时冷却判断。
	clock.Store(base.Add(time.Second).Unix())
	if got := claim(p1); got.State.Today != 2 || count(88001, 1001) != 300 {
		t.Fatalf("midnight: %v", got)
	}
	// 跳过第三天，第四天直接领取第四档，第三天标记漏签。
	clock.Store(base.Add(48*time.Hour + time.Second).Unix())
	if got := claim(p1); got.State.Today != 4 || !got.State.Days[2].Missed || count(88001, 1001) != 700 {
		t.Fatalf("missed: %v", got)
	}
	// 第七天多种奖励同事务写入。
	clock.Store(base.Add(5*24*time.Hour + time.Second).Unix())
	if got := claim(p1); got.State.Today != 7 || count(88001, 1001) != 1400 || count(88001, 1002) != 1 {
		t.Fatalf("day7: %v", got)
	}
	// 第八天重置，第1档再次可领。
	clock.Store(base.Add(6*24*time.Hour + time.Second).Unix())
	if got := claim(p1); got.State.Today != 1 || count(88001, 1001) != 1500 {
		t.Fatalf("new cycle: %v", got)
	}
	// 成功回包后立即销毁系统，不依赖离线或定时落库。
	sys.Stop()
	start()
	p1 = spawn(88001)
	if got := claim(p1); !got.Already || count(88001, 1001) != 1500 {
		t.Fatalf("restart: %v", got)
	}
	// 同日排队多次请求也不能重复发奖。
	for i := 0; i < 10; i++ {
		if got := claim(p1); !got.Already {
			t.Fatal("queued duplicate granted")
		}
	}
	var records int
	if err := database.QueryRow(`SELECT COUNT(*) FROM signin_claim WHERE player_id=?`, 88001).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if records != 5 || count(88001, 1001) != 1500 {
		t.Fatalf("records=%d", records)
	}
	// 人为使道具写入失败：签到流水必须一起回滚，之后仍可成功领取。
	p2 = spawn(88002)
	if _, err := database.Exec(`ALTER TABLE player_item ADD CONSTRAINT signin_test_limit CHECK (player_id <> 88002 OR count <= 100)`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = database.Exec(`ALTER TABLE player_item DROP CHECK signin_test_limit`) }()
	failed := &gamev1.SigninClaimResp{}
	call(p2, signin.MsgClaimReq, failed)
	if failed.Code != 1 || count(88002, 1001) != 100 {
		t.Fatalf("failed claim changed backpack: %v", failed)
	}
	if err := database.QueryRow(`SELECT COUNT(*) FROM signin_claim WHERE player_id=?`, 88002).Scan(&records); err != nil {
		t.Fatal(err)
	}
	if records != 1 {
		t.Fatal("failed reward left a signin claim")
	}
	if _, err := database.Exec(`ALTER TABLE player_item DROP CHECK signin_test_limit`); err != nil {
		t.Fatal(err)
	}
	if got := claim(p2); got.Already || count(88002, 1001) != 200 {
		t.Fatalf("retry after rollback: %v", got)
	}
}
