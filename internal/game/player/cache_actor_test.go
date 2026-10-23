package player

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/bag"
	"game_study/internal/game/module/signin"
	"game_study/pkg/actor"
	"game_study/pkg/db"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/writebehind"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type legacyReward struct {
	*bag.Module
	committed bool
}

func (*legacyReward) MsgIDs() []uint32 { return []uint32{9001} }
func (m *legacyReward) OnMessage(ctx *module.Context, id uint32, body []byte) error {
	if !m.committed {
		m.committed = true
		ctx.State.AddItem(1001, 50)
	} // 模拟旧 SQL 领取事实已成功。
	raw, _ := proto.Marshal(&gamev1.SigninClaimResp{})
	return ctx.Reply(9002, raw)
}

func TestLegacySQLRewardIsRetainedUntilCacheRecovers(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	defer rdb.Close()
	store := writebehind.NewStore(rdb)
	if _, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}, 0); err != nil {
		t.Fatal(err)
	}
	database, _, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	repo := NewRepository(&db.DB{DB: database})
	repo.SetCache(store)
	logger := log.New("legacy-test", log.WithLevel("error"))
	registry := module.NewRegistry()
	mod := &legacyReward{Module: bag.New(logger)}
	registry.Register(mod)
	responses := make(chan uint32, 4)
	a := NewActor(7, registry, logger, repo, nil, func(_ uint64, id uint32, _ []byte) error { responses <- id; return nil })
	state, version, err := repo.LoadCached(ctx, 7)
	require.NoError(t, err)
	a.setState(state)
	a.cacheVersion = version
	a.loaded = true
	a.sessionID = 1
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("legacy", a)
	require.NoError(t, err)
	send := func() uint32 {
		t.Helper()
		if err := sys.Send(pid, &PacketMsg{MsgID: 9001}); err != nil {
			t.Fatal(err)
		}
		select {
		case id := <-responses:
			return id
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
			return 0
		}
	}
	mr.SetError("cache unavailable")
	if id := send(); id != 20 {
		t.Fatalf("unexpected success %d", id)
	}
	if a.state.GetItemCount(1001) != 60 {
		t.Fatal("SQL-confirmed reward discarded")
	}
	mr.SetError("")
	if id := send(); id != 9002 {
		t.Fatal(id)
	}
	record, err := store.Load(ctx, 7)
	if err != nil || record.State.Items[1001] != 60 {
		t.Fatal(record, err)
	}
}

func TestCacheUnavailableDuringBindStillRepliesToCurrentSession(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	mr.SetError("unavailable")
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	defer rdb.Close()
	database, _, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	repo := NewRepository(&db.DB{DB: database})
	repo.SetCache(writebehind.NewStore(rdb))
	logger := log.New("bind-test", log.WithLevel("error"))
	registry := module.NewRegistry()
	registry.Register(signin.New(logger))
	replies := make(chan uint64, 1)
	a := NewActor(7, registry, logger, repo, nil, func(session uint64, id uint32, _ []byte) error {
		if id == 6604 {
			replies <- session
		}
		return nil
	})
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("bind-failure", a)
	require.NoError(t, err)
	if err := sys.Send(pid, &BindMsg{SessionID: 9, PlayerID: 7}); err != nil {
		t.Fatal(err)
	}
	if err := sys.Send(pid, &PacketMsg{MsgID: 6603}); err != nil {
		t.Fatal(err)
	}
	select {
	case session := <-replies:
		if session != 9 {
			t.Fatal(session)
		}
	case <-time.After(time.Second):
		t.Fatal("failed bind hid the cache error response")
	}
}

type loseCommitReply struct{ armed atomic.Bool }

func (*loseCommitReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *loseCommitReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && (cmd.Name() == "eval" || cmd.Name() == "evalsha") && len(cmd.Args()) == 9 && h.armed.CompareAndSwap(true, false) {
			return errors.New("injected lost Redis reply")
		}
		return err
	}
}
func (*loseCommitReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestSigninCacheFailureAndUnknownResult(t *testing.T) {
	for _, failure := range []string{"none", "before", "after"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			mr := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
			defer rdb.Close()
			hook := &loseCommitReply{}
			rdb.AddHook(hook)
			store := writebehind.NewStore(rdb)
			if _, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: 7, Profile: writebehind.Profile{Level: 1}, Items: map[int32]int64{1001: 10}}, 0); err != nil {
				t.Fatal(err)
			}
			sqlDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer sqlDB.Close()
			repo := NewRepository(&db.DB{DB: sqlDB})
			repo.SetCache(store)
			logger := log.New("cache-test", log.WithLevel("error"))
			registry := module.NewRegistry()
			registry.Register(signin.New(logger))
			replies := make(chan *gamev1.SigninClaimResp, 8)
			a := NewActor(7, registry, logger, repo, nil, func(_ uint64, id uint32, body []byte) error {
				if id == 6604 {
					out := &gamev1.SigninClaimResp{}
					if err := proto.Unmarshal(body, out); err != nil {
						return err
					}
					replies <- out
				}
				return nil
			})
			sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 16})
			sys.Start(ctx)
			defer sys.Stop()
			pid, err := sys.Spawn("cached-player", a)
			require.NoError(t, err)
			// 先从预置 Redis 恢复；mock 没有 SQL 预期，任何回源都会导致测试失败。
			a.sessionID = 1
			a.loaded = true
			state, version, err := repo.LoadCached(ctx, 7)
			require.NoError(t, err)
			a.setState(state)
			a.cacheVersion = version
			if failure == "before" {
				mr.SetError("injected cache unavailable")
			}
			if failure == "after" {
				hook.armed.Store(true)
			}
			call := func() *gamev1.SigninClaimResp {
				t.Helper()
				if err := sys.Send(pid, &PacketMsg{MsgID: 6603}); err != nil {
					t.Fatal(err)
				}
				select {
				case out := <-replies:
					return out
				case <-time.After(5 * time.Second):
					t.Fatal("reply timeout")
					return nil
				}
			}
			first := call()
			if failure == "none" {
				if first.Code != 0 || first.Already {
					t.Fatal(first)
				}
			} else {
				if first.Code != 1 {
					t.Fatalf("failed cache write replied success: %v", first)
				}
				if a.state.GetItemCount(1001) != 10 || len(a.state.Ext("signin")) != 0 {
					t.Fatal("failed cache update leaked memory")
				}
			}
			mr.SetError("")
			retry := call()
			if retry.Code != 0 {
				t.Fatal(retry)
			}
			expectedAlready := failure != "before"
			if retry.Already != expectedAlready {
				t.Fatalf("retry=%v", retry)
			}
			// 签到奖励按当天配置发（周一到周日不同），
			// 不能写死 100——否则测试只在特定的星期几通过。
			var want int64 = 10
			for _, r := range retry.Rewards {
				if r.ItemId == 1001 {
					want += int64(r.Count)
				}
			}
			record, err := store.Load(ctx, 7)
			if err != nil || record.State.Items[1001] != want {
				t.Fatalf("items[1001] = %d, want %d (%v)", record.State.Items[1001], want, err)
			}
			// 发奖消息超时/提交结果未知后重投，由缓存中的业务键保护。
			hook.armed.Store(true)
			grant := func() error {
				done := make(chan error, 1)
				if err := sys.Send(pid, &GrantItemsMsg{Source: "recharge", BizID: "order-1", Items: []GrantItem{{ItemID: 1001, Count: 50}}, Done: done}); err != nil {
					return err
				}
				select {
				case err := <-done:
					return err
				case <-time.After(5 * time.Second):
					return errors.New("grant timeout")
				}
			}
			if err := grant(); err == nil {
				t.Fatal("expected unknown grant result")
			}
			if err := grant(); err != nil {
				t.Fatal(err)
			}
			record, err = store.Load(ctx, 7)
			if err != nil || record.State.Items[1001] != want+50 || !record.State.AppliedGrants["recharge:order-1"] {
				t.Fatalf("grant repeated: %+v %v", record, err)
			}
			sys.Stop()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
