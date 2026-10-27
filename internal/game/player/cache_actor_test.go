package player

import (
	"context"
	"errors"
	"fmt"
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
	gatev1 "game_study/pkg/pb/gate/v1"
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
	_, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 10}}, 0)
	require.NoError(t, err)
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
	// 测试绕过 preload 直接注入状态，需补一个会话令牌（真实路径由 preload 获取）。
	a.token, err = repo.cache.AcquireLease(ctx, 7)
	require.NoError(t, err)
	a.loaded = true
	a.sessionID = 1
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("legacy", a)
	require.NoError(t, err)
	send := func() uint32 {
		t.Helper()
		err := sys.Send(pid, &PacketMsg{MsgID: 9001})
		require.NoError(t, err)
		select {
		case id := <-responses:
			return id
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
			return 0
		}
	}
	mr.SetError("cache unavailable")
	id := send()
	require.EqualValues(t, 9002, id,
		"内存权威：副本同步失败不该拦住业务，回业务响应而非错误通知")
	require.EqualValues(t, 60, a.state.GetItemCount(1001), "奖励必须留在内存")
	mr.SetError("")
	id = send()
	require.EqualValues(t, 9002, id)
	record, err := store.Load(ctx, 7)
	require.NoError(t, err)
	require.EqualValues(t, 60, record.State.Items[1001], "副本恢复后补齐")
}

// capturedReply 记录一次下行（协议号 + 原始 body）。
//
// 缓存/守卫这类框架级失败走统一错误通知（ErrorNotify 20），业务成功走
// 各自的响应号；两者都算「回了」，测试按 id 分支断言。
type capturedReply struct {
	id   uint32
	body []byte
}

func decodeClaim(t *testing.T, r capturedReply) *gamev1.SigninClaimResp {
	t.Helper()
	require.EqualValues(t, 6604, r.id, "业务响应应走 SigninClaimResp")
	out := &gamev1.SigninClaimResp{}
	require.NoError(t, proto.Unmarshal(r.body, out))
	return out
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
	registry.Register(signin.M)
	replies := make(chan capturedReply, 1)
	sessions := make(chan uint64, 1)
	a := NewActor(7, registry, logger, repo, nil, func(session uint64, id uint32, body []byte) error {
		if id == 20 || id == 6604 {
			sessions <- session
			replies <- capturedReply{id: id, body: body}
		}
		return nil
	})
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("bind-failure", a)
	require.NoError(t, err)
	err = sys.Send(pid, &BindMsg{SessionID: 9, PlayerID: 7})
	require.NoError(t, err)
	err = sys.Send(pid, &PacketMsg{MsgID: 6603})
	require.NoError(t, err)
	select {
	case session := <-sessions:
		require.EqualValues(t, 9, session)
	case <-time.After(time.Second):
		t.Fatal("failed bind hid the cache error response")
	}
	got := <-replies
	require.EqualValues(t, 20, got.id, "缓存不可用应走统一错误通知")
	notify := &gatev1.ErrorNotify{}
	require.NoError(t, proto.Unmarshal(got.body, notify))
	require.EqualValues(t, 1005, notify.Code)
}

type loseCommitReply struct{ armed atomic.Bool }

func (*loseCommitReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *loseCommitReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		// commit 脚本有 3 个 key + 4 个参数（共 10 个 argv），AcquireLease 只有
		// 3 个——用 >=8 区分「提交」与「取令牌」，避免脚本参数一变测试就失效。
		if err == nil && (cmd.Name() == "eval" || cmd.Name() == "evalsha") && len(cmd.Args()) >= 8 && h.armed.CompareAndSwap(true, false) {
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
			_, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: 7, Profile: writebehind.Profile{Level: 1}, Items: map[int32]int64{1001: 10}}, 0)
			require.NoError(t, err)
			sqlDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer sqlDB.Close()
			repo := NewRepository(&db.DB{DB: sqlDB})
			repo.SetCache(store)
			logger := log.New("cache-test", log.WithLevel("error"))
			registry := module.NewRegistry()
			registry.Register(signin.M)
			replies := make(chan capturedReply, 8)
			a := NewActor(7, registry, logger, repo, nil, func(_ uint64, id uint32, body []byte) error {
				if id == 6604 || id == 20 {
					replies <- capturedReply{id: id, body: body}
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
			// 测试绕过 preload：补一个有效令牌。
			a.token, err = repo.cache.AcquireLease(ctx, 7)
			require.NoError(t, err)
			if failure == "before" {
				mr.SetError("injected cache unavailable")
			}
			if failure == "after" {
				hook.armed.Store(true)
			}
			call := func() capturedReply {
				t.Helper()
				err := sys.Send(pid, &PacketMsg{MsgID: 6603})
				require.NoError(t, err)
				select {
				case out := <-replies:
					return out
				case <-time.After(5 * time.Second):
					t.Fatal("reply timeout")
					return capturedReply{}
				}
			}
			// 内存权威：副本同步失败不再是业务失败。三种注入下业务都应当
			// 成功——改动留在内存并保持脏标记，下一次提交会把累积的改动
			// 一起补上（副本落后不影响内存，也不影响落库）。
			first := call()
			firstResp := decodeClaim(t, first)
			require.EqualValues(t, 0, firstResp.Code, "副本同步失败不应让业务失败")
			require.False(t, firstResp.Already, "首次领取不应是已领")
			mr.SetError("")
			retry := call()
			retryResp := decodeClaim(t, retry)
			require.EqualValues(t, 0, retryResp.Code)
			require.True(t, retryResp.Already,
				"重试必须幂等：内存里的领取标记不因副本同步失败而丢")
			// 签到奖励按当天配置发（周一到周日不同），
			// 不能写死 100——否则测试只在特定的星期几通过。
			var want int64 = 10
			for _, r := range retryResp.Rewards {
				if r.ItemId == 1001 {
					want += int64(r.Count)
				}
			}
			record, err := store.Load(ctx, 7)
			require.NoError(t, err, fmt.Sprintf("items[1001] = %d, want %d (%v)", record.State.Items[1001], want, err))
			require.EqualValues(t, want, record.State.Items[1001])
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
			err = grant()
			require.NoError(t, err, "内存权威：副本同步失败不影响发货回执")
			err = grant()
			require.NoError(t, err, "重投幂等：业务键已置位")
			record, err = store.Load(ctx, 7)
			require.False(t, err != nil || record.State.Items[1001] != want+50 || !record.State.AppliedGrants["recharge:order-1"], fmt.Sprintf("grant repeated: %+v %v", record, err))
			sys.Stop()
			err = mock.ExpectationsWereMet()
			require.NoError(t, err)
		})
	}
}

// TestResyncCacheKeepsSnapshotComplete 同步落库后必须「重建」而不是只删。
//
// 背景：只 Drop 的话，下一次提交走 cache_actor 的 DirtyFields，只会写
// 这次变化的那几个字段——于是 hash 以「有 profile、没 items」的形态重建。
// 快照残缺对本人加载无害（加载侧会用库版本判定并重建），但跨玩家只读
// 会读到空背包，且无法与「真的没有道具」区分。
func TestResyncCacheKeepsSnapshotComplete(t *testing.T) {
	sqlDB, _, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()

	repo := NewRepository(&db.DB{DB: sqlDB})
	store := newTestStore(t)
	repo.SetCache(store)

	logger := log.New("resync-test", log.WithLevel("error"))
	a := NewActor(7, module.NewRegistry(), logger, repo, nil, nil)
	a.state.SetProfile(module.Profile{Name: "hero", Level: 5})
	a.state.SeedItem(1001, 10)

	require.NoError(t, a.resyncCache(11))

	rec, err := store.Load(context.Background(), 7)
	require.NoError(t, err)
	require.EqualValues(t, 11, rec.Version)
	require.Equal(t, "hero", rec.State.Profile.Name)
	require.EqualValues(t, 10, rec.State.Items[1001],
		"快照必须整份重建：只删不建会让下一次提交写出残缺 hash")
	require.EqualValues(t, 11, a.cacheVersion)
}
