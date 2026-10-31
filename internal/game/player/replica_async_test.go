// internal/game/player/replica_async_test.go —— R14：异步回写与下线强制落盘
//
// 两条要保住的行为：
//  1. 改数据的消息**不等** Redis（内存权威的核心收益）；
//  2. 下线不等 flusher 的 60s 窗口，直接落库 + 刷副本（关服前也一样）。
package player

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/actor"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/writebehind"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newAsyncRepo 装配「缓存 + 异步回写」的仓储（后台循环不启动，测试自己驱动 Flush）。
//
// 只填 cache/replica 两个字段：这两条测试关心的是「副本什么时候写」，
// 不碰 SQL，所以 base 留空——不要为了凑一个完整的 Repository 去连库。
func newAsyncRepo(t *testing.T, playerID int64) (*Repository, *writebehind.Replica, *writebehind.Store) {
	t.Helper()
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	store := writebehind.NewStore(rdb)
	_, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: playerID, Items: map[int32]int64{}}, 0)
	require.NoError(t, err)

	rep := writebehind.NewReplica(store, writebehind.ReplicaConfig{}, log.New("replica", log.WithLevel("error")))
	repo := &Repository{}
	repo.SetCache(store)
	repo.SetReplica(rep)
	return repo, rep, store
}

// loadActorForTest 直接注入一份空状态与会话令牌（真实路径由 preload 获取）。
//
// 不走 LoadCached：那要连库拿 player_writebehind.version，本文件的测试
// 只验证副本时序，不该被 SQL 拖进来。
func loadActorForTest(t *testing.T, a *Actor, repo *Repository, playerID int64) {
	t.Helper()
	ctx := context.Background()
	a.setState(NewState(playerID))
	var err error
	a.token, err = repo.cache.AcquireLease(ctx, playerID)
	require.NoError(t, err)
	a.loaded = true
	a.online = true
	a.sessionID = 1
}

// TestMessageDoesNotWaitForRedisReplica 改数据的消息只入队，不写 Redis。
//
// 断言方式：消息处理完之后副本还是旧值（pending=1），Flush 之后才变成新值。
// 如果这条消息同步写了 Redis，第二段断言会在 Flush 之前就成立——测试失败。
func TestMessageDoesNotWaitForRedisReplica(t *testing.T) {
	ctx := context.Background()
	const playerID = int64(601)
	repo, rep, store := newAsyncRepo(t, playerID)
	logger := log.New("async-test", log.WithLevel("error"))
	a := NewActor(playerID, module.NewRegistry(), logger, repo, nil, nil)
	loadActorForTest(t, a, repo, playerID)

	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("async", a)
	require.NoError(t, err)

	done := make(chan error, 1)
	require.NoError(t, sys.Send(pid, &GrantItemsMsg{
		Items: []GrantItem{{ItemID: 1001, Count: 7}},
		Done:  done,
	}))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("grant did not reply")
	}

	require.Equal(t, 1, rep.Pending(), "改动应停在异步队列里，而不是已经写进 Redis")
	cached, err := store.Load(ctx, playerID)
	require.NoError(t, err)
	require.EqualValues(t, 0, cached.State.Items[1001], "消息处理期间副本不应被改动")

	require.NoError(t, rep.Flush(ctx))
	cached, err = store.Load(ctx, playerID)
	require.NoError(t, err)
	require.EqualValues(t, 7, cached.State.Items[1001], "Flush 之后副本必须是新值")
}

// TestUnbindForcesPersistAndReplicaFlush 下线不等 60s：直接落库 + 刷副本。
func TestUnbindForcesPersistAndReplicaFlush(t *testing.T) {
	ctx := context.Background()
	const playerID = int64(602)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	store := writebehind.NewStore(rdb)
	_, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: playerID, Items: map[int32]int64{}}, 0)
	require.NoError(t, err)

	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	// 关服/下线路径只要「事务成功」就算落库成功，这里不追究具体语句；
	// 顺序无关 + 多给几条余量，让测试只表达「有没有真的走落库」。
	mock.MatchExpectationsInOrder(false)
	for i := 0; i < 8; i++ {
		mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectBegin()
	mock.ExpectCommit()

	rep := writebehind.NewReplica(store, writebehind.ReplicaConfig{}, log.New("replica", log.WithLevel("error")))
	repo := NewRepository(&db.DB{DB: database})
	repo.SetCache(store)
	repo.SetReplica(rep)

	var dirtyCalls int32
	logger := log.New("unbind-test", log.WithLevel("error"))
	a := NewActor(playerID, module.NewRegistry(), logger, repo,
		func(int64) { atomic.AddInt32(&dirtyCalls, 1) }, nil)
	loadActorForTest(t, a, repo, playerID)

	// 改一个道具并让它进异步队列（等价于一条业务消息的收尾）
	a.state.AddItem(1001, 3)
	fields, removed := a.state.DirtyFields()
	require.NoError(t, a.enqueueReplica(fields, removed))
	a.state.ClearDirtyFields()
	require.Equal(t, 1, rep.Pending())
	// AddItem 自己也会标脏（业务改动的正常路径），基线取在这里——
	// 下面要验证的是「下线这一步有没有额外标脏」。
	dirtyBefore := atomic.LoadInt32(&dirtyCalls)

	a.onUnbind(&actor.Context{}, &UnbindMsg{SessionID: 1})

	require.Equal(t, 0, rep.Pending(), "下线必须把副本队列刷平")
	cached, err := store.Load(ctx, playerID)
	require.NoError(t, err)
	require.EqualValues(t, 3, cached.State.Items[1001], "下线后副本应是最新值")
	require.EqualValues(t, dirtyBefore, atomic.LoadInt32(&dirtyCalls),
		"落库成功就不该再回落成脏标记（否则同一份数据要写两遍）")
}

// TestUnbindFallsBackToFlusherWhenPersistFails 落库失败不能丢数据：
// 必须回落成脏标记，交给 flusher 重试。
func TestUnbindFallsBackToFlusherWhenPersistFails(t *testing.T) {
	ctx := context.Background()
	const playerID = int64(603)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	store := writebehind.NewStore(rdb)
	_, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: playerID, Items: map[int32]int64{}}, 0)
	require.NoError(t, err)

	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectBegin().WillReturnError(context.DeadlineExceeded) // 模拟库不可用

	repo := NewRepository(&db.DB{DB: database})
	repo.SetCache(store)

	var dirtyCalls int32
	logger := log.New("unbind-fail-test", log.WithLevel("error"))
	a := NewActor(playerID, module.NewRegistry(), logger, repo,
		func(int64) { atomic.AddInt32(&dirtyCalls, 1) }, nil)
	loadActorForTest(t, a, repo, playerID)

	a.onUnbind(&actor.Context{}, &UnbindMsg{SessionID: 1})

	require.EqualValues(t, 1, atomic.LoadInt32(&dirtyCalls), "落库失败必须回落成脏标记，等 flusher 重试")
	require.False(t, a.online, "会话仍要正常结束")
}
