// r21_atomic_test.go —— R2.1 契约验收：消费与奖励同成同败、业务键随快照落库
package player

import (
	"context"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/bag"
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

const (
	msgSpendReq  uint32 = 9101
	msgSpendResp uint32 = 9102
	costItem     int32  = 1002
	gainItem     int32  = 1001
)

// atomicSpend 模拟「扣 100 材料 → 得 200 金币」：只改内存，不碰 SQL。
// 落库交给快照提交与 Write-Behind，因此扣费与发奖天然同一次 CAS。
type atomicSpend struct{ *bag.Module }

func (*atomicSpend) MsgIDs() []uint32         { return []uint32{msgSpendReq} }
func (*atomicSpend) Rollbackable(uint32) bool { return true }

func (m *atomicSpend) OnMessage(ctx *module.Context, _ uint32, _ []byte) error {
	if !ctx.State.RemoveItem(costItem, 100) {
		raw, _ := proto.Marshal(&gamev1.BagChangeNotify{})
		return ctx.Reply(msgSpendResp, raw)
	}
	// 用 ApplyOnce 演示幂等键：真业务里 key 是订单号/邮件 ID。
	ctx.State.AddItem(gainItem, 200)
	ctx.State.MarkDirty("bag")
	raw, _ := proto.Marshal(&gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{{
		ItemId: gainItem,
		Count:  int32(ctx.State.GetItemCount(gainItem)),
		Change: 1,
	}}})
	return ctx.Reply(msgSpendResp, raw)
}

// spendRig 是「缓存 Actor + miniredis」的最小夹具。
type spendRig struct {
	actor     *Actor
	sys       *actor.System
	pid       *actor.PID
	store     *writebehind.Store
	mr        *miniredis.Miniredis
	responses chan uint32
}

func newSpendRig(t *testing.T, items map[int32]int64) *spendRig {
	t.Helper()
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	store := writebehind.NewStore(rdb)
	if _, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: 7, Items: items}, 0); err != nil {
		t.Fatal(err)
	}
	database, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { database.Close() })
	repo := NewRepository(&db.DB{DB: database})
	repo.SetCache(store)
	logger := log.New("r21-test", log.WithLevel("error"))
	registry := module.NewRegistry()
	registry.Register(&atomicSpend{Module: bag.New(logger)})
	responses := make(chan uint32, 8)
	a := NewActor(7, registry, logger, repo, nil,
		func(_ uint64, id uint32, _ []byte) error { responses <- id; return nil })
	state, version, err := repo.LoadCached(ctx, 7)
	require.NoError(t, err)
	a.setState(state)
	a.cacheVersion = version
	// 测试绕过 preload：补一个有效令牌。
	a.token, err = repo.cache.AcquireLease(ctx, 7)
	require.NoError(t, err)
	a.loaded = true
	a.sessionID = 1

	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	t.Cleanup(sys.Stop)
	pid, err := sys.Spawn("r21", a)
	require.NoError(t, err)
	return &spendRig{actor: a, sys: sys, pid: pid, store: store, mr: mr, responses: responses}
}

func (r *spendRig) send(t *testing.T) uint32 {
	t.Helper()
	if err := r.sys.Send(r.pid, &PacketMsg{MsgID: msgSpendReq}); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-r.responses:
		return id
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
		return 0
	}
}

// TestSpendAndRewardCommitTogether 扣费与发奖在同一个快照里一起生效。
func TestSpendAndRewardCommitTogether(t *testing.T) {
	rig := newSpendRig(t, map[int32]int64{costItem: 100})
	if id := rig.send(t); id != msgSpendResp {
		t.Fatalf("unexpected reply %d", id)
	}
	record, err := rig.store.Load(context.Background(), 7)
	require.NoError(t, err)
	if record.State.Items[costItem] != 0 || record.State.Items[gainItem] != 200 {
		t.Fatalf("snapshot items: %v", record.State.Items)
	}
}

// TestSpendResetsToAuthorityWhenCacheFails Redis 提交失败时，客户端收到错误，
// 内存不做回滚而是标记重载——下一条消息前从 Redis 整份恢复，因此玩家看到的
// 结果仍是「材料没扣、金币没加」，只是实现从「回滚」换成了「重置」。
//
// 为什么重置更好：回滚要 clone 一份完整状态（每条消息两次 map 分配 + 两次
// 深拷贝），而重载只在失败时发生一次，且 Redis 就是权威，重置不会算错。
func TestSpendResetsToAuthorityWhenCacheFails(t *testing.T) {
	rig := newSpendRig(t, map[int32]int64{costItem: 100})
	before, err := rig.store.Load(context.Background(), 7)
	require.NoError(t, err)

	rig.mr.SetError("cache unavailable")
	if id := rig.send(t); id != 20 { // 网关通用错误通知
		t.Fatalf("failed commit must reply with error, got %d", id)
	}
	// R2.5：失败后内存保留改动（不回滚），由 cacheReload 在下条消息前重置。
	if !rig.actor.cacheReload {
		t.Fatal("failed commit must schedule reload")
	}
	// Redis 权威状态保持提交前版本。
	rig.mr.SetError("")
	after, err := rig.store.Load(context.Background(), 7)
	require.NoError(t, err)
	if after.Version != before.Version || after.State.Items[costItem] != 100 {
		t.Fatalf("snapshot changed after failed commit: %+v", after)
	}
}
