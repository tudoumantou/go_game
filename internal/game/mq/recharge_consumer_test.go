// internal/game/mq/recharge_consumer_test.go —— 充值发货消费者单测（内存依赖，不连 Kafka/Redis）
package mq

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"game_study/internal/game/module/mail"
	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	"github.com/stretchr/testify/require"
)

// captureActor 捕获 GrantItemsMsg。
type captureActor struct {
	mu     sync.Mutex
	items  []player.GrantItem
	grants int
	fail   bool
}

func (a *captureActor) Receive(ctx *actor.Context) {
	msg, ok := ctx.Message().(*player.GrantItemsMsg)
	if !ok {
		return
	}
	a.mu.Lock()
	if !a.fail {
		a.items = append(a.items, msg.Items...)
		a.grants++
	}
	fail := a.fail
	a.mu.Unlock()

	if msg.Done != nil {
		if fail {
			msg.Done <- context.DeadlineExceeded
			return
		}
		msg.Done <- nil
	}
}

func (a *captureActor) snapshot() (int, []player.GrantItem) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]player.GrantItem, len(a.items))
	copy(out, a.items)
	return a.grants, out
}

// fakeRouter 固定返回一个 PID。
type fakeRouter struct {
	pid *actor.PID
}

func (r *fakeRouter) Get(playerID int64) (*actor.PID, bool) { return r.pid, r.pid != nil }
func (r *fakeRouter) GetOrSpawn(ctx context.Context, playerID int64) (*actor.PID, error) {
	return r.pid, nil
}

// memPresence 内存在线状态。
type memPresence struct {
	mu   sync.Mutex
	node map[int64]string
	pend map[int64][][]byte
}

func newMemPresence() *memPresence {
	return &memPresence{node: make(map[int64]string), pend: make(map[int64][][]byte)}
}

func (p *memPresence) IsOnline(ctx context.Context, playerID int64) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n, ok := p.node[playerID]
	return n, ok
}

func (p *memPresence) PendPush(ctx context.Context, biz string, playerID int64, value []byte, maxLen int64, ttl time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pend[playerID] = append(p.pend[playerID], value)
	return nil
}

// memDeduper 内存幂等器。
type memDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newMemDeduper() *memDeduper { return &memDeduper{seen: make(map[string]bool)} }

func (d *memDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := pkgmq.IdemKey(topic, bizID)
	if d.seen[key] {
		return false, nil
	}
	d.seen[key] = true
	return true, nil
}

func (d *memDeduper) Forget(ctx context.Context, topic, bizID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, pkgmq.IdemKey(topic, bizID))
	return nil
}

// fakeMail 记录离线发放，验证 Tier-1 兜底。
type fakeMail struct {
	delivered []string
	fail      error
}

func (f *fakeMail) Deliver(_ context.Context, msg *pkgmq.MailMsg) (int64, error) {
	return msg.MailID, nil
}

func (f *fakeMail) DeliverOffline(_ context.Context, _ int64, bizID, _, _ string, _ []mail.Attachment) (bool, error) {
	if f.fail != nil {
		return false, f.fail
	}
	f.delivered = append(f.delivered, bizID)
	return true, nil
}

func setupConsumer(t *testing.T, presence *memPresence, deduper *memDeduper) (*RechargeConsumer, *captureActor) {
	t.Helper()
	sys := actor.NewSystem(actor.Config{ShardCount: 4, MailboxSize: 64})
	ctx, cancel := context.WithCancel(context.Background())
	sys.Start(ctx)
	t.Cleanup(func() {
		cancel()
		sys.Stop()
	})

	act := &captureActor{}
	pid, err := sys.Spawn("player:1001", act)
	require.NoError(t, err, "spawn")

	deps := Deps{
		Deduper:  deduper,
		Presence: presence,
		Players:  &fakeRouter{pid: pid},
		Sys:      sys,
		Mail:     &fakeMail{},
		Logger:   log.New("test"),
		NodeID:   "game-1",
	}
	return NewRechargeConsumer(nil, deps), act
}

func mustEnvelope(t *testing.T, orderID string, playerID int64, amountFen int32) *pkgmq.Envelope {
	t.Helper()
	env, err := pkgmq.NewEnvelope(orderID, pkgmq.TopicRechargeOrder, orderID, pkgmq.RechargeOrderMsg{
		OrderID: orderID, PlayerID: playerID, AmountFen: amountFen, ProductID: 1, Channel: 1,
	})
	require.NoError(t, err, "envelope")
	return env
}

func TestRechargeDeliversOnce(t *testing.T) {
	presence := newMemPresence()
	deduper := newMemDeduper()
	presence.node[1001] = "game-1"

	c, act := setupConsumer(t, presence, deduper)
	env := mustEnvelope(t, "O1001", 1001, 600)

	err := c.handle(context.Background(), env)
	require.NoError(t, err, fmt.Sprintf("handle: %v", err))
	grants, items := act.snapshot()
	require.EqualValues(t, 1, grants)
	require.Len(t, items, 1, fmt.Sprintf("items = %+v", items))
	require.EqualValues(t, 1001, items[0].ItemID)
	require.EqualValues(t, 600, items[0].Count)

	// 重复消费：幂等跳过
	err = c.handle(context.Background(), env)
	require.NoError(t, err, fmt.Sprintf("second handle: %v", err))
	grants, _ = act.snapshot()
	require.EqualValues(t, 1, grants)
}

func TestRechargeSkipOtherNode(t *testing.T) {
	presence := newMemPresence()
	deduper := newMemDeduper()
	presence.node[1001] = "game-2" // 玩家在别的服

	c, act := setupConsumer(t, presence, deduper)
	err := c.handle(context.Background(), mustEnvelope(t, "O1002", 1001, 100))
	require.NoError(t, err, fmt.Sprintf("handle: %v", err))
	grants, _ := act.snapshot()
	require.EqualValues(t, 0, grants, fmt.Sprintf("should not grant on other node, grants = %d", grants))
	// 关键：异服改走离线发放（biz_id 幂等），不占在线幂等键
	fm, ok := c.deps.Mail.(*fakeMail)
	require.True(t, ok, fmt.Sprintf("offline delivery not recorded: %+v", c.deps.Mail))
	require.Len(t, fm.delivered, 1)
	require.EqualValues(t, "O1002", fm.delivered[0])
	require.False(t, deduper.seen[pkgmq.IdemKey(pkgmq.TopicRechargeOrder, "O1002")], "offline path must not set online idem key")

	presence.node[1001] = "game-1"
	err = c.handle(context.Background(), mustEnvelope(t, "O1002", 1001, 100))
	require.NoError(t, err, fmt.Sprintf("handle after migrate: %v", err))
	grants, _ = act.snapshot()
	require.EqualValues(t, 1, grants, fmt.Sprintf("after migrate grants = %d, want 1", grants))
}

func TestRechargeSkipOffline(t *testing.T) {
	presence := newMemPresence()
	deduper := newMemDeduper()
	// 不在线
	c, act := setupConsumer(t, presence, deduper)
	err := c.handle(context.Background(), mustEnvelope(t, "O1003", 1001, 100))
	require.NoError(t, err, fmt.Sprintf("handle: %v", err))
	grants, _ := act.snapshot()
	require.EqualValues(t, 0, grants, "offline player must not be granted in actor")
	fm, ok := c.deps.Mail.(*fakeMail)
	require.True(t, ok, fmt.Sprintf("offline delivery not recorded: %+v", fm))
	require.Len(t, fm.delivered, 1)
	require.EqualValues(t, "O1003", fm.delivered[0])
}

func TestRechargeInvalidPayload(t *testing.T) {
	presence := newMemPresence()
	deduper := newMemDeduper()
	c, _ := setupConsumer(t, presence, deduper)

	env, _ := pkgmq.NewEnvelope("bad", pkgmq.TopicRechargeOrder, "bad", pkgmq.RechargeOrderMsg{OrderID: "bad", PlayerID: 0})
	err := c.handle(context.Background(), env)
	require.Error(t, err, "invalid payload should fail")
}
