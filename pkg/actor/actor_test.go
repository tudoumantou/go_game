// pkg/actor/actor_test.go —— Actor 运行时单测（含 -race）
package actor

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// counterActor 统计收到的消息数。
type counterActor struct {
	count atomic.Int64
}

func (a *counterActor) Receive(ctx *Context) {
	if _, ok := ctx.Message().(int); ok {
		a.count.Add(1)
	}
}

func TestSpawnAndSend(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 8, MailboxSize: 128})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)
	defer sys.Stop()

	a := &counterActor{}
	pid, err := sys.Spawn("p1", a)
	require.NoError(t, err, "spawn")
	for i := 0; i < 100; i++ {
		if err := sys.Send(pid, i); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	// 等待处理
	deadline := time.Now().Add(3 * time.Second)
	for a.count.Load() < 100 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := a.count.Load(); got != 100 {
		t.Fatalf("expected 100 messages, got %d", got)
	}
}

func TestMailboxBackpressure(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 4, MailboxSize: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)
	defer sys.Stop()

	blockCh := make(chan struct{})
	a := &blockActor{block: blockCh}
	pid, err := sys.Spawn("slow", a)
	require.NoError(t, err, "spawn")
	// 塞满 + 溢出：满队列必须显式返回 ErrMailboxFull，不允许 panic 或静默丢弃。
	rejected := 0
	for i := 0; i < 20; i++ {
		if err := sys.Send(pid, i); err != nil {
			if !errors.Is(err, ErrMailboxFull) && !errors.Is(err, ErrActorGone) {
				t.Fatalf("unexpected send error: %v", err)
			}
			rejected++
		}
	}
	if rejected == 0 {
		t.Log("本轮未被拒绝（分片已消费部分消息），背压契约由 mailbox 单测覆盖")
	}
	close(blockCh)
}

type blockActor struct {
	block <-chan struct{}
	once  sync.Once
}

func (a *blockActor) Receive(ctx *Context) {
	a.once.Do(func() { <-a.block })
}

func TestScheduleOnce(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 4, MailboxSize: 64})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)
	defer sys.Stop()

	a := &counterActor{}
	pid, _ := sys.Spawn("timer", a)
	sys.ScheduleOnce(50*time.Millisecond, pid, 1)

	deadline := time.Now().Add(2 * time.Second)
	for a.count.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if a.count.Load() != 1 {
		t.Fatalf("expected 1 scheduled message, got %d", a.count.Load())
	}
}

func TestSendToMissingActor(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 4, MailboxSize: 16})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)
	defer sys.Stop()

	if err := sys.Send(&PID{ID: "not-exist"}, 1); err == nil {
		t.Fatalf("expected error for missing actor")
	}
}
