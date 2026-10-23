package actor

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestMailboxRejectsWhenFull(t *testing.T) {
	m := newMailbox(2)
	if err := m.push(envelope{message: 1}); err != nil {
		t.Fatalf("push 1: %v", err)
	}
	if err := m.push(envelope{message: 2}); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	// 满队列必须拒绝新消息，而不是挤掉旧消息。
	if err := m.push(envelope{message: 3}); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("want ErrMailboxFull, got %v", err)
	}
	if got := m.rejectedCount(); got != 1 {
		t.Fatalf("rejected count = %d, want 1", got)
	}
	// 旧消息按先进先出原样保留。
	for _, want := range []int{1, 2} {
		env, ok := m.pop()
		if !ok || env.message != want {
			t.Fatalf("pop = %v ok=%v, want %d", env.message, ok, want)
		}
	}
	if _, ok := m.pop(); ok {
		t.Fatal("mailbox should be empty")
	}
}

func TestMailboxCloseRejectsPush(t *testing.T) {
	m := newMailbox(2)
	m.close()
	m.close() // 重复关闭必须安全
	if err := m.push(envelope{message: 1}); !errors.Is(err, ErrActorGone) {
		t.Fatalf("want ErrActorGone, got %v", err)
	}
}

// TestShardPerActorBudget 验证每轮预算是「每 Actor 100 条」而不是整分片共享。
//
// 共享额度时，第一个被遍历的 Actor 会吃掉全部 100 条，第二个一条都处理不到；
// 按 Actor 分配后，两个各处理 100 条，剩余 50 条留到下一轮。
func TestShardPerActorBudget(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 1, MailboxSize: 512})
	sh := sys.shards[0]
	a1, a2 := &counterActor{}, &counterActor{}
	c1 := newActorCell(&PID{ID: "a", Shard: 0}, a1, 512, sys)
	c2 := newActorCell(&PID{ID: "b", Shard: 0}, a2, 512, sys)
	if err := sh.add(c1); err != nil {
		t.Fatal(err)
	}
	if err := sh.add(c2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 150; i++ {
		if err := c1.mbox.push(envelope{message: i}); err != nil {
			t.Fatal(err)
		}
		if err := c2.mbox.push(envelope{message: i}); err != nil {
			t.Fatal(err)
		}
	}
	if got := sh.drain(100); got != 200 {
		t.Fatalf("drain processed %d, want 200 (100 per actor)", got)
	}
	if a1.count.Load() != 100 || a2.count.Load() != 100 {
		t.Fatalf("per-actor budget violated: a1=%d a2=%d", a1.count.Load(), a2.count.Load())
	}
}

// TestConcurrentSendTerminate 覆盖「投递与终止并发」：不允许 panic，
// 失败必须是调用方可识别的 ErrMailboxFull / ErrActorGone。
func TestConcurrentSendTerminate(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 2, MailboxSize: 8})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)
	defer sys.Stop()

	pid, err := sys.Spawn("gone", &counterActor{})
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				err := sys.Send(pid, j)
				if err != nil && !errors.Is(err, ErrMailboxFull) && !errors.Is(err, ErrActorGone) {
					t.Errorf("unexpected send error: %v", err)
					return
				}
			}
		}()
	}
	time.Sleep(5 * time.Millisecond)
	sys.Terminate(pid)
	wg.Wait()
}
