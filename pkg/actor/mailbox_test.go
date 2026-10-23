package actor

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestMailboxRejectsWhenFull(t *testing.T) {
	m := newMailbox(2)
	err := m.push(envelope{message: 1})
	require.NoError(t, err, fmt.Sprintf("push 1: %v", err))
	err = m.push(envelope{message: 2})
	require.NoError(t, err, fmt.Sprintf("push 2: %v", err))
	// 满队列必须拒绝新消息，而不是挤掉旧消息。
	err = m.push(envelope{message: 3})
	require.False(t, !errors.Is(err, ErrMailboxFull), fmt.Sprintf("want ErrMailboxFull, got %v", err))
	got := m.rejectedCount()
	require.EqualValues(t, 1, got, fmt.Sprintf("rejected count = %d, want 1", got))
	// 旧消息按先进先出原样保留。
	for _, want := range []int{1, 2} {
		env, ok := m.pop()
		require.True(t, ok, fmt.Sprintf("pop = %v ok=%v, want %d", env.message, ok, want))
		require.EqualValues(t, want, env.message)
	}
	_, ok := m.pop()
	require.False(t, ok, "mailbox should be empty")
}

func TestMailboxCloseRejectsPush(t *testing.T) {
	m := newMailbox(2)
	m.close()
	m.close() // 重复关闭必须安全
	err := m.push(envelope{message: 1})
	require.False(t, !errors.Is(err, ErrActorGone), fmt.Sprintf("want ErrActorGone, got %v", err))
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
	err := sh.add(c1)
	require.NoError(t, err)
	err = sh.add(c2)
	require.NoError(t, err)
	for i := 0; i < 150; i++ {
		err := c1.mbox.push(envelope{message: i})
		require.NoError(t, err)
		err = c2.mbox.push(envelope{message: i})
		require.NoError(t, err)
	}
	got := sh.drain(100)
	require.EqualValues(t, 200, got, fmt.Sprintf("drain processed %d, want 200 (100 per actor)", got))
	require.EqualValues(t, 100, a1.count.Load(), fmt.Sprintf("per-actor budget violated: a1=%d a2=%d", a1.count.Load(), a2.count.Load()))
	require.EqualValues(t, 100, a2.count.Load())
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
