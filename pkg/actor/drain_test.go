// pkg/actor/drain_test.go —— 关闭前排空语义
package actor

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

// slowActor 每条消息处理固定时长，用于验证 Drain 会等在途消息真正执行完，
// 而不是看到信箱空了就返回。
type slowActor struct {
	delay   time.Duration
	done    atomic.Int64
	started chan struct{}
}

func (a *slowActor) Receive(*Context) {
	if a.started != nil {
		select {
		case a.started <- struct{}{}:
		default:
		}
	}
	if a.delay > 0 {
		time.Sleep(a.delay)
	}
	a.done.Add(1)
}

// TestDrainWaitsForInFlight 信箱被取空之后，handler 可能仍在执行；
// Drain 必须等到 done == 投递数，否则关闭时最后一条业务会被截断。
func TestDrainWaitsForInFlight(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 1, MailboxSize: 512})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)
	defer sys.Stop()

	a := &slowActor{delay: time.Millisecond}
	pid, err := sys.Spawn("slow", a)
	require.NoError(t, err)
	const n = 50
	for i := 0; i < n; i++ {
		err := sys.Send(pid, i)
		require.NoError(t, err)
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	err = sys.Drain(drainCtx)
	require.NoError(t, err, fmt.Sprintf("drain: %v", err))
	got := a.done.Load()
	require.EqualValues(t, n, got, fmt.Sprintf("drain returned before handlers finished: %d/%d", got, n))
}

// TestDrainTimesOut 处理中的消息超过截止时间时，Drain 必须如实报错，
// 让关闭流程记录「这批可能没保存完」，而不是假装成功。
func TestDrainTimesOut(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 1, MailboxSize: 8})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys.Start(ctx)
	defer sys.Stop()

	a := &slowActor{delay: 200 * time.Millisecond, started: make(chan struct{}, 1)}
	pid, err := sys.Spawn("slow", a)
	require.NoError(t, err)
	err = sys.Send(pid, 1)
	require.NoError(t, err)
	select {
	case <-a.started:
	case <-time.After(time.Second):
		t.Fatal("actor did not start")
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer drainCancel()
	err = sys.Drain(drainCtx)
	require.Error(t, err, "drain should time out while a handler is still running")
}

// TestDrainIdleSystem 空系统立即返回，不引入固定等待。
func TestDrainIdleSystem(t *testing.T) {
	sys := NewSystem(Config{ShardCount: 4})
	err := sys.Drain(context.Background())
	require.NoError(t, err, fmt.Sprintf("idle drain: %v", err))
}
