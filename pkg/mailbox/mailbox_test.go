package mailbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestTrySendRejectsWhenFull 有界邮箱满时明确拒收，而不是阻塞或丢。
func TestTrySendRejectsWhenFull(t *testing.T) {
	m := New[int](2)
	if err := m.TrySend(1); err != nil {
		t.Fatalf("第一次投递应成功: %v", err)
	}
	if err := m.TrySend(2); err != nil {
		t.Fatalf("第二次投递应成功: %v", err)
	}
	if err := m.TrySend(3); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("满了应返回 ErrOverloaded，实际: %v", err)
	}
	if m.Len() != 2 || m.Cap() != 2 {
		t.Fatalf("Len/Cap 不符: len=%d cap=%d", m.Len(), m.Cap())
	}
}

// TestTrySendAfterClose 关闭后投递返回错误而不是 panic。
//
// 这条守的是「先检查 closed 再 send」那个竞态：如果实现里没有 RWMutex，
// 并发 Close 与 TrySend 会 panic: send on closed channel。
func TestTrySendAfterClose(t *testing.T) {
	m := New[int](4)
	m.Close()
	if err := m.TrySend(1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后应返回 ErrClosed，实际: %v", err)
	}
}

// TestCloseIsIdempotent 重复关闭不 panic。
func TestCloseIsIdempotent(t *testing.T) {
	m := New[int](1)
	m.Close()
	m.Close()
}

// TestRecvDrainsThenFalse 关闭后已入队的消息仍可取完，取空返回 false。
//
// 「关闭即丢弃」会让已经接受的命令凭空消失；这里保证「已接受的会处理完」，
// 这是 Actor 优雅退出（Drain）能成立的前提。
func TestRecvDrainsThenFalse(t *testing.T) {
	m := New[int](4)
	_ = m.TrySend(7)
	m.Close()

	v, ok := m.Recv()
	if !ok || v != 7 {
		t.Fatalf("关闭前入队的消息应能取到，实际 v=%d ok=%v", v, ok)
	}
	if _, ok := m.Recv(); ok {
		t.Fatal("取空后应返回 false")
	}
	if _, ok := m.TryRecv(); ok {
		t.Fatal("TryRecv 在关闭且为空时应返回 false")
	}
}

// TestSendUnblocksOnContext 阻塞投递在 ctx 取消时返回，不永久挂住。
func TestSendUnblocksOnContext(t *testing.T) {
	m := New[int](1)
	_ = m.TrySend(1) // 塞满

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := m.Send(ctx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("满邮箱的 Send 应按 ctx 超时返回，实际: %v", err)
	}
}

// TestSendAfterClose 关闭后的 Send 立即返回 ErrClosed（不等 ctx）。
func TestSendAfterClose(t *testing.T) {
	m := New[int](1)
	m.Close()
	if err := m.Send(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Send 应返回 ErrClosed，实际: %v", err)
	}
}

// TestZeroCapacityBecomeOne 容量 <=0 收紧到 1，避免「永远投递失败」的陷阱。
func TestZeroCapacityBecomeOne(t *testing.T) {
	for _, c := range []int{0, -5} {
		m := New[int](c)
		if m.Cap() != 1 {
			t.Fatalf("capacity=%d 应修正为 1，实际 %d", c, m.Cap())
		}
		if err := m.TrySend(1); err != nil {
			t.Fatalf("修正后应能投递: %v", err)
		}
	}
}

// TestConcurrentSendClose 并发投递与关闭不 panic（配合 -race 跑）。
func TestConcurrentSendClose(t *testing.T) {
	for i := 0; i < 50; i++ {
		m := New[int](1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = m.TrySend(j) // 成功/满/已关闭都算正常返回
			}
		}()
		go func() {
			defer wg.Done()
			m.Close()
		}()
		wg.Wait()
	}
}
