package scenemap

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Mailbox 是有界消息邮箱。每个 Actor 一个，容量固定。
//
// 为什么必须有界：无界队列会把"过载"从"明确拒绝"变成"内存缓慢耗尽"，
// 后者在线上表现为延迟越来越大、最后 OOM，比直接拒绝难排查得多。
//
// 语义：
//   - TrySend 永不阻塞，满即返回 ErrOverloaded（调用方决定拒收还是降级）；
//   - 关闭后 TrySend 返回 ErrClosed 而不是 panic（用 RWMutex 消除
//     "检查 closed 与 send 之间的竞态"）；
//   - Recv 阻塞；关闭且取空后返回 false。
type Mailbox[T any] struct {
	mu     sync.RWMutex
	ch     chan T
	closed bool
	once   sync.Once
}

// NewMailbox 创建容量为 capacity 的邮箱。capacity <= 0 时取 1：
// 容量 0 的无缓冲 channel 会让 TrySend 永远失败，是配置陷阱。
func NewMailbox[T any](capacity int) *Mailbox[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &Mailbox[T]{ch: make(chan T, capacity)}
}

// TrySend 非阻塞投递。
func (m *Mailbox[T]) TrySend(v T) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	select {
	case m.ch <- v:
		return nil
	default:
		return ErrOverloaded
	}
}

// Send 阻塞投递，带上下文取消。用于"必须送达"的控制消息（关闭、迁移）。
func (m *Mailbox[T]) Send(ctx context.Context, v T) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return ErrClosed
	}
	ch := m.ch
	m.mu.RUnlock()
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Recv 阻塞取一条；关闭且取空返回 (零值,false)。
func (m *Mailbox[T]) Recv() (T, bool) {
	v, ok := <-m.ch
	return v, ok
}

// TryRecv 非阻塞取一条。主循环用它做"预算内批量处理"。
func (m *Mailbox[T]) TryRecv() (T, bool) {
	var zero T
	select {
	case v, ok := <-m.ch:
		if !ok {
			return zero, false
		}
		return v, true
	default:
		return zero, false
	}
}

// C 暴露只读 channel，供 select 与外部循环使用。
func (m *Mailbox[T]) C() <-chan T { return m.ch }

// Close 关闭邮箱，幂等。
func (m *Mailbox[T]) Close() {
	m.once.Do(func() {
		m.mu.Lock()
		m.closed = true
		close(m.ch)
		m.mu.Unlock()
	})
}

// Len 返回排队数量（监控/测试用）。
func (m *Mailbox[T]) Len() int { return len(m.ch) }

// Call 是"带超时的同步调用"辅助。
//
// 契约：fn 负责把结果写入 reply（容量必须 >= 1），Call 只等结果。
// 超时返回 ErrTimeout —— **结果未知**，调用方不能当失败重试。
//
// 为什么不让调用方直接等 channel：超时后那个 goroutine 仍会写 reply，
// 如果 reply 无缓冲，写方会永久阻塞（goroutine 泄漏）。这里强制容量 1
// 并检查，把"忘记给缓冲"变成构造期错误。
func Call[T any](ctx context.Context, timeout time.Duration, reply chan T, fn func() error) (T, error) {
	var zero T
	if cap(reply) < 1 {
		return zero, fmt.Errorf("%w: reply 必须至少 1 个缓冲", ErrInvalidArgument)
	}
	if err := fn(); err != nil {
		return zero, err
	}
	if timeout <= 0 {
		timeout = 3 * time.Second // 默认 3s：与协议超时同量级
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-reply:
		return v, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-timer.C:
		return zero, ErrTimeout
	}
}

// SafeRun 在独立 goroutine 里运行 fn，并捕获 panic。
//
// 为什么必须有：Go 的 goroutine panic 会直接杀进程；而一个 MGR 里
// 可能承载几百只怪与几十个玩家，因为某个玩法的越界 panic 把整个进程
// 带走是不可接受的。捕获后交给 onPanic（通常是"关掉这张图 + 上报"）。
func SafeRun(name string, onPanic func(error), fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("scenemap: %s panic: %v", name, r)
				if onPanic != nil {
					onPanic(err)
				}
			}
		}()
		fn()
	}()
}
