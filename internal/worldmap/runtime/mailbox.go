package runtime

import (
	"errors"
	"sync"
)

// ErrMailboxClosed 邮箱已关闭：说明场景正在退出，调用方不应重试。
var ErrMailboxClosed = errors.New("worldmap/runtime: 邮箱已关闭")

// Mailbox 是有界邮箱（文档 4.2）。
//
// 为什么用泛型：命令邮箱装 routedCommand、控制邮箱装 ControlMessage，
// 两者行为完全一致。用泛型可以只实现一次边界逻辑，同时保持编译期类型安全
// （用 any 则每次 Recv 都要类型断言，断言失败只能 panic）。
//
// 语义要点：
//   - TrySend 永不阻塞。满了立刻返回错误——场景邮箱满时必须明确拒收，
//     让上游得到 Overloaded，而不是把整个网关/Game 服拖死在这里。
//   - 关闭后 TrySend 返回错误而不是 panic。用 RWMutex 保护「closed 标志
//     检查 + 发送」这个复合动作：如果先检查 flags 再 send，close 可能
//     恰好插在中间导致 send on closed channel panic。
//   - Recv 在关闭且取空后返回 false，调用方据此退出。
type Mailbox[T any] struct {
	mu       sync.RWMutex
	ch       chan T
	closed   bool
	closeOne sync.Once
}

// NewMailbox 创建容量为 capacity 的邮箱。容量必须为正：
// 无界队列会把背压问题从"明确拒收"变成"内存缓慢耗尽"，后者更难排查。
func NewMailbox[T any](capacity int) *Mailbox[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &Mailbox[T]{ch: make(chan T, capacity)}
}

// TrySend 非阻塞入队。
func (m *Mailbox[T]) TrySend(v T) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrMailboxClosed
	}
	select {
	case m.ch <- v:
		return nil
	default:
		// 用 errors.Is(err, ErrOverloaded) 判定，调用方据此回 Overloaded。
		return ErrOverloaded
	}
}

// Recv 阻塞取一条；邮箱关闭且已取空时返回 (零值,false)。
func (m *Mailbox[T]) Recv() (T, bool) {
	v, ok := <-m.ch
	return v, ok
}

// TryRecv 非阻塞取一条。场景循环用它实现"预算内批量处理"：
// 取够预算就停下，把 CPU 让给定时器与控制通道。
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

// C 暴露只读 channel，供 select 多路复用。
// 注意：直接 range 这个 channel 会在 Close 后退出，这是预期行为。
func (m *Mailbox[T]) C() <-chan T { return m.ch }

// Close 关闭邮箱，幂等。
func (m *Mailbox[T]) Close() {
	m.closeOne.Do(func() {
		m.mu.Lock()
		m.closed = true
		close(m.ch)
		m.mu.Unlock()
	})
}

// Len 返回当前排队数量（仅用于监控与测试断言）。
func (m *Mailbox[T]) Len() int { return len(m.ch) }

// Cap 返回容量。
func (m *Mailbox[T]) Cap() int { return cap(m.ch) }
