// Package mailbox 有界消息邮箱：Actor 信箱的通用实现。
//
// # 为什么要有这个包
//
// scenemap 与 worldmap 各自实现过一份 Mailbox[T]（152 行 / 95 行），方法
// 名完全相同（TrySend/Recv/TryRecv/C/Len/Close），逻辑都是
// 「RWMutex + chan + closed + Once」。两者的 doc.go 都写着「互不 import」，
// 但互不 import 说明的是业务模型不同（坐标系、并发模型、持久化路径），
// 不等于该各造一个邮箱。邮箱是纯粹的通道边界逻辑，与领域无关。
//
// # 为什么必须有界
//
// 无界队列把「过载」从「明确拒绝」变成「内存缓慢耗尽」：后者在线上表现为
// 延迟越来越大、最后 OOM，比直接拒绝难排查得多。所以 TrySend 永不阻塞，
// 满即返回 ErrOverloaded，由调用方决定拒收还是降级。
//
// # 错误语义
//
// 本包只定义两个哨兵：ErrOverloaded（满了，可重试/可降级）与 ErrClosed
// （Actor 已退出，不要重试）。使用方通常会在自己的包装层把它们映射成
// 领域错误——例如 scenemap 的 ErrClosed 还要表达「房间池已关闭」，不能
// 让一个业务错误被基础设施包的哨兵取代。
package mailbox

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrOverloaded 邮箱已满：调用方未获得投递权，可降级或重试。
	//
	// 用 errors.Is 判定：使用方的包装层会把它映射成自己的领域错误，
	// 映射后仍要能被识别，所以哨兵只判断不比较字符串。
	ErrOverloaded = errors.New("mailbox: 邮箱已满")
	// ErrClosed 邮箱已关闭：Actor 正在退出，调用方不应重试。
	ErrClosed = errors.New("mailbox: 邮箱已关闭")
)

// Mailbox 有界邮箱。
//
// 为什么用泛型：命令邮箱装 Command、控制邮箱装 ControlMessage，两者行为
// 完全一致。泛型只实现一次边界逻辑，同时保持编译期类型安全（用 any 则
// 每次 Recv 都要断言，断言失败只能 panic）。
//
// 语义要点：
//   - TrySend 永不阻塞。满了立刻返回错误——邮箱满时必须明确拒收，让上游
//     得到 Overloaded，而不是把整个网关/游戏服拖死在这里。
//   - 关闭后 TrySend 返回错误而不是 panic。用 RWMutex 保护「closed 标志
//     检查 + 发送」这个复合动作：如果先检查 flags 再 send，close 可能
//     恰好插在中间，导致 send on closed channel panic。
//   - Recv 在关闭且取空后返回 false，调用方据此退出。
type Mailbox[T any] struct {
	mu     sync.RWMutex
	ch     chan T
	closed bool
	once   sync.Once
}

// New 创建容量为 capacity 的邮箱。
//
// capacity <= 0 时取 1：容量 0 的无缓冲 channel 会让 TrySend 永远失败
// （没有接收者时非阻塞发送必然走 default），这是配置陷阱而不是有意义的
// 配置，所以静默纠正成最小值。
func New[T any](capacity int) *Mailbox[T] {
	if capacity <= 0 {
		capacity = 1
	}
	return &Mailbox[T]{ch: make(chan T, capacity)}
}

// TrySend 非阻塞投递：满返回 ErrOverloaded，关闭返回 ErrClosed。
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

// Send 阻塞投递，带上下文取消。用于「必须送达」的控制消息（关闭、迁移）。
//
// 与 TrySend 的分工：TrySend 是背压（宁可拒），Send 是可靠性（宁可等）。
// 控制消息不能因为业务流量把邮箱打满就丢掉——但也不能无限等，所以带 ctx。
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

// Recv 阻塞取一条；邮箱关闭且已取空时返回 (零值, false)。
func (m *Mailbox[T]) Recv() (T, bool) {
	v, ok := <-m.ch
	return v, ok
}

// TryRecv 非阻塞取一条。Actor 主循环用它实现「预算内批量处理」：
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
//
// 先置 closed 再 close(ch)：置位之后新的 TrySend 会在 RWMutex 的读锁里
// 看到 closed 而直接返回错误，不会再往即将关闭的 channel 发送。
func (m *Mailbox[T]) Close() {
	m.once.Do(func() {
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
