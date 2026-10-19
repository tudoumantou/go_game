// pkg/actor/mailbox.go —— 有界信箱
package actor

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrMailboxFull 信箱已满：消息未被接收，调用方必须显式处理（重试/拒绝/降级）。
	//
	// 不默认丢弃最旧消息：旧消息可能是发奖、保存、绑定等不可重建的工作，
	// 用「新消息被拒 + 调用方决策」取代「静默挤掉旧消息」。
	ErrMailboxFull = errors.New("actor mailbox full")

	// ErrActorGone Actor 已终止（或在投递途中被终止），消息未被接收。
	ErrActorGone = errors.New("actor terminated")
)

// mailbox 有界信箱。
//
// 背压策略：满时**拒绝新消息**并返回 ErrMailboxFull，旧消息原样保留。
// 调用方决定重试、拒绝还是降级；框架不做静默丢弃。
//
// 并发说明：push 会被多个发送方 goroutine 并发调用；close 由分片
// （Terminate）调用。push 与 close 用 RWMutex 串行化，避免
// 「向已关闭 channel 发送」的 panic。pop 只由分片单线程调用，
// 从已关闭 channel 读取是安全的（返回零值 false）。
type mailbox struct {
	queue   chan envelope
	mu      sync.RWMutex
	closed  bool
	rejects atomic.Int64
}

// newMailbox 创建信箱，size <= 0 时使用默认 1024。
func newMailbox(size int) *mailbox {
	if size <= 0 {
		size = 1024
	}
	return &mailbox{queue: make(chan envelope, size)}
}

// push 投递消息；满时拒绝（不挤掉旧消息），已关闭时返回 ErrActorGone。
func (m *mailbox) push(env envelope) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrActorGone
	}
	select {
	case m.queue <- env:
		return nil
	default:
		m.rejects.Add(1)
		return ErrMailboxFull
	}
}

// pop 非阻塞取出消息，返回 ok=false 表示当前为空。
func (m *mailbox) pop() (envelope, bool) {
	select {
	case env, ok := <-m.queue:
		return env, ok
	default:
		return envelope{}, false
	}
}

// len 当前积压消息数（channel 的 len 是原子读，可在其他 goroutine 安全调用）。
func (m *mailbox) len() int { return len(m.queue) }

// rejectedCount 累计拒绝消息数（满信箱背压，供指标观测）。
func (m *mailbox) rejectedCount() int64 { return m.rejects.Load() }

// close 关闭信箱；重复调用安全。
func (m *mailbox) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.queue)
}
