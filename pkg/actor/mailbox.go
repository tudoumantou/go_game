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
//
// 字段：queue 是有界缓冲 channel，容量即信箱大小；mu+closed 保护关闭
// 状态；rejects 为原子计数器，只增不减，供分片循环做增量指标上报。
type mailbox struct {
	queue   chan envelope
	mu      sync.RWMutex
	closed  bool
	rejects atomic.Int64
}

// newMailbox 创建信箱，size <= 0 时使用默认 1024。
//
// 兜底默认值与 System 的默认 MailboxSize 保持一致：配置漏填时行为
// 可预期，不会意外得到无界信箱（无界 = 背压失效 = OOM 风险）。
func newMailbox(size int) *mailbox {
	if size <= 0 {
		size = 1024
	}
	return &mailbox{queue: make(chan envelope, size)}
}

// push 投递消息；满时拒绝（不挤掉旧消息），已关闭时返回 ErrActorGone。
func (m *mailbox) push(env envelope) error {
	// 读锁足够：close 持写锁改 closed 并 close(chan)，互斥保证
	// 「检查未关闭」与「发送」之间不会插入关闭动作。
	m.mu.RLock()
	defer m.mu.RUnlock()
	// 关记号优先于发送：与上面的锁双保险，杜绝向已关闭 channel 发送的 panic
	if m.closed {
		return ErrActorGone
	}
	// 非阻塞投递：满立即走 default。发送方可能是网关/Kafka goroutine，
	// 在这里阻塞会把背压传导成整条链路雪崩。
	select {
	case m.queue <- env:
		return nil
	default:
		m.rejects.Add(1)
		return ErrMailboxFull
	}
}

// pop 非阻塞取出消息，返回 ok=false 表示当前为空。
//
// 信箱关闭后从 channel 读取立即返回零值+false，因此无需额外判关闭；
// 这也保证 Terminate 之后分片循环仍能安全排空残余消息。
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
//
// closed 标志位是必需的：Go 里重复 close(channel) 直接 panic，
// 而 Terminate 与 Stop 路径都可能触发关闭。
func (m *mailbox) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.queue)
}
