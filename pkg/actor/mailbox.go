// pkg/actor/mailbox.go —— 有界信箱（Actor 模型的消息缓冲单元）
//
// 架构职责：每个 actorCell 持有一个 mailbox，是消息进入 Actor 前的
// 唯一缓冲区，承载「背压」与「关闭安全」两个核心职责。
// 协作关系：push 由 system.go 的 shard.deliver（多发送方 goroutine）
// 调用；pop 由分片事件循环单 goroutine 调用；close 由 Terminate 路径
// 触发。
// 关键取舍：
//   - 有界 channel（容量=信箱大小）：满则拒绝，拒绝可见、可观测；
//   - RWMutex + closed 标志双重防「向已关闭 channel 发送」的 panic；
//   - rejects 用原子计数器而非锁：单变量只增，atomic 足够且无锁竞争。
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
	// Go 知识点：哨兵错误（sentinel error）模式——包级 var + errors.New，
	// 调用方用 errors.Is 判定类别；deliver 里用 %w 包装它并附带 id，
	// 错误链上仍可 errors.Is(err, ErrMailboxFull)。
	ErrMailboxFull = errors.New("actor mailbox full")

	// ErrActorGone Actor 已终止（或在投递途中被终止），消息未被接收。
	//
	// 同样建议用 errors.Is 判定；deliver 返回的包装错误还带 actor id。
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
//
// Go 知识点（channel 语义，本结构全部踩到）：
//   - 缓冲 channel 满时发送方阻塞，因此 push 用 select+default 做非阻塞
//     投递，把「背压决策」交还给发送方而不是让它睡在这里；
//   - nil channel 上发送/接收都会永久阻塞——queue 由 newMailbox 保证
//     非 nil，不存在这种静默死锁；
//   - close 后接收立即返回零值+ok=false，所以 pop 不需要额外判关闭；
//     close 后发送则直接 panic，这正是 mu+closed 存在的原因。
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
// make(chan envelope, size) 一次性分配环形缓冲，之后不再扩容——
// 「有界」在创建时就被物理固定。
func newMailbox(size int) *mailbox {
	if size <= 0 {
		size = 1024
	}
	return &mailbox{queue: make(chan envelope, size)}
}

// push 投递消息；满时拒绝（不挤掉旧消息），已关闭时返回 ErrActorGone。
//
// 选 RWMutex 而非 Mutex：push 是高频多发送方路径，读锁允许多个
// goroutine 并发投递；close 极低频，写锁等待可忽略。读锁期间完成
// 「检查 + 发送」，与 close 的写锁互斥，不存在插入窗口。
func (m *mailbox) push(env envelope) error {
	// 读锁足够：close 持写锁改 closed 并 close(chan)，互斥保证
	// 「检查未关闭」与「发送」之间不会插入关闭动作。
	m.mu.RLock()
	// defer 的调用在函数返回前一刻执行（参数已在此刻求值完毕），
	// 保证三个 return 路径都释放锁。
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
		// 原子自增：多发送方并发拒绝时计数不丢（rejects++ 非原子会漏计）
		m.rejects.Add(1)
		return ErrMailboxFull
	}
}

// pop 非阻塞取出消息，返回 ok=false 表示当前为空。
//
// 信箱关闭后从 channel 读取立即返回零值+false，因此无需额外判关闭；
// 这也保证 Terminate 之后分片循环仍能安全排空残余消息。
// 单消费者假设：只由分片 goroutine 调用，pop 内部无需锁——channel
// 的接收本身并发安全，且全局只有一个接收者。
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
//
// atomic.Load：与 push 里的 Add 构成「写-读」原子对，无需持锁。
func (m *mailbox) rejectedCount() int64 { return m.rejects.Load() }

// close 关闭信箱；重复调用安全。
//
// closed 标志位是必需的：Go 里重复 close(channel) 直接 panic，
// 而 Terminate 与 Stop 路径都可能触发关闭。
// 写锁保证与 push 的读锁互斥：任何进行中的 push 都结束后才 close，
// 不存在「push 刚通过 closed 检查、close 插入、随后发送 panic」的窗口。
func (m *mailbox) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	close(m.queue)
}
