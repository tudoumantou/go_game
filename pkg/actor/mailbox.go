// pkg/actor/mailbox.go —— 有界信箱（Actor 模型的消息缓冲单元）
//
// 架构职责：每个 actorCell 持有一个 mailbox，是消息进入 Actor 前的
// 唯一缓冲区，承载「背压」与「关闭安全」两个核心职责。
// 协作关系：push 由 system.go 的 shard.deliver（多发送方 goroutine）
// 调用；pop 由分片事件循环单 goroutine 调用；close 由 Terminate 路径触发。
//
// 实现落在 pkg/mailbox（与 scenemap、worldmap 共用同一份通道边界逻辑）：
// 「有界 + 关闭安全 + 非阻塞投递」这三件事在全仓只该有一个答案，此前
// actor、scenemap、worldmap 各写了一遍，逻辑一致、哨兵名不同。
//
// 本层只加两样 actor 特有的东西：
//   - rejects 计数器（满信箱的背压事件要进指标，见 rejectedCount）；
//   - 错误映射（对外是 ErrMailboxFull / ErrActorGone，比基础设施哨兵更
//     贴近调用方的处置方式：一个说「重试/降级」，一个说「别重试了」）。
package actor

import (
	"errors"
	"sync/atomic"

	mbox "game_study/pkg/mailbox"
)

var (
	// ErrMailboxFull 信箱已满：消息未被接收，调用方必须显式处理（重试/拒绝/降级）。
	//
	// 不默认丢弃最旧消息：旧消息可能是发奖、保存、绑定等不可重建的工作，
	// 用「新消息被拒 + 调用方决策」取代「静默挤掉旧消息」。
	// 哨兵错误（sentinel error）模式——包级 var + errors.New，调用方用
	// errors.Is 判定类别；deliver 里用 %w 包装它并附带 id，错误链上仍可识别。
	ErrMailboxFull = errors.New("actor mailbox full")

	// ErrActorGone Actor 已终止（或在投递途中被终止），消息未被接收。
	// 同样建议用 errors.Is 判定；deliver 返回的包装错误还带 actor id。
	ErrActorGone = errors.New("actor terminated")
)

// mailbox 有界信箱。
//
// 背压策略：满时**拒绝新消息**并返回 ErrMailboxFull，旧消息原样保留。
// 调用方决定重试、拒绝还是降级；框架不做静默丢弃。
//
// 线程契约：push 被多个发送方 goroutine 并发调用，close 由分片
// （Terminate）调用——两者的互斥由 pkg/mailbox 的 RWMutex 保证
// （否则会出现「向已关闭 channel 发送」的 panic）。pop 只由分片单线程
// 调用，从已关闭 channel 读取是安全的（返回零值 false）。
type mailbox struct {
	box *mbox.Mailbox[envelope]
	// rejects 累计拒绝数：单变量只增，atomic 足够且无锁竞争，
	// 供分片循环做增量指标上报。
	rejects atomic.Int64
}

// newMailbox 创建信箱，size <= 0 时使用默认 1024。
//
// 兜底默认值与 System 的默认 MailboxSize 保持一致：配置漏填时行为可预期，
// 不会意外得到无限信箱（无限 = 背压失效 = OOM 风险）。
func newMailbox(size int) *mailbox {
	if size <= 0 {
		size = 1024
	}
	return &mailbox{box: mbox.New[envelope](size)}
}

// push 投递消息；满时拒绝（不挤掉旧消息），已关闭时返回 ErrActorGone。
func (m *mailbox) push(env envelope) error {
	err := m.box.TrySend(env)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mbox.ErrClosed):
		return ErrActorGone
	default:
		// 满：原子自增后拒绝。多发送方并发拒绝时计数不丢。
		m.rejects.Add(1)
		return ErrMailboxFull
	}
}

// pop 非阻塞取出消息，返回 ok=false 表示当前为空。
//
// 信箱关闭后从 channel 读取立即返回零值+false，因此无需额外判关闭；
// 这也保证 Terminate 之后分片循环仍能安全排空残余消息。
func (m *mailbox) pop() (envelope, bool) { return m.box.TryRecv() }

// len 当前积压消息数（channel 的 len 是原子读，可在其他 goroutine 安全调用）。
func (m *mailbox) len() int { return m.box.Len() }

// rejectedCount 累计拒绝消息数（满信箱背压，供指标观测）。
func (m *mailbox) rejectedCount() int64 { return m.rejects.Load() }

// close 关闭信箱；重复调用安全（Terminate 与 Stop 路径都可能触发）。
func (m *mailbox) close() { m.box.Close() }
