// Package relay 跨节点消息中转（设计文档 9.3）
//
// 游戏服之间不直连，控制面消息（组队邀请、公会战指令、全服广播）经中心服转发：
//
//	GameServer#3 ──RelayRequest{to:"game-07"}──▶ CenterServer ──RelayPush──▶ GameServer#7
//
// 可靠性约定：中转尽力而为 —— 未送达时按指数退避重试至多 N 次，仍失败则丢弃并计数。
// 中心服**不持久化**中转消息：丢消息由业务层幂等/重查兜底（9.3）。
//
// 与 bridge.go（跨实例）的分工：本文件管同实例内的内存转发与重试，
// bridge.go 管目标在其它中心服实例时的 Kafka 转发（刻意不重试）。
package relay

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultRetry 默认重试次数。
// 5 次 + 200ms 起步的指数退避 ≈ 6 秒总窗口：覆盖节点瞬断重连的
// 典型时长，又不让一条中转占着 pending 太久。
const DefaultRetry = 5

// Pusher 下发能力（由 center/rpc 的 session 实现）。
// 与 registry.Sender 分离：relay 包不依赖 registry，
// 由 server 侧用 senderAdapter 做适配——包间依赖保持单向。
type Pusher interface {
	NodeID() string
	SendPush(push *PushPayload) error
}

// PushPayload 对端要发送的内容（解耦 protobuf，便于单测）。
// 不直接带 *internalv2.RelayPush：relay 包因此不依赖生成的 pb 代码，
// 测试用普通字符串/字节即可构造用例。
type PushPayload struct {
	RelayID  string
	FromNode string
	Payload  []byte
}

// Router 中转路由器。
//
// pending 只存未送达的点对点消息（广播不重试，见 Relay）：relay_id
// 是主键，重试到上限即丢弃并计入 Failed——「尽力而为」的终点。
// seq 用于生成缺失的 relay_id，atomic 保证并发 Enqueue 时不重号。
type Router struct {
	maxRetry int
	retryGap time.Duration

	// pending relay_id → 待重试条目
	mu      sync.Mutex
	pending map[string]*attempt
	seq     atomic.Uint64

	stats Stats
}

type attempt struct {
	payload Payload
	retries int
	// nextAt 下次重试时刻：由 NextRetry 推进（指数退避），
	// Relay 重复失败时只刷新它、不动 retries——计数权归 NextRetry
	nextAt time.Time
}

// Payload 一次中转请求（已解析的最小信息）。
type Payload struct {
	RelayID   string
	FromNode  string
	ToNode    string
	Broadcast bool
	GroupID   string
	Body      []byte
}

// Stats 中转统计。
// 四个 atomic.Int64：Relay 与 NextRetry 在不同调用方（gRPC handler 与
// 定时器）并发执行，计数器必须无锁；StatsSnapshot 用 Load 取快照。
type Stats struct {
	Sent      atomic.Int64
	Delivered atomic.Int64
	Failed    atomic.Int64
	Broadcast atomic.Int64
}

// NewRouter 创建路由器。
// maxRetry/retryGap 非正时落默认值：配置缺项不应让中转退化为一轮游，
// 默认值（5 次 / 200ms）是生产验证过的起点。
func NewRouter(maxRetry int, retryGap time.Duration) *Router {
	if maxRetry <= 0 {
		maxRetry = DefaultRetry
	}
	if retryGap <= 0 {
		retryGap = 200 * time.Millisecond
	}
	return &Router{
		maxRetry: maxRetry,
		retryGap: retryGap,
		pending:  make(map[string]*attempt),
	}
}

// Relay 执行一次中转。targets 由调用方按 to_node/broadcast 解析后传入。
//
// 返回成功投递的节点数。未投递的进 pending，由 NextRetry 驱动重试。
//
// 三条路径：全部送达 → 清 pending（可能的重试条目作废）；部分未达且广播
// → 直接计失败不重试（广播重试会在故障期放大流量）；部分未达且点对点
// → 进 pending 等 NextRetry。delivered 逐目标累加：部分成功也如实计数。
func (r *Router) Relay(p Payload, targets []Pusher) int {
	r.stats.Sent.Add(1)
	if p.Broadcast {
		r.stats.Broadcast.Add(1)
	}
	if p.RelayID == "" {
		p.RelayID = r.nextID()
	}

	delivered := 0
	for _, t := range targets {
		if err := t.SendPush(&PushPayload{
			RelayID:  p.RelayID,
			FromNode: p.FromNode,
			Payload:  p.Body,
		}); err != nil {
			continue
		}
		delivered++
		r.stats.Delivered.Add(1)
	}

	if delivered == len(targets) {
		// 全部送达：清掉可能存在的重试条目（否则会一直重投到上限）
		r.mu.Lock()
		delete(r.pending, p.RelayID)
		r.mu.Unlock()
		return delivered
	}

	// 有未送达
	if p.Broadcast {
		// 广播不重试，避免故障时放大流量
		// 广播的目标是一整个组：任一节点离线就重试会把消息重复推给
		// 已收到的人（Pusher 无去重），比丢一条更糟
		r.stats.Failed.Add(int64(len(targets) - delivered))
		return delivered
	}
	r.mu.Lock()
	if a, ok := r.pending[p.RelayID]; ok {
		// 重试仍失败：保留原 retries（由 NextRetry 递增），刷新下次时间
		// 计数权归一：retries 只在 NextRetry 里 ++，避免两条路径
		// 同时递增导致退避曲线跳档
		a.nextAt = time.Now().Add(r.retryGap * time.Duration(1<<min(a.retries, 10)))
	} else {
		r.pending[p.RelayID] = &attempt{
			payload: p,
			retries: 0,
			nextAt:  time.Now().Add(r.retryGap),
		}
	}
	r.mu.Unlock()
	return delivered
}

// NextRetry 返回到期待重试的 payload（由定时器周期调用）。
// 超过重试上限的条目被丢弃并计入 Failed。
//
// 返回副本而非 pending 里的指针：调用方（tickLoop）拿到后还要再调
// Relay，而 Relay 会加锁改 pending——直接交出内部指针等于把锁内数据
// 暴露给锁外。到点判定用 now.Before 反式表达：未到点的跳过。
func (r *Router) NextRetry(now time.Time) []Payload {
	r.mu.Lock()
	var due []Payload
	for id, a := range r.pending {
		if now.Before(a.nextAt) {
			continue
		}
		if a.retries >= r.maxRetry {
			delete(r.pending, id)
			r.stats.Failed.Add(1)
			continue
		}
		a.retries++
		// 先 ++ 再算下次：首次重试等 2×gap、第二次 4×gap……
		// 指数增长给对端留出恢复时间，也避免本实例空转
		a.nextAt = now.Add(r.retryGap * time.Duration(1<<a.retries))
		due = append(due, a.payload)
	}
	r.mu.Unlock()
	return due
}

// PendingCount 待重试条目数。
func (r *Router) PendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// StatsSnapshot 统计快照。
func (r *Router) StatsSnapshot() (sent, delivered, failed, broadcast int64) {
	return r.stats.Sent.Load(), r.stats.Delivered.Load(),
		r.stats.Failed.Load(), r.stats.Broadcast.Load()
}

// min 返回较小值。
// 本地定义而非用内建 min（Go 1.21+）：项目 go 版本可能低于 1.21，
// 且内建 min 不支持这里的 int 窄类型推断场景之外的编译期检查。
// 用途：把退避指数封顶在 2^10，防止 retries 记满后 shift 溢出。
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// nextID 生成缺失的 relay_id。
// 时间戳 + seq 双段：nanosecond 级时间戳保证跨进程视角的大致有序，
// seq 保证同纳秒内的多个请求不重号（atomic.Add 返回新值可直接用）。
func (r *Router) nextID() string {
	return fmt.Sprintf("relay-%d-%d", time.Now().UnixNano(), r.seq.Add(1))
}
