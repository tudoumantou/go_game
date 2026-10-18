// Package relay 跨节点消息中转（设计文档 9.3）
//
// 游戏服之间不直连，控制面消息（组队邀请、公会战指令、全服广播）经中心服转发：
//
//	GameServer#3 ──RelayRequest{to:"game-07"}──▶ CenterServer ──RelayPush──▶ GameServer#7
//
// 可靠性约定：中转尽力而为 —— 未送达时按指数退避重试至多 N 次，仍失败则丢弃并计数。
// 中心服**不持久化**中转消息：丢消息由业务层幂等/重查兜底（9.3）。
package relay

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultRetry 默认重试次数。
const DefaultRetry = 5

// Pusher 下发能力（由 center/rpc 的 session 实现）。
type Pusher interface {
	NodeID() string
	SendPush(push *PushPayload) error
}

// PushPayload 对端要发送的内容（解耦 protobuf，便于单测）。
type PushPayload struct {
	RelayID  string
	FromNode string
	Payload  []byte
}

// Router 中转路由器。
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
	nextAt  time.Time
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
type Stats struct {
	Sent      atomic.Int64
	Delivered atomic.Int64
	Failed    atomic.Int64
	Broadcast atomic.Int64
}

// NewRouter 创建路由器。
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
		r.stats.Failed.Add(int64(len(targets) - delivered))
		return delivered
	}
	r.mu.Lock()
	if a, ok := r.pending[p.RelayID]; ok {
		// 重试仍失败：保留原 retries（由 NextRetry 递增），刷新下次时间
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
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (r *Router) nextID() string {
	return fmt.Sprintf("relay-%d-%d", time.Now().UnixNano(), r.seq.Add(1))
}
