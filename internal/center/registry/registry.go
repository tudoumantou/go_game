// Package registry 节点注册表（设计文档 9.2）
//
// 维护 node_id → 节点信息 + 活跃 Report 会话。心跳超时由 Tick 摘除。
// 并发模型：读写锁保护 map，Tick 与查询可并发。
package registry

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// 节点类型。
const (
	TypeGame  = "game"
	TypeCross = "cross"
)

// Node 节点信息（注册时上报 + 心跳更新）。
type Node struct {
	ID        string
	Type      string
	Addr      string
	GroupID   string
	Version   string
	StartAt   int64
	PlayerNum int
	CPULoad   int
	MemLoad   int
	MsgQPS    int64

	// LastSeen 最近一次收到心跳/任意上报的时间
	LastSeen time.Time
}

// Registry 节点注册表。
type Registry struct {
	mu     sync.RWMutex
	nodes  map[string]*Node
	// senders node_id → 下发通道（由 Report 流注册）
	senders map[string]Sender

	timeout time.Duration
	logger  Logger
}

// Sender 向节点下发指令的能力（一条 Report 流）。
type Sender interface {
	SendCommand(cmd any) error
	NodeID() string
}

// Logger 最小日志接口。
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
}

// New 创建注册表。
func New(timeout time.Duration, logger Logger) *Registry {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &Registry{
		nodes:   make(map[string]*Node),
		senders: make(map[string]Sender),
		timeout: timeout,
		logger:  logger,
	}
}

// Register 注册/更新节点并绑定下发通道。
func (r *Registry) Register(n *Node, s Sender) {
	if n == nil || n.ID == "" {
		return
	}
	n.LastSeen = time.Now()

	r.mu.Lock()
	old, existed := r.nodes[n.ID]
	if existed && old.Type != n.Type {
		// 同 ID 换类型属于配置错误，覆盖但告警
		r.logger.Warn("node type changed", "node", n.ID, "from", old.Type, "to", n.Type)
	}
	r.nodes[n.ID] = n
	if s != nil {
		r.senders[n.ID] = s
	}
	r.mu.Unlock()

	if existed {
		r.logger.Info("node re-registered", "node", n.ID, "type", n.Type, "group", n.GroupID)
	} else {
		r.logger.Info("node registered", "node", n.ID, "type", n.Type, "group", n.GroupID, "addr", n.Addr)
	}
}

// Heartbeat 更新心跳（节点未注册时返回 false，由调用方要求补发 Register）。
func (r *Registry) Heartbeat(nodeID string, playerNum, cpuLoad, memLoad int, qps int64, version string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[nodeID]
	if !ok {
		return false
	}
	n.LastSeen = time.Now()
	n.PlayerNum = playerNum
	n.CPULoad = cpuLoad
	n.MemLoad = memLoad
	n.MsgQPS = qps
	if version != "" {
		n.Version = version
	}
	return true
}

// Unregister 摘除节点（流断开）。
func (r *Registry) Unregister(nodeID string) {
	r.mu.Lock()
	_, existed := r.nodes[nodeID]
	delete(r.nodes, nodeID)
	delete(r.senders, nodeID)
	r.mu.Unlock()

	if existed {
		r.logger.Info("node unregistered", "node", nodeID)
	}
}

// Get 查询节点。
func (r *Registry) Get(nodeID string) (*Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.nodes[nodeID]
	if !ok {
		return nil, false
	}
	cp := *n
	return &cp, true
}

// Sender 返回节点的下发通道。
func (r *Registry) SenderFor(nodeID string) (Sender, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.senders[nodeID]
	return s, ok
}

// ListByType 按类型列出节点（按 ID 排序，保证结果稳定）。
func (r *Registry) ListByType(t string) []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		if n.Type == t {
			cp := *n
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ListByGroup 列出分组内节点（可指定类型，空表示全部）。
func (r *Registry) ListByGroup(groupID, nodeType string) []*Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		if n.GroupID != groupID {
			continue
		}
		if nodeType != "" && n.Type != nodeType {
			continue
		}
		cp := *n
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Groups 返回现有分组 ID 列表。
func (r *Registry) Groups() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	set := make(map[string]struct{}, len(r.nodes))
	for _, n := range r.nodes {
		if n.GroupID != "" {
			set[n.GroupID] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for g := range set {
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

// Tick 摘除心跳超时节点，返回被摘除的节点（供上层广播）。
func (r *Registry) Tick(now time.Time) []*Node {
	r.mu.Lock()
	var dead []*Node
	for id, n := range r.nodes {
		if now.Sub(n.LastSeen) > r.timeout {
			cp := *n
			dead = append(dead, &cp)
			delete(r.nodes, id)
			delete(r.senders, id)
		}
	}
	r.mu.Unlock()

	for _, n := range dead {
		r.logger.Warn("node heartbeat timeout, removed",
			"node", n.ID, "type", n.Type, "silence", now.Sub(n.LastSeen).String())
	}
	return dead
}

// Count 节点数量。
func (r *Registry) Count() (total, game, cross int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, n := range r.nodes {
		total++
		switch n.Type {
		case TypeGame:
			game++
		case TypeCross:
			cross++
		}
	}
	return
}

// String 便于日志。
func (n *Node) String() string {
	return fmt.Sprintf("%s(%s,%s)", n.ID, n.Type, n.GroupID)
}
