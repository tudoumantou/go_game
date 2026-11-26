// internal/game/remote/remote.go —— 跨节点 Actor 投递发送端（P0）
//
// 架构职责：实现 actor.RemoteSender，把本节点 Actor 发出的远程消息
// 经 NATS 直达目标游戏节点。中心服是纯业务中心，**不参与**服间消息转发：
//
//	relay.node.{node}   → 目标节点订阅后投入本地 ActorSystem
//	relay.group.{group} → 组内各节点订阅（广播）
//
// 未配 NATS 时投递显式失败（不静默丢弃、不经中心服降级），由调用方按
// fire-and-forget 处理；推送丢失由客户端重连拉全量权威状态兜底。
//
// 为什么点对点用 node_id 而跨服玩法用 server_id：Actor 的目标 PID.Node
// 就是 node_id（部署态），投递语义是「某台机器上的那个 Actor」，与按
// 区服路由的玩法数据不是一回事。NATS 按 node_id 直投，无需中心服反查。
//
// 接收侧实现见 internal/game/relay_nats.go。
package remote

import (
	"fmt"
	"sync/atomic"

	"game_study/pkg/actor"
	"game_study/pkg/log"
)

// NATSRelay NATS 直达通道（由装配层的 relayAccessor 实现）。
//
// 按 subject 发布，不依赖中心服的节点目录；nil 表示未配 NATS，
// Deliver 会显式报错而不是静默丢弃。
type NATSRelay interface {
	// SendRelayNode 点对点发布到 relay.node.{toNode}。
	SendRelayNode(relayID, toNode string, payload []byte) error
	// SendRelayGroup 广播发布到 relay.group.{groupID}。
	SendRelayGroup(relayID, groupID string, payload []byte) error
}

// Sender 跨节点投递器。
type Sender struct {
	natsRelay NATSRelay
	nodeID    string
	logger    *log.Logger
	// seq 中转 ID 序号（接收端按 relay_id 去重，保证唯一即可）。
	seq atomic.Int64
}

// NewSender 创建发送器。natsRelay 为 nil 属于装配错误，Deliver 显式失败。
func NewSender(natsRelay NATSRelay, nodeID string, logger *log.Logger) *Sender {
	return &Sender{natsRelay: natsRelay, nodeID: nodeID, logger: logger}
}

// Deliver 实现 actor.RemoteSender。
//
// 两步：消息 → 信封（actor.EncodeRemote）→ NATS 发布。
// 非 RemoteMessage 的消息显式拒绝：带 Done channel 的请求-响应消息
// （如 GrantItemsMsg）天然不可跨节点，让它在这里失败，调用方的
// 重试/对账路径接管，而不是序列化到一半才炸。
func (s *Sender) Deliver(to *actor.PID, msg any) error {
	rm, ok := msg.(actor.RemoteMessage)
	if !ok {
		return fmt.Errorf("remote: message %T is not remotable (no actor.RemoteMessage)", msg)
	}
	env, err := actor.EncodeRemote(to, rm)
	if err != nil {
		return err
	}
	relayID := fmt.Sprintf("%s-%d", s.nodeID, s.seq.Add(1))
	return s.send(relayID, to.Node, false, "", env)
}

// SendBroadcast 向一个节点分组广播一条远程消息（供上层主动广播用）。
//
// 与 Deliver 共用信封编码与 relay_id 生成，只是目标是一组节点。
func (s *Sender) SendBroadcast(groupID string, msg any) error {
	rm, ok := msg.(actor.RemoteMessage)
	if !ok {
		return fmt.Errorf("remote: message %T is not remotable (no actor.RemoteMessage)", msg)
	}
	env, err := actor.EncodeRemote(&actor.PID{ID: "", Node: ""}, rm)
	if err != nil {
		return err
	}
	return s.send(fmt.Sprintf("%s-%d", s.nodeID, s.seq.Add(1)), "", true, groupID, env)
}

// send 发布到 NATS subject。未经中心服，无降级路径。
func (s *Sender) send(relayID, toNode string, broadcast bool, groupID string, payload []byte) error {
	if s.natsRelay == nil {
		return fmt.Errorf("remote: no NATS relay wired (node %s): cross-node delivery unavailable", s.nodeID)
	}
	var err error
	if broadcast {
		err = s.natsRelay.SendRelayGroup(relayID, groupID, payload)
	} else {
		err = s.natsRelay.SendRelayNode(relayID, toNode, payload)
	}
	if err != nil && s.logger != nil {
		s.logger.Warn("relay nats publish failed", "relay_id", relayID, "err", err)
	}
	return err
}
