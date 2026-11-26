// internal/game/remote/remote.go —— 跨节点 Actor 投递发送端（P0）
//
// 架构职责：实现 actor.RemoteSender，把本节点 Actor 发出的远程消息
// 投递给目标游戏节点。中心服退为纯业务中心后，主路径是 **NATS 直达**：
//
//	relay.node.{node}   → 目标节点订阅后投入本地 ActorSystem
//	relay.group.{group} → 组内各节点订阅（广播）
//
// 未配 NATS 时降级走中心服的 gRPC 中转（CenterRelay），保留单机/无
// 基础设施环境下的可用性。两条路径对上层 Deliver 完全透明。
//
// 为什么点对点用 node_id 而跨服玩法用 server_id：Actor 的目标 PID.Node
// 就是 node_id（部署态），投递语义是「某台机器上的那个 Actor」，与按
// 区服路由的玩法数据不是一回事。NATS 按 node_id 直投，中心服无需反查。
//
// 接收侧对称实现见 internal/game/relay_nats.go（NATS）与
// server_adapters.go 的 centerHandler.OnRelayPush（gRPC 降级）。
package remote

import (
	"fmt"
	"sync/atomic"

	"game_study/pkg/actor"
	"game_study/pkg/log"
)

// CenterRelay gRPC 降级通道（*gamecenter.Client 实现；测试可注入内存实现）。
//
// 仅在没有 NATS 时使用。只声明本包用到的方法，Sender 不依赖整个中心服客户端。
type CenterRelay interface {
	// SendRelay 投递中转消息；toNode 为空 + broadcast=true 表示组播。
	SendRelay(relayID, toNode string, broadcast bool, groupID string, payload []byte) error
}

// NATSRelay NATS 直达通道（由装配层的 relayAccessor 实现）。
//
// 与 CenterRelay 分开声明：NATS 分支按 subject 发布，不需要中心服的
// 节点目录反查；nil 表示未配 NATS，走 gRPC 降级。
type NATSRelay interface {
	// SendRelayNode 点对点发布到 relay.node.{toNode}。
	SendRelayNode(relayID, toNode string, payload []byte) error
	// SendRelayGroup 广播发布到 relay.group.{groupID}。
	SendRelayGroup(relayID, groupID string, payload []byte) error
}

// Sender 跨节点投递器。
//
// 优先 NATS 直达，未配 NATS 时退回中心服中转。两条路径都不可用时
// （natsRelay 与 centerRelay 均为 nil）Deliver 显式报错。
type Sender struct {
	natsRelay   NATSRelay
	centerRelay CenterRelay
	nodeID      string
	logger      *log.Logger
	// seq 中转 ID 序号（接收端按 relay_id 去重，保证唯一即可）。
	seq atomic.Int64
}

// NewSender 创建发送器。
//
// natsRelay 为 nil 时退化为纯 gRPC 中转（老行为）；centerRelay 为 nil 时
// 只走 NATS。两者都 nil 属于装配错误，Deliver 会显式失败。
func NewSender(natsRelay NATSRelay, centerRelay CenterRelay, nodeID string, logger *log.Logger) *Sender {
	return &Sender{natsRelay: natsRelay, centerRelay: centerRelay, nodeID: nodeID, logger: logger}
}

// Deliver 实现 actor.RemoteSender。
//
// 两步：消息 → 信封（actor.EncodeRemote）→ 传输层投递。
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

// send 按 NATS 优先、gRPC 降级的顺序投递。
//
// 降级只在 NATS **发布失败**时发生（未建连 / 重连窗口 Ready=false），
// 不会造成重复投递：发布成功即返回，失败时这条消息并未进入 NATS，
// 交给 gRPC 重发一条是安全的。两个 relay_id 相同，接收端幂等兜底。
func (s *Sender) send(relayID, toNode string, broadcast bool, groupID string, payload []byte) error {
	if s.natsRelay != nil {
		var err error
		if broadcast {
			err = s.natsRelay.SendRelayGroup(relayID, groupID, payload)
		} else {
			err = s.natsRelay.SendRelayNode(relayID, toNode, payload)
		}
		if err == nil {
			return nil
		}
		if s.logger != nil {
			s.logger.Warn("relay nats publish failed, falling back to center",
				"relay_id", relayID, "err", err)
		}
		// 落到下面的 gRPC 降级路径。
	}
	if s.centerRelay != nil {
		return s.centerRelay.SendRelay(relayID, toNode, broadcast, groupID, payload)
	}
	return fmt.Errorf("remote: no relay available (node %s): nats publish failed and no center relay", s.nodeID)
}
