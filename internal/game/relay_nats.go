// internal/game/relay_nats.go —— 通用 relay 的 NATS 接收侧
//
// 中心服退为业务中心后，业务服之间的消息不再经它中转。本文件订阅两条
// subject，把收到的 RelayPush 解封后投入本地 ActorSystem：
//
//	relay.node.{self}   点对点：别的服发给本节点某个 Actor
//	relay.group.{group} 广播：组内任一服发的组播
//
// 与 gRPC 降级路径共用 Server.onRelayPush（server_adapters.go），两条
// 路径的解码/投递语义完全一致：尽力而为 + 客户端重连拉全量兜底。
//
// 去重不在本层做：relay_id 是发送端的单调序号，NATS Core 是 at-most-once
// （不重投），同一条消息不会因传输重复。若将来换 JetStream（at-least-once），
// 在这一层按 (from_node, relay_id) 加去重即可，subject 约定不变。
package game

import (
	"game_study/pkg/natsx"
	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/protobuf/proto"
)

// startRelayNATS 订阅本节点的点对点 subject。
//
// 必须在 natsBus 建连成功后调用（server_start.go 的 NATS 装配段）。
// 广播 subject 由 subscribeRelayGroup 在收到 GroupUpdate 时补订阅。
func (s *Server) startRelayNATS(bus *natsx.Bus) {
	if bus == nil || s.cfg.Server.Name == "" {
		return
	}
	nodeSubject := natsx.RelayNodeSubject(s.cfg.Server.Name)
	if err := bus.Subscribe(nodeSubject,
		func() proto.Message { return &internalv2.RelayPush{} },
		func(msg proto.Message) error {
			push, ok := msg.(*internalv2.RelayPush)
			if !ok {
				return nil
			}
			s.onRelayPush(push)
			return nil
		}); err != nil {
		s.logger.Warn("subscribe relay node failed", "subject", nodeSubject, "err", err)
		return
	}
	s.logger.Info("relay nats subscribed", "node", nodeSubject)

	// 若分组已经先于 NATS 建连到达（GroupUpdate 走 gRPC 流，可能更早），
	// 这里补一次广播订阅。
	if s.relayGroupID != "" {
		s.subscribeRelayGroup(s.relayGroupID)
	}
}

// subscribeRelayGroup 订阅 relay.group.{groupID}。
//
// 由 OnGroupUpdate 在分组确定/变化时调用。未建 NATS 时跳过——建连后
// startRelayNATS 会补订。每个 groupID 只订阅一次：GroupUpdate 可能重复
// 下发同一分组（成员变化），重复订阅会让同一条广播被处理多次。
func (s *Server) subscribeRelayGroup(groupID string) {
	if groupID == "" || s.natsBus == nil {
		return
	}
	subject := natsx.RelayGroupSubject(groupID)
	// groupSubscriptions 去重：同一分组只订阅一次。
	if s.groupSubscriptions == nil {
		s.groupSubscriptions = make(map[string]struct{})
	}
	if _, ok := s.groupSubscriptions[groupID]; ok {
		return
	}
	if err := s.natsBus.Subscribe(subject,
		func() proto.Message { return &internalv2.RelayPush{} },
		func(msg proto.Message) error {
			push, ok := msg.(*internalv2.RelayPush)
			if !ok {
				return nil
			}
			s.onRelayPush(push)
			return nil
		}); err != nil {
		s.logger.Warn("subscribe relay group failed", "subject", subject, "err", err)
		return
	}
	s.groupSubscriptions[groupID] = struct{}{}
	s.logger.Info("relay group subscribed", "subject", subject)
}
