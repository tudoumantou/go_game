// internal/game/relay_nats.go —— 通用 relay 的 NATS 接收侧
//
// 中心服是纯业务中心，业务服之间的消息不经它中转。本文件订阅两条
// subject，把收到的 RelayPush 解封后投入本地 ActorSystem：
//
//	relay.node.{self}   点对点：别的服发给本节点某个 Actor
//	relay.group.{group} 广播：组内任一服发的组播
//
// 投递语义是「尽力而为 + 对账兜底」：玩家下线时 DeliverRemote 返回
// ErrActorGone，记日志即可——客户端重连会拉全量权威状态。
//
// 去重不在本层做：relay_id 是发送端的单调序号，NATS Core 是 at-most-once
// （不重投），同一条消息不会因传输重复。若将来换 JetStream（at-least-once），
// 在这一层按 (from_node, relay_id) 加去重即可，subject 约定不变。
package game

import (
	"game_study/pkg/actor"
	"game_study/pkg/natsx"
	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/protobuf/proto"
)

// startRelayNATS 订阅本节点的点对点 subject，并尽量订阅广播 subject。
//
// 必须在 natsBus 建连成功后调用（server_start.go 的 NATS 装配段）。
// 广播的 group 来源优先级：**配置 `server.group` > 中心服 GroupUpdate 下发**。
// 配置了就启动即订阅，不等分组消息——这样中心服不可用时，新起的游戏服
// 仍然能收发组内广播（否则"广播能力"会隐式依赖中心服的 gRPC 流在线）。
func (s *Server) startRelayNATS(bus *natsx.Bus) {
	if bus == nil || s.cfg.Server.Name == "" {
		return
	}
	nodeSubject := bus.RelayNodeSubject(s.cfg.Server.Name)
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

	// 配置了 server.group 就启动即订阅；未配置时留空，等 GroupUpdate 下发。
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
	subject := s.natsBus.RelayGroupSubject(groupID)
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

// onRelayPush 是 relay 接收侧的唯一实现。payload 是 actor.RemoteEnvelope
// （JSON）：解出目标 ActorID 与消息后投入本地 ActorSystem。语义是
// 「尽力而为 + 对账兜底」——玩家下线时 DeliverRemote 返回 ErrActorGone，
// 记日志即可：客户端重连会拉全量权威状态。
func (s *Server) onRelayPush(push *internalv2.RelayPush) {
	if push == nil {
		return
	}
	actorID, msg, err := actor.DecodeRemoteEnvelope(push.GetPayload())
	if err != nil {
		s.logger.Warn("relay push decode failed",
			"from", push.GetFromNode(), "relay_id", push.GetRelayId(), "err", err)
		return
	}
	if err := s.actorSys.DeliverRemote(actorID, msg); err != nil {
		s.logger.Debug("relay push deliver failed",
			"from", push.GetFromNode(), "actor", actorID, "err", err)
		return
	}
	s.logger.Debug("relay push delivered",
		"from", push.GetFromNode(), "actor", actorID, "relay_id", push.GetRelayId())
}
