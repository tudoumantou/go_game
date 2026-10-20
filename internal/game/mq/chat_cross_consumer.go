// internal/game/mq/chat_cross_consumer.go —— 跨服私聊消费者（gs.chat.cross，17.3）
//
// 架构职责：消费跨服私聊消息，完成「本地性判定 → 幂等 → 落库 + 推送」。
//
// 协作关系：消息由发送方所在节点的 chat.Messenger.ProduceCross 生产
// （server.go）；本消费者与充值/邮件消费者并列，由 group.go 统一装配。
//
// 设计取舍：
//   - 广播消费（每实例独立 group）而不是集群共享 group：私聊的接收方
//     分散在各节点，只有每个节点都收到全量、自己挑本地的，才能把消息
//     推到正确的玩家——共享 group 下消息只会落在一个节点，其余节点
//     的接收方永远收不到；
//   - 幂等用 msg_id + Redis SETNX（FirstTime）：跨服链路（生产方重投 /
//     消费者重试）可能重复投递同一消息，SETNX 保证只有第一票算数；
//   - DeliverInbound 失败要 Forget 撤销幂等标记：否则重试时被自己的
//     去重标记挡住，消息永远补不进来——「先落地后标记」的顺序不能反。
package mq

import (
	"context"
	"fmt"

	pkgmq "game_study/pkg/mq"
)

// ChatConsumer 跨服私聊消费者（广播消费 + 本地性判定，17.3）。
type ChatConsumer struct {
	consumer *pkgmq.Consumer
	deps     Deps
}

// NewChatConsumer 创建消费者。
func NewChatConsumer(consumer *pkgmq.Consumer, deps Deps) *ChatConsumer {
	return &ChatConsumer{consumer: consumer, deps: deps}
}

// Run 阻塞消费。
func (c *ChatConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭。
func (c *ChatConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 跨服私聊投递：
//  1. 本地性判定：接收方不在本服 → 丢弃（由发送方 pend 兜底）
//  2. 幂等（msg_id）
//  3. 落库 chat_record（direction=in，uk_owner_msg 双保险）
//
// 返回 nil 表示「消息处理完毕」（含主动丢弃）；返回 error 才会触发
// Kafka 重投——只有「处理失败」才配重试，「不是我的消息」不配。
func (c *ChatConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.ChatCrossMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode chat: %w", err)
	}
	if msg.MsgID <= 0 || msg.ToID <= 0 || msg.FromID <= 0 {
		return fmt.Errorf("invalid chat payload")
	}

	// 本地性判定：presence 说接收方在别的节点（或不在线）时直接丢弃。
	// 不在线不是错误——发送方侧已做 PendOffline 暂存（见 chatMessenger）。
	node, online := c.deps.Presence.IsOnline(ctx, msg.ToID)
	if !online || (c.deps.NodeID != "" && node != c.deps.NodeID) {
		c.deps.Logger.Debug("chat skip (receiver not on this node)", "msg_id", msg.MsgID, "to", msg.ToID)
		return nil
	}

	// 幂等：FirstTime 内部是 Redis SETNX + TTL；err 走重试路径，
	// !first 是「别人已处理过」，按完成处理
	first, err := c.deps.Deduper.FirstTime(ctx, pkgmq.TopicChatCross, fmt.Sprintf("%d", msg.MsgID), pkgmq.DefaultIdemTTL)
	if err != nil {
		return err
	}
	if !first {
		return nil
	}

	if c.deps.Chat == nil {
		return fmt.Errorf("chat module not initialized")
	}
	if err := c.deps.Chat.DeliverInbound(ctx, &msg); err != nil {
		// 撤销幂等标记：重试消息必须还能进来，否则一次失败 = 永久丢失
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicChatCross, fmt.Sprintf("%d", msg.MsgID))
		return fmt.Errorf("deliver chat: %w", err)
	}
	c.deps.Logger.Debug("chat delivered", "msg_id", msg.MsgID, "to", msg.ToID)
	return nil
}
