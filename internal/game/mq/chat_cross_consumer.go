// internal/game/mq/chat_cross_consumer.go —— 跨服私聊消费者（gs.chat.cross）
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
func (c *ChatConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.ChatCrossMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode chat: %w", err)
	}
	if msg.MsgID <= 0 || msg.ToID <= 0 || msg.FromID <= 0 {
		return fmt.Errorf("invalid chat payload")
	}

	node, online := c.deps.Presence.IsOnline(ctx, msg.ToID)
	if !online || (c.deps.NodeID != "" && node != c.deps.NodeID) {
		c.deps.Logger.Debug("chat skip (receiver not on this node)", "msg_id", msg.MsgID, "to", msg.ToID)
		return nil
	}

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
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicChatCross, fmt.Sprintf("%d", msg.MsgID))
		return fmt.Errorf("deliver chat: %w", err)
	}
	c.deps.Logger.Debug("chat delivered", "msg_id", msg.MsgID, "to", msg.ToID)
	return nil
}
