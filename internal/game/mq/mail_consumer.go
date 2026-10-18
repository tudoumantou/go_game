// internal/game/mq/mail_consumer.go —— 邮件投递消费者（gs.mail）
package mq

import (
	"context"
	"fmt"

	pkgmq "game_study/pkg/mq"
)

// MailConsumer 邮件投递消费者。
type MailConsumer struct {
	consumer *pkgmq.Consumer
	deps     Deps
}

// NewMailConsumer 创建消费者。
func NewMailConsumer(consumer *pkgmq.Consumer, deps Deps) *MailConsumer {
	return &MailConsumer{consumer: consumer, deps: deps}
}

// Run 阻塞消费。
func (c *MailConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭。
func (c *MailConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 邮件投递：
//  1. 幂等（mail_id）
//  2. 落库到「玩家所在服」的 mail 表（不要求玩家在线：邮件是离线可达的）
//  3. 玩家在线时后续由 mail 模块推送未读数（P6）
func (c *MailConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.MailMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode mail: %w", err)
	}
	if msg.PlayerID <= 0 || msg.MailID <= 0 {
		return fmt.Errorf("invalid mail payload")
	}

	// 本地性判定：玩家不在本服时，由 pend 暂存兜底（13.3 模式②）。
	node, online := c.deps.Presence.IsOnline(ctx, msg.PlayerID)
	if online && c.deps.NodeID != "" && node != c.deps.NodeID {
		// 玩家在其它服：暂存，登录 drain
		if err := c.deps.Presence.PendPush(ctx, "mail", msg.PlayerID, env.Payload, 200, 0); err != nil {
			c.deps.Logger.Warn("pend mail failed", "player_id", msg.PlayerID, "err", err)
		}
		return nil
	}

	first, err := c.deps.Deduper.FirstTime(ctx, pkgmq.TopicMail, fmt.Sprintf("%d", msg.MailID), pkgmq.DefaultIdemTTL)
	if err != nil {
		return err
	}
	if !first {
		return nil
	}

	if c.deps.Mail == nil {
		return fmt.Errorf("mail module not initialized")
	}
	mailID, err := c.deps.Mail.Deliver(ctx, &msg)
	if err != nil {
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicMail, fmt.Sprintf("%d", msg.MailID))
		return fmt.Errorf("deliver mail: %w", err)
	}
	c.deps.Logger.Info("mail delivered", "mail_id", msg.MailID, "db_id", mailID, "player_id", msg.PlayerID)
	return nil
}
