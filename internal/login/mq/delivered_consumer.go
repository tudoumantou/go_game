// internal/login/mq/delivered_consumer.go —— 发货确认消费者（gs.recharge.delivered）
package mq

import (
	"context"
	"fmt"

	"game_study/internal/login/service"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// DeliveredConsumer 发货确认消费者：游戏服发货成功后更新订单状态（订单表在登录服库）。
type DeliveredConsumer struct {
	consumer *pkgmq.Consumer
	svc      *service.RechargeService
	logger   *log.Logger
}

// NewDeliveredConsumer 创建消费者。
func NewDeliveredConsumer(consumer *pkgmq.Consumer, svc *service.RechargeService, logger *log.Logger) *DeliveredConsumer {
	return &DeliveredConsumer{consumer: consumer, svc: svc, logger: logger}
}

// Run 阻塞消费直到 ctx 取消。
func (c *DeliveredConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭消费者（提交 offset 后退出）。
func (c *DeliveredConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 处理发货确认。
// 幂等：MarkDelivered 使用 CAS（已支付→已发货），重复消费自然安全。
func (c *DeliveredConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.RechargeDeliveredMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode delivered: %w", err)
	}
	if err := c.svc.MarkDelivered(ctx, msg.OrderID); err != nil {
		return fmt.Errorf("mark delivered %s: %w", msg.OrderID, err)
	}
	c.logger.Info("order delivered", "order_id", msg.OrderID, "player_id", msg.PlayerID)
	return nil
}
