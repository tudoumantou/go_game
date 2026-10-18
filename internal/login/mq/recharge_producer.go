// internal/login/mq/recharge_producer.go —— 充值订单生产者（gs.recharge.order）
package mq

import (
	"context"
	"fmt"
	"time"

	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
)

// RechargeProducer 充值订单生产者。
type RechargeProducer struct {
	producer *pkgmq.Producer
}

// NewRechargeProducer 创建生产者。
func NewRechargeProducer(p *pkgmq.Producer) *RechargeProducer {
	return &RechargeProducer{producer: p}
}

// 编译期断言：实现 service.OrderProducer。
var _ interface {
	ProduceOrder(ctx context.Context, order *model.RechargeOrder) error
} = (*RechargeProducer)(nil)

// ProduceOrder 发送订单消息（Key=order_id，保证同订单分区有序）。
func (p *RechargeProducer) ProduceOrder(ctx context.Context, order *model.RechargeOrder) error {
	if p.producer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}
	msg := pkgmq.RechargeOrderMsg{
		OrderID:   order.OrderId,
		PlayerID:  order.PlayerId,
		ProductID: order.ProductId,
		AmountFen: order.AmountFen,
		Channel:   order.Channel,
	}
	env, err := pkgmq.NewEnvelope(order.OrderId, pkgmq.TopicRechargeOrder, order.OrderId, msg)
	if err != nil {
		return err
	}
	env.Headers = map[string]string{"trace_id": fmt.Sprintf("recharge-%d", time.Now().UnixNano())}
	return p.producer.ProduceEnvelope(ctx, env)
}
