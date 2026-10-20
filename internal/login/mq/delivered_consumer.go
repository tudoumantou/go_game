// internal/login/mq/delivered_consumer.go —— 发货确认消费者（gs.recharge.delivered）
//
// 架构职责：充值链路的「回环闭环」——游戏服发货成功后往
// gs.recharge.delivered 发确认，本消费者把订单从「已支付」推进到
// 「已发货」并写发货流水。订单表在登录服库，发货动作在游戏服，
// 两侧靠这条 MQ 消息最终一致。
// 协作关系：游戏服是生产者；RechargeService.MarkDelivered 是状态
// 推进的执行者（CAS）；登录服 server.go 以单实例消费组启动本消费者。
// 设计取舍：① 幂等优先——MarkDelivered 用 CAS（已支付→已发货），
// 重复消息自然安全，因此消费失败可以放心重试/重投；② 单实例消费组
// （kafkaGroup）——订单表只在登录服库，多实例会互相踩状态；
// 宕机恢复靠 Kafka offset + 每小时对账补偿，不用消费组扩并发。
package mq

import (
	"context"
	"fmt"

	"game_study/internal/login/service"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// DeliveredConsumer 发货确认消费者：游戏服发货成功后更新订单状态（订单表在登录服库）。
// 只依赖 Consumer（收发）与 RechargeService（状态推进）：
// 消息语义到状态推进的映射全部在 handle 一个方法里，便于单测。
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
// 由 login server 的 Start 在独立 goroutine 中启动；ctx 取消
// （进程停机）时 Consumer.Run 返回，本函数随之返回。
func (c *DeliveredConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭消费者（提交 offset 后退出）。
// 停机顺序由 login.Shutdown 保证：先停消费（不再拉新消息、提交
// 已处理 offset）→ Flush 生产者 → 关连接。反序会丢「已处理未提交」
// 的消息导致重复发货（有幂等兜底但没必要）。
func (c *DeliveredConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 处理发货确认。
// 幂等：MarkDelivered 使用 CAS（已支付→已发货），重复消费自然安全。
// 错误处理契约：返回 error 即视为「本条未处理完」，由 Consumer 决定
// 重试或记 offset 跳过——解码失败（消息格式坏）重试无意义，
// 状态推进失败（DB 抖动）值得重试，两种错误在此不区分，
// 统一上抛，把策略留给 pkgmq.Consumer。
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
