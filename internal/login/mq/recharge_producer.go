// internal/login/mq/recharge_producer.go —— 充值订单生产者（gs.recharge.order）
//
// 架构职责：把「已支付」订单事件投递到 Kafka topic gs.recharge.order，
// 触发游戏服发货。这是支付主链路「登录服记事实 → MQ 传事实 →
// 游戏服执行动作」分工中的传递环节。
// 协作关系：RechargeService.PayCallback/ReplayPaidUndelivered 经
// OrderProducer 接口调用本实现；对端是游戏服的订单消费者；
// Topic/消息结构定义在 pkg/mq。
// 设计取舍：① Kafka Key = order_id——同一订单的所有消息落同一分区，
// 分区内有序保证「支付→重投补偿」不会乱序执行；② biz_id = order_id
// 让消息天然可按业务单号去重/排查；③ trace_id 用时间戳纳米值，
// 跨服务日志串联时按它 grep 全链路。④ 未配 Kafka 时上层注入
// NopOrderProducer——订单仍落库，靠每小时对账补偿投递，
// 支付链路不因 MQ 缺失而断（见 recharge_service.go）。
package mq

import (
	"context"
	"fmt"
	"time"

	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
)

// RechargeProducer 充值订单生产者。
// 薄封装：只做「领域模型 → 消息信封」的转换，无状态、可并发使用
// （底层 pkgmq.Producer 自身保证 Send 的并发安全）。
type RechargeProducer struct {
	producer *pkgmq.Producer
}

// NewRechargeProducer 创建生产者。
func NewRechargeProducer(p *pkgmq.Producer) *RechargeProducer {
	return &RechargeProducer{producer: p}
}

// 编译期断言：实现 service.OrderProducer。
// 知识点：`var _ Interface = (*T)(nil)` 是零成本的接口实现检查——
// 不满足时编译失败，错误信息直接指到此行；比运行期反射断言更早、
// 更省。放在文件级 var 处，读文件即知「本类型扮演什么角色」。
var _ interface {
	ProduceOrder(ctx context.Context, order *model.RechargeOrder) error
} = (*RechargeProducer)(nil)

// ProduceOrder 发送订单消息（Key=order_id，保证同订单分区有序）。
// producer 为 nil（未配 Kafka 却错误地注入了本实现）时报错而非
// 静默丢弃——支付链路丢消息 = 玩家付了钱没到货，必须响。
func (p *RechargeProducer) ProduceOrder(ctx context.Context, order *model.RechargeOrder) error {
	if p.producer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}
	// DTO 转换：只带发货必需字段，不带订单全量——消息契约最小化，
	// 下游改字段不影响本生产端
	msg := pkgmq.RechargeOrderMsg{
		OrderID:   order.OrderId,
		PlayerID:  order.PlayerId,
		ProductID: order.ProductId,
		AmountFen: order.AmountFen,
		Channel:   order.Channel,
	}
	// Key 与 biz_id 同为 order_id：分区有序 + 业务幂等一举两得
	env, err := pkgmq.NewEnvelope(order.OrderId, pkgmq.TopicRechargeOrder, order.OrderId, msg)
	if err != nil {
		return err
	}
	// Headers 透传链路追踪 id：对账/排障时按 trace_id 串起
	// 「下单 HTTP → 支付回调 → 发货」全链路日志
	env.Headers = map[string]string{"trace_id": fmt.Sprintf("recharge-%d", time.Now().UnixNano())}
	return p.producer.ProduceEnvelope(ctx, env)
}
