// Package mq Kafka 封装。
//
// 能力：统一信封与 biz_id 幂等（message.go）、生产者、消费者（手动 commit，死信成功才提交）、
// topic 常量集中定义。
//
// 架构职责：为 login/center/game 三侧进程提供唯一的 Kafka 出入口——信封格式、
// 幂等协议、提交语义全部收敛在本包，业务只面对 Envelope 与 Handler。
// 协作关系：生产者侧（login 服支付回调、game 服发货确认）调 Producer；
// 消费侧（game 服三个消费者 + center 服中转）经 Consumer.Run 驱动；
// 幂等键读写走 Redis 实现（internal 注入 Deduper）。
// 设计取舍：
//   - 统一信封而非裸 payload：biz_id/topic/headers 让跨服务链路可追溯
//     （一条充值消息从登录服到游戏服全程可查），代价是每包多约 100B。
//   - 手动 commit + 死信成功才提交：把「不丢不重」的最终保证放在消费端，
//     而非依赖 Kafka 的 at-most-once 自动提交（见 kafka.go 的提交规则）。
package mq
