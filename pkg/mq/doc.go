// Package mq Kafka 封装。
//
// 能力：统一信封与 biz_id 幂等（message.go）、生产者、消费者（手动 commit，死信成功才提交）、
// topic 常量集中定义。
package mq
