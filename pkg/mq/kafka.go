// pkg/mq/kafka.go —— Kafka 生产者/消费者封装（segmentio/kafka-go）
// P1-P4 仅保留接口占位，P5 实现完整逻辑。
package mq

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// Config Kafka 配置。
type Config struct {
	Brokers []string
}

// Producer 生产者占位。
type Producer struct {
	writer *kafka.Writer
}

// NewProducer 创建生产者。
func NewProducer(cfg Config) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(cfg.Brokers...),
			Balancer: &kafka.LeastBytes{},
		},
	}
}

// Produce 异步发送消息（P4 不启用）。
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	if p.writer == nil {
		return fmt.Errorf("producer not initialized")
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

// Close 关闭生产者。
func (p *Producer) Close() error {
	if p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// Consumer 消费者占位。
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer 创建消费者。
func NewConsumer(cfg Config, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.Brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

// Close 关闭消费者。
func (c *Consumer) Close() error {
	if c.reader == nil {
		return nil
	}
	return c.reader.Close()
}
