// pkg/mq/kafka.go —— Kafka 生产者/消费者封装（segmentio/kafka-go）
// 可靠性约定见设计文档 13.2/13.4：
//   - Producer：Idempotent + acks=all + Hash 分区（同 Key 有序）+ Lz4 压缩
//   - Consumer：Group 消费，手动 commit；失败重试 N 次后写死信 Topic 并 commit（不阻塞分区）
package mq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/metrics"

	"github.com/segmentio/kafka-go"
)

// Config Kafka 配置。
type Config struct {
	Brokers       []string
	RequiredAcks  string // all / leader / none，默认 all
	ConsumerGroup string
	BatchSize     int
	BatchTimeout  time.Duration
	MaxRetries    int // 消费失败重试次数，默认 3
	// StartOffset 新消费组首次拉取的位点："first"（默认，全量重放）或 "last"（只消费新消息）。
	//
	// 广播类 topic（每实例独立 group）必须用 "last"：每个实例重启都会创建全新 group，
	// 若从 first 开始，会把历史消息全部重放一遍 —— 中转消息尤其致命（重复投递 + 内存放大）。
	StartOffset string
}

// Producer Kafka 生产者。
type Producer struct {
	writer  *kafka.Writer
	retry   *kafka.Writer // 死信写入
	maxRetry int
}

// NewProducer 创建生产者（幂等 + acks=all）。
func NewProducer(cfg Config) *Producer {
	acks := kafka.RequireAll
	switch cfg.RequiredAcks {
	case "leader":
		acks = kafka.RequireOne
	case "none":
		acks = kafka.RequireNone
	}
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = 100
	}
	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 10 * time.Millisecond
	}
	retries := cfg.MaxRetries
	if retries <= 0 {
		retries = 3
	}

	newWriter := func(topic string) *kafka.Writer {
		return &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // Key 相同落同一分区 → 同玩家/同订单有序
			RequiredAcks: acks,
			Compression:  kafka.Lz4,
			BatchSize:    batch,
			BatchTimeout: batchTimeout,
			Async:        false, // 同步写：WriteMessages 返回即已收到 ack
			// 注：segmentio/kafka-go v0.4 未提供幂等 Producer（Idempotent），
			// 这里以 acks=all + 同步写 + 消费端 biz_id 幂等（13.4）共同保证「不丢不重」。
		}
	}

	return &Producer{
		writer:   newWriter(""),
		retry:    newWriter(TopicFlushRetry),
		maxRetry: retries,
	}
}

// Produce 发送裸消息。Writer 未固定 Topic 时按消息自带 Topic 路由。
func (p *Producer) Produce(ctx context.Context, topic, key string, value []byte) error {
	if p.writer == nil {
		return fmt.Errorf("producer not initialized")
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	})
}

// ProduceEnvelope 发送信封（自动使用 env.Key 作为分区键）。
func (p *Producer) ProduceEnvelope(ctx context.Context, env *Envelope) error {
	value, err := env.Marshal()
	if err != nil {
		return err
	}
	return p.Produce(ctx, env.Topic, env.Key, value)
}

// DeadLetter 写死信 Topic（gs.flush.retry），保留原 topic/biz_id 便于补偿。
func (p *Producer) DeadLetter(ctx context.Context, env *Envelope, cause error) error {
	if p.retry == nil {
		return fmt.Errorf("dead letter writer not initialized")
	}
	headers := map[string]string{}
	for k, v := range env.Headers {
		headers[k] = v
	}
	headers["dlq_source_topic"] = env.Topic
	headers["dlq_cause"] = cause.Error()
	env.Headers = headers

	value, err := env.Marshal()
	if err != nil {
		return err
	}
	return p.retry.WriteMessages(ctx, kafka.Message{
		Key:   []byte(env.Key),
		Value: value,
	})
}

// Flush 等待缓冲区消息全部发送（优雅关闭屏障，18.2 防线④）。
// 注：Writer 为同步模式（Async=false），WriteMessages 返回即已 ack，此处无需额外 Flush。
func (p *Producer) Flush(ctx context.Context) error {
	_ = ctx
	return nil
}

// Close 关闭生产者。
func (p *Producer) Close() error {
	var errs []error
	if p.writer != nil {
		errs = append(errs, p.writer.Close())
	}
	if p.retry != nil {
		errs = append(errs, p.retry.Close())
	}
	return errors.Join(errs...)
}

// Handler 消费处理函数；返回 error 表示处理失败（触发重试）。
type Handler func(ctx context.Context, env *Envelope) error

// Consumer Kafka 消费者（Group 消费 + 手动 commit）。
type Consumer struct {
	reader   *kafka.Reader
	producer *Producer // 死信写入
	maxRetry int
	topic    string
}

// NewConsumer 创建消费者。
func NewConsumer(cfg Config, topic, groupID string, producer *Producer) *Consumer {
	retries := cfg.MaxRetries
	if retries <= 0 {
		retries = 3
	}
	startOffset := kafka.FirstOffset
	if cfg.StartOffset == "last" {
		startOffset = kafka.LastOffset
	}
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.Brokers,
			GroupID:        groupID,
			Topic:          topic,
			StartOffset:    startOffset,
			MinBytes:       1,
			MaxBytes:       10 << 20, // 10MB
			CommitInterval: 0,        // 关闭自动提交，由 Run 手动 commit
		}),
		producer: producer,
		maxRetry: retries,
		topic:    topic,
	}
}

// Run 阻塞消费直到 ctx 取消。
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	backoff := 100 * time.Millisecond
	consumed := 0
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch %s: %w", c.topic, err)
		}

		env, err := Unmarshal(msg.Value)
		if err != nil {
			// 非法消息直接 commit，避免卡分区
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		var lastErr error
		for attempt := 0; attempt <= c.maxRetry; attempt++ {
			lastErr = h(ctx, env)
			if lastErr == nil {
				break
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(backoff * time.Duration(attempt+1)):
			}
		}

		if lastErr != nil && c.producer != nil {
			dlqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.producer.DeadLetter(dlqCtx, env, lastErr)
			cancel()
		}

		// 指标（20.3）：ok / retry / dead 三态
		if metrics.Enabled() {
			switch {
			case lastErr == nil:
				metrics.KafkaConsumed(c.topic, "ok")
			case c.producer != nil:
				metrics.KafkaConsumed(c.topic, "dead")
			default:
				metrics.KafkaConsumed(c.topic, "retry")
			}
			// 消费 lag：Stats() 需加锁，每 256 条采样一次即可反映趋势
			if consumed++; consumed%256 == 1 {
				st := c.reader.Stats()
				metrics.KafkaLag(c.topic, st.Lag)
			}
		}

		// 无论成功还是进死信，都提交 offset：失败消息已进死信，不能阻塞分区
		if err := c.reader.CommitMessages(ctx, msg); err != nil && ctx.Err() == nil {
			return fmt.Errorf("commit %s: %w", c.topic, err)
		}
	}
}

// Close 关闭消费者（提交 offset 后退出）。
func (c *Consumer) Close() error {
	if c.reader == nil {
		return nil
	}
	return c.reader.Close()
}
