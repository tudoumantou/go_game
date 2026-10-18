// Package relay 的中转桥（设计文档 9.3「中心服集群下的跨实例中转」）
//
// 问题：中心服多实例部署时，节点 A 连 #1、节点 B 连 #2。
// #1 的 gRPC 流上只有 A，#1 无法直接把消息推给 B。
//
// 方案（9.3）：把 RelayRequest 写进 Kafka `center.relay`，每个中心服实例都用
// **独立的消费组**（`center.relay.{instanceID}`，广播语义）消费，只投递自己承载的节点。
// Key 用目标 node_id（点对点）或 group_id（广播），保证同一目标的消息有序。
//
// 可靠性边界（与 9.3 一致，中转本身是尽力而为）：
//   - 不持久化、不重放：消费组首次订阅从 last 开始，实例重启期间的消息不补；
//   - 不重试跨实例投递：Kafka 已 ack，本地投递失败由业务层幂等/重查兜底；
//   - 丢消息可接受，重复消息不可接受 → 靠 OriginInstance 跳过自发自收，
//     靠「只投递自己承载的节点」避免多实例重复推送。
package relay

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"game_study/pkg/log"
	"game_study/pkg/metrics"
	pkgmq "game_study/pkg/mq"
)

// DeliverFunc 在本实例内投递一条跨实例中转消息，返回实际投递成功的节点数。
//
// 返回 0 是正常情况（目标节点不在本实例上，由另一个实例负责）。
type DeliverFunc func(p Payload) int

// BridgeConfig 中转桥配置。
type BridgeConfig struct {
	Brokers    []string
	Topic      string        // 空则用 mq.TopicCenterRelay
	InstanceID string        // 本实例唯一标识（消费组后缀 + 自发自收去重）
	Members    int           // Kafka 集群 broker 数（仅日志）
	RetryGap   time.Duration // 保留字段，跨实例不重试

	// StartOffset "last"（默认，只消费上线后的消息）或 "first"（从头消费）。
	//
	// 生产必须用 last：实例重启会创建新消费组，从 first 会把历史中转全部重放一遍。
	// 测试用 first 是为了避开「消费组初始化晚于消息写入」的竞态。
	StartOffset string
}

// Bridge 跨实例中转桥。
type Bridge struct {
	cfg      BridgeConfig
	producer *pkgmq.Producer
	consumer *pkgmq.Consumer
	deliver  DeliverFunc
	logger   *log.Logger

	forwarded atomic.Int64 // 写入 Kafka 的消息数
	received  atomic.Int64 // 从 Kafka 收到的消息数
	delivered atomic.Int64 // 收到后成功投递的节点数
	skipped   atomic.Int64 // 跳过（自发自收 / 无本地目标）
}

// NewBridge 创建中转桥（未配置 brokers 时返回 nil，调用方按「单实例模式」处理）。
func NewBridge(cfg BridgeConfig, deliver DeliverFunc, logger *log.Logger) *Bridge {
	// 过滤空串：配置里写 ${KAFKA_BROKERS} 而环境变量未设置时，展开结果是 [""]
	brokers := make([]string, 0, len(cfg.Brokers))
	for _, b := range cfg.Brokers {
		if b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 || cfg.InstanceID == "" {
		return nil
	}
	cfg.Brokers = brokers
	if cfg.Topic == "" {
		cfg.Topic = pkgmq.TopicCenterRelay
	}

	startOffset := cfg.StartOffset
	if startOffset == "" {
		// 中转消息不重放：实例重启期间的消息丢了就丢了（9.3 尽力而为）
		startOffset = "last"
	}
	mqCfg := pkgmq.Config{
		Brokers:     cfg.Brokers,
		StartOffset: startOffset,
	}

	return &Bridge{
		cfg:      cfg,
		producer: pkgmq.NewProducer(mqCfg),
		consumer: pkgmq.NewConsumer(mqCfg, cfg.Topic, cfg.Topic+"."+cfg.InstanceID, nil),
		deliver:  deliver,
		logger:   logger,
	}
}

// Forward 把一条中转请求写入 Kafka，由承载目标节点的实例消费后投递。
func (b *Bridge) Forward(ctx context.Context, p Payload) error {
	if b == nil {
		return fmt.Errorf("relay bridge not configured")
	}

	msg := &pkgmq.RelayCrossMsg{
		RelayID:        p.RelayID,
		OriginInstance: b.cfg.InstanceID,
		FromNode:       p.FromNode,
		ToNode:         p.ToNode,
		Broadcast:      p.Broadcast,
		GroupID:        p.GroupID,
		Payload:        p.Body,
		CreatedAt:      time.Now().UnixMilli(),
	}
	env, err := pkgmq.NewEnvelope(p.RelayID, b.cfg.Topic, relayKey(p), msg)
	if err != nil {
		return fmt.Errorf("build relay envelope: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := b.producer.ProduceEnvelope(writeCtx, env); err != nil {
		return fmt.Errorf("produce cross relay: %w", err)
	}

	b.forwarded.Add(1)
	if metrics.Enabled() {
		metrics.KafkaProduced(b.cfg.Topic, true)
	}
	return nil
}

// Run 阻塞消费跨实例中转消息，直到 ctx 取消。
func (b *Bridge) Run(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.logger.Info("relay bridge started",
		"topic", b.cfg.Topic, "instance", b.cfg.InstanceID)

	return b.consumer.Run(ctx, func(ctx context.Context, env *pkgmq.Envelope) error {
		b.received.Add(1)

		var msg pkgmq.RelayCrossMsg
		if err := env.DecodePayload(&msg); err != nil {
			// 非法消息直接丢弃：中转不能因为一条坏消息阻塞分区
			b.logger.Warn("bad relay message", "relay_id", env.BizID, "err", err)
			return nil
		}

		// 自发自收：本实例已经把消息投给本地目标了，跳过
		if msg.OriginInstance == b.cfg.InstanceID {
			b.skipped.Add(1)
			return nil
		}

		n := b.deliver(Payload{
			RelayID:   msg.RelayID,
			FromNode:  msg.FromNode,
			ToNode:    msg.ToNode,
			Broadcast: msg.Broadcast,
			GroupID:   msg.GroupID,
			Body:      msg.Payload,
		})
		if n == 0 {
			// 目标节点不归本实例管 —— 正常情况，另一个实例会处理
			b.skipped.Add(1)
			if metrics.Enabled() {
				metrics.KafkaConsumed(b.cfg.Topic, "skip")
			}
			return nil
		}

		b.delivered.Add(int64(n))
		if metrics.Enabled() {
			metrics.KafkaConsumed(b.cfg.Topic, "ok")
		}
		b.logger.Debug("cross-instance relay delivered",
			"relay_id", msg.RelayID, "from", msg.FromNode,
			"to", msg.ToNode, "broadcast", msg.Broadcast, "nodes", n)
		return nil
	})
}

// Close 释放 Kafka 资源。
func (b *Bridge) Close() {
	if b == nil {
		return
	}
	if b.consumer != nil {
		_ = b.consumer.Close()
	}
	if b.producer != nil {
		_ = b.producer.Close()
	}
}

// Stats 中转桥统计（指标/诊断用）。
func (b *Bridge) Stats() (forwarded, received, delivered, skipped int64) {
	if b == nil {
		return 0, 0, 0, 0
	}
	return b.forwarded.Load(), b.received.Load(), b.delivered.Load(), b.skipped.Load()
}

// Enabled 是否启用跨实例中转。
func (b *Bridge) Enabled() bool { return b != nil }

// relayKey 分区键：点对点按目标节点，广播按分组 —— 同类消息落同一分区，保证有序。
func relayKey(p Payload) string {
	if p.Broadcast {
		if p.GroupID != "" {
			return "group:" + p.GroupID
		}
		return "broadcast"
	}
	return "node:" + p.ToNode
}
