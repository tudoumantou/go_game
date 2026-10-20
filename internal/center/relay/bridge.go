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
//
// 与 relay.go（同实例内存路由）的分工：relay.go 管「一跳可达」的目标，
// bridge.go 管「目标在别的实例」的场景；两者由 server.OnRelayRequest
// 按目标归属二选一，互不重叠。
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
// 由 server.deliverLocal 实现——它只投本地注册表里的节点，
// 投不出去的返回 0 而不是错误。
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
	//
	// 注意消费组名 = Topic + "." + InstanceID：每个实例一个独立组，
	// 同一条消息被所有组各收一份——这正是「广播给所有实例、各自投本地」的
	// 实现基础；代价是实例数 × 消息量的放大，实例规模必须受控。
	StartOffset string
}

// Bridge 跨实例中转桥。
//
// 四个计数器用 atomic.Int64 而非 mutex+int：它们被 Forward（写路径）
// 与消费回调（读路径）并发触碰，且只增不减——atomic 的 RMW 语义正好，
// 还省去一把锁。注意 Go 1.19+ 的 atomic.Int64 是结构体，不可拷贝，
// Bridge 必须指针使用（本包全部方法都是指针接收者）。
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
//
// 返回 nil 而不是 error：未配 Kafka 是合法的部署形态（单实例中心服），
// 调用方用 Enabled() 判断走不走跨实例路径，不需要把「没配」当故障处理。
// 过滤空串是必须的：${VAR} 未设置时展开成 [""]，不过滤会让 Kafka client
// 拿着空 broker 地址重试到超时。
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
// nil 接收者返回错误而非 panic：server 侧对 bridge 可空的调用点很多，
// 统一用「 Enabled 检查 + nil 安全方法」双保险，漏检查时至少不崩。
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
// nil 接收者返回 nil：未启用时 server 直接不调用，但防御性 nil 检查
// 让「忘了判断 Enabled」也安全。整个处理函数永不返回 error——
// 见下面各分支的注释，中转链路刻意不重试。
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
			// 返回 nil（不返回 error）——消费框架不会重投，坏消息自然跳过
			b.logger.Warn("bad relay message", "relay_id", env.BizID, "err", err)
			return nil
		}

		// 自发自收：本实例已经把消息投给本地目标了，跳过
		// OriginInstance 由 Forward 打上，是整个「不重复投递」约定的核心
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
			// 这里正是「广播语义」的关键：每个实例都收到全部中转消息，
			// 但只投递本地节点，合起来恰好每个目标收一次
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
// 先 consumer 后 producer：关闭顺序与启动相反；nil 检查让未启用的
// bridge 也能安全调 Close（server 关闭路径统一调用，不逐个判空）。
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
// 四个计数器的语义：forwarded=本实例发出的、received=本实例收到的、
// delivered=收到后投递成功的节点数、skipped=跳过数（自发自收 + 非本地目标）。
// 健康画像：delivered 长期为 0 说明本实例总在替别人做嫁衣（分组不均）。
func (b *Bridge) Stats() (forwarded, received, delivered, skipped int64) {
	if b == nil {
		return 0, 0, 0, 0
	}
	return b.forwarded.Load(), b.received.Load(), b.delivered.Load(), b.skipped.Load()
}

// Enabled 是否启用跨实例中转。
// 用 nil 接收者判断（b != nil）：NewBridge 在无 Kafka 配置时返回 nil，
// 一个方法同时表达「未启用」与「已启用」，省一个 bool 字段。
func (b *Bridge) Enabled() bool { return b != nil }

// relayKey 分区键：点对点按目标节点，广播按分组 —— 同类消息落同一分区，保证有序。
//
// 同一 key 必落同一分区（Kafka 按 key hash 选分区）：中转消息的顺序性
// 完全依赖这一点——先到先得的邀请/后到先得的解散，乱序会出逻辑事故。
func relayKey(p Payload) string {
	if p.Broadcast {
		if p.GroupID != "" {
			return "group:" + p.GroupID
		}
		return "broadcast"
	}
	return "node:" + p.ToNode
}
