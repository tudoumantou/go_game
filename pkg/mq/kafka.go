// pkg/mq/kafka.go —— Kafka 生产者/消费者封装（segmentio/kafka-go）
// 可靠性约定见设计文档 13.2/13.4：
//   - Producer：Idempotent + acks=all + Hash 分区（同 Key 有序）+ Lz4 压缩
//   - Consumer：Group 消费，手动 commit；失败重试 N 次后写死信 Topic 并 commit（不阻塞分区）
//
// 架构职责：屏蔽 segmentio/kafka-go 的 API 细节，对上提供 Producer/Consumer
// 两个类型，统一攒批、重试、死信、commit 与指标埋点。
// 协作关系：生产者被 login 服（订单）、game 服（发货确认）持有；
// 消费者被 game 服三个业务消费者（登录日志/邮件/充值）经 Group 装配；
// 死信 topic（gs.flush.retry）由补偿任务另行消费。
// 设计取舍：
//   - 同步写（Async=false）而非异步：WriteMessages 返回即已拿到 ack，
//     调用方（如充值确认）可以直接把「发送成功」当事实；异步的吞吐优势
//     在本项目量级（每秒几百~几千条）不值得用「丢消息风险」换。
//   - 手动 commit：自动提交会在「消息已取、业务未做完」时推进 offset，
//     进程崩溃即丢消息（at-most-once）；手动提交把决策权放到业务完成后。
//   - 消费端幂等兜底：kafka-go 未提供幂等 Producer，重复发送由
//     idempotent.go 的 biz_id 去重消除， producer 侧重试不造成重复应用。
package mq

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/metrics"

	"github.com/segmentio/kafka-go"
)

// Config Kafka 配置。
//
// 字段语义：
//   - Brokers：broker 地址列表（host:port），生产环境至少配两个防单点；
//   - ConsumerGroup：同服务多实例共用一个 group 分摊分区；广播类 topic
//     （如 center.relay）则每实例独立 group，重启即全新 group；
//   - BatchSize/BatchTimeout：生产者攒批条数与最长等待（默认 100 条 / 10ms）；
//   - MaxRetries：消费失败重试次数（默认 3）；
//   - RequiredAcks：写入确认级别（默认 all）。
//   - StartOffset：新消费组首次拉取的位点（见字段注释）。
//
// 零值安全性：本包所有入口（NewProducer/NewConsumer）对非正数配置做兜底，
// 因为 kafka-go 对 BatchSize=0 等值是「不攒批立即发」的另一套语义，
// 直接透传会让「忘了配」和「故意关攒批」无法区分。
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
	// 语义来自 Kafka FirstOffset/LastOffset：只对「没有已提交 offset 的新 group」生效，
	// 老 group 重启仍从已提交位点继续，与这两个值无关。
	StartOffset string
}

// Producer Kafka 生产者。
//
// writer 发业务消息（Topic 留空、按消息自带 Topic 路由）；retry 专发死信 topic；
// maxRetry 是归一化后的重试配置，仅随结构传递，同步发送路径本身不重试。
// 两个 Writer 分离的原因：死信与业务消息混用同一个 writer 时，死信写入
// 失败会难以归因；分开后指标/告警可按 topic 维度分别观测。
type Producer struct {
	writer   *kafka.Writer
	retry    *kafka.Writer // 死信写入
	maxRetry int
}

// NewProducer 创建生产者（幂等 + acks=all）。
// 归一化在这里一次完成，此后所有路径直接用安全值。
func NewProducer(cfg Config) *Producer {
	// acks 语义：all=等 ISR 全部副本确认（最安全，延迟最高）；
	// leader=只等 leader（折衷）；none=不等（最快，可能丢）。
	acks := kafka.RequireAll
	switch cfg.RequiredAcks {
	case "leader":
		acks = kafka.RequireOne
	case "none":
		acks = kafka.RequireNone
	}
	// 四项参数统一兜底：0/负值传进 kafka.Writer 会造成不攒批、
	// 无限重试等未定义行为，在入口收敛成安全默认值。
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

	// 闭包工厂：业务 writer 与死信 writer 仅 Topic 不同，其余参数必须一致
	// （压缩/acks/均衡器），抽出来防止两边配置漂移。
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
// writer==nil 的判空服务于「零值 Producer 直接用」的误用场景，返回错误而非 panic。
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
// 经 env.Marshal 发出：biz_id 缺失在此被拦截，非法信封根本进不了 Kafka。
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
	// 注意：本函数会就地改写 env.Headers 追加死信元数据，
	// 调用方不得再把同一个 env 投向业务 topic。
	headers := map[string]string{}
	// 拷贝而非直接改 env.Headers：调用方可能持有同一个 map 引用
	// （如从消息里解出来的信封），就地写会污染原对象。
	for k, v := range env.Headers {
		headers[k] = v
	}
	// dlq_ 前缀与业务头隔离，避免键冲突；补偿任务按
	// dlq_source_topic 路由、按 dlq_cause 定位失败原因。
	headers["dlq_source_topic"] = env.Topic
	headers["dlq_cause"] = cause.Error()
	env.Headers = headers

	value, err := env.Marshal()
	if err != nil {
		return err
	}
	// 保留 env.Key 作分区键：同一业务键的死信仍落同一分区，补偿时有序可依。
	return p.retry.WriteMessages(ctx, kafka.Message{
		Key:   []byte(env.Key),
		Value: value,
	})
}

// Flush 等待缓冲区消息全部发送（优雅关闭屏障，18.2 防线④）。
// 注：Writer 为同步模式（Async=false），WriteMessages 返回即已 ack，此处无需额外 Flush。
// 保留该方法是为了 shutdown 编排的调用形状统一（每个可刷组件都有 Flush）。
func (p *Producer) Flush(ctx context.Context) error {
	_ = ctx
	return nil
}

// Close 关闭生产者。
// errors.Join（Go 1.20）聚合两个 writer 的关闭错误：只要有一个失败，
// 返回值就非 nil，调用方（shutdown）能如实看到；不丢第一个错误。
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
// 契约：handler 必须可安全地重复调用（幂等由业务层用 biz_id 保证），
// 因为 at-least-once 语义下同一条消息可能被投递多次。
type Handler func(ctx context.Context, env *Envelope) error

// messageReader 是 Consumer 对 kafka.Reader 的最小依赖，抽出来便于注入故障测试。
// 窄接口（只 4 个方法）让测试替身几百行即可覆盖 FetchMessage 报错、
// CommitMessages 失败等故障路径，不必起真 broker。
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
	Close() error
}

// deadLetterWriter 是死信写入的最小依赖。
type deadLetterWriter interface {
	DeadLetter(ctx context.Context, env *Envelope, cause error) error
}

// Consumer Kafka 消费者（Group 消费 + 手动 commit）。
//
// reader/producer 抽成窄接口（messageReader/deadLetterWriter）只为测试可注入故障；
// topic 字段仅用于错误信息与指标标签，不参与路由。
// 线程契约：Run 单 goroutine 串行调用，Close 可在另一 goroutine 触发
// （关服时经 ctx 取消让 FetchMessage 返回）。
type Consumer struct {
	reader   messageReader
	producer deadLetterWriter // 死信写入
	maxRetry int
	topic    string
}

// NewConsumer 创建消费者。
// groupID 决定消费语义：共享 group = 分区级负载均衡（登录流水）；
// 每实例独立 group = 广播（每实例全量，见 group.go 的组装）。
func NewConsumer(cfg Config, topic, groupID string, producer *Producer) *Consumer {
	retries := cfg.MaxRetries
	if retries <= 0 {
		retries = 3
	}
	// FirstOffset/LastOffset 只影响「全新 group 首次启动」的位点。
	startOffset := kafka.FirstOffset
	if cfg.StartOffset == "last" {
		startOffset = kafka.LastOffset
	}
	// MinBytes=1：有数据立即返回，不为凑批牺牲消费延迟；
	// CommitInterval=0 关掉自动提交，offset 推进完全由 Run 掌控（R1.4）。
	// 关掉自动提交是关键：自动提交按时间推进，会在业务未完成时确认消息，
	// 崩溃即丢（at-most-once）；手动提交才能做到处理完才确认。
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
//
// 提交规则（R1.4）：只有「业务成功」或「失败消息已可靠写入死信」才推进 offset。
// 死信写不进去时绝不能提交——那等于既没处理、也没留下痕迹，消息凭空消失。
// 这条路径上宁可让分区停住（返回错误、由上层重启后重放），也不越过失败消息。
// 返回值语义：ctx 取消导致的退出返回 nil（正常关服）；其余错误（fetch/死信/
// commit 失败）返回 error，由上层记录并决定重启——分区停住优于丢消息。
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	// 线性退避基数：第 n 次重试等待 n×100ms（见 handle），
	// 给 MySQL/Redis 等下游留恢复窗口，又不用固定 sleep 放大故障。
	backoff := 100 * time.Millisecond
	consumed := 0
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// 关服取消与 broker 断开都表现为 FetchMessage 报错，靠
			// ctx.Err() 区分：取消 = 正常退出（nil），其余 = 真故障。
			// errors.Is 而不是 ==：错误可能被客户端库多层包装。
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch %s: %w", c.topic, err)
		}

		env, err := Unmarshal(msg.Value)
		if err != nil {
			// 无法解析就没有业务语义，进死信后提交；死信写不进去仍不提交。
			// 解析失败直接走死信而不重试：载荷坏是确定性错误，重试 N 次结果一样。
			if derr := c.moveToDeadLetter(msg, nil, fmt.Errorf("unmarshal: %w", err)); derr != nil {
				metrics.KafkaConsumed(c.topic, "dead_failed")
				return fmt.Errorf("dead letter malformed %s@%d: %w", c.topic, msg.Offset, derr)
			}
			metrics.KafkaConsumed(c.topic, "malformed")
			if cerr := c.commit(ctx, msg); cerr != nil {
				return cerr
			}
			continue
		}

		lastErr := c.handle(ctx, h, env, backoff)
		if lastErr != nil && ctx.Err() != nil {
			// 关服中断：不提交，重启后从该 offset 重放。
			// 此时消息可能已部分生效（业务副作用），靠 biz_id 幂等兜住重复应用。
			return nil
		}
		if lastErr != nil {
			if derr := c.moveToDeadLetter(msg, env, lastErr); derr != nil {
				metrics.KafkaConsumed(c.topic, "dead_failed")
				return fmt.Errorf("dead letter %s@%d after %d attempts: %w", c.topic, msg.Offset, c.maxRetry, derr)
			}
			metrics.KafkaConsumed(c.topic, "dead")
		} else {
			metrics.KafkaConsumed(c.topic, "ok")
		}

		// 消费 lag：Stats() 需加锁，每 256 条采样一次即可反映趋势
		// （%256==1 保证第一条就采到，lag 从启动起可见）。
		if consumed++; consumed%256 == 1 {
			st := c.reader.Stats()
			metrics.KafkaLag(c.topic, st.Lag)
		}

		if cerr := c.commit(ctx, msg); cerr != nil {
			return cerr
		}
	}
}

// handle 带线性退避重试；ctx 取消时立即返回 ctx 错误，交给调用方决定不提交。
// 重试次数含首次：attempt 从 0 到 maxRetry，共 1+maxRetry 次调用。
func (c *Consumer) handle(ctx context.Context, h Handler, env *Envelope, backoff time.Duration) error {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetry; attempt++ {
		lastErr = h(ctx, env)
		if lastErr == nil {
			return nil
		}
		if attempt == c.maxRetry {
			break
		}
		// 线性退避（100ms、200ms、300ms…）；ctx 取消时立即让路，
		// 保证关服路径不被退避 sleep 拖住。
		// 用 select 两个 case 而不是裸 Sleep：裸 Sleep 无法被取消，
		// 关服时最坏要睡满 (1+2+…+N)×100ms。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff * time.Duration(attempt+1)):
		}
	}
	return lastErr
}

// moveToDeadLetter 写死信；没有死信通道同样算失败——不能假装消息已处理。
//
// 用独立的超时上下文：关服时业务 ctx 可能已取消，但已经把消息取出来的这一条
// 仍要尽力留下痕迹，否则只能靠重放。
// 5s 上限的依据：broker 正常时写入是毫秒级，5s 足够覆盖一次选举/重连。
func (c *Consumer) moveToDeadLetter(msg kafka.Message, env *Envelope, cause error) error {
	if c.producer == nil {
		return fmt.Errorf("no dead letter writer configured")
	}
	if env == nil {
		// 解析失败的消息：原始字节以 base64 存进头，payload 用 null 保持信封合法 JSON。
		// base64 而非明文：msg.Value 是二进制，塞进 string 头会乱码/截断；
		// partition/offset 一并记录，补偿时能回到 Kafka 精确定位原消息。
		env = &Envelope{
			Topic: c.topic,
			Key:   string(msg.Key),
			Headers: map[string]string{
				"dlq_raw":             base64.StdEncoding.EncodeToString(msg.Value),
				"dlq_partition":       fmt.Sprint(msg.Partition),
				"dlq_offset":          fmt.Sprint(msg.Offset),
				"dlq_unmarshal_error": cause.Error(),
			},
		}
	}
	// context.Background() 而非 ctx：死信写入是「这条消息的临终抢救」，
	// 不应被调用方取消连带取消；WithTimeout 保证它自己不会永久挂住。
	dlqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.producer.DeadLetter(dlqCtx, env, cause)
}

// commit 提交 offset；关服期间的提交失败不算错误（消息会重放）。
func (c *Consumer) commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		// ctx 已取消时提交失败是预期内的：进程要退了，offset 不提交，
		// 重启后从这条重放（幂等兜底）。此时返回错误只会污染关服日志。
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("commit %s@%d: %w", c.topic, msg.Offset, err)
	}
	return nil
}

// Close 关闭消费者（提交 offset 后退出）。
// nil 判空服务于未初始化/测试替身场景；reader.Close 会触发 group 离组，
// 已分配分区被释放，rebalance 由其余实例承接。
func (c *Consumer) Close() error {
	if c.reader == nil {
		return nil
	}
	return c.reader.Close()
}
