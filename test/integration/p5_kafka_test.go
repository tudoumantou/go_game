//go:build integration

// test/integration/p5_kafka_test.go —— P5 Kafka 链路集成测试
//
// 前置：需要可用的 Kafka broker，通过环境变量提供：
//
//	KAFKA_BROKERS=127.0.0.1:9092 go test -tags=integration ./test/integration/ -run TestKafkaRechargeFlow -v
//
// 本地可用 `make dev`（docker-compose 的 bitnami/kafka）先起 broker。
// 未设置 KAFKA_BROKERS 时自动跳过，不阻塞 CI 的默认集成测试。
package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gamemq "game_study/internal/game/mq"
	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"

	"github.com/segmentio/kafka-go"
)

// grantActor 捕获 GrantItemsMsg 的测试 Actor：记录发奖次数并暂存道具列表，
// 替代真实玩家 Actor，用于断言「充值消息 → Kafka → 消费 → 发奖」的端到端结果。
type grantActor struct {
	// mu 保护 grants/items：Receive 在消费 goroutine 调用，count() 在测试 goroutine 读
	mu     sync.Mutex
	grants int
	items  []player.GrantItem
}

// Receive 实现 actor.Actor：只关心 GrantItemsMsg，其余消息直接忽略；
// msg.Done 非空时回写 nil，让发送方（消费者）确认投递完成。
func (a *grantActor) Receive(ctx *actor.Context) {
	msg, ok := ctx.Message().(*player.GrantItemsMsg)
	if !ok {
		return
	}
	a.mu.Lock()
	a.grants++
	a.items = append(a.items, msg.Items...)
	a.mu.Unlock()
	if msg.Done != nil {
		msg.Done <- nil
	}
}

// count 已收到的发奖次数（测试 goroutine 安全读取）。
func (a *grantActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.grants
}

// staticRouter 永远返回同一个 PID 的路由（测试替身）：本用例只涉及玩家 2001，
// 不需要真实的 GetOrSpawn 逻辑。
type staticRouter struct{ pid *actor.PID }

// Get 总是返回预置 PID 且存在。
func (r *staticRouter) Get(int64) (*actor.PID, bool) { return r.pid, true }

// GetOrSpawn 与 Get 相同：测试里不真的 spawn。
func (r *staticRouter) GetOrSpawn(ctx context.Context, _ int64) (*actor.PID, error) {
	return r.pid, nil
}

// alwaysOnline 在线判定桩：任何玩家都「在线」且在指定节点，
// 省去真实 presence（etcd/Redis）装配，把用例聚焦在 Kafka 链路本身。
type alwaysOnline struct{ node string }

// IsOnline 恒返回 (node, true)。
func (a alwaysOnline) IsOnline(ctx context.Context, playerID int64) (string, bool) {
	return a.node, true
}

// PendPush 空实现：离线暂存路径不在本用例覆盖范围内。
func (a alwaysOnline) PendPush(ctx context.Context, biz string, playerID int64, value []byte, maxLen int64, ttl time.Duration) error {
	return nil
}

// TestKafkaRechargeFlow 充值订单 → Kafka → 消费 → 发奖 端到端（P5）。
//
// 前置：KAFKA_BROKERS 环境变量，未设置则跳过（不阻塞 CI 默认集成测试）。
// 隔离手段：独立 topic + 唯一消费组，保证本次测试独占数据、读不到历史残留。
//
// 关键断言：① 一条订单恰好触发一次发奖；② 同一 envelope 重复投递被幂等器
// 拦下（Deduper.FirstTime 第二次返回 false），不重复发奖 —— 对应线上
// 「Kafka at-least-once 投递」的真实语义。
func TestKafkaRechargeFlow(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		t.Skip("KAFKA_BROKERS not set, skip kafka integration test")
	}
	brokers := strings.Split(brokersEnv, ",")

	// 每次测试用独立 topic，避免历史消息干扰（group 唯一 + FirstOffset 会重放旧数据）
	topic := fmt.Sprintf("%s.it.%d", pkgmq.TopicRechargeOrder, time.Now().UnixNano())
	if err := ensureTopics(brokers, topic); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	logger := log.New("kafka-test", log.WithLevel("warn"))

	// 生产者
	producer := pkgmq.NewProducer(pkgmq.Config{
		Brokers:      brokers,
		RequiredAcks: "all",
	})
	defer func() { _ = producer.Close() }()

	// Actor 系统（承接发奖）
	sys := actor.NewSystem(actor.Config{ShardCount: 4, MailboxSize: 64})
	actorCtx, actorCancel := context.WithCancel(ctx)
	sys.Start(actorCtx)
	defer func() {
		actorCancel()
		sys.Stop()
	}()

	act := &grantActor{}
	pid, err := sys.Spawn("player:2001", act)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// 消费者（唯一 group，保证本次测试独占消费）
	groupID := fmt.Sprintf("it-recharge-%d", time.Now().UnixNano())
	consumer := pkgmq.NewConsumer(pkgmq.Config{Brokers: brokers}, topic, groupID, producer)
	defer func() { _ = consumer.Close() }()

	deps := gamemq.Deps{
		Deduper:  &itDeduper{seen: map[string]bool{}},
		Presence: alwaysOnline{node: "it-node"},
		Players:  &staticRouter{pid: pid},
		Sys:      sys,
		Logger:   logger,
		NodeID:   "it-node",
	}
	rc := gamemq.NewRechargeConsumer(consumer, deps)
	go func() {
		if err := rc.Run(actorCtx); err != nil && actorCtx.Err() == nil {
			t.Logf("consumer stopped: %v", err)
		}
	}()

	// 等消费组分配分区
	time.Sleep(3 * time.Second)

	// 生产一条充值订单
	orderID := fmt.Sprintf("OIT%d", time.Now().UnixNano())
	msg := pkgmq.RechargeOrderMsg{
		OrderID: orderID, PlayerID: 2001, ProductID: 1, AmountFen: 300, Channel: 1,
	}
	env, err := pkgmq.NewEnvelope(orderID, topic, orderID, msg)
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	if err := producer.ProduceEnvelope(ctx, env); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// 等待消费 + 发奖
	deadline := time.Now().Add(30 * time.Second)
	for act.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if act.count() != 1 {
		t.Fatalf("expected 1 grant, got %d", act.count())
	}

	// 重复投递：验证幂等
	if err := producer.ProduceEnvelope(ctx, env); err != nil {
		t.Fatalf("produce duplicate: %v", err)
	}
	time.Sleep(3 * time.Second)
	if act.count() != 1 {
		t.Fatalf("duplicate delivery should be idempotent, grants = %d", act.count())
	}
}

// itDeduper 内存幂等器。
type itDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
}

// FirstTime 首次见到 (topic,bizID) 返回 true，之后返回 false。
// 内存实现替代 Redis 版幂等器，只为验证消费侧「先判重再发奖」的调用契约。
func (d *itDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := pkgmq.IdemKey(topic, bizID)
	if d.seen[key] {
		return false, nil
	}
	d.seen[key] = true
	return true, nil
}

// Forget 删除幂等记录（本用例未走到，仅为接口完整）。
func (d *itDeduper) Forget(ctx context.Context, topic, bizID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.seen, pkgmq.IdemKey(topic, bizID))
	return nil
}

// ensureTopics 显式创建 topic（NumPartitions=3, RF=1），已存在则忽略错误。
func ensureTopics(brokers []string, topics ...string) error {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	ctrlConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer func() { _ = ctrlConn.Close() }()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, tp := range topics {
		configs = append(configs, kafka.TopicConfig{
			Topic:             tp,
			NumPartitions:     3,
			ReplicationFactor: 1,
		})
	}
	// 已存在时返回 topic already exists，忽略
	if err := ctrlConn.CreateTopics(configs...); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}
	return nil
}
