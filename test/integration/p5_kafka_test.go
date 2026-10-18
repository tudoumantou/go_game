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

// grantActor 捕获发奖消息。
type grantActor struct {
	mu     sync.Mutex
	grants int
	items  []player.GrantItem
}

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

func (a *grantActor) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.grants
}

type staticRouter struct{ pid *actor.PID }

func (r *staticRouter) Get(int64) (*actor.PID, bool)         { return r.pid, true }
func (r *staticRouter) GetOrSpawn(int64) (*actor.PID, error) { return r.pid, nil }

type alwaysOnline struct{ node string }

func (a alwaysOnline) IsOnline(ctx context.Context, playerID int64) (string, bool) {
	return a.node, true
}
func (a alwaysOnline) PendPush(ctx context.Context, biz string, playerID int64, value []byte, maxLen int64, ttl time.Duration) error {
	return nil
}

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
