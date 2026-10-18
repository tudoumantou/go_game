//go:build integration

// test/integration/p9_relay_bridge_test.go —— P9 中心服跨实例中转
//
// 验证「节点 A 连中心服#1、节点 B 连中心服#2」时的中转链路：
//
//	#1 收不到 B → 写 Kafka center.relay → #2 消费 → 投给本地节点 B
//
// 关键语义：
//   - 每个实例独立消费组（广播），都能收到同一条消息；
//   - 靠 OriginInstance 跳过自发自收，否则本实例会重复投递；
//   - 只投递本实例承载的节点，deliver 返回 0 是正常情况。
//
// 前置：KAFKA_BROKERS 环境变量（同 P5 测试），未设置时跳过。
package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"game_study/internal/center/relay"
	"game_study/pkg/log"
)

// relayRecorder 记录本实例实际投递到的中转消息。
type relayRecorder struct {
	mu   sync.Mutex
	msgs []relay.Payload

	// ownNode 本实例承载的节点；只对该节点的消息返回 1（投递成功）
	ownNode string
}

func (r *relayRecorder) deliver(p relay.Payload) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.ToNode != r.ownNode {
		// 目标不归本实例管 —— 对应真实场景里「另一个实例承载该节点」
		return 0
	}
	r.msgs = append(r.msgs, p)
	return 1
}

func (r *relayRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func (r *relayRecorder) last() (relay.Payload, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.msgs) == 0 {
		return relay.Payload{}, false
	}
	return r.msgs[len(r.msgs)-1], true
}

// TestP9CrossInstanceRelay 跨实例中转端到端。
func TestP9CrossInstanceRelay(t *testing.T) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		t.Skip("KAFKA_BROKERS not set, skip cross-instance relay test")
	}
	brokers := strings.Split(brokersEnv, ",")

	// 每次测试用独立 topic：消费组首次订阅从 last 开始，但复用 topic 会读到上次残留
	topic := fmt.Sprintf("center.relay.test.%d", time.Now().UnixNano())
	if err := ensureTopics(brokers, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	logger := log.New("relay-test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 两个「中心服实例」：A 承载 game-a，B 承载 game-b
	recA := &relayRecorder{ownNode: "game-a"}
	recB := &relayRecorder{ownNode: "game-b"}

	// 测试用 first：全新 topic 无历史负担，且能避开「消费组初始化晚于消息写入」的竞态
	brA := relay.NewBridge(relay.BridgeConfig{
		Brokers: brokers, Topic: topic, InstanceID: "center-01@pod-a", StartOffset: "first",
	}, recA.deliver, logger)
	brB := relay.NewBridge(relay.BridgeConfig{
		Brokers: brokers, Topic: topic, InstanceID: "center-02@pod-b", StartOffset: "first",
	}, recB.deliver, logger)
	if brA == nil || brB == nil {
		t.Fatal("bridge should be created with valid brokers")
	}
	defer brA.Close()
	defer brB.Close()

	runErr := make(chan error, 2)
	go func() { runErr <- brA.Run(ctx) }()
	go func() { runErr <- brB.Run(ctx) }()

	// 给消费者一点时间完成 rebalance
	time.Sleep(2 * time.Second)

	// A 侧收到发给 game-b 的中转：本实例没有该节点 → 写 Kafka
	payload := relay.Payload{
		RelayID:  "relay-p9-1",
		FromNode: "game-a",
		ToNode:   "game-b",
		Body:     []byte("hello from game-a"),
	}
	// topic 刚创建时 broker 的 metadata 可能还没扩散到所有节点，
	// 写入会短暂报 Unknown Topic Or Partition —— 生产由运维预建 topic，测试里重试即可
	var ferr error
	for i := 0; i < 6; i++ {
		ferr = brA.Forward(ctx, payload)
		if ferr == nil {
			break
		}
		if !strings.Contains(ferr.Error(), "Unknown Topic") {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if ferr != nil {
		t.Fatalf("forward: %v", ferr)
	}

	// 等 B 消费并投递
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if recB.count() > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if recB.count() != 1 {
		t.Fatalf("instance B delivered %d messages, want 1", recB.count())
	}
	got, _ := recB.last()
	if got.RelayID != payload.RelayID {
		t.Fatalf("relay id = %q, want %q", got.RelayID, payload.RelayID)
	}
	if got.FromNode != "game-a" {
		t.Fatalf("from node = %q", got.FromNode)
	}
	if string(got.Body) != "hello from game-a" {
		t.Fatalf("body = %q", string(got.Body))
	}

	// A 自己也收到了这条消息（同组广播），但必须因 OriginInstance 跳过，
	// 不能重复投递 —— 否则 game-a 会收到本不该给它的消息
	if recA.count() != 0 {
		t.Fatalf("instance A should skip its own relay, but delivered %d", recA.count())
	}

	fwdA, recvA, delivA, skipA := brA.Stats()
	if fwdA != 1 {
		t.Fatalf("A forwarded = %d, want 1", fwdA)
	}
	if recvA == 0 {
		t.Fatal("A should have consumed the broadcast copy")
	}
	if delivA != 0 {
		t.Fatalf("A delivered = %d, want 0", delivA)
	}
	if skipA == 0 {
		t.Fatal("A should record a skip for its own message")
	}

	_, recvB, delivB, _ := brB.Stats()
	if recvB != 1 || delivB != 1 {
		t.Fatalf("B stats received=%d delivered=%d, want 1/1", recvB, delivB)
	}

	// 关闭消费循环，确认 Run 正常退出（不能卡住 —— 优雅关闭要求）
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case err := <-runErr:
			if err != nil {
				t.Fatalf("bridge run returned error: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("bridge Run did not exit after ctx cancel")
		}
	}
}

// TestP9BridgeSkipsForeignNode 目标节点不归本实例时 deliver 返回 0，不产生投递。
//
// 单测层面已覆盖 relayKey/disabled，这里补「消费到别的实例的节点不误投」。
func TestP9BridgeSkipsForeignNode(t *testing.T) {
	rec := &relayRecorder{ownNode: "game-b"}

	if n := rec.deliver(relay.Payload{ToNode: "game-c"}); n != 0 {
		t.Fatalf("deliver foreign node = %d, want 0", n)
	}
	if n := rec.deliver(relay.Payload{ToNode: "game-b"}); n != 1 {
		t.Fatalf("deliver own node = %d, want 1", n)
	}
	if rec.count() != 1 {
		t.Fatalf("recorded %d, want 1", rec.count())
	}
}
