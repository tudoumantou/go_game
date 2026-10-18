package relay

import (
	"context"
	"testing"

	"game_study/pkg/log"
)

// TestRelayKey 分区键：同一目标必须落同一分区，否则消息乱序。
func TestRelayKey(t *testing.T) {
	cases := []struct {
		name string
		p    Payload
		want string
	}{
		{"点对点", Payload{ToNode: "game-07"}, "node:game-07"},
		{"广播有分组", Payload{Broadcast: true, GroupID: "group-01"}, "group:group-01"},
		{"广播无分组", Payload{Broadcast: true}, "broadcast"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := relayKey(c.p); got != c.want {
				t.Fatalf("relayKey = %q, want %q", got, c.want)
			}
		})
	}
}

// TestNewBridgeDisabled 未配置 brokers 时返回 nil（调用方按单实例模式处理）。
func TestNewBridgeDisabled(t *testing.T) {
	cases := []struct {
		name string
		cfg  BridgeConfig
	}{
		{"无 brokers", BridgeConfig{InstanceID: "c1"}},
		{"brokers 全空串（环境变量未展开）", BridgeConfig{Brokers: []string{"", ""}, InstanceID: "c1"}},
		{"无 instanceID", BridgeConfig{Brokers: []string{"127.0.0.1:9092"}}},
	}
	logger := log.New("test")
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if b := NewBridge(c.cfg, func(Payload) int { return 0 }, logger); b != nil {
				t.Fatalf("expect nil bridge, got %+v", b)
			}
		})
	}
}

// TestBridgeNilSafe nil bridge 的所有方法都必须是安全 no-op。
//
// 单实例部署下 relayBridge 为 nil，OnRelayRequest 会直接调用 Forward —— 若这里
// panic，等于「不开 Kafka 就跑不起来」。
func TestBridgeNilSafe(t *testing.T) {
	var b *Bridge

	if b.Enabled() {
		t.Fatal("nil bridge should report disabled")
	}
	if err := b.Forward(context.Background(), Payload{ToNode: "x"}); err == nil {
		t.Fatal("nil bridge Forward should return error")
	}
	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("nil bridge Run should be no-op, got %v", err)
	}
	b.Close() // 不应 panic

	fwd, recv, deliv, skip := b.Stats()
	if fwd+recv+deliv+skip != 0 {
		t.Fatal("nil bridge stats should be zero")
	}
}

// TestBridgeConfigDefaults topic 为空时落默认 topic（center.relay）。
func TestBridgeConfigDefaults(t *testing.T) {
	b := NewBridge(BridgeConfig{
		Brokers:    []string{"127.0.0.1:9092"},
		InstanceID: "center-01@pod-a",
	}, func(Payload) int { return 0 }, log.New("test"))
	if b == nil {
		t.Fatal("bridge should be created")
	}
	defer b.Close()

	if b.cfg.Topic != "center.relay" {
		t.Fatalf("default topic = %q, want center.relay", b.cfg.Topic)
	}
	if got := b.cfg.Topic + "." + b.cfg.InstanceID; got != "center.relay.center-01@pod-a" {
		t.Fatalf("consumer group = %q", got)
	}
}

// TestBridgeStatsAccumulate 统计只增不减（便于确认转发/消费计数对得上）。
func TestBridgeStatsAccumulate(t *testing.T) {
	b := &Bridge{}
	b.forwarded.Add(3)
	b.received.Add(5)
	b.delivered.Add(2)
	b.skipped.Add(4)

	fwd, recv, deliv, skip := b.Stats()
	if fwd != 3 || recv != 5 || deliv != 2 || skip != 4 {
		t.Fatalf("stats = %d/%d/%d/%d", fwd, recv, deliv, skip)
	}
}
