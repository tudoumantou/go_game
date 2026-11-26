package remote

import (
	"testing"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"
	"game_study/pkg/natsx"
	internalv2 "game_study/pkg/pb/internalv2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// busNATSRelay 把 natsx.Bus 适配为 NATSRelay，等价于装配层的 relayAccessor
// （这里省去 *Server 依赖，直接按 subject 发布）。
type busNATSRelay struct {
	bus    *natsx.Bus
	nodeID string
}

func (r busNATSRelay) SendRelayNode(relayID, toNode string, payload []byte) error {
	return r.bus.Publish(natsx.RelayNodeSubject(toNode), &internalv2.RelayPush{
		RelayId: relayID, FromNode: r.nodeID, Payload: payload,
	})
}

func (r busNATSRelay) SendRelayGroup(relayID, groupID string, payload []byte) error {
	return r.bus.Publish(natsx.RelayGroupSubject(groupID), &internalv2.RelayPush{
		RelayId: relayID, FromNode: r.nodeID, Payload: payload,
	})
}

// TestEndToEndViaNATS 验证完整链路：Sender.Deliver → 信封 → RelayPush →
// relay.node subject → 订阅端解出 RelayPush → DecodeRemoteEnvelope 还原消息。
// 这是「中心服退出转发后 A/B 两服直达」的最小闭环。
func TestEndToEndViaNATS(t *testing.T) {
	registerOnce() // 注册测试消息 codec（与 remote_test.go 共享的包级 OnceFunc）
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1})
	require.NoError(t, err)
	go ns.Start()
	if !ns.ReadyForConnections(3 * time.Second) {
		t.Fatal("nats server not ready")
	}
	defer ns.Shutdown()

	pubBus, err := natsx.Connect(natsx.Config{URL: ns.ClientURL(), Timeout: time.Second})
	require.NoError(t, err)
	defer pubBus.Close()
	subBus, err := natsx.Connect(natsx.Config{URL: ns.ClientURL(), Timeout: time.Second})
	require.NoError(t, err)
	defer subBus.Close()

	got := make(chan *internalv2.RelayPush, 1)
	err = subBus.Subscribe(natsx.RelayNodeSubject("game-02"),
		func() proto.Message { return &internalv2.RelayPush{} },
		func(m proto.Message) error { got <- m.(*internalv2.RelayPush); return nil })
	require.NoError(t, err)
	require.NoError(t, subBus.Flush()) // 确保订阅已注册到服务端

	sender := NewSender(busNATSRelay{bus: pubBus, nodeID: "game-01"}, "game-01", log.New("test"))
	require.NoError(t, sender.Deliver(
		&actor.PID{ID: "player:42", Node: "game-02"},
		testRemoteMsg{Text: "hello-over-nats"}))

	select {
	case push := <-got:
		require.Equal(t, "game-01", push.GetFromNode())
		require.NotEmpty(t, push.GetRelayId())
		actorID, msg, err := actor.DecodeRemoteEnvelope(push.GetPayload())
		require.NoError(t, err)
		require.Equal(t, "player:42", actorID)
		require.Equal(t, testRemoteMsg{Text: "hello-over-nats"}, msg)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for relayed message")
	}
}
