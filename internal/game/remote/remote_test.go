// internal/game/remote/remote_test.go —— 跨节点发送器单测（P0）
//
// 覆盖三条路径：
//  1. 非 RemoteMessage 显式拒绝（带 Done channel 的请求-响应消息天然
//     不可远程，让它在这里失败，调用方的重试/对账路径接管）；
//  2. RemoteMessage 经信封编码后发布到 NATS，relay_id 唯一、目标节点
//     取自 PID.Node；
//  3. NATS 未配或发布失败时上抛错误，不静默丢弃。
package remote

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"game_study/pkg/actor"
	"game_study/pkg/log"

	"github.com/stretchr/testify/require"
)

// testRemoteMsg 实现 actor.RemoteMessage 的测试消息。
type testRemoteMsg struct{ Text string }

func (testRemoteMsg) RemoteTypeURL() string { return "game_study.test.TestRemoteMsg" }

func (m testRemoteMsg) MarshalRemote() ([]byte, error) { return []byte(m.Text), nil }

// plainMsg 未实现 RemoteMessage 的消息（模拟 GrantItemsMsg）。
type plainMsg struct{ Done chan error }

// 注册测试 codec：重复注册会 panic，用 OnceFunc 语义防多包引用。
var registerOnce = sync.OnceFunc(func() {
	actor.RegisterRemote("game_study.test.TestRemoteMsg", func(b []byte) (any, error) {
		return testRemoteMsg{Text: string(b)}, nil
	})
})

// fakeNATSRelay 记录 NATS 直达调用（测试替身）。
type fakeNATSRelay struct {
	mu      sync.Mutex
	nodeIDs []string
	groups  []string
	ids     []string
	payload [][]byte
	err     error
}

func (f *fakeNATSRelay) SendRelayNode(relayID, toNode string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, relayID)
	f.nodeIDs = append(f.nodeIDs, toNode)
	f.payload = append(f.payload, payload)
	return f.err
}

func (f *fakeNATSRelay) SendRelayGroup(relayID, groupID string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ids = append(f.ids, relayID)
	f.groups = append(f.groups, groupID)
	f.payload = append(f.payload, payload)
	return f.err
}

func (f *fakeNATSRelay) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ids)
}

func newTestSender(relay NATSRelay) *Sender {
	registerOnce()
	return NewSender(relay, "game-1", log.New("test"))
}

// TestDeliverRejectsNonRemote 非 RemoteMessage 显式拒绝，不投递。
func TestDeliverRejectsNonRemote(t *testing.T) {
	relay := &fakeNATSRelay{}
	s := newTestSender(relay)

	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, &plainMsg{Done: make(chan error, 1)})
	require.Error(t, err, "非 RemoteMessage 应被拒绝")
	require.True(t, strings.Contains(err.Error(), "not remotable"),
		"错误信息应说明原因，got %v", err)
	require.Zero(t, relay.calls(), "被拒绝的消息不应投递")
}

// TestDeliverEncodesAndRelays RemoteMessage 经信封编码后经 NATS 直达，
// toNode 取自 PID.Node，relay_id 唯一。
func TestDeliverEncodesAndRelays(t *testing.T) {
	relay := &fakeNATSRelay{}
	s := newTestSender(relay)

	to := &actor.PID{ID: "player:42", Node: "game-2"}
	for i := 0; i < 3; i++ {
		require.NoErrorf(t, s.Deliver(to, testRemoteMsg{Text: "hello"}), "Deliver #%d", i)
	}

	require.Equal(t, 3, relay.calls(), "应投递 3 次")
	require.Len(t, uniqueStrings(relay.ids), 3, "relay_id 应互不重复：%v", relay.ids)
	for _, node := range relay.nodeIDs {
		require.Equal(t, "game-2", node, "目标节点应取自 PID.Node")
	}
	require.Empty(t, relay.groups, "点对点投递不应带 group subject")

	// 信封应可被接收侧解码还原（编码/解码对称）。
	for _, payload := range relay.payload {
		actorID, msg, err := actor.DecodeRemoteEnvelope(payload)
		require.NoError(t, err, "信封解码失败")
		require.Equal(t, "player:42", actorID, "信封 actorID")
		rm, ok := msg.(testRemoteMsg)
		require.True(t, ok, "信封消息类型还原失败: %#v", msg)
		require.Equal(t, "hello", rm.Text, "信封消息内容")
	}
}

// TestDeliverRelayError NATS 发布失败时错误上抛（调用方按 fire-and-forget
// 记日志；推送丢失由客户端重连拉全量兜底）。
func TestDeliverRelayError(t *testing.T) {
	boom := errors.New("nats disconnected")
	s := newTestSender(&fakeNATSRelay{err: boom})

	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, testRemoteMsg{Text: "x"})
	require.ErrorIs(t, err, boom, "发布错误应原样上抛")
}

// TestDeliverNoRelay 未配 NATS 时显式报错，不静默丢弃、不经中心服降级。
func TestDeliverNoRelay(t *testing.T) {
	s := newTestSender(nil)
	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, testRemoteMsg{Text: "x"})
	require.Error(t, err, "无 NATS relay 时应报错")
	require.Contains(t, err.Error(), "no NATS relay")
}

// TestSendBroadcast 广播走 NATS 的 group subject，relay_id 唯一。
func TestSendBroadcast(t *testing.T) {
	registerOnce()
	nats := &fakeNATSRelay{}
	s := NewSender(nats, "game-1", log.New("test"))
	for i := 0; i < 2; i++ {
		require.NoError(t, s.SendBroadcast("g1", testRemoteMsg{Text: "b"}))
	}
	require.Equal(t, 2, nats.calls())
	require.Equal(t, []string{"g1", "g1"}, nats.groups)
	require.Empty(t, nats.nodeIDs, "广播不应走点对点 subject")
	require.Len(t, uniqueStrings(nats.ids), 2, "relay_id 应互不重复")
}

// uniqueStrings 返回去重后的字符串切片（用于断言 relay_id 唯一）。
func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
