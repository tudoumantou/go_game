// internal/game/remote/remote_test.go —— 跨节点发送器单测（P0）
//
// 覆盖三条路径：
//  1. 非 RemoteMessage 显式拒绝（带 Done channel 的请求-响应消息天然
//     不可远程，让它在这里失败，调用方的重试/对账路径接管）；
//  2. RemoteMessage 经信封编码后送中心服中转，relay_id 唯一、目标节点
//     取自 PID.Node；
//  3. relay 为 nil 属于装配错误，Deliver 显式报错而不是 panic。
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

// fakeRelay 记录中转调用（测试替身）。
type fakeRelay struct {
	mu        sync.Mutex
	relayIDs  []string
	toNodes   []string
	payloads  [][]byte
	broadcast []bool
	err       error // 非 nil 时 SendRelay 返回该错误
}

func (f *fakeRelay) SendRelay(relayID, toNode string, broadcast bool, _ string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.relayIDs = append(f.relayIDs, relayID)
	f.toNodes = append(f.toNodes, toNode)
	f.broadcast = append(f.broadcast, broadcast)
	f.payloads = append(f.payloads, payload)
	return f.err
}

func (f *fakeRelay) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.relayIDs)
}

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

func newTestSender(relay CenterRelay) *Sender {
	registerOnce()
	return NewSender(relay, "game-1", log.New("test"))
}

// TestDeliverRejectsNonRemote 非 RemoteMessage 显式拒绝，不送中转。
func TestDeliverRejectsNonRemote(t *testing.T) {
	relay := &fakeRelay{}
	s := newTestSender(relay)

	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, &plainMsg{Done: make(chan error, 1)})
	require.Error(t, err, "非 RemoteMessage 应被拒绝")
	require.True(t, strings.Contains(err.Error(), "not remotable"),
		"错误信息应说明原因，got %v", err)
	require.Zero(t, relay.calls(), "被拒绝的消息不应送到中心服")
}

// TestDeliverEncodesAndRelays RemoteMessage 经信封编码中转，
// toNode 取自 PID.Node，relay_id 唯一。
func TestDeliverEncodesAndRelays(t *testing.T) {
	relay := &fakeRelay{}
	s := newTestSender(relay)

	to := &actor.PID{ID: "player:42", Node: "game-2"}
	for i := 0; i < 3; i++ {
		require.NoErrorf(t, s.Deliver(to, testRemoteMsg{Text: "hello"}), "Deliver #%d", i)
	}

	require.Equal(t, 3, relay.calls(), "应中转 3 次")
	require.Len(t, uniqueStrings(relay.relayIDs), 3, "relay_id 应互不重复：%v", relay.relayIDs)
	for _, node := range relay.toNodes {
		require.Equal(t, "game-2", node, "目标节点应取自 PID.Node")
	}
	for _, b := range relay.broadcast {
		require.False(t, b, "点对点投递 broadcast 应为 false")
	}

	// 信封应可被接收侧解码还原（编码/解码对称）。
	for _, payload := range relay.payloads {
		actorID, msg, err := actor.DecodeRemoteEnvelope(payload)
		require.NoError(t, err, "信封解码失败")
		require.Equal(t, "player:42", actorID, "信封 actorID")
		rm, ok := msg.(testRemoteMsg)
		require.True(t, ok, "信封消息类型还原失败: %#v", msg)
		require.Equal(t, "hello", rm.Text, "信封消息内容")
	}
}

// TestDeliverRelayError 中转失败时错误上抛（调用方按 fire-and-forget
// 记日志；推送丢失由客户端重连拉全量兜底）。
func TestDeliverRelayError(t *testing.T) {
	boom := errors.New("center unavailable")
	s := newTestSender(&fakeRelay{err: boom})

	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, testRemoteMsg{Text: "x"})
	require.ErrorIs(t, err, boom, "中转错误应原样上抛")
}

// TestDeliverNilRelay relay 为 nil 是装配错误：显式报错而非 panic。
func TestDeliverNilRelay(t *testing.T) {
	s := NewSender(nil, "game-1", log.New("test"))
	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, testRemoteMsg{Text: "x"})
	require.Error(t, err, "relay 为 nil 时应报错")
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
