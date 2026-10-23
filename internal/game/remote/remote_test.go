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

func init() {
	// 注册测试 codec：重复注册会 panic，用 OnceFunc 语义防多包引用。
	registerOnce()
}

var registerOnce = sync.OnceFunc(func() {
	actor.RegisterRemote("game_study.test.TestRemoteMsg", func(b []byte) (any, error) {
		return testRemoteMsg{Text: string(b)}, nil
	})
})

func newTestSender(relay CenterRelay) *Sender {
	return NewSender(relay, "game-1", log.New("test"))
}

// TestDeliverRejectsNonRemote 非 RemoteMessage 显式拒绝，不送中转。
func TestDeliverRejectsNonRemote(t *testing.T) {
	relay := &fakeRelay{}
	s := newTestSender(relay)

	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, &plainMsg{Done: make(chan error, 1)})
	if err == nil {
		t.Fatal("非 RemoteMessage 应被拒绝")
	}
	if !strings.Contains(err.Error(), "not remotable") {
		t.Fatalf("错误信息应说明原因，got %v", err)
	}
	if relay.calls() != 0 {
		t.Fatal("被拒绝的消息不应送到中心服")
	}
}

// TestDeliverEncodesAndRelays RemoteMessage 经信封编码中转，
// toNode 取自 PID.Node，relay_id 唯一。
func TestDeliverEncodesAndRelays(t *testing.T) {
	relay := &fakeRelay{}
	s := newTestSender(relay)

	to := &actor.PID{ID: "player:42", Node: "game-2"}
	for i := 0; i < 3; i++ {
		if err := s.Deliver(to, testRemoteMsg{Text: "hello"}); err != nil {
			t.Fatalf("Deliver #%d: %v", i, err)
		}
	}

	if relay.calls() != 3 {
		t.Fatalf("应中转 3 次，got %d", relay.calls())
	}
	seen := map[string]bool{}
	for _, id := range relay.relayIDs {
		if seen[id] {
			t.Fatalf("relay_id 重复：%q", id)
		}
		seen[id] = true
	}
	for _, node := range relay.toNodes {
		if node != "game-2" {
			t.Fatalf("目标节点应取自 PID.Node=game-2，got %q", node)
		}
	}
	for _, b := range relay.broadcast {
		if b {
			t.Fatal("点对点投递 broadcast 应为 false")
		}
	}
	// 信封应可被接收侧解码还原（编码/解码对称）。
	for _, payload := range relay.payloads {
		actorID, msg, err := actor.DecodeRemoteEnvelope(payload)
		if err != nil {
			t.Fatalf("信封解码失败: %v", err)
		}
		if actorID != "player:42" {
			t.Fatalf("信封 actorID 应为 player:42，got %q", actorID)
		}
		if rm, ok := msg.(testRemoteMsg); !ok || rm.Text != "hello" {
			t.Fatalf("信封消息还原失败: %#v", msg)
		}
	}
}

// TestDeliverRelayError 中转失败时错误上抛（调用方按 fire-and-forget
// 记日志；推送丢失由客户端重连拉全量兜底）。
func TestDeliverRelayError(t *testing.T) {
	boom := errors.New("center unavailable")
	relay := &fakeRelay{err: boom}
	s := newTestSender(relay)

	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, testRemoteMsg{Text: "x"})
	if !errors.Is(err, boom) {
		t.Fatalf("中转错误应原样上抛，got %v", err)
	}
}

// TestDeliverNilRelay relay 为 nil 是装配错误：显式报错而非 panic。
func TestDeliverNilRelay(t *testing.T) {
	s := NewSender(nil, "game-1", log.New("test"))
	err := s.Deliver(&actor.PID{ID: "player:42", Node: "game-2"}, testRemoteMsg{Text: "x"})
	if err == nil {
		t.Fatal("relay 为 nil 时应报错")
	}
}
