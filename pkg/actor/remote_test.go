// pkg/actor/remote_test.go —— 跨节点投递单测：路由决策、信封编解码、双 System 端到端。
package actor

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

// remoteTestMsg 演示消息：纯数据，实现 RemoteMessage。
type remoteTestMsg struct {
	Text string
}

func (m *remoteTestMsg) RemoteTypeURL() string { return "test.RemoteMsg" }
func (m *remoteTestMsg) MarshalRemote() ([]byte, error) {
	return []byte(m.Text), nil
}

// registerTestRemote 注册测试消息类型（多次调用只注册一次——
// RegisterRemote 重复注册会 panic，测试包里共用同一 type_url）。
var registerTestRemote = sync.OnceFunc(func() {
	RegisterRemote("test.RemoteMsg", func(b []byte) (any, error) {
		return &remoteTestMsg{Text: string(b)}, nil
	})
})

// fakeSender 记录远程投递（不真正传输）。
type fakeSender struct {
	mu    sync.Mutex
	calls []fakeCall
	err   error
}

type fakeCall struct {
	to  *PID
	msg any
}

func (f *fakeSender) Deliver(to *PID, msg any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{to: to, msg: msg})
	return f.err
}

func (f *fakeSender) last() (fakeCall, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return fakeCall{}, false
	}
	return f.calls[len(f.calls)-1], true
}

// recordActor 记录收到的消息（验证本地/远程投递终点）。
type recordActor struct {
	mu   sync.Mutex
	got  []any
	done chan struct{}
}

func newRecordActor() *recordActor {
	return &recordActor{done: make(chan struct{}, 16)}
}

func (a *recordActor) Receive(ctx *Context) {
	a.mu.Lock()
	a.got = append(a.got, ctx.Message())
	a.mu.Unlock()
	select {
	case a.done <- struct{}{}:
	default:
	}
}

func (a *recordActor) messages() []any {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]any, len(a.got))
	copy(out, a.got)
	return out
}

func (a *recordActor) waitFor(t *testing.T, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if len(a.messages()) >= want {
			return
		}
		select {
		case <-a.done:
		case <-deadline:
			t.Fatalf("timeout waiting for %d messages, got %d", want, len(a.messages()))
		}
	}
}

// TestSendRoutesRemoteWhenNodeDiffers 验证 Node 非本节点时走 RemoteSender。
func TestSendRoutesRemoteWhenNodeDiffers(t *testing.T) {
	registerTestRemote()
	rs := &fakeSender{}
	sys := NewSystem(Config{NodeID: "game-01", Remote: rs})
	sys.Start(context.Background())
	defer sys.Stop()

	target := &PID{ID: "player:123", Shard: 7, Node: "game-02"}
	msg := &remoteTestMsg{Text: "hello"}
	if err := sys.Send(target, msg); err != nil {
		t.Fatal(err)
	}
	call, ok := rs.last()
	if !ok {
		t.Fatal("remote sender not called")
	}
	if call.to.Node != "game-02" || call.to.ID != "player:123" {
		t.Fatalf("wrong target: %+v", call.to)
	}
	if call.msg != msg {
		t.Fatal("message not passed through")
	}
}

// TestSendStaysLocalForOwnNodeOrEmptyNode 验证本节点/空 Node 不走远程。
func TestSendStaysLocalForOwnNodeOrEmptyNode(t *testing.T) {
	registerTestRemote()
	rs := &fakeSender{}
	sys := NewSystem(Config{NodeID: "game-01", Remote: rs})
	sys.Start(context.Background())
	defer sys.Stop()

	rec := newRecordActor()
	if _, err := sys.Spawn("player:1", rec); err != nil {
		t.Fatal(err)
	}
	// Node 为空：本地 PID
	if err := sys.Send(&PID{ID: "player:1"}, &remoteTestMsg{Text: "a"}); err != nil {
		t.Fatal(err)
	}
	// Node = 本节点：远端视图也按本地投递
	if err := sys.Send(&PID{ID: "player:1", Node: "game-01"}, &remoteTestMsg{Text: "b"}); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, 2)
	if _, ok := rs.last(); ok {
		t.Fatal("local sends must not hit remote sender")
	}
	if got := rec.messages(); len(got) != 2 {
		t.Fatalf("local actor got %d messages", len(got))
	}
}

// TestSingleNodeModeStillWorks 验证单机模式（无 NodeID/Remote）行为不变。
func TestSingleNodeModeStillWorks(t *testing.T) {
	sys := NewSystem(Config{})
	sys.Start(context.Background())
	defer sys.Stop()

	rec := newRecordActor()
	if _, err := sys.Spawn("player:1", rec); err != nil {
		t.Fatal(err)
	}
	// 远程 PID 在单机模式下按本地找：存在就投递，不存在报错
	if err := sys.Send(&PID{ID: "player:1", Node: "ghost"}, "x"); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, 1)
	if err := sys.Send(&PID{ID: "player:404", Node: "ghost"}, "x"); err == nil {
		t.Fatal("missing actor should error even in single-node mode")
	}
}

// TestDeliverRemoteDeliversToLocalActor 验证中转入口把消息投入本地 Actor。
func TestDeliverRemoteDeliversToLocalActor(t *testing.T) {
	registerTestRemote()
	sys := NewSystem(Config{})
	sys.Start(context.Background())
	defer sys.Stop()

	rec := newRecordActor()
	if _, err := sys.Spawn("player:9", rec); err != nil {
		t.Fatal(err)
	}
	msg, err := DecodeRemote("test.RemoteMsg", []byte("from-other-node"))
	require.NoError(t, err)
	if err := sys.DeliverRemote("player:9", msg); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, 1)
	if got, ok := rec.messages()[0].(*remoteTestMsg); !ok || got.Text != "from-other-node" {
		t.Fatalf("wrong message: %+v", rec.messages()[0])
	}
	// Actor 不存在：返回可判定的错误
	if err := sys.DeliverRemote("player:missing", msg); err == nil {
		t.Fatal("missing actor should error")
	} else if !errors.Is(err, ErrActorGone) {
		t.Fatalf("want ErrActorGone, got %v", err)
	}
}

// TestEnvelopeRoundTrip 验证信封编解码与未注册类型报错。
func TestEnvelopeRoundTrip(t *testing.T) {
	registerTestRemote()
	env, err := EncodeRemote(&PID{ID: "player:5", Node: "game-02"}, &remoteTestMsg{Text: "payload"})
	require.NoError(t, err)
	actorID, msg, err := DecodeRemoteEnvelope(env)
	require.NoError(t, err)
	if actorID != "player:5" {
		t.Fatalf("actorID = %q", actorID)
	}
	if got, ok := msg.(*remoteTestMsg); !ok || got.Text != "payload" {
		t.Fatalf("wrong message: %+v", msg)
	}
	// 未注册的 type_url：显式报错（启动期漏注册在运行期暴露）
	if _, _, err := DecodeRemoteEnvelope([]byte(`{"actor_id":"x","type_url":"nope","payload":"eA=="}`)); err == nil {
		t.Fatal("unregistered type should error")
	}
	// 缺 actor_id 的信封：拒绝
	if _, _, err := DecodeRemoteEnvelope([]byte(`{"type_url":"test.RemoteMsg","payload":"eA=="}`)); err == nil {
		t.Fatal("envelope without actor_id should error")
	}
}

// TestRemoteAcrossSystems 端到端：两个 System 模拟两个节点，
// A 的 RemoteSender 把信封交给 B 的 DeliverRemote（即中心服中转的本地等价物）。
func TestRemoteAcrossSystems(t *testing.T) {
	registerTestRemote()

	nodeB := NewSystem(Config{NodeID: "game-02"})
	nodeB.Start(context.Background())
	defer nodeB.Stop()

	rec := newRecordActor()
	if _, err := nodeB.Spawn("player:123", rec); err != nil {
		t.Fatal(err)
	}

	// A 的远程发送器 = 「经中心服中转」的最小模拟：信封投递给 B。
	relay := &fakeSender{err: nil}
	nodeA := NewSystem(Config{NodeID: "game-01", Remote: senderFunc(func(to *PID, msg any) error {
		env, err := EncodeRemote(to, msg.(RemoteMessage))
		if err != nil {
			return err
		}
		actorID, decoded, err := DecodeRemoteEnvelope(env)
		if err != nil {
			return err
		}
		return nodeB.DeliverRemote(actorID, decoded)
	})})
	nodeA.Start(context.Background())
	defer nodeA.Stop()

	if err := nodeA.Send(&PID{ID: "player:123", Node: "game-02"}, &remoteTestMsg{Text: "cross-node"}); err != nil {
		t.Fatal(err)
	}
	rec.waitFor(t, 1)
	if got, ok := rec.messages()[0].(*remoteTestMsg); !ok || got.Text != "cross-node" {
		t.Fatalf("node B actor got wrong message: %+v", rec.messages()[0])
	}
	_ = relay // relay 未用于本链路（A 的发送器直接模拟了中转两端）
}

// senderFunc 把函数适配成 RemoteSender。
type senderFunc func(to *PID, msg any) error

func (f senderFunc) Deliver(to *PID, msg any) error { return f(to, msg) }

// TestRegisterRemoteDuplicatePanics 验证重复注册启动期 panic。
func TestRegisterRemoteDuplicatePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate RegisterRemote should panic")
		}
	}()
	RegisterRemote("test.Dup", func(b []byte) (any, error) { return string(b), nil })
	RegisterRemote("test.Dup", func(b []byte) (any, error) { return string(b), nil })
}

var _ = fmt.Sprintf // 保留：未来断言消息格式时直接用
