// internal/cross/battle/room_test.go —— 房间 Actor 逻辑测试（不依赖网络）
package battle

import (
	"context"
	"sync"
	"testing"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/stretchr/testify/require"
)

// mockSink 记录下行帧，供断言。
type mockSink struct {
	mu     sync.Mutex
	frames []sentFrame
	closed []uint64
	nodes  map[int64]string // player_id -> node_id
}

type sentFrame struct {
	node  string
	frame *internalv2.CrossFrame
}

func newMockSink() *mockSink {
	return &mockSink{nodes: make(map[int64]string)}
}

func (m *mockSink) SendToGame(nodeID string, frame *internalv2.CrossFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frames = append(m.frames, sentFrame{node: nodeID, frame: frame})
	return nil
}

func (m *mockSink) NodeOf(playerID int64) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n, ok := m.nodes[playerID]
	return n, ok
}

func (m *mockSink) RoomClosed(roomID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = append(m.closed, roomID)
}

func (m *mockSink) framesOf(msgType string) []sentFrame {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []sentFrame
	for _, f := range m.frames {
		switch msgType {
		case "ready":
			if f.frame.GetCtrl() != nil && f.frame.GetCtrl().Type == internalv2.CrossCtrl_ROOM_READY {
				out = append(out, f)
			}
		case "battle":
			if f.frame.GetBattle() != nil {
				out = append(out, f)
			}
		case "result":
			if f.frame.GetResult() != nil {
				out = append(out, f)
			}
		}
	}
	return out
}

// newTestSystem 起一个最小 Actor 系统。
func newTestSystem(t *testing.T) *actor.System {
	t.Helper()
	sys := actor.NewSystem(actor.Config{ShardCount: 4, MailboxSize: 256})
	sys.Start(context.Background())
	t.Cleanup(sys.Stop)
	return sys
}

func TestRoomSettlesWhenAllReported(t *testing.T) {
	sink := newMockSink()
	sink.nodes[1] = "game-a"
	sink.nodes[2] = "game-b"

	sys := newTestSystem(t)
	logger := log.New("test", log.WithLevel("error"))
	room := NewRoom(100, "arena", 2, 5*time.Second, sink, logger)
	pid, err := sys.Spawn("room:100", room)
	require.NoError(t, err, "spawn")
	room.SetPID(pid)

	// 两人入房 → 满员开战
	for _, p := range []int64{1, 2} {
		if err := sys.Send(pid, &JoinMsg{NodeID: sink.nodes[p], Snapshot: &internalv2.PlayerSnapshot{
			RoomId: 100, BattleToken: tokenOf(p), PlayerId: p,
		}}); err != nil {
			t.Fatalf("join: %v", err)
		}
	}
	waitFor(t, func() bool { return len(sink.framesOf("ready")) == 2 }, "ROOM_READY x2")

	// 双方上报 → 结算
	for _, p := range []int64{1, 2} {
		res := 0
		if p == 2 {
			res = 1
		}
		_ = sys.Send(pid, &ReportMsg{NodeID: sink.nodes[p], Result: &internalv2.BattleResult{
			RoomId: 100, PlayerId: p, Result: int32(res), BattleToken: tokenOf(p),
		}})
	}

	waitFor(t, func() bool { return len(sink.framesOf("result")) == 2 }, "结算 x2")

	results := sink.framesOf("result")
	got := map[int64]int32{}
	for _, f := range results {
		got[f.frame.GetResult().PlayerId] = f.frame.GetResult().Result
	}
	if got[1] != 0 || got[2] != 1 {
		t.Fatalf("result mismatch: %+v", got)
	}
	// 房间应通知管理器摘除
	waitFor(t, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return len(sink.closed) == 1 && sink.closed[0] == 100
	}, "room closed callback")
}

func TestRoomRelaysBattleMsgToOpponentOnly(t *testing.T) {
	sink := newMockSink()
	sink.nodes[1] = "game-a"
	sink.nodes[2] = "game-b"

	sys := newTestSystem(t)
	room := NewRoom(200, "arena", 2, 5*time.Second, sink, log.New("test", log.WithLevel("error")))
	pid, _ := sys.Spawn("room:200", room)
	room.SetPID(pid)

	for _, p := range []int64{1, 2} {
		_ = sys.Send(pid, &JoinMsg{NodeID: sink.nodes[p], Snapshot: &internalv2.PlayerSnapshot{
			RoomId: 200, BattleToken: tokenOf(p), PlayerId: p,
		}})
	}
	waitFor(t, func() bool { return len(sink.framesOf("ready")) == 2 }, "ready")

	// 玩家 1 发操作 → 只应转发给玩家 2 所在节点
	_ = sys.Send(pid, &BattleMsgIn{NodeID: "game-a", Msg: &internalv2.BattleMsg{
		RoomId: 200, PlayerId: 1, MsgId: 9001, Body: []byte("op-1"),
	}})
	waitFor(t, func() bool { return len(sink.framesOf("battle")) == 1 }, "relay once")

	relayed := sink.framesOf("battle")[0]
	if relayed.node != "game-b" {
		t.Fatalf("relay target: want game-b, got %s", relayed.node)
	}
	if string(relayed.frame.GetBattle().Body) != "op-1" {
		t.Fatalf("body must be forwarded verbatim, got %q", relayed.frame.GetBattle().Body)
	}
}

func TestRoomRejectsBadToken(t *testing.T) {
	sink := newMockSink()
	sink.nodes[1] = "game-a"
	sink.nodes[2] = "game-b"

	sys := newTestSystem(t)
	room := NewRoom(300, "arena", 2, 5*time.Second, sink, log.New("test", log.WithLevel("error")))
	pid, _ := sys.Spawn("room:300", room)
	room.SetPID(pid)

	for _, p := range []int64{1, 2} {
		_ = sys.Send(pid, &JoinMsg{NodeID: sink.nodes[p], Snapshot: &internalv2.PlayerSnapshot{
			RoomId: 300, BattleToken: tokenOf(p), PlayerId: p,
		}})
	}
	waitFor(t, func() bool { return len(sink.framesOf("ready")) == 2 }, "ready")

	// 玩家 1 用错误令牌上报 → 应被拒（不结算）
	_ = sys.Send(pid, &ReportMsg{NodeID: "game-a", Result: &internalv2.BattleResult{
		RoomId: 300, PlayerId: 1, Result: 0, BattleToken: "forged-token",
	}})
	time.Sleep(200 * time.Millisecond)
	if n := len(sink.framesOf("result")); n != 0 {
		t.Fatalf("forged token must not settle, got %d result frames", n)
	}

	// 正确令牌 → 正常结算
	for _, p := range []int64{1, 2} {
		_ = sys.Send(pid, &ReportMsg{NodeID: sink.nodes[p], Result: &internalv2.BattleResult{
			RoomId: 300, PlayerId: p, Result: 2, BattleToken: tokenOf(p),
		}})
	}
	waitFor(t, func() bool { return len(sink.framesOf("result")) == 2 }, "settle after valid token")
}

func TestRoomTimeoutSettlesAsDraw(t *testing.T) {
	sink := newMockSink()
	sink.nodes[1] = "game-a"
	sink.nodes[2] = "game-b"

	sys := newTestSystem(t)
	// 超时设很短，验证兜底
	room := NewRoom(400, "arena", 2, 300*time.Millisecond, sink, log.New("test", log.WithLevel("error")))
	pid, _ := sys.Spawn("room:400", room)
	room.SetPID(pid)

	for _, p := range []int64{1, 2} {
		_ = sys.Send(pid, &JoinMsg{NodeID: sink.nodes[p], Snapshot: &internalv2.PlayerSnapshot{
			RoomId: 400, BattleToken: tokenOf(p), PlayerId: p,
		}})
	}
	waitFor(t, func() bool { return len(sink.framesOf("ready")) == 2 }, "ready")

	// 都不上报 → 超时后按平局结算
	waitFor(t, func() bool { return len(sink.framesOf("result")) == 2 }, "timeout settle")

	for _, f := range sink.framesOf("result") {
		if f.frame.GetResult().Result != 2 {
			t.Fatalf("timeout must settle as draw(2), got %d", f.frame.GetResult().Result)
		}
	}
}

func TestRoomDuplicateReportIdempotent(t *testing.T) {
	sink := newMockSink()
	sink.nodes[1] = "game-a"
	sink.nodes[2] = "game-b"

	sys := newTestSystem(t)
	room := NewRoom(500, "arena", 2, 5*time.Second, sink, log.New("test", log.WithLevel("error")))
	pid, _ := sys.Spawn("room:500", room)
	room.SetPID(pid)

	for _, p := range []int64{1, 2} {
		_ = sys.Send(pid, &JoinMsg{NodeID: sink.nodes[p], Snapshot: &internalv2.PlayerSnapshot{
			RoomId: 500, BattleToken: tokenOf(p), PlayerId: p,
		}})
	}
	waitFor(t, func() bool { return len(sink.framesOf("ready")) == 2 }, "ready")

	// 玩家 1 连发 3 次（模拟重发），玩家 2 报一次
	for i := 0; i < 3; i++ {
		_ = sys.Send(pid, &ReportMsg{NodeID: "game-a", Result: &internalv2.BattleResult{
			RoomId: 500, PlayerId: 1, Result: 0, BattleToken: tokenOf(1),
		}})
	}
	_ = sys.Send(pid, &ReportMsg{NodeID: "game-b", Result: &internalv2.BattleResult{
		RoomId: 500, PlayerId: 2, Result: 1, BattleToken: tokenOf(2),
	}})

	waitFor(t, func() bool { return len(sink.framesOf("result")) == 2 }, "settle once per player")
	time.Sleep(200 * time.Millisecond)
	if n := len(sink.framesOf("result")); n != 2 {
		t.Fatalf("duplicate report must not double settle, got %d frames", n)
	}
}

func tokenOf(playerID int64) string {
	return "tok-" + string(rune('0'+playerID))
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}
