// internal/center/registry/registry_test.go —— 节点注册表测试
package registry

import (
	"testing"
	"time"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Info(msg string, kv ...any) { l.t.Logf("INFO %s %v", msg, kv) }
func (l testLogger) Warn(msg string, kv ...any) { l.t.Logf("WARN %s %v", msg, kv) }

type testSender struct{ id string }

func (s *testSender) NodeID() string            { return s.id }
func (s *testSender) SendCommand(cmd any) error { return nil }

func TestRegisterAndQuery(t *testing.T) {
	r := New(time.Second, testLogger{t})

	r.Register(&Node{ID: "game-01", Type: TypeGame, Addr: "1.1.1.1:9100", GroupID: "g1"}, &testSender{"game-01"})
	r.Register(&Node{ID: "game-02", Type: TypeGame, Addr: "1.1.1.2:9100", GroupID: "g1"}, &testSender{"game-02"})
	r.Register(&Node{ID: "cross-01", Type: TypeCross, Addr: "1.1.1.3:9300", GroupID: "g1"}, &testSender{"cross-01"})

	total, game, cross := r.Count()
	if total != 3 || game != 2 || cross != 1 {
		t.Fatalf("count mismatch: total=%d game=%d cross=%d", total, game, cross)
	}

	games := r.ListByType(TypeGame)
	if len(games) != 2 || games[0].ID != "game-01" || games[1].ID != "game-02" {
		t.Fatalf("ListByType(game) not sorted as expected: %+v", games)
	}

	groupGames := r.ListByGroup("g1", TypeGame)
	if len(groupGames) != 2 {
		t.Fatalf("expect 2 games in g1, got %d", len(groupGames))
	}
	crosses := r.ListByGroup("g1", TypeCross)
	if len(crosses) != 1 || crosses[0].Addr != "1.1.1.3:9300" {
		t.Fatalf("cross node mismatch: %+v", crosses)
	}

	if groups := r.Groups(); len(groups) != 1 || groups[0] != "g1" {
		t.Fatalf("groups mismatch: %v", groups)
	}

	if _, ok := r.Get("game-01"); !ok {
		t.Fatal("game-01 should exist")
	}
	if _, ok := r.Get("nope"); ok {
		t.Fatal("missing node should not be found")
	}
	if s, ok := r.SenderFor("cross-01"); !ok || s.NodeID() != "cross-01" {
		t.Fatal("sender lookup failed")
	}
}

func TestHeartbeatUpdatesNode(t *testing.T) {
	r := New(time.Second, testLogger{t})
	r.Register(&Node{ID: "game-01", Type: TypeGame}, nil)

	if !r.Heartbeat("game-01", 100, 30, 40, 500, "1.0.1") {
		t.Fatal("heartbeat for registered node should succeed")
	}
	n, _ := r.Get("game-01")
	if n.PlayerNum != 100 || n.CPULoad != 30 || n.MemLoad != 40 || n.MsgQPS != 500 {
		t.Fatalf("heartbeat fields not applied: %+v", n)
	}
	if n.Version != "1.0.1" {
		t.Fatalf("version not updated: %s", n.Version)
	}

	// 未注册节点的心跳应返回 false（上层要求补发 Register）
	if r.Heartbeat("ghost", 1, 0, 0, 0, "") {
		t.Fatal("heartbeat for unknown node should return false")
	}
}

func TestTickRemovesTimeoutNodes(t *testing.T) {
	r := New(50*time.Millisecond, testLogger{t})
	r.Register(&Node{ID: "game-01", Type: TypeGame}, nil)
	r.Register(&Node{ID: "game-02", Type: TypeGame}, nil)

	// game-02 持续心跳，game-01 不心跳
	for i := 0; i < 3; i++ {
		time.Sleep(30 * time.Millisecond)
		r.Heartbeat("game-02", 1, 0, 0, 0, "")
	}

	dead := r.Tick(time.Now())
	if len(dead) != 1 || dead[0].ID != "game-01" {
		t.Fatalf("expect only game-01 removed, got %+v", dead)
	}
	if _, ok := r.Get("game-01"); ok {
		t.Fatal("game-01 should be removed")
	}
	if _, ok := r.Get("game-02"); !ok {
		t.Fatal("game-02 should still exist")
	}
}

func TestUnregisterRemovesSender(t *testing.T) {
	r := New(time.Second, testLogger{t})
	r.Register(&Node{ID: "game-01", Type: TypeGame}, &testSender{"game-01"})

	r.Unregister("game-01")
	if _, ok := r.Get("game-01"); ok {
		t.Fatal("node should be removed")
	}
	if _, ok := r.SenderFor("game-01"); ok {
		t.Fatal("sender should be removed")
	}
}
