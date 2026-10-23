// internal/center/registry/registry_test.go —— 节点注册表测试
package registry

import (
	"fmt"
	"github.com/stretchr/testify/require"
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
	require.EqualValues(t, 3, total, fmt.Sprintf("count mismatch: total=%d game=%d cross=%d", total, game, cross))
	require.EqualValues(t, 2, game)
	require.EqualValues(t, 1, cross)

	games := r.ListByType(TypeGame)
	require.Len(t, games, 2, fmt.Sprintf("ListByType(game) not sorted as expected: %+v", games))
	require.EqualValues(t, "game-01", games[0].ID)
	require.EqualValues(t, "game-02", games[1].ID)

	groupGames := r.ListByGroup("g1", TypeGame)
	require.Len(t, groupGames, 2)
	crosses := r.ListByGroup("g1", TypeCross)
	require.Len(t, crosses, 1, fmt.Sprintf("cross node mismatch: %+v", crosses))
	require.EqualValues(t, "1.1.1.3:9300", crosses[0].Addr)

	groups := r.Groups()
	require.Len(t, groups, 1, fmt.Sprintf("groups mismatch: %v", groups))
	require.EqualValues(t, "g1", groups[0])

	_, ok := r.Get("game-01")
	require.True(t, ok, "game-01 should exist")
	_, ok = r.Get("nope")
	require.False(t, ok, "missing node should not be found")
	s, ok := r.SenderFor("cross-01")
	require.True(t, ok, "sender lookup failed")
	require.EqualValues(t, "cross-01", s.NodeID())
}

func TestHeartbeatUpdatesNode(t *testing.T) {
	r := New(time.Second, testLogger{t})
	r.Register(&Node{ID: "game-01", Type: TypeGame}, nil)

	require.False(t, !r.Heartbeat("game-01", 100, 30, 40, 500, "1.0.1"), "heartbeat for registered node should succeed")
	n, _ := r.Get("game-01")
	require.EqualValues(t, 100, n.PlayerNum, fmt.Sprintf("heartbeat fields not applied: %+v", n))
	require.EqualValues(t, 30, n.CPULoad)
	require.EqualValues(t, 40, n.MemLoad)
	require.EqualValues(t, 500, n.MsgQPS)
	require.EqualValues(t, "1.0.1", n.Version)

	// 未注册节点的心跳应返回 false（上层要求补发 Register）
	require.False(t, r.Heartbeat("ghost", 1, 0, 0, 0, ""), "heartbeat for unknown node should return false")
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
	require.Len(t, dead, 1, fmt.Sprintf("expect only game-01 removed, got %+v", dead))
	require.EqualValues(t, "game-01", dead[0].ID)
	_, ok := r.Get("game-01")
	require.False(t, ok, "game-01 should be removed")
	_, ok = r.Get("game-02")
	require.True(t, ok, "game-02 should still exist")
}

func TestUnregisterRemovesSender(t *testing.T) {
	r := New(time.Second, testLogger{t})
	r.Register(&Node{ID: "game-01", Type: TypeGame}, &testSender{"game-01"})

	r.Unregister("game-01")
	_, ok := r.Get("game-01")
	require.False(t, ok, "node should be removed")
	_, ok = r.SenderFor("game-01")
	require.False(t, ok, "sender should be removed")
}
