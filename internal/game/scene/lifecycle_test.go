package scene

import (
	"testing"
	"time"

	"game_study/pkg/actor"
)

// lifeRig 空闲超时很短，便于在测试里触发兜底清理。
func lifeRig(t *testing.T) (*Actor, *actor.System, *actor.PID, map[int64]*collector, map[int64]*actor.PID) {
	t.Helper()
	sc, sys, scPID := newRigWith(t, Config{
		ID: 1, Name: "life", Width: 100, Height: 100, Cell: 10, MaxPlayers: 10,
		IdleTimeout: 40 * time.Millisecond,
	})
	cols := map[int64]*collector{}
	pids := map[int64]*actor.PID{}
	for _, id := range []int64{1, 2} {
		pid, c := spawnCollector(t, sys, "player:"+itoa(id))
		cols[id] = c
		pids[id] = pid
	}
	return sc, sys, scPID, cols, pids
}

func countOf(t *testing.T, sys *actor.System, scPID *actor.PID) int {
	t.Helper()
	reply := make(chan int, 1)
	send(t, sys, scPID, &CountMsg{Reply: reply})
	select {
	case n := <-reply:
		return n
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for count")
		return -1
	}
}

// TestIdleSweepRemovesGhost 心跳超时的实体被清理，并向仍在场的玩家广播 Leave。
//
// 这是「下线通知失败」的兜底：Actor 终止时 Send 可能返回 ErrActorGone，
// 没有兜底就会留下永久幽灵——占名额、别人还看得见、本人重进被挡。
func TestIdleSweepRemovesGhost(t *testing.T) {
	_, sys, scPID, cols, pids := lifeRig(t)

	join(t, sys, scPID, pids[1], cols[1], 1, 5, 5)
	join(t, sys, scPID, pids[2], cols[2], 2, 6, 5)
	if n := countOf(t, sys, scPID); n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}
	drainPushes(cols[2])

	// 超过空闲阈值后扫描：两个都超时（都没发心跳）。
	time.Sleep(60 * time.Millisecond)
	send(t, sys, scPID, &SweepMsg{})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && countOf(t, sys, scPID) != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if n := countOf(t, sys, scPID); n != 0 {
		t.Fatalf("count = %d, want 0（超时应被清理）", n)
	}
}

// TestHeartbeatKeepsAlive 持续心跳的实体不会被清掉。
func TestHeartbeatKeepsAlive(t *testing.T) {
	_, sys, scPID, cols, pids := lifeRig(t)

	join(t, sys, scPID, pids[1], cols[1], 1, 5, 5)

	// 20ms 一次心跳，60ms 时扫描：距上次心跳只有 20ms，不应超时。
	for i := 0; i < 3; i++ {
		time.Sleep(20 * time.Millisecond)
		send(t, sys, scPID, &HeartbeatMsg{PlayerID: 1})
	}
	send(t, sys, scPID, &SweepMsg{})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && countOf(t, sys, scPID) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if n := countOf(t, sys, scPID); n != 1 {
		t.Fatalf("count = %d, want 1（有心跳不该被清）", n)
	}
}

// TestSweepBroadcastsLeaveToRemaining 幽灵被清理时，同场景其他玩家要收到 Leave。
func TestSweepBroadcastsLeaveToRemaining(t *testing.T) {
	_, sys, scPID, cols, pids := lifeRig(t)

	join(t, sys, scPID, pids[1], cols[1], 1, 5, 5)
	join(t, sys, scPID, pids[2], cols[2], 2, 6, 5)
	drainPushes(cols[1])

	// 只让玩家 2 超时（1 持续心跳）。
	send(t, sys, scPID, &HeartbeatMsg{PlayerID: 1})
	time.Sleep(60 * time.Millisecond)
	send(t, sys, scPID, &HeartbeatMsg{PlayerID: 1})
	send(t, sys, scPID, &SweepMsg{})

	types := drainPushes(cols[1])
	if len(types) != 1 || types[0] != 3 {
		t.Fatalf("玩家 1 应收到 1 条 Leave，got %v", types)
	}
}
