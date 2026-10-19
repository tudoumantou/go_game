package scene

import (
	"context"
	"testing"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/log"
	"game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// collector 收集场景 Actor 发给玩家的消息。
type collector struct {
	msgs chan any
}

func (c *collector) Receive(ctx *actor.Context) {
	c.msgs <- ctx.Message()
}

func newRig(t *testing.T) (*Actor, *actor.System, *actor.PID) {
	t.Helper()
	sys := actor.NewSystem(actor.Config{ShardCount: 4, MailboxSize: 256})
	ctx, cancel := context.WithCancel(context.Background())
	sys.Start(ctx)
	t.Cleanup(func() {
		cancel()
		sys.Stop()
	})

	sc := NewActor(Config{ID: 1, Name: "test", Width: 100, Height: 100, Cell: 10, MaxPlayers: 2}, sys, log.New("test", log.WithLevel("error")))
	pid, err := sys.Spawn("scene:1", sc)
	if err != nil {
		t.Fatalf("spawn scene: %v", err)
	}
	return sc, sys, pid
}

// send 通过真实 System 投递：保证与 Actor 内部执行有 happens-before 关系。
func send(t *testing.T, sys *actor.System, pid *actor.PID, msg any) {
	t.Helper()
	if err := sys.Send(pid, msg); err != nil {
		t.Fatalf("send %T: %v", msg, err)
	}
}

func spawnCollector(t *testing.T, sys *actor.System, id string) (*actor.PID, *collector) {
	t.Helper()
	c := &collector{msgs: make(chan any, 32)}
	pid, err := sys.Spawn(id, c)
	if err != nil {
		t.Fatalf("spawn collector: %v", err)
	}
	return pid, c
}

func waitResult(t *testing.T, c *collector) *player.SceneResult {
	t.Helper()
	select {
	case m := <-c.msgs:
		r, ok := m.(*player.SceneResult)
		if !ok {
			t.Fatalf("expected SceneResult, got %T", m)
		}
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for scene result")
		return nil
	}
}

func waitPush(t *testing.T, c *collector) *player.ScenePush {
	t.Helper()
	select {
	case m := <-c.msgs:
		p, ok := m.(*player.ScenePush)
		if !ok {
			t.Fatalf("expected ScenePush, got %T", m)
		}
		return p
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for scene push")
		return nil
	}
}

func decodeEnter(t *testing.T, r *player.SceneResult) *gamev1.SceneEnterResp {
	t.Helper()
	var resp gamev1.SceneEnterResp
	if err := proto.Unmarshal(r.Body, &resp); err != nil {
		t.Fatalf("decode enter resp: %v", err)
	}
	return &resp
}

// TestEnterClampsAndSnapshots 进入被钳制到地图内，回执带全量视野。
func TestEnterClampsAndSnapshots(t *testing.T) {
	_, sys, scPID := newRig(t)
	pid, c := spawnCollector(t, sys, "player:1")

	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: pid, Name: "a", Level: 1, X: -5, Y: 999})
	r := waitResult(t, c)
	resp := decodeEnter(t, r)
	if resp.Code != codeOK {
		t.Fatalf("code = %d", resp.Code)
	}
	if resp.X != 0 || resp.Y != 99 {
		t.Fatalf("clamped = (%d,%d), want (0,99)", resp.X, resp.Y)
	}
	if !r.SetScene {
		t.Fatal("first successful enter must set scene")
	}
}

// TestEnterFull 人数上限：第 3 个玩家被拒，且不占用名额。
func TestEnterFull(t *testing.T) {
	sc, sys, scPID := newRig(t)
	p1, c1 := spawnCollector(t, sys, "player:1")
	p2, c2 := spawnCollector(t, sys, "player:2")
	p3, c3 := spawnCollector(t, sys, "player:3")

	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: p1, Name: "a", Level: 1, X: 10, Y: 10})
	waitResult(t, c1)
	send(t, sys, scPID, &EnterMsg{PlayerID: 2, PID: p2, Name: "b", Level: 1, X: 20, Y: 20})
	waitResult(t, c2)

	send(t, sys, scPID, &EnterMsg{PlayerID: 3, PID: p3, Name: "c", Level: 1, X: 30, Y: 30})
	resp := decodeEnter(t, waitResult(t, c3))
	if resp.Code != codeFull {
		t.Fatalf("code = %d, want %d", resp.Code, codeFull)
	}
	if sc.Count() != 2 {
		t.Fatalf("count = %d, want 2（被拒的玩家不能占位）", sc.Count())
	}
}

// TestEnterTwiceDoesNotDoubleCount 重连/重发不重复占名额。
func TestEnterTwiceDoesNotDoubleCount(t *testing.T) {
	sc, sys, scPID := newRig(t)
	p1, c1 := spawnCollector(t, sys, "player:1")

	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: p1, X: 10, Y: 10})
	waitResult(t, c1)
	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: p1, X: 12, Y: 12})
	r := waitResult(t, c1)
	if sc.Count() != 1 {
		t.Fatalf("count = %d, want 1", sc.Count())
	}
	if r.SetScene {
		t.Fatal("re-enter must not re-set scene（避免无意义的标脏）")
	}
}

// TestMoveRejectsTeleport 超出速度上限不移动，回权威坐标。
func TestMoveRejectsTeleport(t *testing.T) {
	sc, sys, scPID := newRig(t)
	p1, c1 := spawnCollector(t, sys, "player:1")
	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: p1, X: 10, Y: 10})
	waitResult(t, c1)

	send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 90, Y: 90, Seq: 1})
	r := waitResult(t, c1)
	var resp gamev1.SceneMoveResp
	_ = proto.Unmarshal(r.Body, &resp)
	if resp.Code != codeTooFast {
		t.Fatalf("code = %d, want %d", resp.Code, codeTooFast)
	}
	if resp.X != 10 || resp.Y != 10 {
		t.Fatalf("authority = (%d,%d), want (10,10)", resp.X, resp.Y)
	}
	if e := sc.ents[1]; e.x != 10 || e.y != 10 {
		t.Fatalf("entity moved despite rejection: (%d,%d)", e.x, e.y)
	}
}

// TestMoveBroadcastsToOthers 移动广播给场景内其他玩家，不回给自己。
func TestMoveBroadcastsToOthers(t *testing.T) {
	_, sys, scPID := newRig(t)
	p1, c1 := spawnCollector(t, sys, "player:1")
	p2, c2 := spawnCollector(t, sys, "player:2")

	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: p1, Name: "a", X: 10, Y: 10})
	waitResult(t, c1)
	send(t, sys, scPID, &EnterMsg{PlayerID: 2, PID: p2, Name: "b", X: 20, Y: 20})
	waitResult(t, c2)
	waitPush(t, c1) // 玩家 2 进场，玩家 1 收到 Appear

	// 原地不动不广播：c1 不应再收到任何消息。
	send(t, sys, scPID, &MoveMsg{PlayerID: 2, X: 20, Y: 20, Seq: 1})
	waitResult(t, c2)
	select {
	case m := <-c1.msgs:
		t.Fatalf("stationary move must not broadcast, got %T", m)
	case <-time.After(150 * time.Millisecond):
	}

	// 真正移动才广播。
	send(t, sys, scPID, &MoveMsg{PlayerID: 2, X: 22, Y: 20, Seq: 2})
	waitResult(t, c2)
	push := waitPush(t, c1)
	var notify gamev1.SceneEntityNotify
	if err := proto.Unmarshal(push.Body, &notify); err != nil {
		t.Fatal(err)
	}
	if notify.Type != 2 || notify.Entity.PlayerId != 2 || notify.Entity.X != 22 {
		t.Fatalf("notify = %s", notify.String())
	}
}

// TestLeaveIsIdempotent 离开幂等；离开后广播 Leave。
func TestLeaveIsIdempotent(t *testing.T) {
	sc, sys, scPID := newRig(t)
	p1, c1 := spawnCollector(t, sys, "player:1")
	p2, c2 := spawnCollector(t, sys, "player:2")

	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: p1, X: 10, Y: 10})
	waitResult(t, c1)
	send(t, sys, scPID, &EnterMsg{PlayerID: 2, PID: p2, X: 20, Y: 20})
	waitResult(t, c2)
	waitPush(t, c1)

	send(t, sys, scPID, &LeaveMsg{PlayerID: 2})
	push := waitPush(t, c1)
	var notify gamev1.SceneEntityNotify
	_ = proto.Unmarshal(push.Body, &notify)
	if notify.Type != 3 || notify.Entity.PlayerId != 2 {
		t.Fatalf("leave notify = %s", notify.String())
	}

	// 重复离开：不报错、不广播。
	send(t, sys, scPID, &LeaveMsg{PlayerID: 2})
	if sc.Count() != 1 {
		t.Fatalf("count = %d, want 1", sc.Count())
	}
	select {
	case m := <-c1.msgs:
		t.Fatalf("duplicate leave must not broadcast, got %T", m)
	case <-time.After(150 * time.Millisecond):
	}
}

// TestSyncReturnsFullView 全量同步返回当前坐标与视野内所有实体。
func TestSyncReturnsFullView(t *testing.T) {
	_, sys, scPID := newRig(t)
	p1, c1 := spawnCollector(t, sys, "player:1")
	p2, c2 := spawnCollector(t, sys, "player:2")

	send(t, sys, scPID, &EnterMsg{PlayerID: 1, PID: p1, X: 10, Y: 10})
	waitResult(t, c1)
	send(t, sys, scPID, &EnterMsg{PlayerID: 2, PID: p2, X: 20, Y: 20})
	waitResult(t, c2)
	waitPush(t, c1)

	send(t, sys, scPID, &SyncMsg{PlayerID: 1, PID: p1})
	r := waitResult(t, c1)
	var resp gamev1.SceneSyncResp
	if err := proto.Unmarshal(r.Body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != codeOK || resp.X != 10 || len(resp.Entities) != 1 || resp.Entities[0].PlayerId != 2 {
		t.Fatalf("sync resp = %s", resp.String())
	}
}
