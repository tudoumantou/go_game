package scene

import (
	"testing"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// aoiRig 三个玩家、格子 10 的场景：x=5 → 格 0，x=15 → 格 1，x=25 → 格 2。
func aoiRig(t *testing.T) (*Actor, *actor.System, *actor.PID, map[int64]*collector, map[int64]*actor.PID) {
	t.Helper()
	sc, sys, scPID := newRigWith(t, Config{ID: 1, Name: "aoi", Width: 100, Height: 100, Cell: 10, MaxPlayers: 10})

	cols := map[int64]*collector{}
	pids := map[int64]*actor.PID{}
	for _, id := range []int64{1, 2, 3} {
		pid, c := spawnCollector(t, sys, "player:"+itoa(id))
		cols[id] = c
		pids[id] = pid
	}
	return sc, sys, scPID, cols, pids
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// join 让玩家进入场景并消费掉「进入回执 + 可能出现的 Appear 推送」。
func join(t *testing.T, sys *actor.System, scPID *actor.PID, pid *actor.PID, c *collector, id int64, x, y int32) {
	t.Helper()
	send(t, sys, scPID, &EnterMsg{PlayerID: id, PID: pid, Name: "p", X: x, Y: y})
	if r := waitResult(t, c); r == nil {
		t.Fatal("no enter result")
	}
}

// drainPushes 取走收集器里所有推送，返回它们的类型序列。
func drainPushes(c *collector) []int32 {
	var types []int32
	for {
		select {
		case p := <-c.pushes:
			var n gamev1.SceneEntityNotify
			if err := proto.Unmarshal(p.Body, &n); err == nil {
				types = append(types, int32(n.Type))
			}
		case <-time.After(120 * time.Millisecond):
			return types
		}
	}
}

// TestAdjacentCellNoFlicker 相邻格跨格时不能误发 Leave/Appear（AOI 最经典的 bug）。
//
// A 在格 0、B 在格 1，两者相邻可见。A 从格 0 移进格 1 后仍在 B 的视野里，
// 按「格坐标差集」实现会误发一对 Leave/Appear，客户端实体闪烁。
func TestAdjacentCellNoFlicker(t *testing.T) {
	sc, sys, scPID, cols, pids := aoiRig(t)
	_ = sc

	join(t, sys, scPID, pids[1], cols[1], 1, 5, 5)  // 格 0
	join(t, sys, scPID, pids[2], cols[2], 2, 15, 5) // 格 1
	drainPushes(cols[1])                            // 清掉 B 进场带来的 Appear

	// A 从 (5,5) 移到 (13,5)：格 0 → 格 1，与 B 仍互相可见。
	send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 13, Y: 5, Seq: 1})
	waitResult(t, cols[1])

	aPushes := drainPushes(cols[1]) // A 自己收到什么
	bPushes := drainPushes(cols[2]) // B 收到什么

	if len(aPushes) != 0 {
		t.Fatalf("A 不应收到任何 Enter/Leave（仍是同一视野），got types=%v", aPushes)
	}
	// B 只应收到一条「移动」通知。
	if len(bPushes) != 1 || bPushes[0] != 2 {
		t.Fatalf("B 应只收到 1 条移动通知，got types=%v", bPushes)
	}
}

// TestEnterView 移入新格后，新出现在视野里的实体要双向 Appear。
func TestEnterView(t *testing.T) {
	_, sys, scPID, cols, pids := aoiRig(t)

	join(t, sys, scPID, pids[1], cols[1], 1, 5, 5)  // 格 0
	join(t, sys, scPID, pids[3], cols[3], 3, 25, 5) // 格 2：超出 A 的视野（0..1）
	drainPushes(cols[1])

	// A 移到格 1，视野变成 0..2，此时才看到 C。
	send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 15, Y: 5, Seq: 1})
	waitResult(t, cols[1])

	aPushes := drainPushes(cols[1])
	cPushes := drainPushes(cols[3])

	if len(aPushes) != 1 || aPushes[0] != 1 {
		t.Fatalf("A 应收到 1 条 Appear，got types=%v", aPushes)
	}
	if len(cPushes) != 1 || cPushes[0] != 1 {
		t.Fatalf("C 应收到 1 条 Appear，got types=%v", cPushes)
	}
}

// TestLeaveView 移出视野要双向 Leave。
func TestLeaveView(t *testing.T) {
	_, sys, scPID, cols, pids := aoiRig(t)

	join(t, sys, scPID, pids[1], cols[1], 1, 15, 5) // 格 1，视野 0..2
	join(t, sys, scPID, pids[3], cols[3], 3, 25, 5) // 格 2，在 A 视野内
	drainPushes(cols[1])

	// A 移到格 0，视野变成 0..1，看不到 C 了。
	send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 5, Y: 5, Seq: 1})
	waitResult(t, cols[1])

	aPushes := drainPushes(cols[1])
	cPushes := drainPushes(cols[3])

	if len(aPushes) != 1 || aPushes[0] != 3 {
		t.Fatalf("A 应收到 1 条 Leave，got types=%v", aPushes)
	}
	if len(cPushes) != 1 || cPushes[0] != 3 {
		t.Fatalf("C 应收到 1 条 Leave，got types=%v", cPushes)
	}
}

// TestEnterLeaveBalanced 来回移动多次：Appear 与 Leave 必须成对，
// 不能出现「收到了 Appear 却永远不 Leave」的幽灵实体。
func TestEnterLeaveBalanced(t *testing.T) {
	_, sys, scPID, cols, pids := aoiRig(t)

	join(t, sys, scPID, pids[1], cols[1], 1, 5, 5)  // 格 0
	join(t, sys, scPID, pids[3], cols[3], 3, 25, 5) // 格 2
	drainPushes(cols[1])
	drainPushes(cols[3])

	// A 在格 0 与格 1 之间来回 5 次：每次进格 1 看到 C，回格 0 就看不到。
	for i := 0; i < 5; i++ {
		send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 15, Y: 5, Seq: int32(i*2 + 1)})
		waitResult(t, cols[1])
		send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 5, Y: 5, Seq: int32(i*2 + 2)})
		waitResult(t, cols[1])
	}

	types := drainPushes(cols[3])
	var appear, leave int
	for _, tp := range types {
		switch tp {
		case 1:
			appear++
		case 3:
			leave++
		}
	}
	if appear != 5 || leave != 5 {
		t.Fatalf("C 应收到 5 次 Appear / 5 次 Leave，got appear=%d leave=%d (types=%v)", appear, leave, types)
	}
}

// TestThrottleRejectsBurst 同秒内超过配额被拒（原地高频小步不能绕过速度校验）。
func TestThrottleRejectsBurst(t *testing.T) {
	_, sys, scPID, cols, pids := aoiRig(t)

	join(t, sys, scPID, pids[1], cols[1], 1, 5, 5)

	// 配额 12：前 12 次正常（原地不动也算一次），第 13 次被限频。
	var throttled bool
	for i := 0; i < moveQuota+1; i++ {
		send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 5, Y: 5, Seq: int32(i + 1)})
		r := waitResult(t, cols[1])
		var resp gamev1.SceneMoveResp
		if err := proto.Unmarshal(r.Body, &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Code == codeThrottle {
			throttled = true
			break
		}
	}
	if !throttled {
		t.Fatalf("超过 %d 次/秒的移动必须被限频", moveQuota)
	}
}

// TestBroadcastCoversViewOnly 广播量只与视野内人数有关，与场景总人数无关。
//
// 10 人分散在 5 列 x 2 行的格子里，某个玩家移动时只需推给视野内的 3 人，
// 而不是全场景 9 人——这是 AOI 相对全量广播的实际收益。
func TestBroadcastCoversViewOnly(t *testing.T) {
	sc, sys, scPID := newRigWith(t, Config{ID: 1, Name: "aoi", Width: 100, Height: 100, Cell: 10, MaxPlayers: 16})

	positions := [][2]int32{
		{5, 5}, {15, 5}, {25, 5}, {35, 5}, {45, 5},
		{5, 15}, {15, 15}, {25, 15}, {35, 15}, {45, 15},
	}
	cols := make([]*collector, len(positions))
	for i, p := range positions {
		id := int64(i + 1)
		pid, c := spawnCollector(t, sys, "player:"+itoa(id))
		cols[i] = c
		join(t, sys, scPID, pid, c, id, p[0], p[1])
	}
	if n := sc.Count(); n != len(positions) {
		t.Fatalf("count = %d, want %d", n, len(positions))
	}
	for _, c := range cols {
		drainPushes(c)
	}

	// 玩家 1 在 (5,5)，视野覆盖格 (0,0)-(1,1)，即 4 人（含自己）。
	send(t, sys, scPID, &MoveMsg{PlayerID: 1, X: 8, Y: 8, Seq: 1})
	waitResult(t, cols[0])

	total := 0
	for _, c := range cols {
		total += len(drainPushes(c))
	}
	// 视野内其他人 = 3，全场景其他人 = 9。
	if total != 3 {
		t.Fatalf("广播数 = %d, want 3（视野内 3 人，不是全场景 9 人）", total)
	}
}
