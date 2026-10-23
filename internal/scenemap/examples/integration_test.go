package examples

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/entity"
	"game_study/internal/scenemap/gameplay"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// 断言辅助
//
// 所有"等待"都必须带超时：裸 time.Sleep 会让测试在慢机器上假绿、在快机器上
// 假红（-race 下调度延迟会被放大）。轮询 + 截止时间是可重复的最小做法。
// ============================================================================

// waitFor 轮询直到 cond 成立，超时则 Fatal。
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		require.False(t, time.Now().After(deadline), fmt.Sprintf("等待超时(%s): %s", timeout, what))
		time.Sleep(2 * time.Millisecond)
	}
}

// neverFor 在一个时间窗内持续轮询，只要 cond 曾经成立就 Fatal。
//
// 用来断言"不应该发生的事"——例如"校验失败的命令不应产生广播"。
// 这类断言必须有窗口：直接断言瞬间值，可能只是消息还没投递到。
func neverFor(t *testing.T, window time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		require.False(t, cond(), fmt.Sprintf("不应发生: %s", what))
		time.Sleep(2 * time.Millisecond)
	}
}

// cellPX / cellPY 把行走格换算成格中心像素（AOI 用的是像素）。
func cellPX(h *Harness, x int32) int32 { return x*h.Geo.CellW + h.Geo.CellW/2 }
func cellPY(h *Harness, y int32) int32 { return y*h.Geo.CellH + h.Geo.CellH/2 }

const (
	playerA scenemap.UUID = 1001
	playerB scenemap.UUID = 1002
)

// ============================================================================
// P0 验收一：视野（出现/移动/阻挡/消失）
// ============================================================================

func TestAOIAppearMoveBlockedLeave(t *testing.T) {
	// 把 (7,1) 设成墙：A 后面要用它验证"校验失败不广播"。
	h := NewHarness(t, 1001, 1,
		WithSize(32, 32),
		WithBlocked(scenemap.GridPos{X: 7, Y: 1}),
		WithEnterProtect(10*time.Second),
		WithTickInterval(50*time.Millisecond),
	)
	sink := h.Sink

	t.Run("九宫格外互不可见", func(t *testing.T) {
		err := h.EnterPlayer(playerA, scenemap.GridPos{X: 0, Y: 0})
		require.NoError(t, err, fmt.Sprintf("A 进图: %v", err))
		// 视野格 = 行走格 / 6：A 在视野格 0，B 在视野格 2，中间隔了一格。
		err = h.EnterPlayer(playerB, scenemap.GridPos{X: 12, Y: 0})
		require.NoError(t, err, fmt.Sprintf("B 进图: %v", err))

		// 等两边的"进图全量包"都到齐，说明两次 enter 都已被 AOI 处理完
		// （AOI 单 goroutine 串行，处理完 enter 才会处理后面的命令）。
		waitFor(t, 2*time.Second, "双方收到进图全量包", func() bool {
			return sink.Count(playerA) >= 1 && sink.Count(playerB) >= 1
		})
		neverFor(t, 100*time.Millisecond, "视野外却收到了出现包", func() bool {
			return sink.AppearCount(playerA, playerB) > 0 || sink.AppearCount(playerB, playerA) > 0
		})
	})

	t.Run("走进九宫格双向出现", func(t *testing.T) {
		// B 从行走格 12 走到 10：视野格 2 → 1，与 A 的视野格 0 相邻。
		err := h.MovePlayer(playerB, cellPX(h, 10), cellPY(h, 0), scenemap.DirLeft)
		require.NoError(t, err, fmt.Sprintf("B 移动: %v", err))
		waitFor(t, 2*time.Second, "双向收到出现包", func() bool {
			return sink.AppearCount(playerA, playerB) > 0 && sink.AppearCount(playerB, playerA) > 0
		})
	})

	t.Run("移动包只发给别人", func(t *testing.T) {
		// A 从 0 走到 6（跨视野格 0 → 1）。分三步走，每步 2 格，
		// 不超过 MGR 的 MaxStep=3；同格/同视野格的中间步不会产生移动包。
		for _, x := range []int32{2, 4, 6} {
			err := h.MovePlayer(playerA, cellPX(h, x), cellPY(h, 0), scenemap.DirRight)
			require.NoError(t, err, fmt.Sprintf("A 移动到 %d: %v", x, err))
		}
		waitFor(t, 2*time.Second, "B 收到 A 的移动包", func() bool {
			return sink.MovesFrom(playerB, playerA) > 0
		})
		// AOI 的移动广播必须排除发送者自己。
		neverFor(t, 100*time.Millisecond, "A 收到了自己的移动包", func() bool {
			return sink.MovesFrom(playerA, playerA) > 0
		})
	})

	t.Run("阻挡格被拒且不广播", func(t *testing.T) {
		before := sink.MovesFrom(playerB, playerA)
		// A 在 (6,0)，目标是墙 (7,1)：切比雪夫距离 1，不会先被 MaxStep 拦下，
		// 因此命中的一定是 Walkable 校验 → ErrBlocked。
		err := h.MovePlayer(playerA, cellPX(h, 7), cellPY(h, 1), scenemap.DirDownRight)
		require.False(t, !errors.Is(err, scenemap.ErrBlocked), fmt.Sprintf("期望 ErrBlocked, got %v", err))
		// 顺序要求：先校验、后广播。校验失败时一个包都不能发。
		neverFor(t, 100*time.Millisecond, "校验失败的移动仍然被广播", func() bool {
			return sink.MovesFrom(playerB, playerA) != before
		})
	})

	t.Run("离图广播消失包", func(t *testing.T) {
		err := h.Leave(playerA, scenemap.LeaveReasonQuit)
		require.NoError(t, err, fmt.Sprintf("A 离图: %v", err))
		waitFor(t, 2*time.Second, "B 收到 A 的消失包", func() bool {
			return sink.LeaveCount(playerB, playerA) > 0
		})
	})
}

// ============================================================================
// P0 验收二：空房按 EnterProtect 自动关，关图走 RoomManager
// ============================================================================

func TestEmptyRoomClosesAfterEnterProtect(t *testing.T) {
	h := NewHarness(t, 1101, 1,
		WithSize(16, 16),
		WithEnterProtect(100*time.Millisecond),
		WithTickInterval(20*time.Millisecond),
	)

	err := h.EnterPlayer(playerA, scenemap.GridPos{X: 1, Y: 1})
	require.NoError(t, err, fmt.Sprintf("进图: %v", err))
	err = h.Leave(playerA, scenemap.LeaveReasonQuit)
	require.NoError(t, err, fmt.Sprintf("离图: %v", err))

	// 关图通知由 MGR 发出，再由装配层转给 RoomManager 收口。
	require.False(t, !h.WaitRoomClosed(3*time.Second), "空房没有在 EnterProtect 后关闭")
	// 收口完成之后，登记表里必须查不到这个房间（不能留下"已关但还登记着"）。
	waitFor(t, 3*time.Second, "RoomManager 摘除房间", func() bool {
		_, ok := h.Room.Get(h.Key)
		return !ok
	})
}

// ============================================================================
// P0 验收三：副本波次 → 通关（OnClear 一次）
// ============================================================================

// dungeonSnap 在 MGR goroutine 里读副本进度（不能直接读，*State 没有锁）。
type dungeonSnap struct {
	wave    int32
	killed  int32
	cleared bool
	npes    int
}

func readDungeon(t *testing.T, h *Harness, d *gameplay.Dungeon) dungeonSnap {
	t.Helper()
	v, err := h.Read(func(s *mgr.State) any {
		return dungeonSnap{wave: d.CurrentWave, killed: d.Killed, cleared: d.Cleared, npes: len(s.NPES)}
	})
	require.NoError(t, err, "读副本状态")
	return v.(dungeonSnap)
}

// killAllNPE 通过 MGR 命令杀死图里所有怪。
//
// notify=true 时走完整的死亡链（Damage → Despawn → HandleNpcDead）；
// notify=false 只把怪移出，模拟"召唤到期/GM 清场"这类**没有死亡回调**的
// 消失路径，用来验证玩法 Loop 的兜底推进。
func killAllNPE(t *testing.T, h *Harness, d *gameplay.Dungeon, notify bool) {
	t.Helper()
	cmd := mgr.NewFunc("test:kill-all", func(s *mgr.State) (any, error) {
		for _, id := range s.NpeUUIDs() {
			info := s.NPES[id]
			if info == nil || !info.Alive() {
				continue
			}
			npcID := info.TemplateID
			x, y := info.GridPos.X, info.GridPos.Y
			// 致死 + 移出 + 死亡回调，三步就是 MGR 死亡链的最小形态。
			if !entity.Damage(s, id, 1<<40, 0) {
				continue
			}
			if _, ok := entity.Despawn(s, id); !ok {
				continue
			}
			if notify {
				_ = d.HandleNpcDead(npcID, id, x, y, s)
			}
		}
		return nil, nil
	})
	// 空图时这条命令什么都不改，允许无效果。
	cmd.AllowNoop = true
	_, err := h.Call(cmd)
	require.NoError(t, err, fmt.Sprintf("清怪: %v", err))
}

func TestDungeonWavesAndClearOnce(t *testing.T) {
	d := gameplay.NewDungeon(2, 2, 9001, []scenemap.GridPos{
		{X: 3, Y: 3}, {X: 4, Y: 3}, {X: 5, Y: 3}, {X: 3, Y: 4},
	})
	clear := make(chan struct{}, 4)
	d.OnClear = func(*mgr.State) { clear <- struct{}{} }

	// TickInterval 设成 1 小时：这张用例要"手动驱动"每一拍（CommandTick），
	// 让 Loop 的调用时机完全确定；否则后台 timer 会和测试抢着推进波次。
	h := NewHarness(t, 1201, 1,
		WithSize(20, 20),
		WithGameplay(d),
		WithEnterProtect(30*time.Second),
		WithTickInterval(time.Hour),
	)

	// MgrInit 里已经刷出第一波（不等第一拍 loop）。
	snap := readDungeon(t, h, d)
	require.EqualValues(t, 1, snap.wave, fmt.Sprintf("第一波未刷出: %+v", snap))
	require.EqualValues(t, 2, snap.npes)

	// 第一波用"非死亡路径"清掉：没有 HandleNpcDead，只能靠 Loop 发现空波。
	killAllNPE(t, h, d, false)
	snap = readDungeon(t, h, d)
	require.EqualValues(t, 0, snap.npes, fmt.Sprintf("第一波未清空: %+v", snap))
	// 手动跑一拍 loop：波次检查发现空波，推进到第二波并立刻刷怪。
	err := h.Tick()
	require.NoError(t, err, fmt.Sprintf("Tick: %v", err))
	snap = readDungeon(t, h, d)
	require.EqualValues(t, 2, snap.wave, fmt.Sprintf("Loop 未推进到第二波: %+v", snap))
	require.EqualValues(t, 2, snap.npes)
	require.False(t, snap.cleared || len(clear) != 0, fmt.Sprintf("未打完却已通关: %+v", snap))

	killAllNPE(t, h, d, true)
	waitFor(t, 2*time.Second, "副本通关", func() bool {
		return readDungeon(t, h, d).cleared
	})
	// 消费掉那次回调：len(chan) 只能"看"，不会把值取走，
	// 不消费的话下面"不应再触发"的断言会误判成重复调用。
	select {
	case <-clear:
	case <-time.After(time.Second):
		t.Fatal("OnClear 未被调用")
	}
	n := len(clear)
	require.EqualValues(t, 0, n, fmt.Sprintf("OnClear 应只调用 1 次, 剩余 %d 次", n))

	// 再推几拍：Loop/死亡回调都不许重复结算。
	for i := 0; i < 3; i++ {
		err := h.Tick()
		require.NoError(t, err, fmt.Sprintf("Tick: %v", err))
	}
	select {
	case <-clear:
		t.Fatal("OnClear 被重复调用")
	default:
	}
	// 只有第二波走了死亡回调，所以击杀数是 2（第一波是"非死亡路径"清的）。
	snap = readDungeon(t, h, d)
	require.EqualValues(t, 2, snap.killed, fmt.Sprintf("累计击杀应为 2, got %d", snap.killed))
}

// ============================================================================
// P0 验收四：主城空房不关
// ============================================================================

func TestCityRoomStaysOpenWhenEmpty(t *testing.T) {
	city := gameplay.NewCity(1301)
	h := NewHarness(t, 1301, 1,
		WithSize(16, 16),
		WithGameplay(city),
		// 用 100ms 的极短保护期：如果主城会关，这里一定关得掉。
		WithEnterProtect(100*time.Millisecond),
		WithTickInterval(20*time.Millisecond),
	)

	err := h.EnterPlayer(playerA, scenemap.GridPos{X: 1, Y: 1})
	require.NoError(t, err, fmt.Sprintf("进图: %v", err))
	require.EqualValues(t, 1, city.Players())
	err = h.Leave(playerA, scenemap.LeaveReasonQuit)
	require.NoError(t, err, fmt.Sprintf("离图: %v", err))

	// 玩法说"不能关"，MGR 必须把 CloseTime 归零：空图也长期存在。
	neverFor(t, 500*time.Millisecond, "主城空房被关闭", func() bool {
		_, ok := h.Room.Get(h.Key)
		return !ok
	})
	if v, err := h.Read(func(s *mgr.State) any { return city.CanCheckStopMap(s) }); err != nil {
		t.Fatalf("读状态: %v", err)
	} else if v.(bool) {
		t.Fatal("主城 CanCheckStopMap 应为 false")
	}
	require.False(t, city.AllowSkill(1301), "主城不应结算技能")
}

// ============================================================================
// 编译期小检查：默认几何与用例里的坐标换算保持一致
// ============================================================================

func TestHarnessGeometryDefaults(t *testing.T) {
	h := NewHarness(t, 1401, 1, WithSize(8, 8), WithEnterProtect(time.Minute))
	require.EqualValues(t, grid.DefaultGeometry(), h.Geo)
	require.NotNil(t, h.AOI, "装配不完整：AOI/MGR 不能为 nil")
	require.NotNil(t, h.Mgr)
	require.EqualValues(t, 1*48+24, cellPX(h, 1))
}
