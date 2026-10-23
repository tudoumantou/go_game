package mgr

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fmt"
	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// 测试依赖（不 import aoi/entity/gameplay 任何具体包，只实现接口）
// ============================================================================

type syncEvent struct {
	uuid   scenemap.UUID
	oldPos scenemap.GridPos
	newPos scenemap.GridPos
}

// stubGameplay 是玩法桩：记录调用次数，canStop 可切换。
type stubGameplay struct {
	Unimplemented

	loops          atomic.Int64
	enters         atomic.Int64
	leaves         atomic.Int64
	terminates     atomic.Int64
	canStop        atomic.Bool
	initErr        error
	playerLeaveErr error

	mu    sync.Mutex
	syncs []syncEvent
}

func newStubGameplay() *stubGameplay {
	g := &stubGameplay{}
	g.canStop.Store(true)
	return g
}

func (g *stubGameplay) MgrInit(*State, ...any) error { return g.initErr }

func (g *stubGameplay) Loop(*State) error {
	g.loops.Add(1)
	return nil
}

func (g *stubGameplay) PlayerEnter(scenemap.UUID, *State) error {
	g.enters.Add(1)
	return nil
}

func (g *stubGameplay) PlayerLeave(scenemap.UUID, scenemap.LeaveReason, *State) error {
	g.leaves.Add(1)
	return g.playerLeaveErr
}

func (g *stubGameplay) EntitySync(uuid scenemap.UUID, oldPos, newPos scenemap.GridPos, _ *State) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.syncs = append(g.syncs, syncEvent{uuid: uuid, oldPos: oldPos, newPos: newPos})
	return nil
}

func (g *stubGameplay) CanCheckStopMap(*State) bool { return g.canStop.Load() }

func (g *stubGameplay) OnTerminate(*State) error {
	g.terminates.Add(1)
	return nil
}

func (g *stubGameplay) syncEvents() []syncEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]syncEvent, len(g.syncs))
	copy(out, g.syncs)
	return out
}

// stubAOI 记录 AOI 调用。
type stubAOI struct {
	mu         sync.Mutex
	enters     int
	syncs      int
	leaves     int
	broadcasts int
	lastEntity AOIEntity
	lastScope  BroadcastScope
}

func (a *stubAOI) Enter(_ context.Context, e AOIEntity) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.enters++
	a.lastEntity = e
	return nil
}

func (a *stubAOI) Sync(_ context.Context, _ scenemap.UUID, _, _ int32, _ scenemap.Dir, _ int32) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.syncs++
	return nil
}

func (a *stubAOI) Leave(_ context.Context, _ scenemap.UUID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.leaves++
	return nil
}

func (a *stubAOI) Broadcast(_ any, scope BroadcastScope) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.broadcasts++
	a.lastScope = scope
	return nil
}

func (a *stubAOI) counts() (enters, syncs, leaves, broadcasts int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.enters, a.syncs, a.leaves, a.broadcasts
}

// fakeClock 是可手动推进的时钟（unix 毫秒）。
type fakeClock struct{ v atomic.Int64 }

func newFakeClock(base int64) *fakeClock {
	c := &fakeClock{}
	c.v.Store(base)
	return c
}

func (c *fakeClock) Now() int64              { return c.v.Load() }
func (c *fakeClock) Advance(d time.Duration) { c.v.Add(d.Milliseconds()) }

// closeWatch 记录 OnClose 调用。
type closeWatch struct {
	n atomic.Int32

	mu      sync.Mutex
	reasons []string
	keys    []scenemap.MapKey
}

func (w *closeWatch) onClose(key scenemap.MapKey, reason string) {
	w.n.Add(1)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.keys = append(w.keys, key)
	w.reasons = append(w.reasons, reason)
}

func (w *closeWatch) count() int { return int(w.n.Load()) }

func (w *closeWatch) lastReason() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.reasons) == 0 {
		return ""
	}
	return w.reasons[len(w.reasons)-1]
}

// ============================================================================
// 假定时器（确定性驱动）
// ============================================================================

// fakeTimers 是 Timer 的测试实现：不消耗真实时间，每一拍由测试显式投递。
//
// 为什么非要这样：断言"2 倍速时 loop 频率也翻倍"如果靠真实时钟，只能写成
// "150ms 内 fast 约是 slow 的两倍"这种区间断言。CI 机器一抖动就落在区间外，
// 这类失败不是代码错了，是测试在测机器。假定时器让拍数成为确定的整数。
type fakeTimers struct {
	mu      sync.Mutex
	created []*fakeTimer
}

func (f *fakeTimers) New(iv time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	ft := &fakeTimer{ch: make(chan time.Time), iv: iv}
	f.created = append(f.created, ft)
	return ft
}

// waitArmed 等待 startTimers 把四个定时器创建出来。
func (f *fakeTimers) waitArmed(t *testing.T) {
	t.Helper()
	require.True(t, waitUntil(t, 2*time.Second, func() bool { return f.at(3) != nil }), "定时器未创建")
}

// at 按创建顺序取定时器：startTimers 固定是 loop, buff, oneSec, halfSec。
func (f *fakeTimers) at(i int) *fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i >= len(f.created) {
		return nil
	}
	return f.created[i]
}

func (f *fakeTimers) loop() *fakeTimer { return f.at(0) }

// fakeTimer 是单个假定时器。
type fakeTimer struct {
	ch chan time.Time

	mu      sync.Mutex
	iv      time.Duration
	stopped bool
}

func (f *fakeTimer) C() <-chan time.Time { return f.ch }

func (f *fakeTimer) SetInterval(iv time.Duration) {
	f.mu.Lock()
	f.iv = iv
	f.mu.Unlock()
}

// Reset 是空实现：下一拍什么时候来由测试决定，不存在"排期"。
func (f *fakeTimer) Reset() {}

func (f *fakeTimer) Stop() {
	f.mu.Lock()
	f.stopped = true
	f.mu.Unlock()
}

func (f *fakeTimer) interval() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.iv
}

// tick 投递一拍。
//
// ch 是无缓冲的：发送会一直阻塞到主循环接收。主循环的顺序是
// 接收 → 处理 → Reset → 回 select，所以每次 tick 返回时上一拍已经处理完，
// 测试与 MGR 天然同步，不需要任何 sleep。
func (f *fakeTimer) tick(t *testing.T) {
	t.Helper()
	select {
	case f.ch <- time.Time{}:
	case <-time.After(2 * time.Second):
		t.Fatal("定时器未被消费（MGR 主循环可能已停止）")
	}
}

// ============================================================================
// 测试环境装配
// ============================================================================

type testEnv struct {
	m        *Mgr
	gp       *stubGameplay
	aoi      *stubAOI
	clock    *fakeClock
	closed   *closeWatch
	walkable atomic.Value // WalkableFunc 的间接层，便于中途改
}

func baseConfig() Config {
	return Config{
		Key:          scenemap.MapKey{MapID: 1, CliKey: "test"},
		Geo:          grid.DefaultGeometry(),
		Size:         grid.Size{Width: 64, Height: 64},
		MailboxSize:  4096,
		TickInterval: 5 * time.Millisecond,
		EnterProtect: time.Hour,
		OfflineGrace: time.Hour,
	}
}

// newEnv 构造并启动一个 MGR。mut 用于在构造前调整配置/依赖。
func newEnv(t *testing.T, cfg Config, mut func(*Config, *Deps)) *testEnv {
	t.Helper()

	gp := newStubGameplay()
	aoi := &stubAOI{}
	watch := &closeWatch{}
	clock := newFakeClock(1_700_000_000_000)

	deps := Deps{
		AOI:      aoi,
		Gameplay: gp,
		Logger:   NopLogger{},
		Clock:    clock.Now,
		OnClose:  watch.onClose,
	}
	if mut != nil {
		mut(&cfg, &deps)
	}
	// 允许 mut 覆盖 Gameplay/OnClose：重新取一遍，保证测试拿到正确句柄。
	if deps.Gameplay != nil {
		if g, ok := deps.Gameplay.(*stubGameplay); ok {
			gp = g
		}
	}
	if deps.AOI != nil {
		if a, ok := deps.AOI.(*stubAOI); ok {
			aoi = a
		}
	}

	m := New(cfg, deps)
	env := &testEnv{m: m, gp: gp, aoi: aoi, clock: clock, closed: watch}
	m.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = m.Stop(ctx)
	})
	return env
}

func (e *testEnv) call(t *testing.T, cmd Command) (any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return e.m.Call(ctx, cmd)
}

func (e *testEnv) mustCall(t *testing.T, cmd Command) any {
	t.Helper()
	v, err := e.call(t, cmd)
	require.NoError(t, err)
	return v
}

// snap 是一次状态快照（全部在 MGR goroutine 里读，天然一致）。
type snap struct {
	users     int
	npes      int
	pets      int
	bullets   int
	closeTime int64
	revision  uint64
	pos       scenemap.GridPos
	pixel     scenemap.PixelPos
	dir       scenemap.Dir
	anim      int32
	blockCnt  int
}

func (e *testEnv) snapshot(t *testing.T, uuid scenemap.UUID) snap {
	t.Helper()
	v := e.mustCall(t, &FuncCommand{CmdName: "snapshot", AllowNoop: true, Fn: func(s *State) (any, error) {
		out := snap{
			users:     len(s.Users),
			npes:      len(s.NPES),
			pets:      len(s.Pets),
			bullets:   len(s.Bullets),
			closeTime: s.CloseTime,
			revision:  s.Revision,
		}
		switch {
		case s.Users[uuid] != nil:
			u := s.Users[uuid]
			out.pos, out.pixel, out.dir, out.anim = u.GridPos, u.Pixel, u.Dir, u.State
		case s.NPES[uuid] != nil:
			n := s.NPES[uuid]
			out.pos, out.pixel, out.dir = n.GridPos, n.Pixel, n.Dir
		default:
			if p, ok := s.Index.At(uuid); ok {
				out.pos = p
			}
		}
		out.blockCnt = s.Index.BlockCount(out.pos)
		return out, nil
	}})
	return v.(snap)
}

func cellCenter(geo grid.Geometry, p scenemap.GridPos) scenemap.PixelPos {
	return geo.CellCenter(p)
}

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// ============================================================================
// 进图 / 离图 / 关图计时
// ============================================================================

func TestEnterPlayerUpdatesIndexAndUsers(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	geo := grid.DefaultGeometry()
	pos := scenemap.GridPos{X: 5, Y: 5}

	reply := env.mustCall(t, CommandEnterPlayer{
		UUID:     1001,
		GridPos:  pos,
		Dir:      scenemap.DirRight,
		CharName: "alice",
	})
	er, ok := reply.(EnterReply)
	require.True(t, ok)
	require.EqualValues(t, pos, er.GridPos)

	s := env.snapshot(t, 1001)
	require.EqualValues(t, 1, s.users)
	require.EqualValues(t, pos, s.pos)
	require.EqualValues(t, cellCenter(geo, pos), s.pixel)
	require.EqualValues(t, 0, s.closeTime)
	got := env.gp.enters.Load()
	require.EqualValues(t, 1, got, fmt.Sprintf("PlayerEnter 调用 %d 次, 期望 1", got))
	enters, _, _, _ := env.aoi.counts()
	require.EqualValues(t, 1, enters, fmt.Sprintf("AOI Enter 调用 %d 次, 期望 1", enters))

	// 重复进图必须被拒。
	_, err := env.call(t, CommandEnterPlayer{UUID: 1001, GridPos: pos})
	require.ErrorIs(t, err, scenemap.ErrAlreadyExists, "重复进图错误 = %v, 期望 ErrAlreadyExists", err)
}

func TestPlayerLeaveOfflineSetsGrace(t *testing.T) {
	cfg := baseConfig()
	cfg.OfflineGrace = 10 * time.Second
	env := newEnv(t, cfg, nil)

	env.mustCall(t, CommandEnterPlayer{UUID: 2001, GridPos: scenemap.GridPos{X: 3, Y: 3}})
	env.mustCall(t, CommandLeave{UUID: 2001, Reason: scenemap.LeaveReasonOffline})

	s := env.snapshot(t, 2001)
	require.EqualValues(t, 0, s.users)
	want := env.clock.Now() + cfg.OfflineGrace.Milliseconds()
	require.EqualValues(t, want, s.closeTime)
	got := env.gp.leaves.Load()
	require.EqualValues(t, 1, got, fmt.Sprintf("PlayerLeave 调用 %d 次", got))

	// grace 未到：不应该关图。
	env.clock.Advance(9 * time.Second)
	time.Sleep(40 * time.Millisecond) // 跑若干拍 loop
	n := env.closed.count()
	require.EqualValues(t, 0, n, fmt.Sprintf("grace 未到就关图了, OnClose=%d", n))

	// 过了 grace：关图一次，且幂等。
	env.clock.Advance(2 * time.Second)
	require.True(t, waitUntil(t, 2*time.Second, func() bool { return env.closed.count() == 1 }), "grace 到期未触发 OnClose")
	time.Sleep(50 * time.Millisecond)
	n = env.closed.count()
	require.EqualValues(t, 1, n, fmt.Sprintf("OnClose 被调用 %d 次, 期望 1（幂等）", n))
}

func TestQuitLeaveBranches(t *testing.T) {
	t.Run("QuitClose 为真时立即关", func(t *testing.T) {
		cfg := baseConfig()
		cfg.QuitClose = true
		env := newEnv(t, cfg, nil)
		env.mustCall(t, CommandEnterPlayer{UUID: 3001, GridPos: scenemap.GridPos{X: 2, Y: 2}})
		env.mustCall(t, CommandLeave{UUID: 3001, Reason: scenemap.LeaveReasonQuit})
		require.True(t, waitUntil(t, 2*time.Second, func() bool { return env.closed.count() == 1 }), "QuitClose 未立即关图")
	})

	t.Run("QuitClose 为假时保留空房", func(t *testing.T) {
		cfg := baseConfig()
		cfg.QuitClose = false
		cfg.EnterProtect = 30 * time.Second
		env := newEnv(t, cfg, nil)
		env.mustCall(t, CommandEnterPlayer{UUID: 3002, GridPos: scenemap.GridPos{X: 2, Y: 2}})
		env.mustCall(t, CommandLeave{UUID: 3002, Reason: scenemap.LeaveReasonQuit})
		s := env.snapshot(t, 3002)
		want := env.clock.Now() + cfg.EnterProtect.Milliseconds()
		require.EqualValues(t, want, s.closeTime)
		time.Sleep(30 * time.Millisecond)
		n := env.closed.count()
		require.EqualValues(t, 0, n, "enter_time 未到就关图了")
	})
}

func TestEmptyMapEnterProtectClosesOnce(t *testing.T) {
	cfg := baseConfig()
	cfg.EnterProtect = 20 * time.Millisecond

	// 用真实时钟，验证"开图后没人 → EnterProtect 到期 → 关图一次"。
	watch := &closeWatch{}
	gp := newStubGameplay()
	m := New(cfg, Deps{Gameplay: gp, AOI: &stubAOI{}, OnClose: watch.onClose})
	m.Start(context.Background())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = m.Stop(ctx)
	}()

	// 开图时就应该有保底关闭时间。
	time.Sleep(5 * time.Millisecond)
	require.True(t, waitUntil(t, 2*time.Second, func() bool { return watch.count() == 1 }), "空房 EnterProtect 到期未关图")
	time.Sleep(80 * time.Millisecond)
	n := watch.count()
	require.EqualValues(t, 1, n, fmt.Sprintf("OnClose 被调用 %d 次, 期望 1", n))
}

func TestCanCheckStopMapFalseKeepsRoom(t *testing.T) {
	cfg := baseConfig()
	cfg.OfflineGrace = 10 * time.Millisecond
	env := newEnv(t, cfg, func(_ *Config, d *Deps) {
		g := newStubGameplay()
		g.canStop.Store(false)
		d.Gameplay = g
	})

	env.mustCall(t, CommandEnterPlayer{UUID: 4001, GridPos: scenemap.GridPos{X: 1, Y: 1}})
	env.mustCall(t, CommandLeave{UUID: 4001, Reason: scenemap.LeaveReasonOffline})

	s := env.snapshot(t, 4001)
	require.EqualValues(t, 0, s.closeTime)
	time.Sleep(60 * time.Millisecond)
	n := env.closed.count()
	require.EqualValues(t, 0, n, fmt.Sprintf("不该关图, OnClose=%d", n))
}

func TestCloseCommandIsIdempotent(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)

	env.mustCall(t, CommandClose{Reason: "gm"})
	env.mustCall(t, CommandClose{Reason: "gm"})

	require.True(t, waitUntil(t, 2*time.Second, func() bool { return env.closed.count() == 1 }), "OnClose 次数 = %d, 期望 1", env.closed.count())
	got := env.gp.terminates.Load()
	require.EqualValues(t, 1, got, fmt.Sprintf("OnTerminate 调用 %d 次, 期望 1", got))
	r := env.closed.lastReason()
	require.EqualValues(t, "gm", r, fmt.Sprintf("关图原因 = %q, 期望 gm", r))
}

// ============================================================================
// 位置同步
// ============================================================================

func TestSyncSameCellDoesNotTriggerEntitySync(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	geo := grid.DefaultGeometry()
	start := scenemap.GridPos{X: 3, Y: 3}
	env.mustCall(t, CommandEnterPlayer{UUID: 5001, GridPos: start, Dir: scenemap.DirNone})

	before := env.snapshot(t, 5001)

	// 同格内偏移 5 像素：仍然落在同一格。
	pixel := geo.CellCenter(start)
	pixel.X += 5
	pixel.Y -= 3
	reply := env.mustCall(t, CommandMove{UUID: 5001, Pixel: pixel, Dir: scenemap.DirLeft, State: 2})
	mr, ok := reply.(MoveReply)
	require.True(t, ok)
	require.False(t, mr.Moved, "同格微调不应报告 Moved")
	got := len(env.gp.syncEvents())
	require.EqualValues(t, 0, got, fmt.Sprintf("同格微调触发了 %d 次 EntitySync", got))

	after := env.snapshot(t, 5001)
	require.EqualValues(t, start, after.pos)
	require.EqualValues(t, pixel, after.pixel)
	require.EqualValues(t, scenemap.DirLeft, after.dir)
	require.EqualValues(t, 2, after.anim)
	require.Greater(t, after.revision, before.revision, "同格微调也必须 Touch（像素/朝向变了），revision %d → %d", before.revision, after.revision)
	_, syncs, _, _ := env.aoi.counts()
	require.EqualValues(t, 1, syncs, fmt.Sprintf("AOI Sync 次数 = %d, 期望 1（同格也要同步像素）", syncs))
}

func TestSyncCrossCellTriggersEntitySync(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	geo := grid.DefaultGeometry()
	start := scenemap.GridPos{X: 3, Y: 3}
	env.mustCall(t, CommandEnterPlayer{UUID: 5002, GridPos: start})

	next := scenemap.GridPos{X: 4, Y: 3}
	reply := env.mustCall(t, CommandMove{UUID: 5002, Pixel: geo.CellCenter(next), Dir: scenemap.DirRight})
	mr := reply.(MoveReply)
	require.True(t, mr.Moved, "换格回执 = %+v", mr)
	require.EqualValues(t, start, mr.From, "换格回执 = %+v", mr)
	require.EqualValues(t, next, mr.To, "换格回执 = %+v", mr)

	events := env.gp.syncEvents()
	require.Len(t, events, 1)
	require.EqualValues(t, 5002, events[0].uuid, "EntitySync 参数 = %+v", events[0])
	require.EqualValues(t, start, events[0].oldPos, "EntitySync 参数 = %+v", events[0])
	require.EqualValues(t, next, events[0].newPos, "EntitySync 参数 = %+v", events[0])
	s := env.snapshot(t, 5002)
	require.EqualValues(t, next, s.pos, fmt.Sprintf("索引位置 = %s, 期望 %s", s.pos, next))
}

func TestSyncRejectsIllegalMoves(t *testing.T) {
	cfg := baseConfig()
	cfg.MaxStep = 3
	env := newEnv(t, cfg, func(_ *Config, d *Deps) {
		// (9,9) 设为静态阻挡。
		d.Walkable = func(p scenemap.GridPos) bool { return !(p.X == 9 && p.Y == 9) }
	})
	geo := grid.DefaultGeometry()
	start := scenemap.GridPos{X: 3, Y: 3}
	env.mustCall(t, CommandEnterPlayer{UUID: 6001, GridPos: start})

	t.Run("单步过大", func(t *testing.T) {
		far := scenemap.GridPos{X: 10, Y: 3}
		_, err := env.call(t, CommandMove{UUID: 6001, Pixel: geo.CellCenter(far)})
		require.ErrorIs(t, err, scenemap.ErrOutOfRange, "err = %v, 期望 ErrOutOfRange", err)
	})

	t.Run("越界", func(t *testing.T) {
		_, err := env.call(t, CommandMove{UUID: 6001, Pixel: scenemap.PixelPos{X: -1, Y: -1}})
		require.ErrorIs(t, err, scenemap.ErrOutOfRange, "err = %v, 期望 ErrOutOfRange", err)
	})

	t.Run("静态阻挡", func(t *testing.T) {
		// (3,3) → (9,9) 单步 6 格会先被 MaxStep 拒，改用相邻的阻挡格。
		d := newEnv(t, baseConfig(), func(_ *Config, dep *Deps) {
			dep.Walkable = func(p scenemap.GridPos) bool { return !(p.X == 4 && p.Y == 3) }
		})
		d.mustCall(t, CommandEnterPlayer{UUID: 6002, GridPos: start})
		_, err := d.call(t, CommandMove{UUID: 6002, Pixel: geo.CellCenter(scenemap.GridPos{X: 4, Y: 3})})
		require.ErrorIs(t, err, scenemap.ErrBlocked, "err = %v, 期望 ErrBlocked", err)
	})

	t.Run("怪物占位", func(t *testing.T) {
		d := newEnv(t, baseConfig(), nil)
		d.mustCall(t, CommandEnterPlayer{UUID: 6003, GridPos: start})
		d.mustCall(t, CommandEnterPlayer{
			UUID:       9001,
			Kind:       scenemap.EntityKindNPE,
			GridPos:    scenemap.GridPos{X: 4, Y: 3},
			TemplateID: 101,
		})
		_, err := d.call(t, CommandMove{UUID: 6003, Pixel: geo.CellCenter(scenemap.GridPos{X: 4, Y: 3})})
		require.ErrorIs(t, err, scenemap.ErrBlocked, "err = %v, 期望 ErrBlocked", err)
	})

	t.Run("未知实体", func(t *testing.T) {
		_, err := env.call(t, CommandMove{UUID: 777777, Pixel: geo.CellCenter(start)})
		require.ErrorIs(t, err, scenemap.ErrNotFound, "err = %v, 期望 ErrNotFound", err)
	})
}

// ============================================================================
// 索引 / 重叠规则
// ============================================================================

func TestPlayersMayOverlapMonstersMayNot(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	pos := scenemap.GridPos{X: 5, Y: 5}

	// 两个玩家同一格：允许。
	env.mustCall(t, CommandEnterPlayer{UUID: 7001, GridPos: pos})
	env.mustCall(t, CommandEnterPlayer{UUID: 7002, GridPos: pos})
	s := env.snapshot(t, 7001)
	require.EqualValues(t, 2, s.blockCnt)

	// 怪物想落在玩家身上：被挪到相邻格。
	reply := env.mustCall(t, CommandEnterPlayer{
		UUID:       9101,
		Kind:       scenemap.EntityKindNPE,
		GridPos:    pos,
		TemplateID: 200,
	})
	er := reply.(EnterReply)
	require.NotEqualValues(t, pos, er.GridPos, "怪物与玩家重叠了: %s", er.GridPos)
	d := grid.Chebyshev(er.GridPos, pos)
	require.EqualValues(t, 1, d, fmt.Sprintf("怪物落点距玩家 %d 格, 期望 1（就近找空位）", d))
	s = env.snapshot(t, 9101)
	require.EqualValues(t, er.GridPos, s.pos, fmt.Sprintf("Index 与 NPES 位置不一致: %s vs %s", s.pos, er.GridPos))

	// 玩家也不能走进怪物格（移动路径上拦住）。
	geo := grid.DefaultGeometry()
	_, err := env.call(t, CommandMove{UUID: 7001, Pixel: geo.CellCenter(er.GridPos)})
	require.ErrorIs(t, err, scenemap.ErrBlocked, "走进怪物格 err = %v, 期望 ErrBlocked", err)
}

func TestMonsterFootprintBlocksNeighbors(t *testing.T) {
	size := grid.Size{Width: 16, Height: 16}
	ix := NewIndex(size)
	center := scenemap.GridPos{X: 8, Y: 8}

	_, err := ix.Enter(1, center, EnterOption{Kind: scenemap.EntityKindNPE, Radius: 1})
	require.NoError(t, err, fmt.Sprintf("Enter: %v", err))
	for y := int32(7); y <= 9; y++ {
		for x := int32(7); x <= 9; x++ {
			n := ix.BlockCount(scenemap.GridPos{X: x, Y: y})
			require.EqualValues(t, 1, n, fmt.Sprintf("(%d,%d) blockCount = %d, 期望 1", x, y, n))
		}
	}
	require.True(t, ix.BlockedFor(2, scenemap.GridPos{X: 7, Y: 7}), "footprint 内的格应阻挡其他实体")
	require.False(t, ix.BlockedFor(1, scenemap.GridPos{X: 7, Y: 7}), "对自身不应判定阻挡")

	// 离开后 footprint 全部回收。
	ix.Leave(1)
	for y := int32(7); y <= 9; y++ {
		for x := int32(7); x <= 9; x++ {
			n := ix.BlockCount(scenemap.GridPos{X: x, Y: y})
			require.EqualValues(t, 0, n, fmt.Sprintf("离开后 (%d,%d) blockCount = %d, 期望 0", x, y, n))
		}
	}
	require.EqualValues(t, 0, ix.Len())
}

func TestIndexSameCellMoveDoesNotMigrate(t *testing.T) {
	ix := NewIndex(grid.Size{Width: 8, Height: 8})
	p := scenemap.GridPos{X: 2, Y: 2}
	_, err := ix.Enter(42, p, EnterOption{Kind: scenemap.EntityKindPlayer})
	require.NoError(t, err, fmt.Sprintf("Enter: %v", err))

	moved, err := ix.Move(42, p)
	require.NoError(t, err, "Move")
	require.False(t, moved, "同格 Move 不应报告迁移")
	got := ix.PlayersAt(p)
	require.EqualValues(t, 1, got, fmt.Sprintf("PlayersAt = %d, 期望 1（不能因为同格移动被重复计数）", got))
	got = ix.BlockCount(p)
	require.EqualValues(t, 1, got, fmt.Sprintf("BlockCount = %d, 期望 1", got))

	moved, err = ix.Move(42, scenemap.GridPos{X: 3, Y: 2})
	require.NoError(t, err, "换格 Move = (%v,%v), 期望 (true,nil)", moved, err)
	require.True(t, moved, "换格 Move = (%v,%v), 期望 (true,nil)", moved, err)
	require.EqualValues(t, 0, ix.BlockCount(p), "旧格占位未回收")
	require.EqualValues(t, 1, ix.BlockCount(scenemap.GridPos{X: 3, Y: 2}), "新格占位未登记")
	require.EqualValues(t, 0, ix.PlayersAt(p), "旧格玩家计数未回收")
}

// ============================================================================
// 命令副作用检测
// ============================================================================

func TestNoEffectCommandIsRejected(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	_, err := env.call(t, NewFunc("noop", func(*State) (any, error) { return nil, nil }))
	require.ErrorIs(t, err, scenemap.ErrNoEffect, "err = %v, 期望 ErrNoEffect", err)

	// 声明 MayNoop 的命令允许无效果。
	_, err = env.call(t, &FuncCommand{CmdName: "noop-ok", AllowNoop: true, Fn: func(*State) (any, error) {
		return 1, nil
	}})
	require.NoError(t, err, "AllowNoop 命令失败")
}

func TestLenientNoEffectOnlyLogs(t *testing.T) {
	cfg := baseConfig()
	cfg.LenientNoEffect = true
	env := newEnv(t, cfg, nil)
	_, err := env.call(t, NewFunc("noop", func(*State) (any, error) { return nil, nil }))
	require.NoError(t, err, fmt.Sprintf("宽松模式下不应报错: %v", err))
}

// ============================================================================
// 定时器
// ============================================================================

func TestLoopTimerSelfRenewsAtExpectedRate(t *testing.T) {
	cfg := baseConfig()
	cfg.TickInterval = 10 * time.Millisecond
	ft := &fakeTimers{}
	env := newEnv(t, cfg, func(_ *Config, d *Deps) { d.NewTimer = ft.New })
	ft.waitArmed(t)

	require.Equal(t, 10*time.Millisecond, ft.loop().interval(), "loop 间隔 = base / 倍速")

	// 连投 12 拍。若定时器不自续期，只有第 1 拍会生效。
	for i := 0; i < 12; i++ {
		ft.loop().tick(t)
	}
	require.True(t, waitUntil(t, 2*time.Second, func() bool { return env.gp.loops.Load() == 12 }), "loop 次数 = %d, 期望 12", env.gp.loops.Load())
}

// TestLoopIntervalScalesWithSpeed 验证倍速只影响间隔换算，是纯函数。
//
// 原来的版本靠跑真实时钟比较次数，既慢又抖；这里直接断言换算结果，
// 顺带覆盖整数截断与非法倍速的兜底。
func TestLoopIntervalScalesWithSpeed(t *testing.T) {
	cases := []struct {
		permille int32
		wantLoop time.Duration
		wantBuff time.Duration
	}{
		{1000, 10 * time.Millisecond, 5 * time.Millisecond},
		{2000, 5 * time.Millisecond, 2500 * time.Microsecond},
		{1500, 6666666 * time.Nanosecond, 3333333 * time.Nanosecond}, // 整数截断
		{500, 20 * time.Millisecond, 10 * time.Millisecond},
		{0, 10 * time.Millisecond, 5 * time.Millisecond}, // 非法值按 1x 处理
	}
	for _, c := range cases {
		cfg := baseConfig()
		cfg.TickInterval = 10 * time.Millisecond
		cfg.SpeedPermille = c.permille
		m := New(cfg, Deps{})

		require.Equal(t, c.wantLoop, m.loopInterval(), fmt.Sprintf("倍速 %d‰ loop 间隔", c.permille))
		require.Equal(t, c.wantBuff, m.buffInterval(), fmt.Sprintf("倍速 %d‰ buff 间隔", c.permille))
	}
}

func TestSetSpeedRearmsWithoutStopping(t *testing.T) {
	cfg := baseConfig()
	cfg.TickInterval = 20 * time.Millisecond
	ft := &fakeTimers{}
	env := newEnv(t, cfg, func(_ *Config, d *Deps) { d.NewTimer = ft.New })
	ft.waitArmed(t)
	require.Equal(t, 20*time.Millisecond, ft.loop().interval())

	ft.loop().tick(t)
	require.True(t, waitUntil(t, 2*time.Second, func() bool { return env.gp.loops.Load() == 1 }), "定时器没有跑起来")

	require.NoError(t, env.m.SetSpeed(3000))
	want := 20 * time.Millisecond * 1000 / 3000 // 6.666666ms
	require.True(t, waitUntil(t, 2*time.Second, func() bool { return ft.loop().interval() == want }), "倍速变了但间隔没重建: %v, 期望 %v", ft.loop().interval(), want)
	require.EqualValues(t, 3000, env.m.Speed())

	// 重建后仍在同一台定时器上继续跑（不是 Stop 后新建）。
	ft.loop().tick(t)
	require.True(t, waitUntil(t, 2*time.Second, func() bool { return env.gp.loops.Load() == 2 }), "提速后 loop 没有继续推进")
}

// ============================================================================
// 并发
// ============================================================================

func TestConcurrentPostSingleWriter(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)

	const goroutines = 16
	const perG = 20

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*perG)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				err := env.m.Post(NewFunc("inc", func(s *State) (any, error) {
					n, _ := s.Ext["n"].(int)
					s.Ext["n"] = n + 1
					s.Touch()
					return nil, nil
				}))
				if err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Post 失败: %v", err)
	}

	// Call 的投递排在所有 Post 之后（同一 mailbox FIFO），因此这里读到的是最终值。
	got := env.mustCall(t, &FuncCommand{CmdName: "read", AllowNoop: true, Fn: func(s *State) (any, error) {
		n, _ := s.Ext["n"].(int)
		return n, nil
	}}).(int)
	require.EqualValues(t, goroutines*perG, got)

	rev := env.snapshot(t, 0).revision
	require.GreaterOrEqual(t, rev, uint64(goroutines*perG), "revision = %d, 少于命令数 %d", rev, goroutines*perG)
}

// ============================================================================
// 其它
// ============================================================================

func TestNpeUUIDsAreSorted(t *testing.T) {
	s := &State{NPES: map[scenemap.UUID]*NpeAIInfo{}}
	for _, id := range []scenemap.UUID{5, 1, 9, 3, 7} {
		s.NPES[id] = &NpeAIInfo{UUID: id}
	}
	got := s.NpeUUIDs()
	for i := 1; i < len(got); i++ {
		require.LessOrEqual(t, got[i-1], got[i], "NpeUUIDs 未排序: %v", got)
	}
	require.Len(t, got, 5)
}

func TestStopBeforeStartAndPostAfterStop(t *testing.T) {
	m := New(baseConfig(), Deps{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := m.Stop(ctx)
	require.NoError(t, err, fmt.Sprintf("Stop: %v", err))
	err = m.Post(CommandTick{})
	require.False(t, !errors.Is(err, scenemap.ErrClosed), fmt.Sprintf("Post after stop = %v, 期望 ErrClosed", err))
}

func TestBcastAndTickCommands(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	env.mustCall(t, CommandBcast{Msg: "hello", Scope: BroadcastAround(scenemap.GridPos{X: 1, Y: 1}, 5)})

	_, _, _, n := env.aoi.counts()
	require.EqualValues(t, 1, n, fmt.Sprintf("Broadcast 次数 = %d, 期望 1", n))
	env.aoi.mu.Lock()
	scope := env.aoi.lastScope
	env.aoi.mu.Unlock()
	require.EqualValues(t, ScopeAround, scope.Kind, "广播范围 = %+v", scope)
	require.EqualValues(t, 5, scope.Radius, "广播范围 = %+v", scope)

	// Tick 允许无效果。
	_, err := env.call(t, CommandTick{})
	require.NoError(t, err, fmt.Sprintf("CommandTick 失败: %v", err))
}

func TestPetsFollowMaster(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	master := scenemap.UUID(8001)
	env.mustCall(t, CommandEnterPlayer{UUID: master, GridPos: scenemap.GridPos{X: 6, Y: 6}})
	env.mustCall(t, CommandEnterPlayer{
		UUID:       8002,
		Kind:       scenemap.EntityKindPet,
		MasterUUID: master,
		GridPos:    scenemap.GridPos{X: 6, Y: 6},
	})
	s := env.snapshot(t, 8002)
	require.EqualValues(t, 1, s.pets, "宠物未登记")
	// 主人离开 → 宠物一起走，且不阻塞关图。
	env.mustCall(t, CommandLeave{UUID: master, Reason: scenemap.LeaveReasonQuit})
	s = env.snapshot(t, 8002)
	require.EqualValues(t, 0, s.pets)
	require.EqualValues(t, 0, s.users)
}
