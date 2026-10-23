package mgr

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
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
	if err != nil {
		t.Fatalf("命令 %s 失败: %v", cmd.Name(), err)
	}
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
	if !ok {
		t.Fatalf("回执类型错误: %T", reply)
	}
	if er.GridPos != pos {
		t.Fatalf("落点 = %s, 期望 %s", er.GridPos, pos)
	}

	s := env.snapshot(t, 1001)
	if s.users != 1 {
		t.Fatalf("Users = %d, 期望 1", s.users)
	}
	if s.pos != pos {
		t.Fatalf("索引位置 = %s, 期望 %s", s.pos, pos)
	}
	if s.pixel != cellCenter(geo, pos) {
		t.Fatalf("像素 = %+v, 期望格中心 %+v", s.pixel, cellCenter(geo, pos))
	}
	if s.closeTime != 0 {
		t.Fatalf("CloseTime = %d, 有玩家时必须是 0", s.closeTime)
	}
	if got := env.gp.enters.Load(); got != 1 {
		t.Fatalf("PlayerEnter 调用 %d 次, 期望 1", got)
	}
	if enters, _, _, _ := env.aoi.counts(); enters != 1 {
		t.Fatalf("AOI Enter 调用 %d 次, 期望 1", enters)
	}

	// 重复进图必须被拒。
	_, err := env.call(t, CommandEnterPlayer{UUID: 1001, GridPos: pos})
	if !errors.Is(err, scenemap.ErrAlreadyExists) {
		t.Fatalf("重复进图错误 = %v, 期望 ErrAlreadyExists", err)
	}
}

func TestPlayerLeaveOfflineSetsGrace(t *testing.T) {
	cfg := baseConfig()
	cfg.OfflineGrace = 10 * time.Second
	env := newEnv(t, cfg, nil)

	env.mustCall(t, CommandEnterPlayer{UUID: 2001, GridPos: scenemap.GridPos{X: 3, Y: 3}})
	env.mustCall(t, CommandLeave{UUID: 2001, Reason: scenemap.LeaveReasonOffline})

	s := env.snapshot(t, 2001)
	if s.users != 0 {
		t.Fatalf("离图后 Users = %d", s.users)
	}
	want := env.clock.Now() + cfg.OfflineGrace.Milliseconds()
	if s.closeTime != want {
		t.Fatalf("CloseTime = %d, 期望 now+grace = %d", s.closeTime, want)
	}
	if got := env.gp.leaves.Load(); got != 1 {
		t.Fatalf("PlayerLeave 调用 %d 次", got)
	}

	// grace 未到：不应该关图。
	env.clock.Advance(9 * time.Second)
	time.Sleep(40 * time.Millisecond) // 跑若干拍 loop
	if n := env.closed.count(); n != 0 {
		t.Fatalf("grace 未到就关图了, OnClose=%d", n)
	}

	// 过了 grace：关图一次，且幂等。
	env.clock.Advance(2 * time.Second)
	if !waitUntil(t, 2*time.Second, func() bool { return env.closed.count() == 1 }) {
		t.Fatalf("grace 到期未触发 OnClose")
	}
	time.Sleep(50 * time.Millisecond)
	if n := env.closed.count(); n != 1 {
		t.Fatalf("OnClose 被调用 %d 次, 期望 1（幂等）", n)
	}
}

func TestQuitLeaveBranches(t *testing.T) {
	t.Run("QuitClose 为真时立即关", func(t *testing.T) {
		cfg := baseConfig()
		cfg.QuitClose = true
		env := newEnv(t, cfg, nil)
		env.mustCall(t, CommandEnterPlayer{UUID: 3001, GridPos: scenemap.GridPos{X: 2, Y: 2}})
		env.mustCall(t, CommandLeave{UUID: 3001, Reason: scenemap.LeaveReasonQuit})
		if !waitUntil(t, 2*time.Second, func() bool { return env.closed.count() == 1 }) {
			t.Fatalf("QuitClose 未立即关图")
		}
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
		if s.closeTime != want {
			t.Fatalf("CloseTime = %d, 期望 %d", s.closeTime, want)
		}
		time.Sleep(30 * time.Millisecond)
		if n := env.closed.count(); n != 0 {
			t.Fatalf("enter_time 未到就关图了")
		}
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
	if !waitUntil(t, 2*time.Second, func() bool { return watch.count() == 1 }) {
		t.Fatalf("空房 EnterProtect 到期未关图")
	}
	time.Sleep(80 * time.Millisecond)
	if n := watch.count(); n != 1 {
		t.Fatalf("OnClose 被调用 %d 次, 期望 1", n)
	}
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
	if s.closeTime != 0 {
		t.Fatalf("CanCheckStopMap=false 时 CloseTime 必须为 0, got %d", s.closeTime)
	}
	time.Sleep(60 * time.Millisecond)
	if n := env.closed.count(); n != 0 {
		t.Fatalf("不该关图, OnClose=%d", n)
	}
}

func TestCloseCommandIsIdempotent(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)

	env.mustCall(t, CommandClose{Reason: "gm"})
	env.mustCall(t, CommandClose{Reason: "gm"})

	if !waitUntil(t, 2*time.Second, func() bool { return env.closed.count() == 1 }) {
		t.Fatalf("OnClose 次数 = %d, 期望 1", env.closed.count())
	}
	if got := env.gp.terminates.Load(); got != 1 {
		t.Fatalf("OnTerminate 调用 %d 次, 期望 1", got)
	}
	if r := env.closed.lastReason(); r != "gm" {
		t.Fatalf("关图原因 = %q, 期望 gm", r)
	}
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
	if !ok {
		t.Fatalf("回执类型错误: %T", reply)
	}
	if mr.Moved {
		t.Fatalf("同格微调不应报告 Moved")
	}
	if got := len(env.gp.syncEvents()); got != 0 {
		t.Fatalf("同格微调触发了 %d 次 EntitySync", got)
	}

	after := env.snapshot(t, 5001)
	if after.pos != start {
		t.Fatalf("格子变了: %s → %s", start, after.pos)
	}
	if after.pixel != pixel {
		t.Fatalf("像素未更新: %+v", after.pixel)
	}
	if after.dir != scenemap.DirLeft {
		t.Fatalf("朝向未更新: %v", after.dir)
	}
	if after.anim != 2 {
		t.Fatalf("动画状态 = %d, 期望 2", after.anim)
	}
	if after.revision <= before.revision {
		t.Fatalf("同格微调也必须 Touch（像素/朝向变了），revision %d → %d", before.revision, after.revision)
	}
	if _, syncs, _, _ := env.aoi.counts(); syncs != 1 {
		t.Fatalf("AOI Sync 次数 = %d, 期望 1（同格也要同步像素）", syncs)
	}
}

func TestSyncCrossCellTriggersEntitySync(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	geo := grid.DefaultGeometry()
	start := scenemap.GridPos{X: 3, Y: 3}
	env.mustCall(t, CommandEnterPlayer{UUID: 5002, GridPos: start})

	next := scenemap.GridPos{X: 4, Y: 3}
	reply := env.mustCall(t, CommandMove{UUID: 5002, Pixel: geo.CellCenter(next), Dir: scenemap.DirRight})
	mr := reply.(MoveReply)
	if !mr.Moved || mr.From != start || mr.To != next {
		t.Fatalf("换格回执 = %+v", mr)
	}

	events := env.gp.syncEvents()
	if len(events) != 1 {
		t.Fatalf("EntitySync 次数 = %d, 期望 1", len(events))
	}
	if events[0].uuid != 5002 || events[0].oldPos != start || events[0].newPos != next {
		t.Fatalf("EntitySync 参数 = %+v", events[0])
	}
	if s := env.snapshot(t, 5002); s.pos != next {
		t.Fatalf("索引位置 = %s, 期望 %s", s.pos, next)
	}
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
		if !errors.Is(err, scenemap.ErrOutOfRange) {
			t.Fatalf("err = %v, 期望 ErrOutOfRange", err)
		}
	})

	t.Run("越界", func(t *testing.T) {
		_, err := env.call(t, CommandMove{UUID: 6001, Pixel: scenemap.PixelPos{X: -1, Y: -1}})
		if !errors.Is(err, scenemap.ErrOutOfRange) {
			t.Fatalf("err = %v, 期望 ErrOutOfRange", err)
		}
	})

	t.Run("静态阻挡", func(t *testing.T) {
		// (3,3) → (9,9) 单步 6 格会先被 MaxStep 拒，改用相邻的阻挡格。
		d := newEnv(t, baseConfig(), func(_ *Config, dep *Deps) {
			dep.Walkable = func(p scenemap.GridPos) bool { return !(p.X == 4 && p.Y == 3) }
		})
		d.mustCall(t, CommandEnterPlayer{UUID: 6002, GridPos: start})
		_, err := d.call(t, CommandMove{UUID: 6002, Pixel: geo.CellCenter(scenemap.GridPos{X: 4, Y: 3})})
		if !errors.Is(err, scenemap.ErrBlocked) {
			t.Fatalf("err = %v, 期望 ErrBlocked", err)
		}
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
		if !errors.Is(err, scenemap.ErrBlocked) {
			t.Fatalf("err = %v, 期望 ErrBlocked", err)
		}
	})

	t.Run("未知实体", func(t *testing.T) {
		_, err := env.call(t, CommandMove{UUID: 777777, Pixel: geo.CellCenter(start)})
		if !errors.Is(err, scenemap.ErrNotFound) {
			t.Fatalf("err = %v, 期望 ErrNotFound", err)
		}
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
	if s.blockCnt != 2 {
		t.Fatalf("blockCount = %d, 期望 2（玩家也计数）", s.blockCnt)
	}

	// 怪物想落在玩家身上：被挪到相邻格。
	reply := env.mustCall(t, CommandEnterPlayer{
		UUID:       9101,
		Kind:       scenemap.EntityKindNPE,
		GridPos:    pos,
		TemplateID: 200,
	})
	er := reply.(EnterReply)
	if er.GridPos == pos {
		t.Fatalf("怪物与玩家重叠了: %s", er.GridPos)
	}
	if d := grid.Chebyshev(er.GridPos, pos); d != 1 {
		t.Fatalf("怪物落点距玩家 %d 格, 期望 1（就近找空位）", d)
	}
	if s := env.snapshot(t, 9101); s.pos != er.GridPos {
		t.Fatalf("Index 与 NPES 位置不一致: %s vs %s", s.pos, er.GridPos)
	}

	// 玩家也不能走进怪物格（移动路径上拦住）。
	geo := grid.DefaultGeometry()
	_, err := env.call(t, CommandMove{UUID: 7001, Pixel: geo.CellCenter(er.GridPos)})
	if !errors.Is(err, scenemap.ErrBlocked) {
		t.Fatalf("走进怪物格 err = %v, 期望 ErrBlocked", err)
	}
}

func TestMonsterFootprintBlocksNeighbors(t *testing.T) {
	size := grid.Size{Width: 16, Height: 16}
	ix := NewIndex(size)
	center := scenemap.GridPos{X: 8, Y: 8}

	if _, err := ix.Enter(1, center, EnterOption{Kind: scenemap.EntityKindNPE, Radius: 1}); err != nil {
		t.Fatalf("Enter: %v", err)
	}
	for y := int32(7); y <= 9; y++ {
		for x := int32(7); x <= 9; x++ {
			if n := ix.BlockCount(scenemap.GridPos{X: x, Y: y}); n != 1 {
				t.Fatalf("(%d,%d) blockCount = %d, 期望 1", x, y, n)
			}
		}
	}
	if !ix.BlockedFor(2, scenemap.GridPos{X: 7, Y: 7}) {
		t.Fatalf("footprint 内的格应阻挡其他实体")
	}
	if ix.BlockedFor(1, scenemap.GridPos{X: 7, Y: 7}) {
		t.Fatalf("对自身不应判定阻挡")
	}

	// 离开后 footprint 全部回收。
	ix.Leave(1)
	for y := int32(7); y <= 9; y++ {
		for x := int32(7); x <= 9; x++ {
			if n := ix.BlockCount(scenemap.GridPos{X: x, Y: y}); n != 0 {
				t.Fatalf("离开后 (%d,%d) blockCount = %d, 期望 0", x, y, n)
			}
		}
	}
	if ix.Len() != 0 {
		t.Fatalf("索引残留 %d 个实体", ix.Len())
	}
}

func TestIndexSameCellMoveDoesNotMigrate(t *testing.T) {
	ix := NewIndex(grid.Size{Width: 8, Height: 8})
	p := scenemap.GridPos{X: 2, Y: 2}
	if _, err := ix.Enter(42, p, EnterOption{Kind: scenemap.EntityKindPlayer}); err != nil {
		t.Fatalf("Enter: %v", err)
	}

	moved, err := ix.Move(42, p)
	if err != nil {
		t.Fatalf("Move: %v", err)
	}
	if moved {
		t.Fatalf("同格 Move 不应报告迁移")
	}
	if got := ix.PlayersAt(p); got != 1 {
		t.Fatalf("PlayersAt = %d, 期望 1（不能因为同格移动被重复计数）", got)
	}
	if got := ix.BlockCount(p); got != 1 {
		t.Fatalf("BlockCount = %d, 期望 1", got)
	}

	moved, err = ix.Move(42, scenemap.GridPos{X: 3, Y: 2})
	if err != nil || !moved {
		t.Fatalf("换格 Move = (%v,%v), 期望 (true,nil)", moved, err)
	}
	if ix.BlockCount(p) != 0 {
		t.Fatalf("旧格占位未回收")
	}
	if ix.BlockCount(scenemap.GridPos{X: 3, Y: 2}) != 1 {
		t.Fatalf("新格占位未登记")
	}
	if ix.PlayersAt(p) != 0 {
		t.Fatalf("旧格玩家计数未回收")
	}
}

// ============================================================================
// 命令副作用检测
// ============================================================================

func TestNoEffectCommandIsRejected(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	_, err := env.call(t, NewFunc("noop", func(*State) (any, error) { return nil, nil }))
	if !errors.Is(err, scenemap.ErrNoEffect) {
		t.Fatalf("err = %v, 期望 ErrNoEffect", err)
	}

	// 声明 MayNoop 的命令允许无效果。
	_, err = env.call(t, &FuncCommand{CmdName: "noop-ok", AllowNoop: true, Fn: func(*State) (any, error) {
		return 1, nil
	}})
	if err != nil {
		t.Fatalf("AllowNoop 命令失败: %v", err)
	}
}

func TestLenientNoEffectOnlyLogs(t *testing.T) {
	cfg := baseConfig()
	cfg.LenientNoEffect = true
	env := newEnv(t, cfg, nil)
	if _, err := env.call(t, NewFunc("noop", func(*State) (any, error) { return nil, nil })); err != nil {
		t.Fatalf("宽松模式下不应报错: %v", err)
	}
}

// ============================================================================
// 定时器
// ============================================================================

func TestLoopTimerSelfRenewsAtExpectedRate(t *testing.T) {
	cfg := baseConfig()
	cfg.TickInterval = 10 * time.Millisecond
	env := newEnv(t, cfg, nil)

	time.Sleep(120 * time.Millisecond)
	n := env.gp.loops.Load()
	if n < 5 || n > 20 {
		t.Fatalf("120ms 内 loop 次数 = %d, 期望约 12（10ms 间隔）", n)
	}
}

func TestTimerSpeedAffectsLoopRate(t *testing.T) {
	run := func(permille int32) int64 {
		cfg := baseConfig()
		cfg.TickInterval = 10 * time.Millisecond
		cfg.SpeedPermille = permille
		gp := newStubGameplay()
		m := New(cfg, Deps{Gameplay: gp, AOI: &stubAOI{}, Clock: func() int64 { return time.Now().UnixMilli() }})
		m.Start(context.Background())
		time.Sleep(150 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = m.Stop(ctx)
		cancel()
		return gp.loops.Load()
	}

	slow := run(1000) // 10ms
	fast := run(2000) // 5ms

	if slow < 5 || fast < 5 {
		t.Fatalf("采样过少: slow=%d fast=%d", slow, fast)
	}
	// 允许调度抖动的宽松区间：约 2:1。
	if fast*2 < slow*3 || fast*2 > slow*5 {
		t.Fatalf("倍速比例异常: 1000‰=%d 次, 2000‰=%d 次, 期望约 2:1", slow, fast)
	}
}

func TestSetSpeedRearmsWithoutStopping(t *testing.T) {
	cfg := baseConfig()
	cfg.TickInterval = 20 * time.Millisecond
	env := newEnv(t, cfg, nil)

	time.Sleep(50 * time.Millisecond)
	base := env.gp.loops.Load()
	if base == 0 {
		t.Fatalf("定时器没有跑起来")
	}
	if err := env.m.SetSpeed(3000); err != nil {
		t.Fatalf("SetSpeed: %v", err)
	}
	if got := env.m.Speed(); got != 3000 {
		t.Fatalf("Speed = %d", got)
	}
	time.Sleep(60 * time.Millisecond)
	if env.gp.loops.Load() <= base {
		t.Fatalf("提速后 loop 没有继续推进")
	}
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
	if got != goroutines*perG {
		t.Fatalf("计数 = %d, 期望 %d（命令丢失或被并发修改）", got, goroutines*perG)
	}

	rev := env.snapshot(t, 0).revision
	if rev < uint64(goroutines*perG) {
		t.Fatalf("revision = %d, 少于命令数 %d", rev, goroutines*perG)
	}
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
		if got[i-1] > got[i] {
			t.Fatalf("NpeUUIDs 未排序: %v", got)
		}
	}
	if len(got) != 5 {
		t.Fatalf("len = %d", len(got))
	}
}

func TestStopBeforeStartAndPostAfterStop(t *testing.T) {
	m := New(baseConfig(), Deps{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := m.Post(CommandTick{}); !errors.Is(err, scenemap.ErrClosed) {
		t.Fatalf("Post after stop = %v, 期望 ErrClosed", err)
	}
}

func TestBcastAndTickCommands(t *testing.T) {
	env := newEnv(t, baseConfig(), nil)
	env.mustCall(t, CommandBcast{Msg: "hello", Scope: BroadcastAround(scenemap.GridPos{X: 1, Y: 1}, 5)})

	if _, _, _, n := env.aoi.counts(); n != 1 {
		t.Fatalf("Broadcast 次数 = %d, 期望 1", n)
	}
	env.aoi.mu.Lock()
	scope := env.aoi.lastScope
	env.aoi.mu.Unlock()
	if scope.Kind != ScopeAround || scope.Radius != 5 {
		t.Fatalf("广播范围 = %+v", scope)
	}

	// Tick 允许无效果。
	if _, err := env.call(t, CommandTick{}); err != nil {
		t.Fatalf("CommandTick 失败: %v", err)
	}
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
	if s := env.snapshot(t, 8002); s.pets != 1 {
		t.Fatalf("宠物未登记")
	}
	// 主人离开 → 宠物一起走，且不阻塞关图。
	env.mustCall(t, CommandLeave{UUID: master, Reason: scenemap.LeaveReasonQuit})
	s := env.snapshot(t, 8002)
	if s.pets != 0 {
		t.Fatalf("主人离开后宠物仍登记, pets=%d", s.pets)
	}
	if s.users != 0 {
		t.Fatalf("Users = %d", s.users)
	}
}
