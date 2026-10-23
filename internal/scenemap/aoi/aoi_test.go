package aoi_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/aoi"
	"game_study/internal/scenemap/grid"
	"github.com/stretchr/testify/require"
)

const (
	testCellW    = int32(48)
	testCellH    = int32(24)
	testGridSize = int32(6)
)

func testGeo() grid.Geometry {
	return grid.Geometry{CellW: testCellW, CellH: testCellH, AoiGrid: testGridSize}
}

// pixelOf 返回视野格中心附近的像素坐标。
//
// 走"视野格 → 行走格中心 → 像素中心"两跳，避免测试自己算错倍率：
// 视野格 6 个行走格宽，直接乘 6 是把行走格像素当视野格像素用。
func pixelOf(c aoi.CellKey) (int32, int32) {
	gx := c.X*testGridSize + testGridSize/2
	gy := c.Y*testGridSize + testGridSize/2
	return gx*testCellW + testCellW/2, gy*testCellH + testCellH/2
}

// delivery 是一条投递记录。
type delivery struct {
	uuid  scenemap.UUID
	msg   any
	batch bool
}

// stubSink 是 Sink 的内存替身：记录每一次投递，并可模拟下游拒绝。
type stubSink struct {
	mu    sync.Mutex
	feed  []delivery
	err   error
	sends int
}

func (s *stubSink) Send(uuid scenemap.UUID, msg any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	if s.err != nil {
		return s.err
	}
	s.feed = append(s.feed, delivery{uuid: uuid, msg: msg})
	return nil
}

func (s *stubSink) SendBatch(uuids []scenemap.UUID, msg any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends++
	if s.err != nil {
		return s.err
	}
	// 批量投递也拆成单条记录：测试关心的是"谁收到了什么"，
	// 批量只是传输层优化，不该影响断言口径。
	for _, u := range uuids {
		s.feed = append(s.feed, delivery{uuid: u, msg: msg, batch: true})
	}
	return nil
}

func (s *stubSink) setErr(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *stubSink) reset() {
	s.mu.Lock()
	s.feed = nil
	s.sends = 0
	s.mu.Unlock()
}

func (s *stubSink) all() []delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]delivery, len(s.feed))
	copy(out, s.feed)
	return out
}

// countTo 统计发给某个实体的、满足条件的消息数。
func (s *stubSink) countTo(uuid scenemap.UUID, match func(any) bool) int {
	n := 0
	for _, d := range s.all() {
		if d.uuid != uuid {
			continue
		}
		if match == nil || match(d.msg) {
			n++
		}
	}
	return n
}

func isAppear(msg any) bool {
	_, ok := msg.(aoi.AppearMsg)
	return ok
}

func isLeave(msg any) bool {
	_, ok := msg.(aoi.LeaveMsg)
	return ok
}

func isEnterView(msg any) bool {
	_, ok := msg.(aoi.EnterViewMsg)
	return ok
}

func isMove(msg any) bool {
	_, ok := msg.(aoi.MoveMsg)
	return ok
}

// newTestAOI 构造一个 100×100 行走格的测试 AOI。
func newTestAOI(t *testing.T, sink aoi.Sink) *aoi.AOI {
	t.Helper()
	a, err := aoi.NewAOI(aoi.Config{
		Key:    scenemap.MapKey{MapID: 1},
		Geo:    testGeo(),
		Size:   grid.Size{Width: 100, Height: 100},
		Sink:   sink,
		Logger: aoi.NopLogger{},
	})
	require.NoError(t, err, "NewAOI")
	t.Cleanup(func() {
		_ = a.Close(context.Background())
	})
	return a
}

func put(t *testing.T, a *aoi.AOI, id int64, kind scenemap.EntityKind, cell aoi.CellKey) {
	t.Helper()
	x, y := pixelOf(cell)
	err := a.CallEnter(context.Background(), aoi.Entity{
		UUID:  scenemap.UUID(id),
		Kind:  kind,
		PX:    x,
		PY:    y,
		HP:    100,
		MaxHP: 100,
	})
	if err != nil {
		t.Fatalf("enter %d: %v", id, err)
	}
}

func moveTo(t *testing.T, a *aoi.AOI, id int64, cell aoi.CellKey) {
	t.Helper()
	x, y := pixelOf(cell)
	if err := a.CallSync(context.Background(), scenemap.UUID(id), x, y, scenemap.DirRight, 0); err != nil {
		t.Fatalf("sync %d: %v", id, err)
	}
}

// ---------------------------------------------------------------------------
// ViewGrid
// ---------------------------------------------------------------------------

func TestViewGridIndex(t *testing.T) {
	g := aoi.NewViewGrid()
	a := aoi.CellKey{X: 1, Y: 1}
	b := aoi.CellKey{X: 2, Y: 1}

	g.Add(1, a)
	g.Add(2, a)
	g.Add(3, b)

	if g.Len() != 3 {
		t.Fatalf("Len=%d, want 3", g.Len())
	}
	// 同一格里的实体：去重后升序
	if got := g.EntitiesIn([]aoi.CellKey{a, a, b}); len(got) != 3 ||
		got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("EntitiesIn = %v, want [1 2 3]", got)
	}

	// 同格不迁移
	old, moved := g.Move(1, a)
	if moved || old != a {
		t.Fatalf("Move 同格: old=%v moved=%v, want %v false", old, moved, a)
	}
	// 跨格迁移
	old, moved = g.Move(1, b)
	if !moved || old != a {
		t.Fatalf("Move 跨格: old=%v moved=%v, want %v true", old, moved, a)
	}
	if cell, _ := g.CellOf(1); cell != b {
		t.Fatalf("CellOf(1)=%v, want %v", cell, b)
	}

	if _, ok := g.Remove(1); !ok {
		t.Fatal("Remove(1) 应成功")
	}
	if _, ok := g.Remove(1); ok {
		t.Fatal("重复 Remove 应返回 false")
	}
	if g.Len() != 2 {
		t.Fatalf("Len=%d, want 2", g.Len())
	}

	// 九宫格：含自身、9 个、顺序固定
	n := aoi.Neighbors9(b)
	if len(n) != 9 || n[0] != b {
		t.Fatalf("Neighbors9 = %v", n)
	}
	want := []aoi.CellKey{
		{2, 1}, {3, 1}, {3, 2}, {2, 2}, {1, 2}, {1, 1}, {1, 0}, {2, 0}, {3, 0},
	}
	for i := range want {
		if n[i] != want[i] {
			t.Fatalf("Neighbors9[%d]=%v, want %v", i, n[i], want[i])
		}
	}

	// 九宫格可见性：切比雪夫距离 <= 1
	if !aoi.ASeeB(aoi.CellKey{X: 1, Y: 1}, aoi.CellKey{X: 2, Y: 2}) {
		t.Fatal("对角相邻应可见")
	}
	if aoi.ASeeB(aoi.CellKey{X: 1, Y: 1}, aoi.CellKey{X: 1, Y: 3}) {
		t.Fatal("距离 2 应不可见")
	}

	// 清空
	g.Clear()
	if g.Len() != 0 {
		t.Fatalf("Clear 后 Len=%d", g.Len())
	}
}

func TestViewCellOfMatchesGeometry(t *testing.T) {
	geo := testGeo()
	// 视野格 (2,3) 的中心行走格是 (2*6+3, 3*6+3) = (15,21)
	x, y := pixelOf(aoi.CellKey{X: 2, Y: 3})
	if got := aoi.ViewCellOf(geo, x, y); got != (aoi.CellKey{X: 2, Y: 3}) {
		t.Fatalf("ViewCellOf=%v, want (2,3)", got)
	}
	// 视野格边界：行走格 17 属于视野格 2（17/6=2），18 属于 3
	if got := aoi.ViewCellOf(geo, 17*testCellW+1, 0); got.X != 2 {
		t.Fatalf("行走格 17 应属视野格 2, got %v", got)
	}
	if got := aoi.ViewCellOf(geo, 18*testCellW+1, 0); got.X != 3 {
		t.Fatalf("行走格 18 应属视野格 3, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// Enter / 视野
// ---------------------------------------------------------------------------

func TestEnterSendsFullView(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	put(t, a, 1, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 1})
	sink.reset()
	put(t, a, 2, scenemap.EntityKindPlayer, aoi.CellKey{X: 2, Y: 2})

	// 新实体收到全量包，里面已经有 1
	var got aoi.EnterViewMsg
	found := false
	for _, d := range sink.all() {
		if d.uuid == 2 {
			if m, ok := d.msg.(aoi.EnterViewMsg); ok {
				got, found = m, true
			}
		}
	}
	if !found {
		t.Fatal("2 没有收到 EnterViewMsg")
	}
	if got.Self.UUID != 2 {
		t.Fatalf("Self=%v, want 2", got.Self.UUID)
	}
	if len(got.Entities) != 1 || got.Entities[0].UUID != 1 {
		t.Fatalf("全量包实体=%v, want [1]", got.Entities)
	}

	// 老实体收到新实体的出现
	if n := sink.countTo(1, isAppear); n != 1 {
		t.Fatalf("1 收到 %d 条 Appear, want 1", n)
	}
	if n := a.Snapshot().Len(); n != 2 {
		t.Fatalf("快照实体数=%d, want 2", n)
	}
}

func TestNineGridVisibility(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	put(t, a, 1, scenemap.EntityKindPlayer, aoi.CellKey{X: 0, Y: 0})
	sink.reset()

	// 距离 3 个视野格：九宫格外，互相不可见。
	// 2 自己会收到一条空的全量包（这是协议约定："进图必有一次全量"），
	// 但 1 不该收到任何出现包。
	put(t, a, 2, scenemap.EntityKindPlayer, aoi.CellKey{X: 0, Y: 3})
	if n := sink.countTo(1, nil); n != 0 {
		t.Fatalf("九宫格外 1 收到 %d 条投递, want 0: %v", n, sink.all())
	}
	for _, d := range sink.all() {
		if d.uuid != 2 {
			t.Fatalf("出现了不该有的投递: %+v", d)
		}
		m, ok := d.msg.(aoi.EnterViewMsg)
		if !ok || len(m.Entities) != 0 {
			t.Fatalf("2 的全量包不该含实体: %+v", d.msg)
		}
	}
	if n := a.Snapshot().Len(); n != 2 {
		t.Fatalf("快照实体数=%d, want 2", n)
	}

	// 走近到相邻格：双向出现
	moveTo(t, a, 2, aoi.CellKey{X: 0, Y: 1})
	if n := sink.countTo(1, isAppear); n != 1 {
		t.Fatalf("1 收到 %d 条 Appear, want 1", n)
	}
	if n := sink.countTo(2, isAppear); n != 1 {
		t.Fatalf("2 收到 %d 条 Appear, want 1", n)
	}

	sink.reset()
	// 再走远：双向消失
	moveTo(t, a, 2, aoi.CellKey{X: 0, Y: 4})
	if n := sink.countTo(1, isLeave); n != 1 {
		t.Fatalf("1 收到 %d 条 Leave, want 1", n)
	}
	if n := sink.countTo(2, isLeave); n != 1 {
		t.Fatalf("2 收到 %d 条 Leave, want 1", n)
	}
}

func TestMoveBroadcastUnion(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	put(t, a, 1, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 1}) // 移动者
	put(t, a, 2, scenemap.EntityKindPlayer, aoi.CellKey{X: 2, Y: 2}) // 旧九宫格内
	put(t, a, 3, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 0}) // 仅旧九宫格内
	put(t, a, 4, scenemap.EntityKindPlayer, aoi.CellKey{X: 5, Y: 5}) // 一直不可见
	sink.reset()

	// 1 从 (1,1) 走到 (1,5)：新旧九宫格不相交
	moveTo(t, a, 1, aoi.CellKey{X: 1, Y: 5})

	// 自己不该收到自己的移动包
	if n := sink.countTo(1, isMove); n != 0 {
		t.Fatalf("移动者收到 %d 条自己的 Move, want 0", n)
	}
	// 旧九宫格内的 2、3 都收到移动包（并集语义）
	for _, id := range []scenemap.UUID{2, 3} {
		if n := sink.countTo(id, isMove); n != 1 {
			t.Fatalf("%d 收到 %d 条 Move, want 1", id, n)
		}
	}
	// 一直不可见的 4 什么都收不到
	if n := sink.countTo(4, nil); n != 0 {
		t.Fatalf("不可见的 4 收到 %d 条投递, want 0", n)
	}
	// 移动者收到 2、3 的消失（走出视野）
	if n := sink.countTo(1, isLeave); n != 2 {
		t.Fatalf("移动者收到 %d 条 Leave, want 2", n)
	}
}

func TestSameCellNoBroadcast(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	put(t, a, 1, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 1})
	put(t, a, 2, scenemap.EntityKindPlayer, aoi.CellKey{X: 2, Y: 2})
	sink.reset()

	// 同一视野格内的像素微调：只更新快照位置，不广播
	x, y := pixelOf(aoi.CellKey{X: 1, Y: 1})
	if err := a.CallSync(context.Background(), 1, x+3, y+2, scenemap.DirDown, 1); err != nil {
		t.Fatalf("CallSync: %v", err)
	}
	if n := len(sink.all()); n != 0 {
		t.Fatalf("同格微调产生 %d 条投递, want 0: %v", n, sink.all())
	}
	b, ok := a.Snapshot().Entity(1)
	if !ok || b.X != x+3 || b.Y != y+2 || b.Dir != scenemap.DirDown || b.State != 1 {
		t.Fatalf("快照位置未更新: %+v ok=%v", b, ok)
	}
}

func TestLeaveNotifiesViewers(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	put(t, a, 1, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 1})
	put(t, a, 2, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 2})
	sink.reset()

	if err := a.CallLeave(context.Background(), 1); err != nil {
		t.Fatalf("CallLeave: %v", err)
	}
	if n := sink.countTo(2, isLeave); n != 1 {
		t.Fatalf("2 收到 %d 条 Leave, want 1", n)
	}
	if _, ok := a.Snapshot().Entity(1); ok {
		t.Fatal("离图后快照里仍有 1")
	}
	if n := a.Snapshot().Len(); n != 1 {
		t.Fatalf("快照实体数=%d, want 1", n)
	}

	// 重复离图：Not Found，不 panic、不误发
	if err := a.CallLeave(context.Background(), 1); !errors.Is(err, scenemap.ErrNotFound) {
		t.Fatalf("重复离图 err=%v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// 广播
// ---------------------------------------------------------------------------

// typedMsg 是带协议号的测试消息。
type typedMsg struct{ typ int32 }

func (m typedMsg) MsgType() int32 { return m.typ }

func TestBroadcastMapSkipsNPE(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	put(t, a, 1, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 1})
	put(t, a, 2, scenemap.EntityKindPlayer, aoi.CellKey{X: 3, Y: 3})
	put(t, a, 3, scenemap.EntityKindNPE, aoi.CellKey{X: 1, Y: 2})
	sink.reset()

	if err := a.CallBroadcast(context.Background(), typedMsg{typ: 4001}, aoi.MapScope()); err != nil {
		t.Fatalf("CallBroadcast: %v", err)
	}
	if n := sink.countTo(1, nil); n != 1 {
		t.Fatalf("玩家 1 收到 %d 条, want 1", n)
	}
	if n := sink.countTo(2, nil); n != 1 {
		t.Fatalf("玩家 2 收到 %d 条, want 1", n)
	}
	// 怪物不收玩家类消息（msgType > 0）
	if n := sink.countTo(3, nil); n != 0 {
		t.Fatalf("怪物收到 %d 条, want 0", n)
	}

	// 系统消息（msgType <= 0）怪物也收：用于 Buff 刷新这类全图同步
	sink.reset()
	if err := a.CallBroadcast(context.Background(), typedMsg{typ: 0}, aoi.MapScope()); err != nil {
		t.Fatalf("CallBroadcast 系统消息: %v", err)
	}
	if n := sink.countTo(3, nil); n != 1 {
		t.Fatalf("怪物收到 %d 条系统消息, want 1", n)
	}
}

func TestBroadcastViewportScope(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	put(t, a, 1, scenemap.EntityKindPlayer, aoi.CellKey{X: 1, Y: 1})
	put(t, a, 2, scenemap.EntityKindPlayer, aoi.CellKey{X: 2, Y: 1})
	put(t, a, 3, scenemap.EntityKindPlayer, aoi.CellKey{X: 5, Y: 5})
	sink.reset()

	if err := a.CallBroadcast(context.Background(), typedMsg{typ: 4100}, aoi.ViewportScope(1)); err != nil {
		t.Fatalf("CallBroadcast: %v", err)
	}
	if n := sink.countTo(2, nil); n != 1 {
		t.Fatalf("视野内 2 收到 %d 条, want 1", n)
	}
	if n := sink.countTo(3, nil); n != 0 {
		t.Fatalf("视野外 3 收到 %d 条, want 0", n)
	}
	if n := sink.countTo(1, nil); n != 0 {
		t.Fatalf("发送者自己收到 %d 条, want 0", n)
	}

	// 发送者不存在
	if err := a.CallBroadcast(context.Background(), typedMsg{typ: 1}, aoi.ViewportScope(99)); !errors.Is(err, scenemap.ErrNotFound) {
		t.Fatalf("err=%v, want ErrNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// 慢消费者 / 快照
// ---------------------------------------------------------------------------

func TestSlowConsumerDoesNotBlock(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	sink.setErr(errors.New("下游信箱已满"))
	// Sink 报错不影响命令执行：AOI 丢弃并记日志，绝不阻塞主循环
	for i := int64(1); i <= 5; i++ {
		put(t, a, i, scenemap.EntityKindPlayer, aoi.CellKey{X: int32(i), Y: 1})
	}
	if n := a.Snapshot().Len(); n != 5 {
		t.Fatalf("快照实体数=%d, want 5（投递失败不应回滚状态）", n)
	}
	if a.Dropped() == 0 {
		t.Fatal("Dropped()=0，投递失败没有计数")
	}

	// 下游恢复后仍能继续处理后续命令（这就是"不被一个慢消费者拖死"）
	sink.setErr(nil)
	put(t, a, 6, scenemap.EntityKindPlayer, aoi.CellKey{X: 6, Y: 1})
	if n := a.Snapshot().Len(); n != 6 {
		t.Fatalf("恢复后快照实体数=%d, want 6", n)
	}
}

func TestSnapshotConcurrentRead(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// 只读：Entities 与 Views 必须自洽
			s := a.Snapshot()
			total := 0
			for _, list := range s.Views {
				total += len(list)
			}
			if total != len(s.Entities) || len(s.CellOf) != len(s.Entities) {
				t.Errorf("快照自洽性被破坏: entities=%d views=%d cellOf=%d",
					len(s.Entities), total, len(s.CellOf))
				return
			}
		}
	}()

	for i := int64(1); i <= 40; i++ {
		put(t, a, i, scenemap.EntityKindPlayer, aoi.CellKey{X: int32(i % 8), Y: int32(i % 5)})
	}
	for round := 0; round < 5; round++ {
		for i := int64(1); i <= 40; i++ {
			moveTo(t, a, i, aoi.CellKey{X: int32((int(i) + round) % 8), Y: int32(round % 5)})
		}
	}
	for i := int64(1); i <= 20; i++ {
		if err := a.CallLeave(context.Background(), scenemap.UUID(i)); err != nil {
			t.Fatalf("CallLeave(%d): %v", i, err)
		}
	}

	close(stop)
	wg.Wait()
}

func TestSnapshotConsistent(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	for i := int64(1); i <= 30; i++ {
		put(t, a, i, scenemap.EntityKindPlayer, aoi.CellKey{X: int32(i % 6), Y: int32(i % 4)})
	}
	moveTo(t, a, 1, aoi.CellKey{X: 7, Y: 7})
	_ = a.CallLeave(context.Background(), 2)

	s := a.Snapshot()
	if s.Revision == 0 {
		t.Fatal("Revision 未推进")
	}
	if len(s.Entities) != len(s.CellOf) {
		t.Fatalf("Entities=%d CellOf=%d, 应一致", len(s.Entities), len(s.CellOf))
	}
	total := 0
	for cell, list := range s.Views {
		total += len(list)
		for _, u := range list {
			if c, ok := s.CellOf[u]; !ok || c != cell {
				t.Fatalf("实体 %d 在 Views[%v] 里，但 CellOf=%v ok=%v", u, cell, c, ok)
			}
			if _, ok := s.Entities[u]; !ok {
				t.Fatalf("实体 %d 在 Views 里但不在 Entities 里", u)
			}
		}
	}
	if total != len(s.Entities) {
		t.Fatalf("Σlen(Views)=%d, len(Entities)=%d, 应一致", total, len(s.Entities))
	}
	// 快照里的切片必须按 UUID 升序（客户端不抖动的前提）
	for cell, list := range s.Views {
		for i := 1; i < len(list); i++ {
			if list[i-1] >= list[i] {
				t.Fatalf("Views[%v] 未排序: %v", cell, list)
			}
		}
	}
}

func TestEnterAsyncEventuallyVisible(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)

	x, y := pixelOf(aoi.CellKey{X: 1, Y: 1})
	// 异步投递：返回 nil 只表示"投进去了"
	if err := a.Enter(context.Background(), aoi.Entity{
		UUID: 1, Kind: scenemap.EntityKindPlayer, PX: x, PY: y,
	}); err != nil {
		t.Fatalf("Enter: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.Snapshot().Len() == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("异步 Enter 未生效，快照实体数=%d", a.Snapshot().Len())
}

// ---------------------------------------------------------------------------
// 房间池
// ---------------------------------------------------------------------------

func TestPoolAcquireReleaseAndReset(t *testing.T) {
	sink := &stubSink{}
	pool := aoi.NewPool(2, aoi.Config{
		Geo:    testGeo(),
		Size:   grid.Size{Width: 100, Height: 100},
		Sink:   sink,
		Logger: aoi.NopLogger{},
	})
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	keyA := scenemap.MapKey{MapID: 1, CliKey: "a"}
	keyB := scenemap.MapKey{MapID: 1, CliKey: "b"}
	keyC := scenemap.MapKey{MapID: 1, CliKey: "c"}

	a1, err := pool.Acquire(keyA)
	require.NoError(t, err, "Acquire A")
	if a1.Key() != keyA {
		t.Fatalf("Key=%v, want %v", a1.Key(), keyA)
	}
	if err := a1.CallEnter(context.Background(), aoi.Entity{
		UUID: 1, Kind: scenemap.EntityKindPlayer, PX: 10, PY: 10,
	}); err != nil {
		t.Fatalf("enter: %v", err)
	}
	if a1.Len() != 1 {
		t.Fatalf("Len=%d, want 1", a1.Len())
	}

	// 容量 2：第二个可以开，第三个必须被拒（池空 ⇒ 明确过载，而不是排队）
	a2, err := pool.Acquire(keyB)
	require.NoError(t, err, "Acquire B")
	if _, err := pool.Acquire(keyC); !errors.Is(err, scenemap.ErrRoomPoolExhausted) {
		t.Fatalf("第三个 Acquire err=%v, want ErrRoomPoolExhausted", err)
	}
	if pool.InUse() != 2 {
		t.Fatalf("InUse=%d, want 2", pool.InUse())
	}

	// 归还前必须先关（Pool 契约）：关掉 A 再还池
	if err := a1.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	pool.Release(a1)
	if pool.Idle() != 1 {
		t.Fatalf("Idle=%d, want 1", pool.Idle())
	}

	// 复用的实例不能带上一局的实体，也不能带旧 Key
	a3, err := pool.Acquire(keyC)
	require.NoError(t, err, "Acquire C")
	if a3.Len() != 0 {
		t.Fatalf("复用实例 Len=%d, want 0（索引未清空）", a3.Len())
	}
	if a3.Key() != keyC {
		t.Fatalf("复用实例 Key=%v, want %v", a3.Key(), keyC)
	}
	_ = a2
}

func TestAOIClosedRejectsCommands(t *testing.T) {
	sink := &stubSink{}
	a := newTestAOI(t, sink)
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := a.Enter(context.Background(), aoi.Entity{UUID: 9, Kind: scenemap.EntityKindPlayer}); err == nil {
		t.Fatal("关闭后 Enter 应报错")
	}
	// 幂等：重复关闭不 panic、不返回错误
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("重复 Close: %v", err)
	}
}
