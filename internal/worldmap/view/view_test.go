package view

import (
	"context"
	"sync"
	"testing"
	"time"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/runtime"
	"game_study/internal/worldmap/spatial"
	"game_study/pkg/pb/internalv2"
	"github.com/stretchr/testify/require"
)

const testUnits = int64(1000)

// fakeLoader 是 Loader 的内存替身。
//
// 与真实实现一样按调用时刻返回"当前权威状态"：投影层只带键，
// Flush 回查时拿到的一定是最新值——这正是测试里能观察到
// "同一轮多次变化合并成最终态"的原因。
type fakeLoader struct {
	mu       sync.Mutex
	topo     domain.Topology
	revision uint64
	objects  map[domain.ObjectKey]*internalv2.ObjectUpsert
}

func newFakeLoader(topo domain.Topology) *fakeLoader {
	return &fakeLoader{
		topo:    topo,
		objects: make(map[domain.ObjectKey]*internalv2.ObjectUpsert),
	}
}

func (f *fakeLoader) put(up *internalv2.ObjectUpsert) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := domain.ObjectKey{WorldID: f.topo.WorldID, ID: domain.ObjectID(up.ObjectId)}
	f.objects[key] = cloneUpsert(up)
}

func (f *fakeLoader) drop(id uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, domain.ObjectKey{WorldID: f.topo.WorldID, ID: domain.ObjectID(id)})
}

func (f *fakeLoader) LoadScene(_ context.Context, sceneID domain.SceneID) (*spatial.SceneGrid, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	grid := spatial.NewSceneGrid(f.topo)
	for key, up := range f.objects {
		kind := domain.ObjectKind(up.Kind)
		if !kind.Valid() {
			continue // LINE 等投影种类不进业务索引
		}
		pos := domain.Pos{X: up.X, Y: up.Y}
		if f.topo.SceneAt(pos) != sceneID {
			continue
		}
		// Put 的 error 只可能来自越界/场景不一致；构造测试数据时保证合法。
		_ = grid.Put(key, kind, sceneID, pos)
	}
	return grid, f.revision, nil
}

func (f *fakeLoader) LoadObject(_ context.Context, key domain.ObjectKey) (*internalv2.ObjectUpsert, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	up, ok := f.objects[key]
	if !ok {
		return nil, nil
	}
	return cloneUpsert(up), nil
}

// testConfig 用 1 小时的 tick，避免后台 goroutine 在测试断言期间
// 额外触发 Flush；所有刷新都由测试显式调用，时序确定。
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.TickInterval = time.Hour
	cfg.QueueSize = 128
	cfg.ViewPendingBytes = 1 << 20
	cfg.MaxViewWidth = 400
	cfg.MaxViewHeight = 400
	return cfg
}

type harness struct {
	t      *testing.T
	topo   domain.Topology
	cfg    Config
	coord  *Coordinator
	loader *fakeLoader
	sv     *SceneView
	sub    *Subscription
	rect   Rect
}

func newHarness(t *testing.T, cfg Config, rect Rect) *harness {
	t.Helper()
	topo := domain.DefaultTopology(1)
	loader := newFakeLoader(topo)
	coord := NewCoordinator(topo.WorldID, topo, cfg, nil)
	coord.SetLoader(loader)
	h := &harness{t: t, topo: topo, cfg: cfg, coord: coord, loader: loader, rect: rect}
	t.Cleanup(coord.Close)
	return h
}

// subscribe 建立订阅并拿到对应的 SceneView（同包测试直接访问内部字段，
// 避免为测试额外暴露生产 API）。
func (h *harness) subscribe(since uint64) {
	h.t.Helper()
	sub, err := h.coord.Subscribe(1, h.rect, since)
	if err != nil {
		h.t.Fatalf("Subscribe: %v", err)
	}
	sv, ok := h.coord.sceneView(1)
	if !ok {
		h.t.Fatal("场景视图未创建")
	}
	h.sub = sub
	h.sv = sv
}

func (h *harness) apply(rev uint64, enters, updates, leaves []domain.ObjectKey) {
	h.t.Helper()
	batch := runtime.ProjectionBatch{
		WorldID:  h.topo.WorldID,
		SceneID:  1,
		Revision: rev,
		Enters:   enters,
		Updates:  updates,
		Leaves:   leaves,
	}
	if err := h.sv.ApplyBatch(context.Background(), batch); err != nil {
		h.t.Fatalf("ApplyBatch: %v", err)
	}
}

func (h *harness) flush() {
	h.t.Helper()
	if err := h.sv.Flush(context.Background()); err != nil {
		h.t.Fatalf("Flush: %v", err)
	}
}

func (h *harness) publish(rev uint64, enters, updates, leaves []domain.ObjectKey) {
	h.t.Helper()
	h.apply(rev, enters, updates, leaves)
	h.flush()
}

func recvEvent(t *testing.T, ch <-chan *internalv2.ViewEvent) *internalv2.ViewEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		require.True(t, ok, "订阅通道已关闭")
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("等待视野事件超时")
		return nil
	}
}

func expectNoEvent(t *testing.T, ch <-chan *internalv2.ViewEvent) {
	t.Helper()
	select {
	case ev := <-ch:
		t.Fatalf("不应收到事件，got %v", ev)
	case <-time.After(30 * time.Millisecond):
	}
}

// troopUpsert 构造一个部队的权威快照。坐标参数是格，写进 proto 时换算成 units。
func troopUpsert(id uint64, cellX, cellY int64) *internalv2.ObjectUpsert {
	x := cellX * testUnits
	y := cellY * testUnits
	return &internalv2.ObjectUpsert{
		ObjectId: id,
		Kind:     int32(domain.ObjectKindTroop),
		X:        x,
		Y:        y,
		Troop: &internalv2.TroopView{
			TroopId:  id,
			PlayerId: 1,
			SlotId:   1,
			Phase:    int32(domain.TroopPhaseMarching),
			X:        x,
			Y:        y,
			Version:  1,
		},
	}
}

func objKey(id uint64) domain.ObjectKey {
	return domain.ObjectKey{WorldID: 1, ID: domain.ObjectID(id)}
}

// TestSubscriptionLifecycle 覆盖"从不进入视野 → 进入 → 移动 → 离开"的完整顺序。
func TestSubscriptionLifecycle(t *testing.T) {
	h := newHarness(t, testConfig(), Rect{MinX: 10, MinY: 10, MaxX: 90, MaxY: 90})
	key := objKey(1001)
	h.loader.put(troopUpsert(1001, 200, 200)) // 订阅时在视野外
	h.subscribe(0)

	h.flush()
	expectNoEvent(t, h.sub.Events())

	// 进入视野：batch 只带键，Flush 回查得到 (40,40)。
	h.loader.put(troopUpsert(1001, 40, 40))
	h.publish(2, nil, []domain.ObjectKey{key}, nil)
	ev := recvEvent(t, h.sub.Events())
	if ev.GetUpsert() == nil {
		t.Fatalf("第 1 步应为 Upsert，got %v", ev)
	}
	if ev.GetRevision() != 2 || ev.GetUpsert().GetX() != 40*testUnits {
		t.Fatalf("进入事件不符: revision=%d x=%d", ev.GetRevision(), ev.GetUpsert().GetX())
	}
	require.EqualValues(t, int32(domain.ObjectKindTroop), ev.GetUpsert().GetKind())

	// 移动：同一对象再次更新，事件里是最终坐标。
	h.loader.put(troopUpsert(1001, 60, 60))
	h.publish(3, nil, []domain.ObjectKey{key}, nil)
	ev = recvEvent(t, h.sub.Events())
	if ev.GetRevision() != 3 || ev.GetUpsert() == nil || ev.GetUpsert().GetX() != 60*testUnits {
		t.Fatalf("更新事件不符: %v", ev)
	}

	// 离开视野（对象被删除）：只剩 Remove。
	h.loader.drop(1001)
	h.publish(4, nil, nil, []domain.ObjectKey{key})
	ev = recvEvent(t, h.sub.Events())
	if ev.GetRemove() == nil || ev.GetRemove().GetObjectId() != 1001 {
		t.Fatalf("离开事件应为 Remove(1001)，got %v", ev)
	}
	expectNoEvent(t, h.sub.Events())
}

// TestProjectionMerge 表驱动覆盖"一轮内合并"的三种情形。
func TestProjectionMerge(t *testing.T) {
	type step struct {
		kind string // enter / update / leave
		x    int64  // enter/update 时的格坐标
	}
	cases := []struct {
		name       string
		initialX   int64 // 0 表示订阅时对象不在视野内（放在视野外）
		steps      []step
		wantEvents int
		wantRemove bool
		wantX      int64
	}{
		{
			name:       "同轮多次变化只发最终态",
			steps:      []step{{kind: "update", x: 40}, {kind: "update", x: 50}},
			wantEvents: 1,
			wantX:      50,
		},
		{
			name:       "Enters+Leaves 同轮净删除不发事件",
			steps:      []step{{kind: "enter", x: 40}, {kind: "leave"}},
			wantEvents: 0,
		},
		{
			name:       "已可见对象更新后离开只发 Remove",
			initialX:   40,
			steps:      []step{{kind: "update", x: 50}, {kind: "leave"}},
			wantEvents: 1,
			wantRemove: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, testConfig(), Rect{MinX: 10, MinY: 10, MaxX: 90, MaxY: 90})
			key := objKey(2001)
			initial := int64(200) // 视野外
			if tc.initialX != 0 {
				initial = tc.initialX
			}
			h.loader.put(troopUpsert(2001, initial, initial))
			h.subscribe(0)
			h.flush()
			if tc.initialX != 0 {
				// 先消化订阅时的全量 Enter。
				recvEvent(t, h.sub.Events())
			} else {
				expectNoEvent(t, h.sub.Events())
			}

			for i, st := range tc.steps {
				rev := uint64(i + 2)
				switch st.kind {
				case "enter", "update":
					h.loader.put(troopUpsert(2001, st.x, st.x))
					h.apply(rev, nil, []domain.ObjectKey{key}, nil)
				case "leave":
					h.loader.drop(2001)
					h.apply(rev, nil, nil, []domain.ObjectKey{key})
				default:
					t.Fatalf("未知步骤 %q", st.kind)
				}
			}
			h.flush() // 所有步骤只 Flush 一次 → 同一轮

			var got []*internalv2.ViewEvent
			for {
				select {
				case ev, ok := <-h.sub.Events():
					require.True(t, ok, "订阅通道已关闭")
					got = append(got, ev)
				default:
					goto drained
				}
			}
		drained:
			require.Len(t, got, tc.wantEvents)
			if tc.wantEvents == 0 {
				return
			}
			ev := got[0]
			if tc.wantRemove {
				if ev.GetRemove() == nil || ev.GetRemove().GetObjectId() != 2001 {
					t.Fatalf("应为 Remove(2001)，got %v", ev)
				}
				return
			}
			if ev.GetUpsert() == nil || ev.GetUpsert().GetX() != tc.wantX*testUnits {
				t.Fatalf("应为 x=%d 的最终态 Upsert，got %v", tc.wantX, ev)
			}
		})
	}
}

// TestSlowConsumerResync 覆盖文档 12.6：
// 积压超限 → 丢弃增量并投递 Resync → 之后只发全量（Upsert），不再补 Remove。
func TestSlowConsumerResync(t *testing.T) {
	cfg := testConfig()
	cfg.ViewPendingBytes = 64 // 很小，两三条事件就会触发
	h := newHarness(t, cfg, Rect{MinX: 10, MinY: 10, MaxX: 90, MaxY: 90})
	key := objKey(3001)
	h.loader.put(troopUpsert(3001, 20, 20))
	h.subscribe(0)
	h.flush() // 全量 Enter 进入 channel，但不读 → 模拟慢消费者

	for i := 0; i < 10; i++ {
		h.loader.put(troopUpsert(3001, int64(21+i), 20))
		h.publish(uint64(2+i), nil, []domain.ObjectKey{key}, nil)
	}

	var got []*internalv2.ViewEvent
	for {
		select {
		case ev, ok := <-h.sub.Events():
			require.True(t, ok, "订阅通道已关闭")
			got = append(got, ev)
		default:
			goto drained
		}
	}
drained:
	if len(got) == 0 || got[0].GetResync() == nil {
		t.Fatalf("积压超限后首个事件应为 Resync，got %v", got)
	}
	if got := h.sub.PendingBytes(); got > cfg.ViewPendingBytes {
		t.Fatalf("Resync 后积压计数未重置: %d > %d", got, cfg.ViewPendingBytes)
	}
	// Resync 之后（最后一次 Resync 起）不允许出现增量 Remove。
	lastResync := -1
	for i, ev := range got {
		if ev.GetResync() != nil {
			lastResync = i
		}
	}
	for _, ev := range got[lastResync+1:] {
		require.Nil(t, ev.GetRemove())
	}

	// 再更新一次：客户端应收到全量 Upsert（而非增量补丁）。
	h.loader.put(troopUpsert(3001, 70, 70))
	h.publish(12, nil, []domain.ObjectKey{key}, nil)
	ev := recvEvent(t, h.sub.Events())
	if ev.GetUpsert() == nil || ev.GetUpsert().GetX() != 70*testUnits {
		t.Fatalf("Resync 后应收到全量 Upsert，got %v", ev)
	}
	expectNoEvent(t, h.sub.Events())
}

// TestRectCrossesBlockBoundary 覆盖"视野跨块边界不漏对象"。
// 场景 1 的块宽 = 1000/4 = 250 格，250 正好是块边界。
func TestRectCrossesBlockBoundary(t *testing.T) {
	h := newHarness(t, testConfig(), Rect{MinX: 240, MinY: 240, MaxX: 260, MaxY: 260})
	h.loader.put(troopUpsert(4001, 249, 245)) // 块 0
	h.loader.put(troopUpsert(4002, 250, 245)) // 块 1（边界上）
	h.loader.put(troopUpsert(4003, 260, 245)) // 右开边界外
	h.subscribe(0)
	h.flush()

	ev1 := recvEvent(t, h.sub.Events())
	ev2 := recvEvent(t, h.sub.Events())
	if ev1.GetUpsert().GetObjectId() != 4001 || ev2.GetUpsert().GetObjectId() != 4002 {
		t.Fatalf("跨块对象不全: %v %v", ev1, ev2)
	}
	expectNoEvent(t, h.sub.Events())
}

// TestViewClamp 覆盖"视野超过上限被裁剪"（文档 12.1）。
func TestViewClamp(t *testing.T) {
	cfg := testConfig()
	cfg.MaxViewWidth = 100
	cfg.MaxViewHeight = 100
	h := newHarness(t, cfg, Rect{MinX: 240, MinY: 240, MaxX: 900, MaxY: 900})
	h.loader.put(troopUpsert(5001, 250, 250)) // 裁剪后仍在视野内
	h.loader.put(troopUpsert(5002, 350, 250)) // x >= 340，被裁掉
	h.subscribe(0)

	want := Rect{MinX: 240, MinY: 240, MaxX: 340, MaxY: 340}
	if got := h.sub.Rect(); got != want {
		t.Fatalf("视野裁剪 = %+v, want %+v", got, want)
	}
	// [240,340)×[240,340) 跨 x/y 两个方向的块边界（块宽 250），
	// 覆盖 4 个桶：块列 0/1 × 块行 0/1。
	if blocks := BlocksInRect(h.topo, want); len(blocks) != 4 {
		t.Fatalf("裁剪后应覆盖 4 个块，got %v", blocks)
	}

	h.flush()
	ev := recvEvent(t, h.sub.Events())
	require.EqualValues(t, 5001, ev.GetUpsert().GetObjectId())
	expectNoEvent(t, h.sub.Events())
}

// TestLineCrossingViewport 覆盖文档 12.5：
// 起点和终点都在视野外、但折线穿过视野时，订阅者必须收到线事件。
func TestLineCrossingViewport(t *testing.T) {
	h := newHarness(t, testConfig(), Rect{MinX: 10, MinY: 10, MaxX: 50, MaxY: 50})
	h.subscribe(0)
	h.flush()
	expectNoEvent(t, h.sub.Events())

	lines := NewLineSet()
	// 视野 units 范围是 [10000,50000)；两端分别在 -50000 和 100000，
	// 都远在视野之外，只有中间段穿过。
	if err := lines.Upsert("move-1", []domain.Pos{
		{X: -50 * testUnits, Y: -50 * testUnits},
		{X: 100 * testUnits, Y: 100 * testUnits},
	}); err != nil {
		t.Fatalf("Upsert line: %v", err)
	}
	h.sv.SetLines(lines)
	h.flush()

	ev := recvEvent(t, h.sub.Events())
	up := ev.GetUpsert()
	if up == nil || up.GetKind() != LineKind {
		t.Fatalf("应收到 kind=%d 的线事件，got %v", LineKind, ev)
	}
	require.EqualValues(t, lineObjectID("move-1"), up.GetObjectId())
	h.flush() // 线没变 → 不应重复下发
	expectNoEvent(t, h.sub.Events())

	// 线删除 → Remove。
	lines.Remove("move-1")
	h.flush()
	ev = recvEvent(t, h.sub.Events())
	if ev.GetRemove() == nil || ev.GetRemove().GetObjectId() != lineObjectID("move-1") {
		t.Fatalf("线删除应发 Remove，got %v", ev)
	}
}
