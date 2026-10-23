package room_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/aoi"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/room"
	"github.com/stretchr/testify/require"
)

// stubSink 只记录投递次数与内容，供 NodeDown 兜底广播断言。
type stubSink struct {
	mu   sync.Mutex
	feed []any
}

func (s *stubSink) Send(_ scenemap.UUID, msg any) error {
	s.mu.Lock()
	s.feed = append(s.feed, msg)
	s.mu.Unlock()
	return nil
}

func (s *stubSink) SendBatch(_ []scenemap.UUID, msg any) error {
	s.mu.Lock()
	s.feed = append(s.feed, msg)
	s.mu.Unlock()
	return nil
}

func (s *stubSink) all() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]any, len(s.feed))
	copy(out, s.feed)
	return out
}

func (s *stubSink) reset() {
	s.mu.Lock()
	s.feed = nil
	s.mu.Unlock()
}

// stubCloser 是 MGR 的内存替身。
type stubCloser struct {
	closeCalls atomic.Int32
	closeErr   error
}

func (c *stubCloser) Close(context.Context) error {
	c.closeCalls.Add(1)
	return c.closeErr
}

// nodeCloser 额外实现 NodeDownHandler。
type nodeCloser struct {
	stubCloser
	nodeCalls atomic.Int32
	nodes     atomic.Value // []string
	nodeErr   error
}

func (c *nodeCloser) NodeDown(_ context.Context, nodes []string) error {
	c.nodeCalls.Add(1)
	c.nodes.Store(append([]string(nil), nodes...))
	return c.nodeErr
}

// stubFactory 记录调用次数，可注入失败。
type stubFactory struct {
	mu      sync.Mutex
	calls   int
	err     error
	closer  room.Closer
	lastCfg room.OpenConfig
}

func (f *stubFactory) Create(_ context.Context, r *room.Room, cfg room.OpenConfig) (room.Closer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastCfg = cfg
	if f.err != nil {
		return nil, f.err
	}
	if r.AOI == nil {
		return nil, errors.New("room.AOI 未注入")
	}
	if f.closer == nil {
		f.closer = &stubCloser{}
	}
	return f.closer, nil
}

func (f *stubFactory) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *stubFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func testConfig(sink aoi.Sink) room.OpenConfig {
	return room.OpenConfig{
		Sink:   sink,
		Geo:    grid.Geometry{CellW: 48, CellH: 24, AoiGrid: 6},
		Size:   grid.Size{Width: 100, Height: 100},
		Logger: aoi.NopLogger{},
	}
}

// newHarness 构造独立 Manager + 显式小池（绝不用 Default()，
// 单例跨用例存活会让下一个用例莫名 ErrAlreadyExists）。
func newHarness(t *testing.T, poolSize int) (*room.Manager, *aoi.Pool, *stubSink, *stubFactory) {
	t.Helper()
	sink := &stubSink{}
	pool := aoi.NewPool(poolSize, aoi.Config{
		Geo:    grid.Geometry{CellW: 48, CellH: 24, AoiGrid: 6},
		Size:   grid.Size{Width: 100, Height: 100},
		Sink:   sink,
		Logger: aoi.NopLogger{},
	})
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	factory := &stubFactory{}
	m := room.NewManager(pool, factory, aoi.NopLogger{})
	return m, pool, sink, factory
}

func open(t *testing.T, m *room.Manager, key scenemap.MapKey, route scenemap.Route, sink aoi.Sink) *room.Room {
	t.Helper()
	r, err := m.Open(context.Background(), key, route, testConfig(sink))
	require.NoError(t, err)
	return r
}

// ---------------------------------------------------------------------------

func TestOpenAndGet(t *testing.T) {
	m, _, sink, _ := newHarness(t, 2)
	key := scenemap.MapKey{MapID: 1, CliKey: "r1"}

	r := open(t, m, key, 1, sink)
	require.EqualValues(t, room.StateRunning, r.CurrentState())
	if r.MGR == nil || r.AOI == nil {
		t.Fatal("Room 的 MGR/AOI 未装配")
	}
	got, ok := m.Get(key)
	if !ok || got != r {
		t.Fatalf("Get=%v,%v want 同一实例", got, ok)
	}
	require.EqualValues(t, 1, m.Len())
	if routes := m.List(key.MapID); len(routes) != 1 || routes[0] != 1 {
		t.Fatalf("List=%v, want [1]", routes)
	}
}

func TestOpenDuplicate(t *testing.T) {
	m, _, sink, _ := newHarness(t, 3)
	key := scenemap.MapKey{MapID: 1, CliKey: "dup"}

	open(t, m, key, 1, sink)
	_, err := m.Open(context.Background(), key, 1, testConfig(sink))
	if !errors.Is(err, scenemap.ErrAlreadyExists) {
		t.Fatalf("重复开房 err=%v, want ErrAlreadyExists", err)
	}
}

// TestOpenConcurrentSameKey 验证占位集合：并发开同一个 Key 只有一个成功。
func TestOpenConcurrentSameKey(t *testing.T) {
	m, _, sink, _ := newHarness(t, 10)
	key := scenemap.MapKey{MapID: 1, CliKey: "race"}

	const n = 10
	var wg sync.WaitGroup
	var okCount atomic.Int32
	var dupCount atomic.Int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Open(context.Background(), key, 1, testConfig(sink)); err != nil {
				if errors.Is(err, scenemap.ErrAlreadyExists) {
					dupCount.Add(1)
				}
				return
			}
			okCount.Add(1)
		}()
	}
	wg.Wait()

	require.EqualValues(t, 1, okCount.Load())
	require.EqualValues(t, n-1, dupCount.Load())
}

func TestOpenFactoryErrorRollsBack(t *testing.T) {
	m, pool, sink, factory := newHarness(t, 1)
	key := scenemap.MapKey{MapID: 1, CliKey: "bad"}

	factory.setErr(errors.New("加载地图失败"))
	if _, err := m.Open(context.Background(), key, 1, testConfig(sink)); err == nil {
		t.Fatal("工厂失败时 Open 应报错")
	}
	if _, ok := m.Get(key); ok {
		t.Fatal("失败后房间不应被登记")
	}
	require.EqualValues(t, 1, pool.Idle())
	// 占位必须被摘掉，否则这张图再也开不起来
	factory.setErr(nil)
	if _, err := m.Open(context.Background(), key, 1, testConfig(sink)); err != nil {
		t.Fatalf("回滚后重开失败: %v", err)
	}
}

func TestCloseReleasesAOIToPool(t *testing.T) {
	m, pool, sink, factory := newHarness(t, 2)
	key := scenemap.MapKey{MapID: 1, CliKey: "close"}
	r := open(t, m, key, 1, sink)

	// 房间里放一个实体，验证还池后不会被下一局看到
	if err := r.AOI.CallEnter(context.Background(), aoi.Entity{
		UUID: 7, Kind: scenemap.EntityKindPlayer, PX: 10, PY: 10,
	}); err != nil {
		t.Fatalf("enter: %v", err)
	}
	require.EqualValues(t, 1, r.AOI.Len())

	if err := m.Close(context.Background(), key, "test"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, ok := m.Get(key); ok {
		t.Fatal("关房后 Get 仍能查到")
	}
	require.EqualValues(t, room.StateClosed, r.CurrentState())
	if c := factory.closer.(*stubCloser); c.closeCalls.Load() != 1 {
		t.Fatalf("MGR.Close 调用 %d 次, want 1", c.closeCalls.Load())
	}
	require.EqualValues(t, 1, pool.Idle())
	require.EqualValues(t, 0, r.AOI.Len())
}

func TestCloseNotFound(t *testing.T) {
	m, _, _, _ := newHarness(t, 1)
	err := m.Close(context.Background(), scenemap.MapKey{MapID: 1, CliKey: "nope"}, "test")
	if !errors.Is(err, scenemap.ErrNotFound) {
		t.Fatalf("err=%v, want ErrNotFound", err)
	}
}

// TestConcurrentCloseOnlyOneWins：10 个 goroutine 同时关，
// 只有摘到登记表的那个成功，其余一律 ErrNotFound；MGR 只关一次。
func TestConcurrentCloseOnlyOneWins(t *testing.T) {
	m, pool, sink, factory := newHarness(t, 2)
	key := scenemap.MapKey{MapID: 1, CliKey: "concurrent"}
	open(t, m, key, 1, sink)

	const n = 10
	var wg sync.WaitGroup
	var okCount, notFoundCount atomic.Int32
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 尽量让 10 个 goroutine 同时冲进去
			err := m.Close(context.Background(), key, "concurrent-test")
			switch {
			case err == nil:
				okCount.Add(1)
			case errors.Is(err, scenemap.ErrNotFound):
				notFoundCount.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	require.EqualValues(t, 1, okCount.Load())
	require.EqualValues(t, n-1, notFoundCount.Load())
	if c := factory.closer.(*stubCloser); c.closeCalls.Load() != 1 {
		t.Fatalf("MGR.Close 调用 %d 次, want 1", c.closeCalls.Load())
	}
	require.EqualValues(t, 1, pool.Idle())
}

func TestPoolExhausted(t *testing.T) {
	m, _, sink, _ := newHarness(t, 1)
	keyA := scenemap.MapKey{MapID: 1, CliKey: "a"}
	keyB := scenemap.MapKey{MapID: 1, CliKey: "b"}

	open(t, m, keyA, 1, sink)
	_, err := m.Open(context.Background(), keyB, 1, testConfig(sink))
	if !errors.Is(err, scenemap.ErrRoomPoolExhausted) {
		t.Fatalf("池耗尽 err=%v, want ErrRoomPoolExhausted", err)
	}
	if _, ok := m.Get(keyA); !ok {
		t.Fatal("开房失败不应影响已有房间")
	}

	// 关掉 A 腾出槽位后 B 才能开
	if err := m.Close(context.Background(), keyA, "free"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := m.Open(context.Background(), keyB, 1, testConfig(sink)); err != nil {
		t.Fatalf("腾槽后开房失败: %v", err)
	}
}

// TestCloseJoinsErrorsButStillCleans：MGR 关失败也要继续关 AOI、还要还池，
// 最后把失败一起返回。
func TestCloseJoinsErrorsButStillCleans(t *testing.T) {
	m, pool, sink, factory := newHarness(t, 1)
	factory.closer = &stubCloser{closeErr: errors.New("mgr boom")}
	key := scenemap.MapKey{MapID: 1, CliKey: "boom"}
	open(t, m, key, 1, sink)

	err := m.Close(context.Background(), key, "test")
	if err == nil || !strings.Contains(err.Error(), "mgr boom") {
		t.Fatalf("err=%v, 应包含 MGR 的失败原因", err)
	}
	if _, ok := m.Get(key); ok {
		t.Fatal("即使失败，房间也必须从登记表摘除（否则永远关不掉）")
	}
	require.EqualValues(t, 1, pool.Idle())
}

func TestListRoutes(t *testing.T) {
	m, _, sink, _ := newHarness(t, 4)
	const mapID = int32(9)

	open(t, m, scenemap.MapKey{MapID: mapID, CliKey: "a"}, 3, sink)
	open(t, m, scenemap.MapKey{MapID: mapID, CliKey: "b"}, 1, sink)
	open(t, m, scenemap.MapKey{MapID: mapID, CliKey: "c"}, 2, sink)

	routes := m.List(mapID)
	want := []scenemap.Route{1, 2, 3}
	require.EqualValues(t, len(want), len(routes))
	for i := range want {
		require.EqualValues(t, want[i], routes[i])
	}

	if err := m.Close(context.Background(), scenemap.MapKey{MapID: mapID, CliKey: "b"}, "test"); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if routes := m.List(mapID); len(routes) != 2 || routes[0] != 2 {
		t.Fatalf("关房后 List=%v, want [2 3]", routes)
	}
	// 未开过的地图返回空
	if routes := m.List(999); routes != nil {
		t.Fatalf("List(999)=%v, want nil", routes)
	}
}

func TestNodeDownRoutesToCrossNodeRooms(t *testing.T) {
	sink := &stubSink{}
	pool := aoi.NewPool(4, aoi.Config{Sink: sink, Logger: aoi.NopLogger{}})
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	m := room.NewManager(pool, nil, aoi.NopLogger{})

	// 两个跨节点房间，Node 不同；每次开房换一个工厂，便于分别观察
	c1 := &nodeCloser{}
	m.SetFactory(&stubFactory{closer: c1})
	if _, err := m.Open(context.Background(), scenemap.MapKey{MapID: 1, CliKey: "x", Node: "n1"}, 1, testConfig(sink)); err != nil {
		t.Fatalf("Open n1: %v", err)
	}
	c2 := &nodeCloser{}
	m.SetFactory(&stubFactory{closer: c2})
	if _, err := m.Open(context.Background(), scenemap.MapKey{MapID: 1, CliKey: "y", Node: "n2"}, 1, testConfig(sink)); err != nil {
		t.Fatalf("Open n2: %v", err)
	}

	if err := m.NodeDown(context.Background(), []string{"n1"}); err != nil {
		t.Fatalf("NodeDown: %v", err)
	}
	require.EqualValues(t, 1, c1.nodeCalls.Load())
	if got, _ := c1.nodes.Load().([]string); len(got) != 1 || got[0] != "n1" {
		t.Fatalf("nodes=%v, want [n1]", got)
	}
	// n2 不在通知列表里，不应被打扰
	require.EqualValues(t, 0, c2.nodeCalls.Load())
}

// TestNodeDownFallbackBroadcast：玩法没实现 NodeDownHandler 时，
// 至少向房间内广播一条通知（不能让玩家毫无感知）。
func TestNodeDownFallbackBroadcast(t *testing.T) {
	sink := &stubSink{}
	pool := aoi.NewPool(2, aoi.Config{Sink: sink, Logger: aoi.NopLogger{}})
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	factory := &stubFactory{}
	m := room.NewManager(pool, factory, aoi.NopLogger{})
	key := scenemap.MapKey{MapID: 1, CliKey: "fallback", Node: "n1"}
	r := open(t, m, key, 1, sink)

	if err := r.AOI.CallEnter(context.Background(), aoi.Entity{
		UUID: 1, Kind: scenemap.EntityKindPlayer, PX: 10, PY: 10,
	}); err != nil {
		t.Fatalf("enter: %v", err)
	}
	sink.reset()

	if err := m.NodeDown(context.Background(), []string{"n1"}); err != nil {
		t.Fatalf("NodeDown: %v", err)
	}
	// 兜底广播走 AOI 的异步邮箱，这里等它落地（最多 2s）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, msg := range sink.all() {
			if _, ok := msg.(room.NodeDownNotice); ok {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("未实现 NodeDownHandler 时应有兜底广播")
}

func TestOpenRequiresFactory(t *testing.T) {
	sink := &stubSink{}
	pool := aoi.NewPool(1, aoi.Config{Sink: sink, Logger: aoi.NopLogger{}})
	t.Cleanup(func() { _ = pool.Close(context.Background()) })

	m := room.NewManager(pool, nil, nil)
	_, err := m.Open(context.Background(), scenemap.MapKey{MapID: 1, CliKey: "nofactory"}, 1, testConfig(sink))
	if !errors.Is(err, scenemap.ErrInvalidState) {
		t.Fatalf("err=%v, want ErrInvalidState", err)
	}
}
