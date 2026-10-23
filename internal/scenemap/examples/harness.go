// Package examples 把 scenemap 的一整套组件装成"可跑的最小场景"，
// 并提供 P0 验收测试。
//
// # 装配顺序（规格 3.2 / 4.2，不能乱）
//
//	玩家/网关
//	   │
//	   ▼
//	RoomManager.Open ──► 房间池取 AOI（AOI 先创建）
//	   │                    │
//	   │                    ▼
//	   │              bridge.NewAOIAdapter（把 mgr.AOIHandle 接到 aoi.AOI）
//	   │                    │
//	   ▼                    ▼
//	Factory.Create ──► mgr.New(Deps{AOI: adapter, Gameplay: 玩法})
//	   │                    │
//	   ▼                    ▼
//	登记 Room ─────────► Mgr.Start ──► Gameplay.MgrInit
//
// 为什么 AOI 必须先创建：MGR 的进图/移动/离图都会同步调 AOI，AOI 句柄为 nil
// 时 MGR 会退化成 NoopAOI——"图能跑但谁都看不见谁"。桥接层放在两者之间，
// 是因为 mgr 与 aoi 各自定义了实体/范围类型，谁 import 谁都会成环。
package examples

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/aoi"
	"game_study/internal/scenemap/bridge"
	"game_study/internal/scenemap/entity"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
	"game_study/internal/scenemap/room"
)

// RecordingSink 是 aoi.Sink 的记录实现：按 UUID 收集收到的消息。
//
// 为什么需要它：AOI 只产出"谁该收到什么"，不负责投递。测试要断言的正是
// 这句话本身——"A 收到了 B 的出现包、但没收到自己的移动包"。
//
// 并发：AOI 在自己的 goroutine 里同步调用 Send/SendBatch，测试在另一个
// goroutine 里读，所以必须加锁。锁只保护一个切片追加，成本可以忽略。
type RecordingSink struct {
	mu   sync.Mutex
	msgs map[scenemap.UUID][]any
}

// NewRecordingSink 构造空的记录器。
func NewRecordingSink() *RecordingSink {
	return &RecordingSink{msgs: make(map[scenemap.UUID][]any)}
}

// Send 实现 aoi.Sink。
func (s *RecordingSink) Send(uuid scenemap.UUID, msg any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.msgs == nil {
		s.msgs = make(map[scenemap.UUID][]any)
	}
	s.msgs[uuid] = append(s.msgs[uuid], msg)
	return nil
}

// SendBatch 实现 aoi.Sink。批量投递是"同一条消息发给多个收件人"，
// 记录时必须逐个展开，否则测试无法回答"谁收到了"。
func (s *RecordingSink) SendBatch(uuids []scenemap.UUID, msg any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.msgs == nil {
		s.msgs = make(map[scenemap.UUID][]any)
	}
	for _, u := range uuids {
		s.msgs[u] = append(s.msgs[u], msg)
	}
	return nil
}

// Messages 返回某个 UUID 收到的全部消息（副本，调用方可安全遍历）。
func (s *RecordingSink) Messages(uuid scenemap.UUID) []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.msgs[uuid]
	out := make([]any, len(src))
	copy(out, src)
	return out
}

// Count 返回某个 UUID 收到的消息条数。
func (s *RecordingSink) Count(uuid scenemap.UUID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs[uuid])
}

// AppearCount 返回 "viewer 看到 target 出现" 的次数。
func (s *RecordingSink) AppearCount(viewer, target scenemap.UUID) int {
	n := 0
	for _, msg := range s.Messages(viewer) {
		if m, ok := msg.(aoi.AppearMsg); ok && m.Entity.UUID == target {
			n++
		}
	}
	return n
}

// AppearCountKind 返回 viewer 收到的出现包总数（不区分是谁）。
func (s *RecordingSink) AppearCountKind(viewer scenemap.UUID) int {
	n := 0
	for _, msg := range s.Messages(viewer) {
		if _, ok := msg.(aoi.AppearMsg); ok {
			n++
		}
	}
	return n
}

// LeaveCount 返回 "viewer 看到 target 消失" 的次数。
func (s *RecordingSink) LeaveCount(viewer, target scenemap.UUID) int {
	n := 0
	for _, msg := range s.Messages(viewer) {
		if m, ok := msg.(aoi.LeaveMsg); ok && m.UUID == target {
			n++
		}
	}
	return n
}

// MovesFrom 返回 "viewer 收到 mover 的移动包" 的次数。
func (s *RecordingSink) MovesFrom(viewer, mover scenemap.UUID) int {
	n := 0
	for _, msg := range s.Messages(viewer) {
		if m, ok := msg.(aoi.MoveMsg); ok && m.UUID == mover {
			n++
		}
	}
	return n
}

// Reset 清空记录（同一批断言之间需要重置时用）。
func (s *RecordingSink) Reset() {
	s.mu.Lock()
	s.msgs = make(map[scenemap.UUID][]any)
	s.mu.Unlock()
}

// Option 是 Harness 的可选参数。
type Option func(*options)

type options struct {
	size         grid.Size
	geo          grid.Geometry
	walkable     mgr.WalkableFunc
	gameplay     mgr.Gameplay
	enterProtect time.Duration
	tickInterval time.Duration
	maxStep      int32
	cliKey       string
}

// defaultOptions 返回默认配置：32×32 的空图、默认几何、60 秒空房保护。
func defaultOptions() options {
	return options{
		size:         grid.Size{Width: 32, Height: 32},
		geo:          grid.DefaultGeometry(),
		enterProtect: 60 * time.Second,
		tickInterval: 50 * time.Millisecond,
		maxStep:      3,
		cliKey:       "test-room",
	}
}

// WithSize 设置地图尺寸。
func WithSize(w, h int32) Option {
	return func(o *options) { o.size = grid.Size{Width: w, Height: h} }
}

// WithGeometry 设置格↔像素换算。
func WithGeometry(g grid.Geometry) Option {
	return func(o *options) { o.geo = g }
}

// WithWalkable 设置静态可走查询（用来制造墙）。
func WithWalkable(fn mgr.WalkableFunc) Option {
	return func(o *options) { o.walkable = fn }
}

// WithBlocked 把若干格设为不可走（测试里造一堵小墙）。
func WithBlocked(cells ...scenemap.GridPos) Option {
	return func(o *options) {
		blocked := make(map[scenemap.GridPos]struct{}, len(cells))
		for _, c := range cells {
			blocked[c] = struct{}{}
		}
		o.walkable = func(p scenemap.GridPos) bool {
			_, hit := blocked[p]
			return !hit
		}
	}
}

// WithGameplay 注入玩法插件（nil 时 MGR 用 Unimplemented，只跑框架）。
func WithGameplay(g mgr.Gameplay) Option {
	return func(o *options) { o.gameplay = g }
}

// WithEnterProtect 设置空房保护时长。
func WithEnterProtect(d time.Duration) Option {
	return func(o *options) { o.enterProtect = d }
}

// WithTickInterval 设置 loop 间隔（同时决定 buff/AI 的间隔）。
func WithTickInterval(d time.Duration) Option {
	return func(o *options) { o.tickInterval = d }
}

// WithMaxStep 设置单次位置同步允许的最大切比雪夫步长。
func WithMaxStep(n int32) Option {
	return func(o *options) { o.maxStep = n }
}

// WithCliKey 设置房间的 CliKey（同一进程里开多个房间时要区分）。
func WithCliKey(key string) Option {
	return func(o *options) { o.cliKey = key }
}

// Harness 是一张"装好的图"：MGR + AOI + bridge + RoomManager + 玩法。
type Harness struct {
	// T 用于在装配失败时直接 Fatal（装配错误不该让调用方再判一次）。
	T *testing.T
	// Mgr 场景权威。
	Mgr *mgr.Mgr
	// AOI 视野与广播（由 RoomManager 从池里取出后交给 Factory）。
	AOI *aoi.AOI
	// Room 房间登记表（关图必须走它的 Close 收口）。
	Room *room.Manager
	// Sink 记录模式下的投递出口。
	Sink *RecordingSink
	// Key 房间标识。
	Key scenemap.MapKey
	// Route 分线号。
	Route scenemap.Route
	// Geo 格↔像素换算（测试算像素坐标要用）。
	Geo grid.Geometry
	// Size 地图尺寸。
	Size grid.Size

	gameplay     mgr.Gameplay
	walkable     mgr.WalkableFunc
	enterProtect time.Duration
	tickInterval time.Duration
	maxStep      int32

	// roomClosed 在收到 MGR 的关图通知时被写入一条（容量 1，防重复）。
	roomClosed chan struct{}
	closeOnce  sync.Once
}

// NewHarness 装配并启动一张图。
//
// 调用方不需要手动 Close：函数内部注册了 t.Cleanup；需要提前收尾时也可以
// 显式调 Close（幂等）。
func NewHarness(t *testing.T, mapID int32, route scenemap.Route, opts ...Option) *Harness {
	t.Helper()
	o := defaultOptions()
	for _, fn := range opts {
		fn(&o)
	}

	h := &Harness{
		T:            t,
		Key:          scenemap.MapKey{MapID: mapID, CliKey: o.cliKey},
		Route:        route,
		Geo:          o.geo,
		Size:         o.size,
		Sink:         NewRecordingSink(),
		gameplay:     o.gameplay,
		walkable:     o.walkable,
		enterProtect: o.enterProtect,
		tickInterval: o.tickInterval,
		maxStep:      o.maxStep,
		roomClosed:   make(chan struct{}, 1),
	}
	h.Room = room.NewManager(nil, &harnessFactory{h: h}, aoi.NopLogger{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.Room.Open(ctx, h.Key, route, room.OpenConfig{
		Sink: h.Sink,
		Geo:  h.Geo,
		Size: h.Size,
	}); err != nil {
		t.Fatalf("examples: 开房失败: %v", err)
	}
	t.Cleanup(h.Close)
	return h
}

// harnessFactory 是 room.Factory 的实现：用房间给的 AOI 建 MGR。
type harnessFactory struct{ h *Harness }

// Create 实现 room.Factory。
//
// 注意这里的顺序：AOI 已经由 RoomManager 创建好（r.AOI），我们先做
// bridge 适配，再把它注入 MGR——"AOI 先创建 → bridge → MGR → 玩法"。
func (f *harnessFactory) Create(_ context.Context, r *room.Room, _ room.OpenConfig) (room.Closer, error) {
	h := f.h
	adapter := bridge.NewAOIAdapter(r.AOI, h.Geo)

	cfg := mgr.Config{
		Key:          r.Key,
		Route:        r.Route,
		MapID:        r.Key.MapID,
		Geo:          h.Geo,
		Size:         h.Size,
		TickInterval: h.tickInterval,
		EnterProtect: h.enterProtect,
		MaxStep:      h.maxStep,
	}
	deps := mgr.Deps{
		AOI:      adapter,
		Gameplay: h.gameplay,
		Logger:   mgr.NopLogger{},
		// 关图只发通知，真正的收口（摘登记表 + 关 AOI + 还池）交给 RoomManager。
		OnClose:  func(key scenemap.MapKey, reason string) { h.onCloseRoom(key, reason) },
		Walkable: h.walkable,
		// AI 钩子：entity 提供 TickAll，MGR 负责"每 500ms 恰好一轮"。
		AITick: entity.TickAll,
	}

	m := mgr.New(cfg, deps)
	// 刷怪器推进走 MGR 的 loop（每秒，吃倍速），entity 自己不排定时器。
	//
	// 返回值是新刷出的怪：它们还要进 AOI。装配层在这里暂时拿不到
	// "NPE 位置 → AOI Enter" 的公开入口（见交付说明的缺失 API），
	// 所以只推进刷怪、不做视野同步——不伪造一条绕开 MGR 写入口的路径。
	m.SetRefresh(func(s *mgr.State, now int64) {
		_ = entity.TickRefresh(s, now)
	})
	// 注意：这里的 ctx 是**开房**的上下文（带 5 秒超时），开完就取消；
	// MGR 的生命周期必须挂在进程级上下文上，否则 Open 返回的瞬间图就没了。
	// 关图由 room.Close → Mgr.Stop 负责，不依赖这个 ctx。
	m.Start(context.Background())

	h.Mgr = m
	h.AOI = r.AOI
	return mgrCloser{m: m}, nil
}

// mgrCloser 把 *mgr.Mgr 适配成 room.Closer。
//
// mgr 目前只有 Stop(ctx)，没有 Close(ctx)（room.Closer 的方法名）。
// 这层胶水本身没有逻辑，但它明确了"mgr 需要补一个 Close 以直接满足
// room.Closer"——见交付说明里的缺失 API 清单。
type mgrCloser struct{ m *mgr.Mgr }

// Close 实现 room.Closer。
func (c mgrCloser) Close(ctx context.Context) error { return c.m.Stop(ctx) }

// onCloseRoom 处理 MGR 的关图通知：走 RoomManager 收口。
//
// 为什么必须异步（MGR 侧已经用 goroutine 调过来）：room.Close 会调
// MGR.Close 并等它退出，如果同步在 MGR 自己的 goroutine 里做就是自锁。
func (h *Harness) onCloseRoom(key scenemap.MapKey, reason string) {
	select {
	case h.roomClosed <- struct{}{}:
	default:
		// 已经通知过：保持 channel 里那一条不动，等待方只需要"发生过"。
	}
	if h.Room == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, ok := h.Room.Get(key); ok {
		_ = h.Room.Close(ctx, key, reason)
	}
}

// EnterPlayer 让一个玩家进图（走 MGR 的 enter 命令）。
func (h *Harness) EnterPlayer(uuid scenemap.UUID, pos scenemap.GridPos) error {
	_, err := h.Call(mgr.CommandEnterPlayer{
		UUID:    uuid,
		GridPos: pos,
		// 格与像素都给：像素用格中心，客户端拿到的落点才不是 (0,0)。
		Pixel: h.Geo.CellCenter(pos),
		Kind:  scenemap.EntityKindPlayer,
	})
	return err
}

// MovePlayer 发一次位置同步（像素坐标）。
func (h *Harness) MovePlayer(uuid scenemap.UUID, px, py int32, dir scenemap.Dir) error {
	_, err := h.Call(mgr.CommandMove{
		UUID:  uuid,
		Pixel: scenemap.PixelPos{X: px, Y: py},
		Dir:   dir,
	})
	return err
}

// MovePlayerTo 是 MovePlayer 的格坐标版本（内部换算成格中心像素）。
func (h *Harness) MovePlayerTo(uuid scenemap.UUID, cell scenemap.GridPos, dir scenemap.Dir) error {
	c := h.Geo.CellCenter(cell)
	return h.MovePlayer(uuid, c.X, c.Y, dir)
}

// Leave 让一个玩家离图。
func (h *Harness) Leave(uuid scenemap.UUID, reason scenemap.LeaveReason) error {
	_, err := h.Call(mgr.CommandLeave{UUID: uuid, Reason: reason})
	return err
}

// Tick 手动推进一次 loop（刷怪 + 玩法 Loop + 关图收割）。
//
// 测试里用它代替"等真实定时器"，让结果确定；生产上这一拍由 MGR 自己走。
func (h *Harness) Tick() error {
	_, err := h.Call(mgr.CommandTick{})
	return err
}

// Call 是带超时的同步命令调用。
func (h *Harness) Call(cmd mgr.Command) (any, error) {
	if h.Mgr == nil {
		return nil, fmt.Errorf("examples: MGR 未就绪")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return h.Mgr.Call(ctx, cmd)
}

// Read 在 MGR goroutine 里读一份状态并返回。
//
// 测试不应该直接读 *mgr.State（它没有锁，跨 goroutine 读就是数据竞争）。
// 把读变成命令，既满足单写者纪律，也顺便验证了"状态读取必须走命令"。
func (h *Harness) Read(fn func(*mgr.State) any) (any, error) {
	cmd := mgr.NewFunc("examples:read", func(s *mgr.State) (any, error) { return fn(s), nil })
	// 只读命令不改变状态，必须声明允许无效果，否则会被 ErrNoEffect 判为 bug。
	cmd.AllowNoop = true
	return h.Call(cmd)
}

// WaitRoomClosed 等待"房间因空房超时被关"的通知，带超时。
func (h *Harness) WaitRoomClosed(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-h.roomClosed:
		return true
	case <-timer.C:
		return false
	}
}

// Close 收掉这张图（幂等）。
func (h *Harness) Close() {
	h.closeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if h.Room == nil {
			return
		}
		if _, ok := h.Room.Get(h.Key); ok {
			_ = h.Room.Close(ctx, h.Key, "harness_close")
		}
	})
}
