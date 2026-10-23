// Package room 是场景房间的全局登记表与生命周期收口（规格 4.5 / 4.6）。
//
// 一个 Room = 一张可玩的地图实例：
//
//	Key/Route  身份（MapKey + 分线号唯一确定一个实例）
//	AOI        视野与广播（从房间池取，关房归还）
//	MGR        场景权威（站位/AI/结算），由注入的 Factory 创建
//
// 本包只做三件事：登记、关房、通知。它不认识 mgr、不认识 gameplay，
// MGR 只以 Closer 接口出现——依赖方向固定为 room → aoi，room ⊥ mgr。
// 这样 mgr 可以自由重构，room 不需要跟着改；测试也能用 stub 工厂
// 单独验证收口逻辑。
package room

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/aoi"
	"game_study/internal/scenemap/grid"
)

// DefaultPoolSize 是单例 Manager 懒创建房间池时的默认容量。
const DefaultPoolSize = 10

// closeTimeout 是关房每一步的超时。
//
// 关房必须"关完"：MGR 少关一步会留下 goroutine 与计时器，AOI 少关一步
// 会让池里的槽位永远不可用。因此调用方的 ctx 即使已经取消，我们也用
// context.WithoutCancel 另起一个有超时的 ctx 把收口走完——只是不让它
// 无限等。
const closeTimeout = 5 * time.Second

// Closer 是 MGR 的关房能力。
//
// 只声明一个方法：room 不关心 MGR 能做什么，只关心它能不能被关掉。
// 用接口而不是具体类型，是为了避免 room → mgr 的 import（那会让两个包
// 互相知道对方的实现细节，也让 room 的测试必须拉起整个 MGR）。
type Closer interface {
	// Close 停止 MGR：Terminate → 玩法 OnTerminate → 停定时器。
	// 必须幂等，且应当在 ctx 超时后尽快返回。
	Close(ctx context.Context) error
}

// Factory 是 MGR 的构造回调。
//
// 由 main 注入（main 同时 import room 与 mgr，依赖关系在那里汇合）。
// room 在调用前已经把 Room.AOI 填好，工厂可以直接用它做进图广播；
// 工厂返回 error 时 room 会回滚（归还 AOI、注销登记）。
type Factory interface {
	Create(ctx context.Context, room *Room, cfg OpenConfig) (Closer, error)
}

// NodeDownHandler 是 MGR 可选实现的"节点宕机"处理器。
//
// 不是 Closer 的一部分：大部分 MGR 不关心跨节点，强制实现只会制造空方法。
// Manager.NodeDown 用类型断言探测，未实现时退化为"向房间广播一条通知"。
type NodeDownHandler interface {
	NodeDown(ctx context.Context, nodes []string) error
}

// OpenConfig 是开房参数。
//
// 它同时承担两个职责：给 AOI 的构造参数（Sink/Behavior/Geo/...），
// 以及给玩法的自定义参数（Meta）。分开成两个结构体也可以，但开房是
// 一次调用，两个参数只会让调用方多写一行没有信息量的字面量。
type OpenConfig struct {
	// Sink 必填：AOI 的投递出口（通常是"玩家投递器"）。
	Sink aoi.Sink
	// Behavior 选填：可见性规则，nil 用 aoi.DefaultBehavior。
	Behavior aoi.Behavior
	// Geo/Size 选填：零值时 AOI 用默认几何、不做边界裁剪。
	Geo  grid.Geometry
	Size grid.Size
	// MailboxSize 选填：AOI 邮箱容量，<=0 用 128。
	MailboxSize int
	// SameScreenLimit 选填：进图时视野内玩家数上限，<=0 不限。
	SameScreenLimit int
	// GridSize 选填：视野格倍率，<=0 用 Geo.AoiGrid 或 6。
	GridSize int32
	// Logger 选填。
	Logger aoi.Logger

	// Meta 玩法自定义参数（副本难度、来源队伍、Boss 配置…）。
	// 框架不解释，原样透传给 Factory。
	Meta map[string]any
}

// State 是房间状态。
//
// 只用于监控与排障：正常路径的判定一律看"登记表里有没有这个 Key"，
// 因为那才是唯一权威。状态字段是给"为什么这个房间关不掉"这类问题
// 提供线索的。
type State int32

const (
	// StateOpening 已取到 AOI，MGR 还在创建。
	StateOpening State = 1
	// StateRunning 正常服务中。
	StateRunning State = 2
	// StateClosing 正在收口（MGR/AOI 逐个关闭）。
	StateClosing State = 3
	// StateClosed 已关完（房间对象可能还被调用方持有）。
	StateClosed State = 4
)

// String 便于日志。
func (s State) String() string {
	switch s {
	case StateOpening:
		return "OPENING"
	case StateRunning:
		return "RUNNING"
	case StateClosing:
		return "CLOSING"
	case StateClosed:
		return "CLOSED"
	default:
		return fmt.Sprintf("State(%d)", int32(s))
	}
}

// Room 是一个房间实例的句柄。
//
// 字段全部导出：main/玩法需要直接拿到 AOI 与 MGR 做业务调用；
// 用 getter 包一层只会让调用点变长。代价是调用方可能误改，
// 因此约定：**只有 Manager 写 State/MGR**，其他代码只读。
type Room struct {
	Key   scenemap.MapKey
	Route scenemap.Route
	// AOI 视野与广播。关房后仍然可读快照（便于排障），但不再接受命令。
	AOI *aoi.AOI
	// MGR 场景权威。由 Factory 创建，room 只负责在关房时调 Close。
	MGR       Closer
	CreatedAt time.Time

	// State 用 atomic.Int32 承载：Manager 在关房路径写，监控/玩法在
	// 任意 goroutine 读。换成普通字段 -race 立刻报错；换成互斥锁
	// 则让一个只读监控字段产生锁竞争。
	State atomic.Int32
}

// CurrentState 返回房间状态。
func (r *Room) CurrentState() State { return State(r.State.Load()) }

// Snapshot 是 Room.AOI.Snapshot 的便捷转发（nil 安全）。
func (r *Room) Snapshot() *aoi.Snapshot {
	if r == nil || r.AOI == nil {
		return aoi.EmptySnapshot()
	}
	return r.AOI.Snapshot()
}

// Manager 是全局房间登记表（进程内单例）。
//
// 为什么需要它：AOI/MGR 是按图分裂的，但"这个房间号是否已被占用"
// "节点挂了要通知哪些房间"这类问题必须是全局视角。把所有房间收在
// 一处，才能保证**开房只有一条路径、关房只有一个收口**（规格 4.5）。
//
// 并发：一把互斥锁保护三张表。开/关房是低频控制面操作，锁竞争可以
// 忽略；热路径（玩家移动）只碰 AOI，不经过这里。
type Manager struct {
	mu sync.Mutex
	// rooms 是 Key → Room 的权威表。
	rooms map[scenemap.MapKey]*Room
	// opening 是"正在创建"的占位集合。
	//
	// 为什么需要：Factory.Create 可能很慢（加载地图、跑玩法 OnCreate），
	// 不能持锁等待；但两个并发的 Open 同一 Key 必须只有一个成功。
	// 用一个短生命周期的占位集合解决，而不是把锁扩大成"整段创建过程"。
	opening map[scenemap.MapKey]struct{}
	// byMap 是 MapID → 已开分线号，供 List 查询与动态分线选择。
	byMap map[int32][]scenemap.Route

	pool     *aoi.Pool
	poolSize int
	factory  Factory
	logger   aoi.Logger

	closed bool
}

// NewManager 构造独立实例。
//
// pool 传 nil 时，Manager 会在第一次 Open 用该次 OpenConfig 懒创建一个
// 容量 poolSize 的池——单例场景不需要在 main 里手工拼池配置。
// 测试请显式传一个小池（不要用 Default()，全局单例会互相污染）。
func NewManager(pool *aoi.Pool, factory Factory, logger aoi.Logger) *Manager {
	if logger == nil {
		logger = aoi.NopLogger{}
	}
	m := &Manager{
		rooms:    make(map[scenemap.MapKey]*Room),
		opening:  make(map[scenemap.MapKey]struct{}),
		byMap:    make(map[int32][]scenemap.Route),
		pool:     pool,
		poolSize: DefaultPoolSize,
		factory:  factory,
		logger:   logger,
	}
	if pool != nil {
		m.poolSize = pool.Size()
	}
	return m
}

var (
	defaultOnce sync.Once
	defaultMgr  *Manager
)

// Default 返回进程级单例。
//
// 用 sync.Once：多个服务装配代码（网关回调、心跳、GM 命令）都可能
// 在启动期并发调用，重复 new 会得到两个互不相知的登记表。
//
// 使用方式：
//
//	room.Default().SetFactory(realFactory)   // 启动期注入
//	room.Default().Open(ctx, key, route, cfg)
//
// 测试不要用它——单例跨用例存活，上一个用例没关的房间会让下一个
// 用例的 Open 返回 ErrAlreadyExists，排查起来非常费劲。
func Default() *Manager {
	defaultOnce.Do(func() {
		defaultMgr = NewManager(nil, nil, nil)
	})
	return defaultMgr
}

// SetFactory 注入 MGR 工厂（启动期调用，可重复覆盖）。
func (m *Manager) SetFactory(f Factory) {
	m.mu.Lock()
	m.factory = f
	m.mu.Unlock()
}

// SetLogger 注入日志。
func (m *Manager) SetLogger(l aoi.Logger) {
	if l == nil {
		l = aoi.NopLogger{}
	}
	m.mu.Lock()
	m.logger = l
	m.mu.Unlock()
}

// SetPoolSize 设置懒创建池的容量（仅对"还没有池"的 Manager 生效）。
func (m *Manager) SetPoolSize(n int) {
	if n <= 0 {
		return
	}
	m.mu.Lock()
	if m.pool == nil {
		m.poolSize = n
	}
	m.mu.Unlock()
}

// Open 开房。
//
// 流程（每一步失败都要能回滚）：
//
//	校验 Key → 占位（防同名并发双开）→ 取 AOI → 建 MGR → 登记 → 返回
//
// 失败回滚的点只有两个：AOI 已取到但 MGR 创建失败（关 AOI + 还池），
// 以及 MGR 建好但登记失败（实际上登记不会失败，占位保证了唯一性）。
func (m *Manager) Open(ctx context.Context, key scenemap.MapKey, route scenemap.Route, cfg OpenConfig) (*Room, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	// Sink 必须显式给：池的"模板配置"来自第一次开房，若这里允许 nil，
	// 第二张图会静默复用第一张图的投递器——串房投递排查起来极痛苦。
	if cfg.Sink == nil {
		return nil, fmt.Errorf("%w: OpenConfig.Sink 不能为空", scenemap.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: 房间管理器已关闭", scenemap.ErrClosed)
	}
	if _, exists := m.rooms[key]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: 房间 %s", scenemap.ErrAlreadyExists, key.Key())
	}
	if _, opening := m.opening[key]; opening {
		m.mu.Unlock()
		return nil, fmt.Errorf("%w: 房间 %s 正在创建", scenemap.ErrAlreadyExists, key.Key())
	}
	factory := m.factory
	m.opening[key] = struct{}{}
	m.mu.Unlock()

	// 无论走哪条返回路径，都要摘掉占位；defer 保证 panic 时也摘。
	defer func() {
		m.mu.Lock()
		delete(m.opening, key)
		m.mu.Unlock()
	}()

	if factory == nil {
		return nil, fmt.Errorf("%w: 未注入 MGR 工厂", scenemap.ErrInvalidState)
	}
	pool, err := m.ensurePool(key, route, cfg)
	if err != nil {
		return nil, err
	}

	a, err := pool.AcquireWith(m.aoiConfig(key, route, cfg))
	if err != nil {
		return nil, err
	}

	r := &Room{Key: key, Route: route, AOI: a, CreatedAt: time.Now()}
	r.State.Store(int32(StateOpening))

	mgr, err := factory.Create(ctx, r, cfg)
	if err != nil {
		// 回滚：AOI 已经启动，必须显式关掉再还池。
		// 这里用 Background 而不是调用方 ctx：ctx 可能就是因为
		// "创建失败"而被取消的，用它关 AOI 会关不掉。
		stopCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		_ = a.Close(stopCtx)
		cancel()
		pool.Release(a)
		return nil, fmt.Errorf("room: 创建 MGR 失败 %s: %w", key.Key(), err)
	}

	r.MGR = mgr
	r.State.Store(int32(StateRunning))

	m.mu.Lock()
	// 二次校验：理论上占位已经保证了唯一，但这里在锁内再确认一次，
	// 防的是"占位被误删"这种未来改动的回归。
	if _, exists := m.rooms[key]; exists {
		m.mu.Unlock()
		stopCtx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		_ = r.MGR.Close(stopCtx)
		_ = a.Close(stopCtx)
		cancel()
		pool.Release(a)
		return nil, fmt.Errorf("%w: 房间 %s", scenemap.ErrAlreadyExists, key.Key())
	}
	m.rooms[key] = r
	m.byMap[key.MapID] = appendRoute(m.byMap[key.MapID], route)
	logger := m.logger
	m.mu.Unlock()

	logger.Info("room: 开房成功",
		"room", key.Key(), "route", int32(route), "map_id", key.MapID)
	return r, nil
}

// ensurePool 返回房间池，必要时懒创建。
func (m *Manager) ensurePool(key scenemap.MapKey, route scenemap.Route, cfg OpenConfig) (*aoi.Pool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("%w: 房间管理器已关闭", scenemap.ErrClosed)
	}
	if m.pool != nil {
		return m.pool, nil
	}
	m.pool = aoi.NewPool(m.poolSize, m.aoiConfigLocked(key, route, cfg))
	return m.pool, nil
}

// aoiConfig 组装 AOI 配置。
func (m *Manager) aoiConfig(key scenemap.MapKey, route scenemap.Route, cfg OpenConfig) aoi.Config {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.aoiConfigLocked(key, route, cfg)
}

// aoiConfigLocked 是持锁版本（ensurePool 内部已在锁内）。
func (m *Manager) aoiConfigLocked(key scenemap.MapKey, route scenemap.Route, cfg OpenConfig) aoi.Config {
	logger := cfg.Logger
	if logger == nil {
		logger = m.logger
	}
	return aoi.Config{
		Key:             key,
		Route:           route,
		Geo:             cfg.Geo,
		Size:            cfg.Size,
		MailboxSize:     cfg.MailboxSize,
		SameScreenLimit: cfg.SameScreenLimit,
		GridSize:        cfg.GridSize,
		Behavior:        cfg.Behavior,
		Sink:            cfg.Sink,
		Logger:          logger,
	}
}

// Close 关房，**唯一收口**（规格 4.5）。
//
// 顺序与理由：
//
//  1. 先摘登记表：并发关两次时，只有摘到的那一次继续，另一次拿 ErrNotFound。
//     先关后摘会让两个 goroutine 同时去关同一个 MGR。
//  2. 关 MGR（Terminate → 玩法 OnTerminate → 停定时器）：玩法在这里
//     结算掉落、写日志、发奖励，必须在 AOI 还活着时做（它可能还要广播）。
//  3. 关 AOI：退出 goroutine，然后归还房间池。
//
// 任何一步失败都记录并继续：半途而废会留下比"报错"严重得多的后果
// （MGR 停了 AOI 还在跑 → 玩家能看到一个永远不动的场景）。
// 最终用 errors.Join 把全部失败一起返回，调用方可以逐条判断。
func (m *Manager) Close(ctx context.Context, key scenemap.MapKey, reason string) error {
	m.mu.Lock()
	r, ok := m.rooms[key]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("%w: 房间 %s", scenemap.ErrNotFound, key.Key())
	}
	delete(m.rooms, key)
	m.byMap[key.MapID] = removeRoute(m.byMap[key.MapID], r.Route)
	pool := m.pool
	logger := m.logger
	m.mu.Unlock()

	r.State.Store(int32(StateClosing))
	// 关房期间不让外部 ctx 的取消把收口打断：没有 ctx 的清理等于泄漏。
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
	defer cancel()

	var errs []error
	if r.MGR != nil {
		if err := r.MGR.Close(stopCtx); err != nil {
			logger.Error("room: 关闭 MGR 失败",
				"room", key.Key(), "reason", reason, "err", err.Error())
			errs = append(errs, fmt.Errorf("关闭 MGR: %w", err))
		}
	}
	if r.AOI != nil {
		if err := r.AOI.Close(stopCtx); err != nil {
			// AOI 没停下来就**不能**还池：复用一条还在跑的 goroutine
			// 会把上一局的实体带进下一局，这比少一个池槽位严重得多。
			logger.Error("room: 关闭 AOI 失败，放弃池槽位",
				"room", key.Key(), "reason", reason, "err", err.Error())
			errs = append(errs, fmt.Errorf("关闭 AOI: %w", err))
		} else if pool != nil {
			pool.Release(r.AOI)
		}
	}
	r.State.Store(int32(StateClosed))

	if len(errs) > 0 {
		logger.Warn("room: 关房完成但有失败步骤",
			"room", key.Key(), "reason", reason, "failed", len(errs))
		return fmt.Errorf("room: 关闭房间 %s(%s) 有 %d 步失败: %w",
			key.Key(), reason, len(errs), errors.Join(errs...))
	}
	logger.Info("room: 关房完成", "room", key.Key(), "reason", reason)
	return nil
}

// Get 查房间。
func (m *Manager) Get(key scenemap.MapKey) (*Room, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[key]
	return r, ok
}

// List 返回某张地图当前已开的分线号（升序去重）。
//
// 动态分线（Route=0 由平衡器挑线）靠它找空位；升序让"优先填满低号线"
// 这类策略不需要额外排序。
func (m *Manager) List(mapID int32) []scenemap.Route {
	m.mu.Lock()
	defer m.mu.Unlock()
	src := m.byMap[mapID]
	if len(src) == 0 {
		return nil
	}
	out := make([]scenemap.Route, len(src))
	copy(out, src)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	// 去重：同一图同一分线理论上只会开一个房间，但 CliKey 不同时可以
	// 出现重复 Route（例如"队伍 A 的 1 线"与"队伍 B 的 1 线"）。
	dedup := out[:0]
	for i, v := range out {
		if i > 0 && out[i-1] == v {
			continue
		}
		dedup = append(dedup, v)
	}
	return dedup
}

// NodeDown 通知节点宕机。
//
// nodes 为指定节点列表；为空表示"全量通知"（运维手动触发）。
// 只通知 Key.Node 命中列表的房间：本节点地图与跨节点地图共用这张表，
// 不筛就会给本地房间发一条毫无意义的通知。
//
// 容错：单个房间处理失败只记录并继续——节点宕机本身就是异常态，
// 这里再中断只会让一半房间收不到通知。返回 errors.Join 汇总。
func (m *Manager) NodeDown(ctx context.Context, nodes []string) error {
	set := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n != "" {
			set[n] = struct{}{}
		}
	}

	m.mu.Lock()
	rooms := make([]*Room, 0, len(m.rooms))
	for _, r := range m.rooms {
		// nodes 为空 = 全量通知（运维手动触发），此时遍历所有房间；
		// 指定了节点则只发给跨节点到该节点的房间（本节点地图没有对端，
		// 通知过去只会让玩法做一次空处理）。
		if len(set) > 0 {
			if _, hit := set[r.Key.Node]; !hit {
				continue
			}
		}
		rooms = append(rooms, r)
	}
	logger := m.logger
	m.mu.Unlock()

	// 排序：遍历顺序影响玩法处理顺序（例如迁移排队），固定成 Key 升序。
	sort.Slice(rooms, func(i, j int) bool { return rooms[i].Key.Key() < rooms[j].Key.Key() })

	var errs []error
	for _, r := range rooms {
		if h, ok := r.MGR.(NodeDownHandler); ok {
			if err := h.NodeDown(ctx, nodes); err != nil {
				logger.Warn("room: 玩法处理 node_down 失败",
					"room", r.Key.Key(), "err", err.Error())
				errs = append(errs, fmt.Errorf("%s: %w", r.Key.Key(), err))
			}
			continue
		}
		// 兜底：玩法没实现处理器时，至少让房间里的玩家知道
		// "对端节点没了"，由客户端/玩法自行处理（例如回主城）。
		if r.AOI != nil {
			_ = r.AOI.Broadcast(NodeDownNotice{Nodes: nodes, At: time.Now()}, aoi.MapScope())
		}
	}
	return errors.Join(errs...)
}

// Len 返回当前房间数（监控用）。
func (m *Manager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rooms)
}

// NodeDownNotice 是 NodeDown 的兜底广播消息（msgType 0，怪物也会收，
// 这是系统消息，不需要过滤）。
type NodeDownNotice struct {
	Nodes []string
	At    time.Time
}

// appendRoute 插入分线号（去重）。插入时就保持有序，读路径只用复制。
func appendRoute(routes []scenemap.Route, route scenemap.Route) []scenemap.Route {
	for _, r := range routes {
		if r == route {
			return routes
		}
	}
	routes = append(routes, route)
	sort.Slice(routes, func(i, j int) bool { return routes[i] < routes[j] })
	return routes
}

// removeRoute 摘除分线号；空了就把整条记录删掉，避免 byMap 里
// 残留一堆空切片（长跑进程里每个开过的 MapID 都会留下一条）。
func removeRoute(routes []scenemap.Route, route scenemap.Route) []scenemap.Route {
	out := routes[:0]
	for _, r := range routes {
		if r != route {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
