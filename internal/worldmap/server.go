package worldmap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
	maprpc "game_study/internal/worldmap/rpc"
	"game_study/internal/worldmap/runtime"
	"game_study/internal/worldmap/spatial"
	"game_study/internal/worldmap/store"
	"game_study/internal/worldmap/view"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/pb/internalv2"
)

// Server 是地图服务进程的装配体（文档 18.1 server.go）。
//
// 职责只有三件：读配置 → 组装运行时 → 管生命周期。
// 不含业务逻辑：命令处理在 application，状态在 runtime，持久化在 store。
//
// 两条运行路径（由 mysql.dsn 决定，对外接口一致）：
//   - 内存模式（dsn 为空）：MemoryCommitter，重启即丢，用于本地联调与测试；
//   - 持久化模式（dsn 非空）：store.Repository + 租约 + 恢复 + 释放。
//
// 进程内只有一个世界管理器；多个世界只是多几组场景循环。
type Server struct {
	cfg    config.MapConfig
	logger runtime.Logger
	clock  runtime.Clock

	manager     *runtime.WorldManager
	planner     *application.DefaultPlanner
	committer   runtime.Committer
	memStore    *runtime.MemoryCommitter
	repo        *store.Repository
	coordinator *view.Coordinator
	api         *maprpc.Server

	database *db.DB
	mysqlCfg config.MySQLConfig
	sink     runtime.ProjectionSink
	ownerID  string
	verifier maprpc.TokenVerifier

	// recovered 是恢复出的场景初始数据，Seed 回调按场景取用。
	mu        sync.RWMutex
	recovered map[domain.SceneID]runtime.SeedData
	epochs    map[domain.WorldID]uint64

	objectSeq atomic.Int64
	closed    atomic.Bool
}

// Option 是 New 的可选参数。
type Option func(*Server)

// WithClock 注入时钟（测试用假时钟）。
func WithClock(c runtime.Clock) Option {
	return func(s *Server) {
		if c != nil {
			s.clock = c
		}
	}
}

// WithProjectionSink 覆盖投影出口（默认是内部构造的 view.Coordinator）。
// 测试用它观察投影内容，避免为了断言投影去起一整套视图层。
func WithProjectionSink(sink runtime.ProjectionSink) Option {
	return func(s *Server) {
		s.sink = sink
	}
}

// WithCommitter 覆盖持久化实现（测试注入内存替身或故障替身）。
func WithCommitter(c runtime.Committer) Option {
	return func(s *Server) {
		if c != nil {
			s.committer = c
		}
	}
}

// WithMySQL 注入数据库连接配置。为空表示内存模式（重启即丢）。
//
// 为什么不在 MapConfig 里加 mysql 段：MySQL 连接是全进程共用资源
// （pkg/config 顶层已有），地图段只描述地图自己的参数。把连接配置
// 复制进地图段会让"改一处忘一处"，也让 map 段无法复用其它服务的配置。
func WithMySQL(c config.MySQLConfig) Option {
	return func(s *Server) { s.mysqlCfg = c }
}

// WithTokenVerifier 设置内部 RPC 的鉴权实现。
func WithTokenVerifier(v maprpc.TokenVerifier) Option {
	return func(s *Server) {
		if v != nil {
			s.verifier = v
		}
	}
}

// New 构造服务。配置非法直接返回错误（不在 Run 里才报）。
func New(cfg config.MapConfig, logger runtime.Logger, opts ...Option) (*Server, error) {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("map 配置无效: %w", err)
	}
	if logger == nil {
		logger = runtime.NopLogger{}
	}
	topo := TopologyFromConfig(cfg)

	rules := application.Rules{
		MarchSpeedUnitsPerSec:    cfg.MarchSpeedUnitsPerSec,
		CapacityUnits:            cfg.CapacityUnits,
		CollectRateMilliPerSec:   cfg.CollectRateMilli,
		MaxBattleQueuePerTarget:  cfg.MaxBattleQueuePerTarget,
		BattleTimeoutMS:          mustDurationMS(cfg.BattleTimeout, 30*time.Second),
		RefreshBatchSize:         cfg.RefreshBatchSize,
		ResourcePositionAttempts: cfg.ResourcePositionAttempts,
		RichBlock:                defaultRichBlock(),
	}
	if err := rules.Validate(); err != nil {
		return nil, fmt.Errorf("map 玩法规则无效: %w", err)
	}

	s := &Server{
		cfg:      cfg,
		logger:   logger,
		clock:    runtime.RealClock{},
		memStore: runtime.NewMemoryCommitter(),
		verifier: maprpc.NopVerifier{},
		epochs:   make(map[domain.WorldID]uint64),
	}
	s.committer = s.memStore
	for _, opt := range opts {
		opt(s)
	}

	planner, err := application.NewPlanner(topo, rules, newIDGen(), s.nextObjectID, seedGen)
	if err != nil {
		return nil, fmt.Errorf("map 规划器构造失败: %w", err)
	}
	s.planner = planner

	// 投影协调器：即使当前没有 AOI 客户端也构造真实实现。
	// 用 NoopSink 会把"投影写错"藏到接入客户端那天才暴露。
	s.coordinator = view.NewCoordinator(
		topo.WorldID, topo,
		view.ConfigFromMap(view.MapConfigValues{
			TickInterval:      cfg.TickInterval,
			ViewPendingBytes:  cfg.ViewPendingBytes,
			PayloadLimitBytes: cfg.AOIPayloadLimitBytes,
			MaxViewWidth:      cfg.MaxViewWidth,
			MaxViewHeight:     cfg.MaxViewHeight,
			QueueSize:         cfg.SceneMailboxSize,
		}),
		logger,
	)
	s.coordinator.SetLoader(&sceneLoader{srv: s})

	sink := runtime.ProjectionSink(s.coordinator)
	if s.sink != nil {
		sink = s.sink
	}

	manager, err := s.buildManager(s.committer, sink)
	if err != nil {
		return nil, err
	}
	s.manager = manager
	s.api = maprpc.NewServer(s.manager, s.coordinator, logger, s.verifier)

	// 持久化初始化放在 New 里（而不是 Run）：api 在 New 时创建并交给
	// gRPC 注册，若 Run 里再重建 manager/api，已注册的服务会指向旧实例。
	// 代价是 New 可能因为连不上库而失败——这正是我们想要的"启动即失败"。
	if s.mysqlCfg.DSN != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.initPersistence(ctx); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// buildManager 按当前 committer / sink 组装世界管理器。
// 持久化模式下 Run 会用真实 Repository 再重建一次。
func (s *Server) buildManager(committer runtime.Committer, sink runtime.ProjectionSink) (*runtime.WorldManager, error) {
	topo := TopologyFromConfig(s.cfg)
	return runtime.NewWorldManager(runtime.ManagerConfig{
		Topo: topo,
		Scene: runtime.SceneConfig{
			MailboxSize:         s.cfg.SceneMailboxSize,
			ControlCapacity:     s.cfg.SceneControlCapacity,
			MaxCommandsPerTurn:  s.cfg.MaxCommandsPerTurn,
			MaxDueEventsPerTurn: s.cfg.MaxDueEventsPerTurn,
			CommandDeadlineMS:   mustDurationMS(s.cfg.CommandDeadline, 3*time.Second),
			TickInterval:        mustDuration(s.cfg.TickInterval, time.Second),
		},
		MaxWorlds: len(s.cfg.WorldIDs) + 1,
	}, runtime.Deps{
		Planner:         s.planner,
		Committer:       committer,
		Sink:            sink,
		Clock:           s.clock,
		Logger:          s.logger,
		Seed:            s.seedFor,
		TxWorkers:       s.cfg.TxWorkers,
		TxQueueCapacity: s.cfg.TxQueueCapacity,
	})
}

// Run 加载配置里的世界并阻塞到 ctx 取消。
//
// 顺序（文档 16.2）：连库 → 抢租约拿 epoch → 恢复数据 → 加载世界
// （Seed 在场景 Start 前注入）→ 对外就绪。任何一步失败直接返回错误：
// 宁可起不来，也不能带着半套数据对外。
func (s *Server) Run(ctx context.Context) error {
	worlds := s.cfg.WorldIDs
	if len(worlds) == 0 {
		worlds = []uint64{1}
	}
	for _, id := range worlds {
		if _, err := s.manager.LoadWorld(ctx, domain.WorldID(id)); err != nil {
			return fmt.Errorf("加载世界 %d 失败: %w", id, err)
		}
	}
	s.logger.Info("地图服务已就绪",
		"worlds", worlds,
		"scenes_per_world", TopologyFromConfig(s.cfg).MaxSceneID(),
		"persistent", s.repo != nil)

	<-ctx.Done()
	s.logger.Info("地图服务收到关闭信号")
	return nil
}

// initPersistence 建立数据库连接、抢租约并恢复数据。
func (s *Server) initPersistence(ctx context.Context) error {
	database, err := db.New(db.Config{
		DSN:     s.mysqlCfg.DSN,
		MaxOpen: s.mysqlCfg.MaxOpenConns,
		MaxIdle: s.mysqlCfg.MaxIdleConns,
	})
	if err != nil {
		return fmt.Errorf("连接 MySQL 失败: %w", err)
	}
	s.database = database

	s.ownerID = fmt.Sprintf("map-%d-%s", time.Now().UnixNano(), randSuffix())
	repo := store.NewRepository(database.DB, s.logger, store.Options{
		OwnerID:   s.ownerID,
		LeaseTTL:  mustDuration(s.cfg.LeaseTTL, 15*time.Second),
		TxTimeout: mustDuration(s.cfg.TxTimeout, 2*time.Second),
	})
	s.repo = repo
	s.committer = repo

	// 重建管理器：把内存替身换成真实仓储。
	if s.manager != nil {
		_ = s.manager.Shutdown(context.Background())
	}
	sink := runtime.ProjectionSink(s.coordinator)
	if s.sink != nil {
		sink = s.sink
	}
	manager, err := s.buildManager(repo, sink)
	if err != nil {
		return err
	}
	s.manager = manager
	s.api = maprpc.NewServer(manager, s.coordinator, s.logger, s.verifier)

	worlds := s.cfg.WorldIDs
	if len(worlds) == 0 {
		worlds = []uint64{1}
	}
	recovered := make(map[domain.SceneID]runtime.SeedData)
	ttl := mustDuration(s.cfg.LeaseTTL, 15*time.Second)
	for _, raw := range worlds {
		worldID := domain.WorldID(raw)
		epoch, err := repo.Acquire(ctx, worldID, s.ownerID, ttl)
		if err != nil {
			return fmt.Errorf("获取世界 %d 租约失败: %w", worldID, err)
		}
		s.epochs[worldID] = epoch
		s.logger.Info("世界租约获取成功", "world", worldID, "epoch", epoch, "owner", s.ownerID)

		rec, err := repo.RecoverWorld(ctx, worldID)
		if err != nil {
			return fmt.Errorf("恢复世界 %d 失败: %w", worldID, err)
		}
		mergeRecovered(recovered, rec)
	}
	s.mu.Lock()
	s.recovered = recovered
	s.mu.Unlock()
	s.logger.Info("世界恢复完成", "scenes", len(recovered))
	return nil
}

// seedFor 是 runtime.Deps.Seed 的实现。
func (s *Server) seedFor(_ domain.WorldID, sceneID domain.SceneID) (runtime.SeedData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.recovered == nil {
		return runtime.SeedData{}, nil
	}
	return s.recovered[sceneID], nil
}

// Shutdown 优雅关闭：停接新写 → 等在途事务 → 关场景 → 关池 → 释放租约。
// 幂等。
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	var firstErr error
	if s.manager != nil {
		if err := s.manager.Shutdown(ctx); err != nil {
			firstErr = err
		}
	}
	if s.coordinator != nil {
		s.coordinator.Close()
	}
	if s.repo != nil {
		// 主动释放：不释放也能靠 TTL 过期被接管，但会让下次发布多等一个 TTL。
		for worldID, epoch := range s.epochs {
			if err := s.repo.Release(ctx, worldID, s.ownerID, epoch); err != nil {
				s.logger.Warn("释放租约失败", "world", worldID, "err", err)
			}
		}
	}
	if s.database != nil {
		_ = s.database.Close()
	}
	return firstErr
}

// Manager 暴露世界管理器（RPC 层与集成测试用）。
func (s *Server) Manager() *runtime.WorldManager { return s.manager }

// API 返回内部 gRPC 服务实现，由 cmd/map 注册到 grpc.Server。
func (s *Server) API() *maprpc.Server { return s.api }

// Coordinator 返回 AOI 协调器（运维/调试用）。
func (s *Server) Coordinator() *view.Coordinator { return s.coordinator }

// Store 暴露内存存储（仅内存模式有效，测试断言用）。
func (s *Server) Store() *runtime.MemoryCommitter { return s.memStore }

// Repository 暴露 MySQL 仓储（持久化模式下非 nil）。
func (s *Server) Repository() *store.Repository { return s.repo }

// Planner 暴露规划器（测试构造命令用）。
func (s *Server) Planner() *application.DefaultPlanner { return s.planner }

// TopologyFromConfig 把配置转成领域拓扑。
func TopologyFromConfig(cfg config.MapConfig) domain.Topology {
	return domain.Topology{
		WorldID:           domain.WorldID(firstWorld(cfg)),
		WidthCells:        cfg.WorldWidthCells,
		HeightCells:       cfg.WorldHeightCells,
		SceneCols:         cfg.SceneCols,
		SceneRows:         cfg.SceneRows,
		PositionUnitsCell: cfg.PositionUnitsPerCell,
	}
}

func firstWorld(cfg config.MapConfig) uint64 {
	if len(cfg.WorldIDs) > 0 {
		return cfg.WorldIDs[0]
	}
	return 1
}

// newIDGen 返回 128 位随机十六进制 ID。
//
// 命令/会话/战斗 ID 只需不重复、不需要有序：随机 128 位碰撞概率可忽略，
// 且不需要中心化分配器。对象 ID 必须自增（SQL 主键），见 nextObjectID。
func newIDGen() application.IDGen {
	return func() string {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("worldmap: 生成 ID 失败: %v", err))
		}
		return hex.EncodeToString(b[:])
	}
}

// nextObjectID 分配对象 ID。内存模式用进程内原子自增；
// 持久化模式下由 map_id_allocator 分配（store.AllocObjectID）。
func (s *Server) nextObjectID() domain.ObjectID {
	return domain.ObjectID(s.objectSeq.Add(1))
}

// randSuffix 生成 4 字节十六进制后缀，用于进程 owner 标识。
func randSuffix() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// seedGen 派生刷新种子。乘法混合保证同一 (block,gen,slot) 永远同一种子。
func seedGen(block, generation, slot int64) int64 {
	x := uint64(block)*0x9E3779B97F4A7C15 ^ uint64(generation)*0xBF58476D1CE4E5B9 ^ uint64(slot)*0x94D049BB133111EB
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return int64(x)
}

// defaultRichBlock 返回内置富饶块规则（M2 由 gamedata XML 注入）。
func defaultRichBlock() application.RichBlockRule {
	return application.RichBlockRule{
		MaxCount: 5,
		Tiers: []application.RichTier{
			{ResourceType: 1, ItemID: 1001, Weight: 5, Minimum: 2, TotalPerNode: 1000},
			{ResourceType: 2, ItemID: 1002, Weight: 3, Minimum: 2, TotalPerNode: 500},
			{ResourceType: 3, ItemID: 1003, Weight: 2, Minimum: 1, TotalPerNode: 200},
		},
	}
}

func mustDuration(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func mustDurationMS(s string, def time.Duration) int64 {
	return mustDuration(s, def).Milliseconds()
}

// mergeRecovered 把恢复结果按场景并入 SeedData。
//
// 归属判定：对象按 OwnerScene；采集会话归到资源点所在场景。
// 找不到归属的直接丢弃：宁可少一条恢复数据（下次操作会重建），
// 也不能塞进错误场景——那会让两个场景同时持有写权限，
// 直接破坏单所有者模型。
func mergeRecovered(dst map[domain.SceneID]runtime.SeedData, rec *store.RecoveredWorld) {
	if rec == nil {
		return
	}
	ownerOf := make(map[domain.ObjectID]domain.SceneID, len(rec.Objects))
	for _, obj := range rec.Objects {
		ownerOf[obj.Key.ID] = obj.OwnerScene
	}
	for _, obj := range rec.Objects {
		sd := dst[obj.OwnerScene]
		sd.Objects = append(sd.Objects, obj)
		dst[obj.OwnerScene] = sd
	}
	for _, sess := range rec.Sessions {
		scene, ok := ownerOf[sess.ResourceID]
		if !ok {
			continue
		}
		sd := dst[scene]
		sd.Sessions = append(sd.Sessions, sess)
		dst[scene] = sd
	}
	for scene, st := range rec.Scenes {
		sd := dst[scene]
		if st.Revision > sd.Revision {
			sd.Revision = st.Revision
		}
		dst[scene] = sd
	}
}

// sceneLoader 实现 view.Loader：投影回查用「场景已提交快照」，
// 而不是再查一次数据库。
//
// 为什么：快照就是该场景最后一次成功提交的权威状态，与投影 revision
// 严格对应；再查库可能读到比 revision 更新的内容（下一个事务已提交但
// 事件还没推），造成"事件说 revision=5、内容却是 revision=6"的错配。
type sceneLoader struct {
	srv *Server
}

// LoadScene 用场景快照构建空间索引。
func (l *sceneLoader) LoadScene(ctx context.Context, sceneID domain.SceneID) (*spatial.SceneGrid, uint64, error) {
	scene, err := l.scene(sceneID)
	if err != nil {
		return nil, 0, err
	}
	snap := scene.Snapshot()
	grid := spatial.NewSceneGrid(snap.Topo())
	for _, obj := range snap.AllObjects() {
		pos, ok := objectPos(obj)
		if !ok {
			continue
		}
		// 场景不一致说明对象正在移交中：以快照里的 OwnerScene 为准，
		// 不一致的跳过（下一个投影批次会补齐）。
		if err := grid.Put(obj.Key, obj.Kind, obj.OwnerScene, pos); err != nil {
			continue
		}
	}
	return grid, snap.Revision(), nil
}

// LoadObject 返回对象的投影形态；对象不存在返回 (nil,nil)（表示删除）。
func (l *sceneLoader) LoadObject(ctx context.Context, key domain.ObjectKey) (*internalv2.ObjectUpsert, error) {
	// 按 key 在世界的全部场景里查：单场景对象在千级，25 个场景全扫
	// 的成本可接受；M3 加 key→scene 二级索引后再优化。
	return l.loadObjectFromWorlds(key)
}

// loadObjectFromWorlds 在各世界/场景里查对象。
func (l *sceneLoader) loadObjectFromWorlds(key domain.ObjectKey) (*internalv2.ObjectUpsert, error) {
	world, ok := l.srv.manager.World(key.WorldID)
	if !ok {
		return nil, nil
	}
	for _, scene := range world.Scenes() {
		snap := scene.Snapshot()
		obj, ok := snap.Object(key)
		if !ok {
			continue
		}
		if obj.Deleted {
			return nil, nil
		}
		return toObjectUpsert(obj), nil
	}
	return nil, nil
}

// scene 返回指定场景；sceneID 为 0 时返回首个可用场景（兼容旧调用）。
func (l *sceneLoader) scene(sceneID domain.SceneID) (*runtime.SceneLoop, error) {
	if l.srv == nil || l.srv.manager == nil {
		return nil, fmt.Errorf("worldmap: 世界管理器未就绪")
	}
	scenes := l.srv.managerWorlds()
	if len(scenes) == 0 {
		return nil, fmt.Errorf("worldmap: 没有已加载的世界")
	}
	if sceneID == 0 {
		return scenes[0], nil
	}
	for _, s := range scenes {
		if s.SceneID() == sceneID {
			return s, nil
		}
	}
	return nil, fmt.Errorf("%w: 场景 %d 未加载", domain.ErrObjectNotFound, sceneID)
}

// managerWorlds 返回全部已加载世界的场景集合（按世界编号排序）。
func (s *Server) managerWorlds() []*runtime.SceneLoop {
	ids := s.manager.Worlds()
	var out []*runtime.SceneLoop
	for _, id := range ids {
		w, ok := s.manager.World(id)
		if !ok {
			continue
		}
		out = append(out, w.Scenes()...)
	}
	return out
}

// objectPos 取对象当前位置（城池/部队/资源点各有一个位置字段）。
func objectPos(obj domain.ObjectSnapshot) (domain.Pos, bool) {
	switch {
	case obj.City != nil:
		return obj.City.Position, true
	case obj.Troop != nil:
		if obj.Troop.Move != nil {
			// 投影用段起点即可：视图层只需要"大致在哪"，
			// 精确位置由 TroopView 的 x/y（服务端推导）承担。
			return obj.Troop.Move.From, true
		}
		return domain.Pos{}, false
	case obj.Resource != nil:
		return obj.Resource.Position, true
	default:
		return domain.Pos{}, false
	}
}

// toObjectUpsert 把领域对象转成投影消息。
//
// 只填视图需要的最小字段：投影消息不是传输快照的地方，
// 客户端需要详情时会走 Query（避免两套字段定义长期漂移）。
func toObjectUpsert(obj domain.ObjectSnapshot) *internalv2.ObjectUpsert {
	out := &internalv2.ObjectUpsert{
		ObjectId: uint64(obj.Key.ID),
		Kind:     int32(obj.Kind),
	}
	if pos, ok := objectPos(obj); ok {
		out.X, out.Y = pos.X, pos.Y
	}
	switch {
	case obj.Troop != nil:
		out.Troop = toTroopView(obj.Troop)
	case obj.Resource != nil:
		r := obj.Resource
		out.Resource = &internalv2.ResourceView{
			ResourceId:      uint64(r.Header.ID),
			X:               r.Position.X,
			Y:               r.Position.Y,
			ResourceType:    r.ResourceType,
			Remaining:       r.Remaining,
			Total:           r.Total,
			ActiveCollectId: r.ActiveCollectID,
			RichBlockId:     r.RichBlockID,
			Version:         r.Header.Version,
		}
	}
	return out
}

// toTroopView 转换部队视图。Cargo 必须排序输出：
// map 遍历顺序随机，不排序会让同一支部队两次查询的顺序不同，
// 客户端的字段级 diff 会一直误判"变了"。
func toTroopView(t *domain.Troop) *internalv2.TroopView {
	view := &internalv2.TroopView{
		TroopId:  uint64(t.Header.ID),
		PlayerId: t.PlayerID,
		SlotId:   t.SlotID,
		Phase:    int32(t.Phase),
		TargetId: uint64(t.Target.TargetID),
		Version:  t.Header.Version,
	}
	if t.Move != nil {
		if pos, err := t.Move.PositionAt(time.Now().UnixMilli()); err == nil {
			view.X, view.Y = pos.X, pos.Y
		}
		view.ArriveAtMs = t.Move.EndAtMS
	}
	ids := make([]int32, 0, len(t.Cargo))
	for id := range t.Cargo {
		ids = append(ids, id)
	}
	sortInt32(ids)
	for _, id := range ids {
		view.Cargo = append(view.Cargo, &internalv2.CargoEntry{ItemId: id, Count: t.Cargo[id]})
	}
	return view
}

// sortInt32 插入排序：Cargo 通常只有个位数条目，插入排序比 sort.Slice 更省。
func sortInt32(a []int32) {
	for i := 1; i < len(a); i++ {
		v := a[i]
		j := i - 1
		for j >= 0 && a[j] > v {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = v
	}
}
