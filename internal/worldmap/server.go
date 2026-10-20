package worldmap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/runtime"
	"game_study/pkg/config"
)

// Server 是地图服务进程的装配体（文档 18.1 server.go）。
//
// 职责只有三件：读配置 → 组装运行时 → 管生命周期。
// 不含任何业务逻辑：命令处理在 application，状态在 runtime，
// 持久化在 Committer 背后。这样换存储、加 AOI、加 RPC 都不动这里。
//
// M1 阶段的真实边界（必须清楚，否则会误以为能上线）：
//   - 存储用 runtime.MemoryCommitter：进程重启数据全丢；
//   - 没有 RPC/AOI：外部只能通过 Manager() 拿到的实例编程调用；
//   - 拓扑来自配置，没有地形表（阻挡恒为空）。
//
// 这三条都是 M2/M3 的交付项，接口已经留好。
type Server struct {
	cfg     config.MapConfig
	logger  runtime.Logger
	clock   runtime.Clock
	manager *runtime.WorldManager
	planner *application.DefaultPlanner
	store   *runtime.MemoryCommitter
	sink    runtime.ProjectionSink

	objectSeq atomic.Int64
	closed    atomic.Bool
}

// Option 是 New 的可选参数。目前只有时钟与投影出口可替换：
// 生产用真实时钟与 NoopSink，测试用假时钟驱动到期事件。
type Option func(*Server)

// WithClock 注入时钟（测试用假时钟）。
func WithClock(c runtime.Clock) Option {
	return func(s *Server) {
		if c != nil {
			s.clock = c
		}
	}
}

// WithProjectionSink 注入投影出口。
func WithProjectionSink(sink runtime.ProjectionSink) Option {
	return func(s *Server) {
		if sink != nil {
			s.sink = sink
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
		cfg:    cfg,
		logger: logger,
		clock:  runtime.RealClock{},
		store:  runtime.NewMemoryCommitter(),
		sink:   &runtime.MemorySink{},
	}
	for _, opt := range opts {
		opt(s)
	}

	planner, err := application.NewPlanner(topo, rules, newIDGen(), s.nextObjectID, seedGen)
	if err != nil {
		return nil, fmt.Errorf("map 规划器构造失败: %w", err)
	}
	s.planner = planner

	manager, err := runtime.NewWorldManager(runtime.ManagerConfig{
		Topo: topo,
		Scene: runtime.SceneConfig{
			MailboxSize:         cfg.SceneMailboxSize,
			ControlCapacity:     cfg.SceneControlCapacity,
			MaxCommandsPerTurn:  cfg.MaxCommandsPerTurn,
			MaxDueEventsPerTurn: cfg.MaxDueEventsPerTurn,
			CommandDeadlineMS:   mustDurationMS(cfg.CommandDeadline, 3*time.Second),
			TickInterval:        mustDuration(cfg.TickInterval, time.Second),
		},
		MaxWorlds: len(cfg.WorldIDs) + 1,
	}, runtime.Deps{
		Planner:         planner,
		Committer:       s.store,
		Sink:            s.sink,
		Clock:           s.clock,
		Logger:          logger,
		TxWorkers:       cfg.TxWorkers,
		TxQueueCapacity: cfg.TxQueueCapacity,
	})
	if err != nil {
		return nil, fmt.Errorf("map 世界管理器构造失败: %w", err)
	}
	s.manager = manager
	return s, nil
}

// Run 加载配置里的世界并阻塞到 ctx 取消。
//
// 返回值语义：正常停服返回 nil（信号触发的取消不是错误）。
// 只有"加载失败"才返回 error，让 main 用非零码退出。
func (s *Server) Run(ctx context.Context) error {
	worlds := s.cfg.WorldIDs
	if len(worlds) == 0 {
		// 没配世界时默认加载 world 1：本地开发不需要每次写一行配置。
		worlds = []uint64{1}
	}
	for _, id := range worlds {
		if _, err := s.manager.LoadWorld(ctx, domain.WorldID(id)); err != nil {
			return fmt.Errorf("加载世界 %d 失败: %w", id, err)
		}
	}
	s.logger.Info("地图服务已就绪", "worlds", worlds, "scenes_per_world", TopologyFromConfig(s.cfg).MaxSceneID())

	<-ctx.Done()
	s.logger.Info("地图服务收到关闭信号")
	return nil
}

// Shutdown 优雅关闭：停接新写 → 等在途事务 → 关场景 → 关池。
// 幂等。
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	return s.manager.Shutdown(ctx)
}

// Manager 暴露世界管理器（RPC 层接入前，集成测试与运维脚本用它）。
func (s *Server) Manager() *runtime.WorldManager { return s.manager }

// Store 暴露内存存储（仅供测试断言）。
func (s *Server) Store() *runtime.MemoryCommitter { return s.store }

// Planner 暴露规划器（仅供测试构造命令）。
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
// 为什么不用自增：命令 ID / 会话 ID / 战斗 ID 会跨进程、跨重启出现，
// 自增需要中心化分配器（多一次 RPC + 单点）。这些 ID 不需要有序，
// 只需要不重复，随机 128 位足够（碰撞概率可忽略）。
// 对象 ID 反而必须自增（SQL 主键），见 nextObjectID。
func newIDGen() application.IDGen {
	return func() string {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			// crypto/rand 失败意味着系统熵源异常，此时任何 ID 都不可信。
			panic(fmt.Sprintf("worldmap: 生成 ID 失败: %v", err))
		}
		return hex.EncodeToString(b[:])
	}
}

// nextObjectID 分配对象 ID。M1 用进程内原子自增；M2 换成 SQL 自增回填。
func (s *Server) nextObjectID() domain.ObjectID {
	return domain.ObjectID(s.objectSeq.Add(1))
}

// seedGen 派生刷新种子。用乘法混合而不是 rand：同一 (block,gen,slot)
// 必须永远得到同一个种子，否则重试会刷出不同结果（幂等要求）。
func seedGen(block, generation, slot int64) int64 {
	x := uint64(block)*0x9E3779B97F4A7C15 ^ uint64(generation)*0xBF58476D1CE4E5B9 ^ uint64(slot)*0x94D049BB133111EB
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return int64(x)
}

// defaultRichBlock 返回 M1 内置的富饶块规则（M2 由 gamedata XML 注入）。
// 三种资源、权重 5:3:2、每档保底 2 个、总量 5 个。
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
