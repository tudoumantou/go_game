package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
)

// ManagerConfig 是世界管理器配置。
type ManagerConfig struct {
	// Topo 世界拓扑。尺寸/整除校验在 LoadWorld 时做，
	// 不在 NewWorldManager 做——同一个管理器可以加载多个不同规模的世界。
	Topo  domain.Topology
	Scene SceneConfig
	// MaxWorlds 单进程最多加载几个世界（防止配置写错时把内存吃满）。
	MaxWorlds int
}

// SeedData 是场景启动前的初始数据。
//
// 为什么要在 Start 之前注入而不是启动后再 Seed：恢复场景需要
// 「先有完整状态，再开始接受命令」。如果先启动再注入，中间会有一个
// 窗口——场景已就绪但对象还没到，此时到达的查询会返回空视野，
// 客户端会看到「我的城不见了」。Start 前注入从根上消除这个窗口。
type SeedData struct {
	Objects  []domain.ObjectSnapshot
	Sessions []*domain.CollectSession
	Revision uint64
}

// Deps 是运行时依赖。全部为接口，便于替换与测试。
type Deps struct {
	Planner   application.Planner
	Committer Committer
	Sink      ProjectionSink
	Clock     Clock
	Logger    Logger
	// Seed 在场景创建后、Start 前注入恢复数据；nil 表示从空场景开始
	// （M1 内存模式）。
	Seed func(worldID domain.WorldID, sceneID domain.SceneID) (SeedData, error)
	// TxWorkers / TxQueueCapacity 事务池参数；0 用默认（4 / 256）。
	TxWorkers       int
	TxQueueCapacity int
}

// WorldManager 管理进程内所有世界（文档 4.1：每世界 25 场景循环）。
//
// 并发：LoadWorld 可能被多个 goroutine 并发调用（不同世界），
// 用 mu 保护 worlds；单个世界的场景循环启动在锁内完成，
// 保证"要么完整可用，要么不存在"。
type WorldManager struct {
	cfg  ManagerConfig
	deps Deps

	mu     sync.RWMutex
	worlds map[domain.WorldID]*WorldInstance

	closed atomic.Bool
}

// NewWorldManager 创建管理器。不加载任何世界（懒加载：
// 只有配置中启用的 world 才占资源）。
func NewWorldManager(cfg ManagerConfig, deps Deps) (*WorldManager, error) {
	if cfg.MaxWorlds <= 0 {
		cfg.MaxWorlds = 8
	}
	if err := cfg.Topo.Validate(); err != nil {
		return nil, err
	}
	if _, err := cfg.Scene.validate(); err != nil {
		return nil, err
	}
	if deps.Planner == nil {
		return nil, fmt.Errorf("%w: planner 不能为空", domain.ErrInvalidArgument)
	}
	if deps.Committer == nil {
		return nil, fmt.Errorf("%w: committer 不能为空", domain.ErrInvalidArgument)
	}
	if deps.Clock == nil {
		deps.Clock = RealClock{}
	}
	if deps.Logger == nil {
		deps.Logger = NopLogger{}
	}
	if deps.Sink == nil {
		deps.Sink = NoopSink{}
	}
	if deps.TxWorkers <= 0 {
		deps.TxWorkers = 4
	}
	if deps.TxQueueCapacity <= 0 {
		deps.TxQueueCapacity = 256
	}
	return &WorldManager{
		cfg:    cfg,
		deps:   deps,
		worlds: make(map[domain.WorldID]*WorldInstance),
	}, nil
}

// LoadWorld 加载（或返回已加载的）世界。
//
// 幂等：重复加载同一个 world 直接返回已有实例。这样启动流程里的
// "配置里列了 world，但恢复流程也调了一次 LoadWorld" 不会重复创建
// 25 个场景循环（重复创建会变成两套单写者同时改同一份数据）。
func (m *WorldManager) LoadWorld(ctx context.Context, worldID domain.WorldID) (*WorldInstance, error) {
	if m.closed.Load() {
		return nil, ErrPoolClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if w, ok := m.worlds[worldID]; ok {
		return w, nil
	}
	if len(m.worlds) >= m.cfg.MaxWorlds {
		return nil, fmt.Errorf("%w: 世界数量达到上限 %d", domain.ErrInvalidArgument, m.cfg.MaxWorlds)
	}

	topo := m.cfg.Topo
	topo.WorldID = worldID
	if err := topo.Validate(); err != nil {
		return nil, err
	}

	pool, err := NewTxWorkerPool(m.deps.TxWorkers, m.deps.TxQueueCapacity, m.deps.Committer, m.deps.Logger)
	if err != nil {
		return nil, err
	}

	worldCtx, cancel := context.WithCancel(context.Background())
	w := &WorldInstance{
		worldID: worldID,
		topo:    topo,
		epoch:   0,
		scenes:  make(map[domain.SceneID]*SceneLoop, topo.MaxSceneID()),
		pool:    pool,
		ctx:     worldCtx,
		cancel:  cancel,
		logger:  m.deps.Logger,
	}

	for id := domain.SceneID(1); id <= topo.MaxSceneID(); id++ {
		scene, err := NewSceneLoop(
			m.cfg.Scene, worldID, id, w.epoch, topo,
			m.deps.Planner, pool, m.deps.Sink, m.deps.Clock, m.deps.Logger,
		)
		if err != nil {
			// 构造失败要回收已建好的场景与池，避免 goroutine 泄漏。
			cancel()
			_ = pool.Shutdown(context.Background())
			return nil, err
		}
		// 恢复注入必须在 Start 之前：场景一旦开跑，外部就不能再直接改状态。
		if m.deps.Seed != nil {
			data, err := m.deps.Seed(worldID, id)
			if err != nil {
				cancel()
				_ = pool.Shutdown(context.Background())
				return nil, fmt.Errorf("注入场景 %d 恢复数据失败: %w", id, err)
			}
			if err := scene.LoadState(data.Objects, data.Sessions, data.Revision); err != nil {
				cancel()
				_ = pool.Shutdown(context.Background())
				return nil, err
			}
		}
		scene.SetPanicHandler(func(err error) {
			// 任一场景 panic：整个世界 fail-stop（文档 4.3）。
			w.MarkNotReady(fmt.Sprintf("场景 panic: %v", err))
		})
		if err := scene.Start(worldCtx); err != nil {
			cancel()
			_ = pool.Shutdown(context.Background())
			return nil, err
		}
		w.scenes[id] = scene
	}
	w.ready.Store(true)
	m.worlds[worldID] = w
	m.deps.Logger.Info("世界加载完成", "world", worldID, "scenes", len(w.scenes), "epoch", w.epoch)
	return w, nil
}

// World 返回已加载的世界。
func (m *WorldManager) World(worldID domain.WorldID) (*WorldInstance, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.worlds[worldID]
	return w, ok
}

// Worlds 返回已加载的世界编号（升序由调用方排序，此处不保证顺序）。
func (m *WorldManager) Worlds() []domain.WorldID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]domain.WorldID, 0, len(m.worlds))
	for id := range m.worlds {
		out = append(out, id)
	}
	return out
}

// Shutdown 关闭所有世界。
//
// 并发关闭：25 个场景 × N 个世界串行关会拖长停服时间；
// 每个世界内部再并发关场景，整体停服时间约等于"最慢那一个事务"。
func (m *WorldManager) Shutdown(ctx context.Context) error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	m.mu.Lock()
	worlds := make([]*WorldInstance, 0, len(m.worlds))
	for _, w := range m.worlds {
		worlds = append(worlds, w)
	}
	m.mu.Unlock()

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []error
	for _, w := range worlds {
		wg.Add(1)
		go func(w *WorldInstance) {
			defer wg.Done()
			if err := w.Shutdown(ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(errs) > 0 {
		return fmt.Errorf("worldmap: 关闭 %d 个世界时有错误: %v", len(worlds), errs)
	}
	return nil
}

// TickRefresh 向所有场景投递富饶块刷新命令。
//
// M1 的刷新不做周期调度器（那需要 world 级的定时任务与持久化的
// generation 推进），只提供一个手动入口：集成测试与运营脚本显式
// 触发，避免"后台定时器"在测试里制造不确定性。M2 由 scheduler 接管。
func (m *WorldManager) TickRefresh(worldID domain.WorldID, blockID, generation int64, slots int) error {
	w, ok := m.World(worldID)
	if !ok {
		return fmt.Errorf("%w: 世界 %d 未加载", domain.ErrObjectNotFound, worldID)
	}
	for slot := 0; slot < slots; slot++ {
		// 刷新按富饶块归属的场景路由：M1 用块编号对场景数取模，
		// 保证同一个块总是落到同一个场景（否则多个场景会并发刷同一块）。
		sceneID := domain.SceneID(int64(slot)%int64(w.topo.MaxSceneID())) + 1
		env := domain.CommandEnvelope{
			WorldID:    worldID,
			PlayerID:   1,
			RequestID:  domain.RefreshKey(worldID, blockID, generation, int64(slot)),
			WorldEpoch: w.epoch,
		}
		err := w.Submit(RoutedCommand{
			SceneID: sceneID,
			Env:     env,
			Cmd: application.Command{
				Kind:        application.CmdRefresh,
				TargetScene: sceneID,
				Refresh: application.RefreshParams{
					RichBlockID: blockID,
					Generation:  generation,
					Slot:        int32(slot),
				},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}
