package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
)

// RoutedCommand 是带目标场景的业务命令。
//
// 为什么要显式路由而不是由场景自己判断归属：客户端命令可能
// 指向"不在本场景"的对象（攻击跨场景目标），若让每个场景都
// 检查一遍，错误会变成"随机某个场景返回 NotOwner"。显式路由
// 让"发给谁"成为调用方的责任，错误归因清晰。
type RoutedCommand struct {
	SceneID domain.SceneID
	Env     domain.CommandEnvelope
	Cmd     application.Command
}

// WorldInstance 是一个世界（world）的运行实例。
//
// 一个世界 = 25 个场景循环 + 1 个事务池 + 1 个 epoch。
// 生命周期（文档 4.3）：context 统一取消、WaitGroup 收敛、
// readiness 状态对外可见。任一场景 panic → 整个世界 NotReady，
// 不"只重建出错场景继续跑"——那样会让世界处于半新半旧的状态。
type WorldInstance struct {
	worldID domain.WorldID
	topo    domain.Topology
	epoch   uint64

	scenes map[domain.SceneID]*SceneLoop
	pool   *TxWorkerPool

	ready     atomic.Bool
	notReady  atomic.Value // string
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	logger    Logger
	closed    atomic.Bool
	closeOnce sync.Once
}

// WorldID 返回世界编号。
func (w *WorldInstance) WorldID() domain.WorldID { return w.worldID }

// Epoch 返回世界 epoch。M1 固定为 0；M2 从 SQL 租约获取并在恢复时递增，
// 旧 epoch 的提交会被栅栏拒绝（文档 8.4）。
func (w *WorldInstance) Epoch() uint64 { return w.epoch }

// Topology 返回拓扑。
func (w *WorldInstance) Topology() domain.Topology { return w.topo }

// Ready 报告世界是否可接受新写。
func (w *WorldInstance) Ready() bool { return w.ready.Load() && !w.closed.Load() }

// NotReadyReason 返回不可用原因（Ready 为 true 时为空）。
func (w *WorldInstance) NotReadyReason() string {
	if v := w.notReady.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// MarkNotReady 把世界置为不可写。用于 panic、租约丢失、恢复中。
// 不停止已有场景（查询仍可服务），只拒绝新写。
func (w *WorldInstance) MarkNotReady(reason string) {
	w.notReady.Store(reason)
	w.ready.Store(false)
	w.logger.Error("世界标记为 NotReady", "world", w.worldID, "reason", reason)
}

// Scene 返回指定场景。
func (w *WorldInstance) Scene(id domain.SceneID) (*SceneLoop, bool) {
	s, ok := w.scenes[id]
	return s, ok
}

// Scenes 返回全部场景（按编号升序，便于确定性遍历与测试）。
func (w *WorldInstance) Scenes() []*SceneLoop {
	out := make([]*SceneLoop, 0, len(w.scenes))
	for id := domain.SceneID(1); id <= w.topo.MaxSceneID(); id++ {
		if s, ok := w.scenes[id]; ok {
			out = append(out, s)
		}
	}
	return out
}

// Submit 提交一条已路由命令。
//
// 拒绝路径都在这里收口：世界未 Ready / 场景不存在 / 邮箱满。
// 三者对客户端都是"未接受"，可用同一 request_id 重试。
func (w *WorldInstance) Submit(r RoutedCommand) error {
	if !w.Ready() {
		return fmt.Errorf("%w: world %d 不可写: %s", ErrSceneClosed, w.worldID, w.NotReadyReason())
	}
	scene, ok := w.scenes[r.SceneID]
	if !ok {
		return fmt.Errorf("%w: 场景 %d 不存在", domain.ErrObjectNotFound, r.SceneID)
	}
	return scene.Submit(r.Env, r.Cmd)
}

// SubmitSync 提交并等待结果。
func (w *WorldInstance) SubmitSync(ctx context.Context, r RoutedCommand) error {
	if !w.Ready() {
		return fmt.Errorf("%w: world %d 不可写: %s", ErrSceneClosed, w.worldID, w.NotReadyReason())
	}
	scene, ok := w.scenes[r.SceneID]
	if !ok {
		return fmt.Errorf("%w: 场景 %d 不存在", domain.ErrObjectNotFound, r.SceneID)
	}
	// 用独立 goroutine 把同步等待转成可取消的等待：
	// 直接调 SubmitSync 会阻塞直到场景回执，调用方的 ctx 就失效了。
	done := make(chan error, 1)
	go func() { done <- scene.SubmitSync(r.Env, r.Cmd) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot 返回指定场景的只读快照。
func (w *WorldInstance) Snapshot(sceneID domain.SceneID) (*application.SceneSnapshot, bool) {
	s, ok := w.scenes[sceneID]
	if !ok {
		return nil, false
	}
	return s.Snapshot(), true
}

// Shutdown 关闭世界：先置 NotReady 拒新写，再并发关场景，最后关事务池。
//
// 顺序不可颠倒：先停 pool 会让在途事务的结果无处可送，场景永远 pending。
func (w *WorldInstance) Shutdown(ctx context.Context) error {
	var errs []error
	w.closeOnce.Do(func() {
		w.ready.Store(false)
		w.closed.Store(true)
		w.cancel()

		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, s := range w.Scenes() {
			wg.Add(1)
			go func(s *SceneLoop) {
				defer wg.Done()
				if err := s.Shutdown(ctx); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(s)
		}
		wg.Wait()
		if err := w.pool.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		w.wg.Wait()
	})
	if len(errs) > 0 {
		return fmt.Errorf("worldmap: 关闭 world %d 时有 %d 个错误: %v", w.worldID, len(errs), errs)
	}
	return nil
}
