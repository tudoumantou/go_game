package view

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/runtime"
	"game_study/internal/worldmap/spatial"
	"game_study/pkg/pb/internalv2"
)

// ErrNoLoader 表示视图层还没有被注入数据源。
//
// 单独定义而不是复用 runtime 的错误：这是装配错误（server 忘了注入），
// 不是运行期过载/关闭，调用方应视为启动失败而不是重试。
var ErrNoLoader = errors.New("worldmap/view: 未注入 Loader")

// Loader 是视图层的数据源，由 server 层注入（M1 用内存替身，M2 从
// store 读已提交快照）。
//
// 两个方法分别服务两类访问：
//   - LoadScene：场景视图首次访问时的全量基线（文档 12.2 的快照）；
//   - LoadObject：Flush 时按 Key 回查详情。ProjectionBatch 只带键，
//     这里补上部队/资源点字段与最新坐标（见 doc.go）。
//
// 契约：
//   - LoadScene 返回的 *spatial.SceneGrid 必须是调用方私有快照。
//     spatial 明确非并发安全，直接把 SceneLoop 正在写的索引交出来
//     会让 race detector 立刻报错，也会读到半更新状态。
//   - LoadObject 对"对象已不存在"返回 (nil, nil)，而不是 error；
//     error 只表示读取失败。两者的区别决定了视图是"删除对象"还是
//     "下一轮重试"——把临时读失败当成删除会造成视野闪烁。
//   - 两个方法都可能被多个场景视图 goroutine 并发调用，实现必须并发安全。
type Loader interface {
	LoadScene(ctx context.Context, sceneID domain.SceneID) (*spatial.SceneGrid, uint64, error)
	LoadObject(ctx context.Context, key domain.ObjectKey) (*internalv2.ObjectUpsert, error)
}

// Coordinator 是一个世界的投影协调器（文档 12.1 的 ViewCoordinator）。
//
// 每个世界一个实例：它持有拓扑与所有场景的 SceneView，负责把
// runtime.ProjectionSink 的批次路由到对应场景。场景循环只接触
// TryPublish 这一个方法，不会因为 AOI 堵塞而停下（文档 12.6：
// 广播堵塞不能阻塞场景 SQL 回执）。
type Coordinator struct {
	worldID domain.WorldID
	topo    domain.Topology
	cfg     Config
	logger  runtime.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	scenes map[domain.SceneID]*SceneView
	loader Loader
	closed bool
}

// 编译期断言：Coordinator 必须实现 runtime.ProjectionSink。
// 依赖方向是 view → runtime：接口定义在 runtime，实现放在 view，
// runtime 从不反向 import view。
var _ runtime.ProjectionSink = (*Coordinator)(nil)

// NewCoordinator 构造协调器。
//
// logger 允许为 nil（用 NopLogger），因为视图层是尽力而为的旁路：
// 日志缺失只影响排障，不应让世界起不来。Loader 通过 SetLoader
// 单独注入，保持构造函数只依赖拓扑/配置这类稳定输入。
func NewCoordinator(worldID domain.WorldID, topo domain.Topology, cfg Config, logger runtime.Logger) *Coordinator {
	if logger == nil {
		logger = runtime.NopLogger{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Coordinator{
		worldID: worldID,
		topo:    topo,
		cfg:     cfg.withDefaults(),
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		scenes:  make(map[domain.SceneID]*SceneView),
	}
}

// SetLoader 注入数据源。允许重复调用（测试里换替身），已有 SceneView
// 通过闭包读取最新 Loader，无需重建。
func (c *Coordinator) SetLoader(l Loader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loader = l
}

// getLoader 返回当前 Loader 快照。读取后立即释放锁，调用方在锁外
// 执行可能很慢的 Load，避免阻塞其它场景创建/TryPublish。
func (c *Coordinator) getLoader() Loader {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loader
}

// TryPublish 实现 runtime.ProjectionSink。
//
// 非阻塞是硬性要求：它运行在各场景的循环 goroutine 里，一旦阻塞，
// 该场景的事务回执、定时事件全部停摆。因此这里只做"有界 channel
// 的非阻塞投递"（select + default），满了就丢弃本批并标记
// needsResync，下一轮 Flush 给所有订阅发 Resync。
//
// 返回错误表示"这一批没推出去"，不改已提交业务（runtime 契约）。
// 调用方（SceneLoop.publish）只记日志，不重试——缺口由 revision
// 与 Resync 兜底。
func (c *Coordinator) TryPublish(batch runtime.ProjectionBatch) error {
	if batch.WorldID != 0 && batch.WorldID != c.worldID {
		return fmt.Errorf("%w: 批次世界 %d 与协调器世界 %d 不一致",
			domain.ErrInvalidArgument, batch.WorldID, c.worldID)
	}
	sv, err := c.view(batch.SceneID)
	if err != nil {
		return err
	}
	select {
	case <-sv.done:
		return runtime.ErrSceneClosed
	default:
	}
	select {
	case sv.queue <- batch:
		return nil
	default:
		// 队列满：丢弃并打标记。这里不能等待，也不能扩大队列——
		// 队列只是吸收突发，持续满说明 AOI 消费者落后于业务写入，
		// 堆更多批次只会把延迟和内存一起放大。
		sv.markDropped()
		return fmt.Errorf("%w: 场景 %d 投影队列已满", runtime.ErrOverloaded, batch.SceneID)
	}
}

// view 返回（必要时创建）场景视图。创建只做内存分配与启动 goroutine，
// 不触发 Loader I/O，因此在 TryPublish 路径上也可以安全调用。
func (c *Coordinator) view(sceneID domain.SceneID) (*SceneView, error) {
	if sceneID < 1 || sceneID > c.topo.MaxSceneID() {
		return nil, fmt.Errorf("%w: 场景 %d 超出拓扑范围", domain.ErrInvalidArgument, sceneID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, runtime.ErrSceneClosed
	}
	if sv, ok := c.scenes[sceneID]; ok {
		return sv, nil
	}
	id := sceneID
	sv := newSceneView(
		c.ctx, id, c.topo, c.cfg, c.logger,
		func(ctx context.Context) (*spatial.SceneGrid, uint64, error) {
			l := c.getLoader()
			if l == nil {
				return nil, 0, ErrNoLoader
			}
			return l.LoadScene(ctx, id)
		},
		func(ctx context.Context, key domain.ObjectKey) (*internalv2.ObjectUpsert, error) {
			l := c.getLoader()
			if l == nil {
				return nil, ErrNoLoader
			}
			return l.LoadObject(ctx, key)
		},
	)
	c.scenes[sceneID] = sv
	return sv, nil
}

// sceneView 是测试/诊断用的只读访问器：只返回已存在的视图，不创建。
func (c *Coordinator) sceneView(sceneID domain.SceneID) (*SceneView, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sv, ok := c.scenes[sceneID]
	return sv, ok
}

// Subscribe 创建视野订阅（文档 12.1）。
//
// 没有 ctx 参数是为了贴近 RPC 层调用点；内部用 Background 并依赖
// 调用方在首访时准备好 Loader。需要取消语义的调用方用 SubscribeContext。
func (c *Coordinator) Subscribe(sceneID domain.SceneID, rect Rect, sinceRevision uint64) (*Subscription, error) {
	return c.SubscribeContext(context.Background(), sceneID, rect, sinceRevision)
}

// SubscribeContext 是带取消的订阅入口。
//
// 流程：
//  1. 裁剪视野（世界边界 + 上限）、拒绝空矩形（文档 12.1）；
//  2. 触发懒加载，确保首帧全量有数据；
//  3. 注册订阅；sinceRevision 无法追平则立刻发 Resync。
//
// sinceRevision 语义：投影层不保留历史增量，因此只有"等于当前 revision"
// 才认为能承接；其余非零值一律 Resync，避免客户端基于旧缓存做增量。
func (c *Coordinator) SubscribeContext(ctx context.Context, sceneID domain.SceneID, rect Rect, sinceRevision uint64) (*Subscription, error) {
	sv, err := c.view(sceneID)
	if err != nil {
		return nil, err
	}
	rect = rect.Clamp(c.topo, c.cfg.MaxViewWidth, c.cfg.MaxViewHeight)
	if rect.Empty() {
		return nil, fmt.Errorf("%w: 视野 %+v 为空或完全在世界外", domain.ErrInvalidArgument, rect)
	}
	if err := sv.ensureLoaded(ctx); err != nil {
		return nil, err
	}

	sv.mu.Lock()
	defer sv.mu.Unlock()
	if sv.closed {
		return nil, runtime.ErrSceneClosed
	}
	sv.seq++
	sub := newSubscription(sv, rect, sinceRevision, sv.seq)
	sv.subs[sub] = struct{}{}
	rev := sv.revision
	if sinceRevision != 0 && sinceRevision != rev {
		// 无法从 since 追平：先给 Resync，客户端清缓存后下一轮收到全量。
		sv.resyncLocked(sub, rev, "since_revision 无法追平")
	}
	return sub, nil
}

// Snapshot 返回场景视图的只读副本（文档 12.2 的全量快照）。
//
// 懒加载：第一次访问时通过 Loader 建立块索引，之后直接复用。
// 返回值做深拷贝，调用方可以安全地序列化/遍历，不影响后续 Flush。
func (c *Coordinator) Snapshot(ctx context.Context, sceneID domain.SceneID) (*SceneSnapshot, error) {
	sv, err := c.view(sceneID)
	if err != nil {
		return nil, err
	}
	if err := sv.ensureLoaded(ctx); err != nil {
		return nil, err
	}
	sv.mu.Lock()
	defer sv.mu.Unlock()
	blocks := make(map[int32]map[domain.ObjectKey]ObjectEntry, len(sv.blocks))
	for block, bucket := range sv.blocks {
		cp := make(map[domain.ObjectKey]ObjectEntry, len(bucket))
		for k, e := range bucket {
			cp[k] = e.clone()
		}
		blocks[block] = cp
	}
	return &SceneSnapshot{SceneID: sceneID, Revision: sv.revision, Blocks: blocks}, nil
}

// SceneIDs 返回已经创建视图的场景编号（升序，便于诊断/测试）。
// map 遍历随机，必须排序后再交给调用方。
func (c *Coordinator) SceneIDs() []domain.SceneID {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]domain.SceneID, 0, len(c.scenes))
	for id := range c.scenes {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Close 关闭协调器：停止所有场景视图 goroutine，关闭所有订阅 channel。
//
// 顺序：先 cancel（让 run 退出并停止 Flush），再逐个 Close 场景视图
// （wg.Wait 收敛，然后统一关闭订阅 channel）。关闭订阅必须在视图
// goroutine 停下之后，否则可能出现"刚 close 又 enqueue"的竞争。
func (c *Coordinator) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	scenes := make([]*SceneView, 0, len(c.scenes))
	for _, sv := range c.scenes {
		scenes = append(scenes, sv)
	}
	c.mu.Unlock()

	c.cancel()
	for _, sv := range scenes {
		sv.Close()
	}
}

// Close 停止场景视图并关闭其订阅 channel。幂等。
//
// 注意 Close 与 run 的配合：cancel 只让 run 从 select 返回；如果它
// 正在 Flush（持锁做 Loader I/O），wg.Wait 会等它完成。因此在视图
// 里做无限阻塞的 Loader 调用会让关闭变慢——Loader 实现必须尊重 ctx。
func (sv *SceneView) Close() {
	sv.closeOnce.Do(func() {
		sv.cancel()
		sv.wg.Wait()
		sv.mu.Lock()
		sv.closed = true
		for s := range sv.subs {
			s.closed = true
			close(s.out)
			delete(sv.subs, s)
		}
		sv.mu.Unlock()
	})
}

// SetLines 注入行军线索引（文档 12.5）。允许 nil（关闭线投影）。
//
// 线的变化不走 ProjectionBatch（LineWorker 直接改索引），所以 Flush
// 在有 LineSet 时每个周期都会重算一次线与视野的交集。这是 MVP 的
// 简单做法：线数量有限，QueryViewport 两阶段过滤后代价可控。
func (sv *SceneView) SetLines(lines *LineSet) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.lines = lines
}
