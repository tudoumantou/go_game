package view

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/runtime"
	"game_study/internal/worldmap/spatial"
	"game_study/pkg/pb/internalv2"
)

// ObjectEntry 是块索引里的一条对象记录。
//
// 为什么不直接存 *internalv2.ObjectUpsert：块索引是"位置 → 对象"的
// 路由结构，值语义更适合 map 覆盖写；Upsert 作为指针单独保存，
// 深拷贝只在产出事件/快照时做，避免每次移动都复制 Cargo 切片。
type ObjectEntry struct {
	Key  domain.ObjectKey
	Kind domain.ObjectKind
	// CellX/CellY 是位置截断到格后的值，过滤视野时不必反复除 1000。
	CellX int64
	CellY int64
	// Upsert 是最近一次回查到的详情（含部队/资源点字段与 units 坐标）。
	Upsert *internalv2.ObjectUpsert
}

func (e ObjectEntry) clone() ObjectEntry {
	cp := e
	cp.Upsert = cloneUpsert(e.Upsert)
	return cp
}

// changeKind 是待处理变更的类型。ApplyBatch 只记录"这个键变了"，
// 具体变成什么由 Flush 回查权威状态决定（见 doc.go 的说明）。
type changeKind uint8

const (
	changeUpsert changeKind = iota + 1
	changeLeave
)

// SceneView 是一个场景的 AOI 视图（文档 12.2 的 Worker 角色）。
//
// 数据结构：
//
//	blocks   blockID -> (ObjectKey -> ObjectEntry)，支撑"视野覆盖了哪些块"
//	byBlock  ObjectKey -> blockID，移动时 O(1) 找到旧块并摘除
//	pending  ObjectKey -> 待处理类型，ApplyBatch 累积、Flush 消费
//	dirty    Flush 内按块聚合，只有覆盖脏块的订阅才需要重算
//	subs     订阅集合，Close 时逐一关闭 channel
//
// 单 writer：块索引只由 Flush（持 flushMu + mu）修改；ApplyBatch 只碰
// pending。因此"应用批次"和"生成事件"之间没有中间态泄漏给订阅者。
type SceneView struct {
	sceneID domain.SceneID
	topo    domain.Topology
	cfg     Config
	logger  runtime.Logger

	// loadScene/loadObject 由 Coordinator 注入的闭包，内部再去读
	// 当前 Loader——这样 SetLoader 可以晚于 SceneView 创建，
	// 闭包捕获的是 Coordinator 的生命周期，不需要更新每个 SceneView。
	loadScene  func(context.Context) (*spatial.SceneGrid, uint64, error)
	loadObject func(context.Context, domain.ObjectKey) (*internalv2.ObjectUpsert, error)

	// loadMu 串行化懒加载：两个订阅同时首访时只让一个真正去 Load。
	loadMu sync.Mutex
	// flushMu 串行化 Flush：Flush 会先清空 pending、锁外回查、再上锁应用；
	// 两个 Flush 并行会打乱"同一轮合并"的语义。
	flushMu sync.Mutex

	mu       sync.Mutex
	loaded   bool
	blocks   map[int32]map[domain.ObjectKey]ObjectEntry
	byBlock  map[domain.ObjectKey]int32
	pending  map[domain.ObjectKey]changeKind
	revision uint64
	subs     map[*Subscription]struct{}
	seq      uint64
	lines    *LineSet
	closed   bool

	// dropped 标记"有投影批次因队列满被丢弃"。用 atomic 而不是 mu：
	// TryPublish 在场景循环里调用，必须尽量不抢锁；Flush 时 Swap 读取。
	dropped atomic.Bool

	queue  chan runtime.ProjectionBatch
	done   chan struct{}
	cancel context.CancelFunc
	wg     sync.WaitGroup

	closeOnce sync.Once
}

// newSceneView 构造场景视图并启动消费 goroutine。
//
// goroutine 的生命周期绑定到 Coordinator 的 ctx（Close 时 cancel），
// 不会因为某个订阅关闭而退出——场景视图比订阅活得久。
func newSceneView(
	ctx context.Context,
	sceneID domain.SceneID,
	topo domain.Topology,
	cfg Config,
	logger runtime.Logger,
	loadScene func(context.Context) (*spatial.SceneGrid, uint64, error),
	loadObject func(context.Context, domain.ObjectKey) (*internalv2.ObjectUpsert, error),
) *SceneView {
	runCtx, cancel := context.WithCancel(ctx)
	sv := &SceneView{
		sceneID:    sceneID,
		topo:       topo,
		cfg:        cfg,
		logger:     logger,
		loadScene:  loadScene,
		loadObject: loadObject,
		blocks:     make(map[int32]map[domain.ObjectKey]ObjectEntry),
		byBlock:    make(map[domain.ObjectKey]int32),
		pending:    make(map[domain.ObjectKey]changeKind),
		subs:       make(map[*Subscription]struct{}),
		queue:      make(chan runtime.ProjectionBatch, cfg.QueueSize),
		done:       make(chan struct{}),
		cancel:     cancel,
	}
	sv.wg.Add(1)
	go sv.run(runCtx)
	return sv
}

// run 是场景视图的唯一消费 goroutine。
//
// select 同时监听队列、兜底 ticker 和退出信号：
//   - 批次一到就 Apply+Flush，保证投影延迟主要取决于 Loader 而不是 tick；
//   - ticker 处理"没有批次但行军线变了"的场景（LineSet 不经过 batch）；
//   - ctx.Done 让 Close 能收敛，Ticker 必须 Stop，否则泄漏计时器。
func (sv *SceneView) run(ctx context.Context) {
	defer sv.wg.Done()
	defer close(sv.done)

	ticker := time.NewTicker(sv.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-sv.queue:
			if err := sv.ApplyBatch(ctx, batch); err != nil {
				sv.logger.Warn("AOI 批次应用失败", "scene", sv.sceneID, "revision", batch.Revision, "err", err)
				continue
			}
			if err := sv.Flush(ctx); err != nil {
				sv.logger.Warn("AOI 刷新失败", "scene", sv.sceneID, "err", err)
			}
		case <-ticker.C:
			if err := sv.Flush(ctx); err != nil {
				sv.logger.Warn("AOI 定时刷新失败", "scene", sv.sceneID, "err", err)
			}
		}
	}
}

// ApplyBatch 记录一批投影变更（runtime.ProjectionSink 的下游）。
//
// 只做两件事：把键放进 pending、推进 revision，不访问 Loader。
// 真正的详情回查推迟到 Flush——这正是"投影只带键"的收益：批次
// 在队列里排一会儿也不会携带过期字段，Flush 时读到的一定是最新
// 已提交状态。同一键在 pending 里被覆盖多次，天然只保留最终意图。
//
// 顺序语义：Enters/Updates 视为 upsert，Leaves 后应用。因此同一批里
// 既 enter 又 leave 的键最终是 leave（净删除），与文档 12.3 一致。
func (sv *SceneView) ApplyBatch(ctx context.Context, batch runtime.ProjectionBatch) error {
	if batch.SceneID != sv.sceneID {
		return fmt.Errorf("%w: 批次场景 %d 与视图场景 %d 不一致",
			domain.ErrInvalidArgument, batch.SceneID, sv.sceneID)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sv.mu.Lock()
	defer sv.mu.Unlock()
	for _, k := range batch.Enters {
		sv.pending[k] = changeUpsert
	}
	for _, k := range batch.Updates {
		sv.pending[k] = changeUpsert
	}
	for _, k := range batch.Leaves {
		sv.pending[k] = changeLeave
	}
	// revision 只增不减：迟到的旧批次不能把水位拉回去。
	if batch.Revision > sv.revision {
		sv.revision = batch.Revision
	}
	return nil
}

// markDropped 记录"有批次被丢弃"。下一次 Flush 会给所有订阅发 Resync。
func (sv *SceneView) markDropped() { sv.dropped.Store(true) }

// ensureLoaded 做首次懒加载（文档 12.2 的快照建立）。
//
// 只加载一次：loadMu 串行化，重复调用直接返回。加载来源是 Loader
// 返回的 spatial.SceneGrid——调用方必须保证这是"私有快照"，不能把
// SceneLoop 正在写的索引直接交出来（spatial 明确非并发安全）。
//
// 网格只含 Kind/Pos，不含部队/资源点业务字段，因此这里再逐对象
// 调 LoadObject 补详情；M2 应在 Loader 内部批量实现，本包只保证
// "首访一次、同一对象不重复补"。补不到时退化为最小 Upsert，
// 至少保证位置/种类可用，后续批次会再覆盖。
func (sv *SceneView) ensureLoaded(ctx context.Context) error {
	sv.loadMu.Lock()
	defer sv.loadMu.Unlock()

	sv.mu.Lock()
	loaded := sv.loaded
	sv.mu.Unlock()
	if loaded {
		return nil
	}

	grid, rev, err := sv.loadScene(ctx)
	if err != nil {
		return err
	}
	if grid == nil {
		grid = spatial.NewSceneGrid(sv.topo)
	}
	entries := grid.EntriesInScene(sv.sceneID)

	loadedEntries := make(map[domain.ObjectKey]ObjectEntry, len(entries))
	loadedBlocks := make(map[int32]map[domain.ObjectKey]ObjectEntry)
	for _, e := range entries {
		up := sv.minimalUpsert(e)
		if detail, derr := sv.loadObject(ctx, e.Key); derr == nil && detail != nil {
			up = detail
		}
		kind := domain.ObjectKind(up.Kind)
		if !kind.Valid() {
			kind = e.Kind
		}
		cx, cy := up.X/sv.topo.PositionUnitsCell, up.Y/sv.topo.PositionUnitsCell
		block := BlockOf(sv.topo, cx, cy)
		if block == 0 {
			continue
		}
		entry := ObjectEntry{Key: e.Key, Kind: kind, CellX: cx, CellY: cy, Upsert: up}
		loadedEntries[e.Key] = entry
		if loadedBlocks[block] == nil {
			loadedBlocks[block] = make(map[domain.ObjectKey]ObjectEntry)
		}
		loadedBlocks[block][e.Key] = entry
	}

	sv.mu.Lock()
	defer sv.mu.Unlock()
	if sv.loaded {
		return nil
	}
	for k, e := range loadedEntries {
		block := BlockOf(sv.topo, e.CellX, e.CellY)
		sv.blocks[block] = loadedBlocks[block]
		sv.byBlock[k] = block
	}
	sv.loaded = true
	if rev > sv.revision {
		sv.revision = rev
	}
	return nil
}

func (sv *SceneView) minimalUpsert(e spatial.Entry) *internalv2.ObjectUpsert {
	return &internalv2.ObjectUpsert{
		ObjectId: uint64(e.Key.ID),
		Kind:     int32(e.Kind),
		X:        e.Pos.X,
		Y:        e.Pos.Y,
	}
}

// Flush 把 pending 变更落进块索引，并按订阅者视野生成/投递事件。
//
// 三段式，刻意让 I/O 发生在锁外：
//  1. 持 mu 摘走 pending 并读 dropped/revision；
//  2. 锁外逐键 LoadObject（可能读 store，慢）；失败/未找到的键
//     下一轮重试，绝不在读失败时误删对象；
//  3. 持 mu 应用索引、聚合脏块、给每个订阅做 diff 并投递。
//
// 事件合并规则：
//   - 同一对象一轮内多次变化：pending 是 map，最后状态直接覆盖，
//     加上第 3 步用"当前视图 vs 已下发集合"做 diff，只可能产出一条；
//   - Enters+Leaves 同轮：对象从未进过索引，diff 看不到它，净删除
//     不发事件；已可见对象被删除则会产出一条 Remove。
func (sv *SceneView) Flush(ctx context.Context) error {
	sv.flushMu.Lock()
	defer sv.flushMu.Unlock()

	if err := sv.ensureLoaded(ctx); err != nil {
		return err
	}

	sv.mu.Lock()
	pending := sv.pending
	sv.pending = make(map[domain.ObjectKey]changeKind)
	dropped := sv.dropped.Swap(false)
	sv.mu.Unlock()

	// 锁外回查详情。用局部 map 保存本轮解析结果。
	resolved := make(map[domain.ObjectKey]*internalv2.ObjectUpsert, len(pending))
	retry := make(map[domain.ObjectKey]changeKind)
	for key, kind := range pending {
		if kind == changeLeave {
			resolved[key] = nil
			continue
		}
		up, err := sv.loadObject(ctx, key)
		if err != nil {
			// 读失败不等于删除：保留意图，下一轮再试。
			retry[key] = kind
			continue
		}
		resolved[key] = up // nil 表示权威侧已不存在 → 按 leave 处理
	}
	if ctx.Err() != nil {
		// 已取消：把未解析的变更放回去，避免丢投影。
		sv.mu.Lock()
		for k, kd := range retry {
			if _, ok := sv.pending[k]; !ok {
				sv.pending[k] = kd
			}
		}
		sv.mu.Unlock()
		return ctx.Err()
	}

	sv.mu.Lock()
	defer sv.mu.Unlock()

	// 回查期间可能来了更新的批次：以新意图为准，本轮跳过旧解析结果，
	// 避免"先删后加"产生一条无意义的 Remove。
	for k, kd := range retry {
		if _, ok := sv.pending[k]; !ok {
			sv.pending[k] = kd
		}
	}
	dirty := make(map[int32]struct{})
	for key, up := range resolved {
		if _, newer := sv.pending[key]; newer {
			continue
		}
		if up == nil {
			sv.removeLocked(key, dirty)
			continue
		}
		sv.putLocked(key, up, dirty)
	}
	rev := sv.revision

	// 订阅集合排序后再投递：同一个批次的日志/测试输出稳定。
	subs := make([]*Subscription, 0, len(sv.subs))
	for s := range sv.subs {
		subs = append(subs, s)
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].seq < subs[j].seq })
	for _, s := range subs {
		sv.flushSubLocked(s, rev, dirty, dropped)
	}
	return nil
}

// putLocked 把最新详情写入块索引，并标脏新旧块。
func (sv *SceneView) putLocked(key domain.ObjectKey, up *internalv2.ObjectUpsert, dirty map[int32]struct{}) {
	kind := domain.ObjectKind(up.Kind)
	if !kind.Valid() {
		// LINE(4) 等投影种类不进业务索引；非法种类按删除处理。
		sv.removeLocked(key, dirty)
		return
	}
	cx := up.X / sv.topo.PositionUnitsCell
	cy := up.Y / sv.topo.PositionUnitsCell
	block := BlockOf(sv.topo, cx, cy)
	if block == 0 {
		sv.removeLocked(key, dirty)
		return
	}
	// 对象可能已跨场景移交：本场景视图不应继续持有它，
	// 目标场景的 SceneView 会通过下一批 Enter 建立记录。
	if sv.topo.SceneAt(domain.Pos{X: up.X, Y: up.Y}) != sv.sceneID {
		sv.removeLocked(key, dirty)
		return
	}
	if oldBlock, ok := sv.byBlock[key]; ok {
		sv.removeFromBlockLocked(key, oldBlock)
		if oldBlock != block {
			dirty[oldBlock] = struct{}{}
		}
	}
	bucket := sv.blocks[block]
	if bucket == nil {
		bucket = make(map[domain.ObjectKey]ObjectEntry)
		sv.blocks[block] = bucket
	}
	bucket[key] = ObjectEntry{Key: key, Kind: kind, CellX: cx, CellY: cy, Upsert: up}
	sv.byBlock[key] = block
	dirty[block] = struct{}{}
}

// removeLocked 从索引摘除对象并标脏它原来所在的块。
func (sv *SceneView) removeLocked(key domain.ObjectKey, dirty map[int32]struct{}) {
	block, ok := sv.byBlock[key]
	if !ok {
		return
	}
	sv.removeFromBlockLocked(key, block)
	dirty[block] = struct{}{}
}

func (sv *SceneView) removeFromBlockLocked(key domain.ObjectKey, block int32) {
	bucket := sv.blocks[block]
	if bucket != nil {
		delete(bucket, key)
		if len(bucket) == 0 {
			delete(sv.blocks, block)
		}
	}
	delete(sv.byBlock, key)
}

// collectRectLocked 返回矩形内的对象（值拷贝，Upsert 仍是共享指针）。
// 只遍历 BlocksInRect 给出的块，不扫整个场景。
func (sv *SceneView) collectRectLocked(rect Rect) map[domain.ObjectKey]ObjectEntry {
	out := make(map[domain.ObjectKey]ObjectEntry)
	for _, block := range BlocksInRect(sv.topo, rect) {
		bucket := sv.blocks[block]
		for k, e := range bucket {
			if rect.ContainsCell(e.CellX, e.CellY) {
				out[k] = e
			}
		}
	}
	return out
}

// rectTouchesDirty 判断视野覆盖的块里是否有脏块。
// 没有脏块就不必重算这个订阅；map 查找 O(1)。
func (sv *SceneView) rectTouchesDirty(rect Rect, dirty map[int32]struct{}) bool {
	for _, block := range BlocksInRect(sv.topo, rect) {
		if _, ok := dirty[block]; ok {
			return true
		}
	}
	return false
}

// flushSubLocked 为单个订阅生成事件并投递。调用方必须持有 sv.mu。
//
// 事件来源有两类：
//   - 对象：当前视野快照 vs 订阅已下发集合（sent）做 diff；
//   - 行军线：BuildLineEvents 每次重算，与 sentLines 比较，只有变化才发。
//
// 触发条件：dropped、needFull、revision 缺口、视野覆盖脏块、
// 或者本场景配置了 LineSet（线的变化不经过 ProjectionBatch）。
func (sv *SceneView) flushSubLocked(s *Subscription, rev uint64, dirty map[int32]struct{}, dropped bool) {
	if s.closed {
		return
	}
	if dropped {
		// 批次被丢弃 → 订阅者的增量已不完整，只能清缓存重来。
		sv.resyncLocked(s, rev, "投影批次被丢弃")
		return
	}
	if s.lastRevision != 0 && rev > s.lastRevision && sv.cfg.MaxResyncGap > 0 && rev-s.lastRevision > sv.cfg.MaxResyncGap {
		sv.resyncLocked(s, rev, "revision 缺口超过上限")
		return
	}
	hasLines := sv.lines != nil
	if !s.needFull && !sv.rectTouchesDirty(s.rect, dirty) && !hasLines {
		return
	}

	cur := sv.collectRectLocked(s.rect)
	curLines := BuildLineEvents(sv, s.rect, sv.lines)

	events := make([]*internalv2.ViewEvent, 0, len(cur)+len(curLines))

	// 对象 diff：新键 → Enter；内容变化 → Update（proto 上看都是 Upsert）；
	// 已下发但现在不在视野 → Remove。needFull 时 sent 已清空，
	// 于是全部走 Upsert，正好就是"全量"。
	keys := make([]domain.ObjectKey, 0, len(cur))
	for k := range cur {
		keys = append(keys, k)
	}
	sortObjectKeys(keys)
	for _, k := range keys {
		e := cur[k]
		if old, seen := s.sent[k]; seen && proto.Equal(old, e.Upsert) {
			continue
		}
		events = append(events, upsertEvent(sv.sceneID, rev, e.Upsert))
	}
	if !s.needFull {
		removed := make([]domain.ObjectKey, 0)
		for k := range s.sent {
			if _, ok := cur[k]; !ok {
				removed = append(removed, k)
			}
		}
		sortObjectKeys(removed)
		for _, k := range removed {
			events = append(events, removeEvent(sv.sceneID, rev, uint64(k.ID)))
		}
	}

	// 行军线 diff：按合成 object_id 比较内容，消失的线发 Remove。
	var curByID map[uint64]*internalv2.ObjectUpsert
	if hasLines {
		curByID = make(map[uint64]*internalv2.ObjectUpsert, len(curLines))
		for _, up := range curLines {
			curByID[up.ObjectId] = up
		}
		ids := make([]uint64, 0, len(curLines))
		for id := range curByID {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			up := curByID[id]
			if old, seen := s.sentLines[id]; seen && proto.Equal(old, up) {
				continue
			}
			events = append(events, upsertEvent(sv.sceneID, rev, up))
		}
		if !s.needFull {
			removed := make([]uint64, 0)
			for id := range s.sentLines {
				if _, ok := curByID[id]; !ok {
					removed = append(removed, id)
				}
			}
			sort.Slice(removed, func(i, j int) bool { return removed[i] < removed[j] })
			for _, id := range removed {
				events = append(events, removeEvent(sv.sceneID, rev, id))
			}
		}
	}

	if len(events) == 0 {
		// 没有可见变化：仍然把 sent 对齐到当前状态（内容相同，成本很低），
		// 并清掉 needFull，避免下一轮重复全量。
		s.adoptLocked(cur, curByID, hasLines)
		s.needFull = false
		return
	}

	for _, ev := range events {
		if !s.enqueueLocked(ev, sv.cfg.ViewPendingBytes) {
			// 慢消费者：丢弃本轮剩余增量，清空队列并改发 Resync。
			sv.resyncLocked(s, rev, "订阅积压超过上限")
			return
		}
	}
	s.adoptLocked(cur, curByID, hasLines)
	s.needFull = false
	s.lastRevision = rev
}

// adoptLocked 把本轮已下发的状态记为基线。只有全部投递成功后才调用，
// 防止"发了一半就更新基线"导致下一轮漏发。
func (s *Subscription) adoptLocked(cur map[domain.ObjectKey]ObjectEntry, lines map[uint64]*internalv2.ObjectUpsert, hasLines bool) {
	sent := make(map[domain.ObjectKey]*internalv2.ObjectUpsert, len(cur))
	for k, e := range cur {
		sent[k] = cloneUpsert(e.Upsert)
	}
	s.sent = sent
	if hasLines {
		sentLines := make(map[uint64]*internalv2.ObjectUpsert, len(lines))
		for id, up := range lines {
			sentLines[id] = cloneUpsert(up)
		}
		s.sentLines = sentLines
	} else if len(s.sentLines) > 0 {
		// 线索引被摘除：清空基线，避免将来重新挂载同一个 LineSet 时
		// 因为 proto.Equal 判定"没变化"而漏发。
		s.sentLines = make(map[uint64]*internalv2.ObjectUpsert)
	}
}

// resyncLocked 丢弃订阅当前积压并投递一个 Resync（文档 12.6）。
//
// 为什么直接 drain channel：这些增量已经没有意义——客户端收到 Resync
// 会清空该场景缓存，随后服务端以全量 Upsert 重建。把旧事件留在队列里
// 反而会让客户端先应用一批基于旧视野的增量，出现"截图后又被补丁覆盖"
// 的闪烁。drain 后 needFull=true，下一轮 Flush 会重新发全量。
//
// pendingBytes 归零再计入 Resync 本身的大小：Resync 是恢复通道，
// 不参与积压惩罚，否则一个满队列的订阅永远发不出恢复信号。
func (sv *SceneView) resyncLocked(s *Subscription, rev uint64, reason string) {
	for {
		select {
		case <-s.out:
		default:
			goto drained
		}
	}
drained:
	s.sizes = nil
	s.pendingBytes = 0
	s.sent = make(map[domain.ObjectKey]*internalv2.ObjectUpsert)
	s.sentLines = make(map[uint64]*internalv2.ObjectUpsert)
	s.needFull = true
	s.lastRevision = rev

	ev := &internalv2.ViewEvent{
		Revision: rev,
		SceneId:  int32(sv.sceneID),
		Body: &internalv2.ViewEvent_Resync{Resync: &internalv2.Resync{
			Revision: rev,
			Reason:   reason,
		}},
	}
	size := proto.Size(ev)
	select {
	case s.out <- ev:
		s.sizes = append(s.sizes, size)
		s.pendingBytes += int64(size)
	default:
		// channel 容量至少为 1（newSubscription 保证），drain 后不可能失败；
		// 真失败说明并发写坏了不变量，静默丢弃比阻塞场景视图安全。
	}
}

// upsertEvent 构造对象事件。事件携带场景 revision，订阅者据此判断缺口。
func upsertEvent(sceneID domain.SceneID, rev uint64, up *internalv2.ObjectUpsert) *internalv2.ViewEvent {
	return &internalv2.ViewEvent{
		Revision: rev,
		SceneId:  int32(sceneID),
		Body:     &internalv2.ViewEvent_Upsert{Upsert: cloneUpsert(up)},
	}
}

func removeEvent(sceneID domain.SceneID, rev uint64, objectID uint64) *internalv2.ViewEvent {
	return &internalv2.ViewEvent{
		Revision: rev,
		SceneId:  int32(sceneID),
		Body:     &internalv2.ViewEvent_Remove{Remove: &internalv2.ObjectRemove{ObjectId: objectID}},
	}
}

// cloneUpsert 深拷贝一条 Upsert。proto 消息可变，事件一旦交给订阅者，
// 订阅者（或 gRPC 序列化层）就不该再与 SceneView 共享底层指针。
func cloneUpsert(in *internalv2.ObjectUpsert) *internalv2.ObjectUpsert {
	if in == nil {
		return nil
	}
	out := &internalv2.ObjectUpsert{
		ObjectId: in.ObjectId,
		Kind:     in.Kind,
		X:        in.X,
		Y:        in.Y,
	}
	if in.Troop != nil {
		// 逐字段构造而不是 *in.Troop 结构体拷贝：生成代码内嵌
		// protoimpl.MessageState（含 sync.Mutex），整体拷贝会复制锁，
		// go vet 会报 copylocks，运行期也是未定义行为。
		t := &internalv2.TroopView{
			TroopId:    in.Troop.TroopId,
			PlayerId:   in.Troop.PlayerId,
			SlotId:     in.Troop.SlotId,
			Phase:      in.Troop.Phase,
			X:          in.Troop.X,
			Y:          in.Troop.Y,
			TargetId:   in.Troop.TargetId,
			ArriveAtMs: in.Troop.ArriveAtMs,
			Version:    in.Troop.Version,
		}
		if in.Troop.Cargo != nil {
			t.Cargo = make([]*internalv2.CargoEntry, len(in.Troop.Cargo))
			for i, c := range in.Troop.Cargo {
				if c == nil {
					continue
				}
				t.Cargo[i] = &internalv2.CargoEntry{ItemId: c.ItemId, Count: c.Count}
			}
		}
		out.Troop = t
	}
	if in.Resource != nil {
		out.Resource = &internalv2.ResourceView{
			ResourceId:      in.Resource.ResourceId,
			X:               in.Resource.X,
			Y:               in.Resource.Y,
			ResourceType:    in.Resource.ResourceType,
			Remaining:       in.Resource.Remaining,
			Total:           in.Resource.Total,
			ActiveCollectId: in.Resource.ActiveCollectId,
			RichBlockId:     in.Resource.RichBlockId,
			Version:         in.Resource.Version,
		}
	}
	return out
}

// Subscription 是一个视野订阅（文档 12.1 的会话粒度）。
//
// 事件通过有界 channel 交付。为什么不用无限队列：慢客户端会把
// 整个进程的内存拖爆；有界 + 超限 Resync 是"宁可让客户端重拉，
// 也不能拖垮服务端"的标准背压策略（文档 12.6）。
type Subscription struct {
	scene *SceneView
	// seq 是创建序号，Flush 按它排序订阅，保证事件投递顺序可测。
	seq  uint64
	rect Rect
	out  chan *internalv2.ViewEvent

	// 以下字段全部受 scene.mu 保护。
	closed       bool
	sent         map[domain.ObjectKey]*internalv2.ObjectUpsert
	sentLines    map[uint64]*internalv2.ObjectUpsert
	lastRevision uint64
	needFull     bool
	// sizes 与 out 队列一一对应（FIFO 保存每条事件字节数）。
	// 消费者读走事件后，下一次 enqueue 用 len(out) 的差值回收字节，
	// 因此 pendingBytes 始终等于"仍在 channel 里的事件总字节"。
	sizes        []int
	pendingBytes int64
}

// Events 返回事件通道。调用方必须持续消费；不消费会触发 Resync。
// Close 后 channel 会被关闭，range 循环可以正常退出。
func (s *Subscription) Events() <-chan *internalv2.ViewEvent { return s.out }

// Rect 返回实际生效的视野（已按世界边界与上限裁剪）。
func (s *Subscription) Rect() Rect { return s.rect }

// PendingBytes 返回当前积压字节数（测试与监控用）。
func (s *Subscription) PendingBytes() int64 {
	s.scene.mu.Lock()
	defer s.scene.mu.Unlock()
	return s.pendingBytes
}

// Close 注销订阅并关闭事件通道。
//
// 失效订阅清理：必须从 scene.subs 里删掉，否则 Flush 会一直为一个
// 已断开的连接构造事件，既浪费 CPU 又让 subs 无限增长。
// 关闭 channel 与 enqueue 都在 scene.mu 下，不会 send on closed channel。
func (s *Subscription) Close() {
	s.scene.mu.Lock()
	defer s.scene.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	delete(s.scene.subs, s)
	close(s.out)
}

// enqueueLocked 尝试把事件放入订阅队列，返回 false 表示积压超限。
//
// 字节账本的技巧：out 是 FIFO，sizes 以同样顺序记录每条事件的
// proto.Size。消费者读走 n 条后 len(sizes)-len(out)==n，把队首 n 个
// 字节数扣掉即可，无需额外的 pump goroutine。channel 满或字节超限
// 都返回 false，由调用方转 Resync。
func (s *Subscription) enqueueLocked(ev *internalv2.ViewEvent, limit int64) bool {
	if s.closed {
		return false
	}
	if consumed := len(s.sizes) - len(s.out); consumed > 0 {
		var freed int64
		for _, n := range s.sizes[:consumed] {
			freed += int64(n)
		}
		s.pendingBytes -= freed
		if s.pendingBytes < 0 {
			s.pendingBytes = 0
		}
		// append 到自身前段等价于原地左移，Go 的 copy 处理重叠安全。
		s.sizes = append(s.sizes[:0], s.sizes[consumed:]...)
	}
	size := proto.Size(ev)
	if len(s.out) >= cap(s.out) {
		return false
	}
	if limit > 0 && s.pendingBytes+int64(size) > limit {
		return false
	}
	// select + default 是非阻塞发送的惯用法：宁可返回失败走 Resync，
	// 也绝不让场景视图的 Flush 卡在一个不读数据的客户端上。
	select {
	case s.out <- ev:
		s.sizes = append(s.sizes, size)
		s.pendingBytes += int64(size)
		return true
	default:
		return false
	}
}

// newSubscription 构造订阅。channel 容量至少 1：Resync 恢复路径依赖
// "drain 后必然能塞进一条"这个不变量。
func newSubscription(sv *SceneView, rect Rect, since uint64, seq uint64) *Subscription {
	capacity := sv.cfg.QueueSize
	if capacity < 1 {
		capacity = 1
	}
	return &Subscription{
		scene:        sv,
		seq:          seq,
		rect:         rect,
		out:          make(chan *internalv2.ViewEvent, capacity),
		sent:         make(map[domain.ObjectKey]*internalv2.ObjectUpsert),
		sentLines:    make(map[uint64]*internalv2.ObjectUpsert),
		lastRevision: since,
		needFull:     true,
	}
}
