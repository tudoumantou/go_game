// Package aoi 实现 scenemap 的九宫格视野与广播（规格 3.5 / 3.6 / 6.5）。
//
// 一个可玩的图对应一个 AOI Actor：
//
//	索引     ViewGrid（视野格 → 实体集合）
//	可见性   Behavior（业务定制：谁看得见谁、谁收哪类消息）
//	投递     Sink（把"谁该收到什么"交给上层，AOI 不 import 玩家 Actor）
//	跨 Actor 读 Snapshot（不可变 + atomic.Pointer，无锁）
//
// AOI 不做的事：不算站位、不判阻挡、不结算伤害。那些是 MGR 的职责。
// 分家的理由见包 scenemap 的 doc.go：视野/广播是高频只读路径，
// 结算/站位是串行强一致路径，抢同一个 goroutine 会互相拖慢。
//
// # 同步与异步的边界（重要）
//
// 所有对外方法（Enter/Sync/Leave/Broadcast）默认**异步投递**：
// 投进有界邮箱就返回，返回的 error 只表示"没投进去"（过载/已关闭），
// 不表示命令执行成功。
//
// 为什么默认异步：AOI 是高频路径。如果 MGR/玩家 Actor 每做一步都等
// AOI 回执，两个 Actor 就被耦合成同步链——AOI 慢一帧，MGR 跟着慢一帧，
// 玩家也跟着卡一帧。异步投递 + 有界邮箱把"慢"隔离在 AOI 自己的邮箱里：
// 满了就拒收，调用方拿到 ErrOverloaded 自己决定降级（丢弃/重试/全量同步）。
//
// 需要确定语义的场景（测试、开房时的首批实体、关房前的最后一笔）用
// CallXxx 变体：它带 reply chan 与超时，能保证"命令已执行完"。
// 但不要在每帧移动上用 Call——那等于把异步设计的收益全部还回去。
package aoi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
)

const (
	// defaultMailboxSize 是有界邮箱的默认容量（规格：预算 128）。
	//
	// 128 的含义：按每人每秒 10 次移动、同屏 30 人估算，一帧（100ms）
	// 大约 30 条命令；128 能吸收 4 帧的突发，再多的堆积说明 AOI 已经
	// 跟不上，此时拒收比排队更有价值（排队只会把延迟越堆越大）。
	defaultMailboxSize = 128

	// batchBudget 是一次循环里最多连续处理的命令数。
	//
	// 处理完一批就 flush 一次快照：既不至于每条命令都拷整图（O(n²)），
	// 也不至于长时间不发快照（读方看到的是过期视野）。
	batchBudget = 64

	// callTimeout 是 CallXxx 的默认超时。与协议超时同量级：
	// 等 3 秒还没轮到自己，说明 AOI 已经严重过载，报错比继续等有用。
	callTimeout = 3 * time.Second
)

// Sink 是 AOI 的投递出口。
//
// AOI 只产出"给谁发什么"，不认识玩家 Actor、不认识协议、不认识网关。
// 这样 aoi 包可以独立编译与测试，也避免了 aoi → player → scenemap 的环。
//
// 契约：
//   - 实现必须**非阻塞或快速失败**。AOI 在自己的 goroutine 里同步调用它，
//     一个阻塞的 Sink 会把整张图的视野停摆。
//   - 返回 error 表示"没送达"，AOI 只记日志并丢弃，不重试（重试只会
//     加剧拥堵，正确性由"全量同步"兜底）。
type Sink interface {
	Send(uuid scenemap.UUID, msg any) error
	SendBatch(uuids []scenemap.UUID, msg any) error
}

// Behavior 是 AOI 的业务定制点（规格 6.5）。
//
// 为什么做成接口而不是 if-else：可见性规则随玩法变（PVP 图里敌对阵营
// 才可见、潜行状态不可见、GM 隐身对所有玩家不可见），把这些写进 AOI
// 会让核心路径变成一堆 switch；做成接口后，每种地图注入自己的实现即可。
//
// 实现必须**纯函数、无副作用、快**：它们在 AOI 的 goroutine 里被
// 高频调用（每次移动对每个邻居都要问一次）。
type Behavior interface {
	// ASeeBForAOI 视野出现/消失（进图、跨格）时的可见性。
	ASeeBForAOI(a, b Brief) bool
	// ASeeBForMsg 广播类消息（移动、技能特效）的可见性。
	// 通常比 ASeeBForAOI 更严（例如潜行单位不可见但仍收系统广播）。
	ASeeBForMsg(a, b Brief, msgType int32) bool
	// NpeRecvMsg 报告非玩家实体是否接收某类消息。
	// 返回 false 的实体不会进入 SendBatch 的目标列表——这是"全图广播
	// 跳过几百只怪"的性能开关。
	NpeRecvMsg(npe Brief, msgType int32) bool
	// MateSyncForOwner 报告主人的"跟随型"实体（宠物/召唤）移动时，
	// 是否需要对主人做额外的直达同步（即使主人已经跑出九宫格）。
	MateSyncForOwner(owner, mate Brief) bool
}

// DefaultBehavior 是通用 RPG 规则：
//
//   - 玩家与任何人互相可见（玩家看得到怪、宠物、其他玩家）；
//   - 非玩家实体之间互不可见（怪看不见怪，省掉大量无意义的出现包）；
//   - 怪物只收系统消息（msgType <= 0），不收玩家产生的广播；
//   - 宠物/召唤移动时只额外同步给主人。
//
// 需要 PVP 阵营、潜行、GM 隐身的图，实现自己的 Behavior 即可。
type DefaultBehavior struct{}

// ASeeBForAOI 实现 Behavior。
func (DefaultBehavior) ASeeBForAOI(a, b Brief) bool {
	if a.UUID == b.UUID || !a.UUID.Valid() || !b.UUID.Valid() {
		return false
	}
	// 只要有一方是玩家就可见。"非玩家只与玩家互见"正是这一行的另一面。
	return a.Kind == scenemap.EntityKindPlayer || b.Kind == scenemap.EntityKindPlayer
}

// ASeeBForMsg 实现 Behavior。默认与视野可见性一致。
func (d DefaultBehavior) ASeeBForMsg(a, b Brief, _ int32) bool {
	return d.ASeeBForAOI(a, b)
}

// NpeRecvMsg 实现 Behavior。
//
// 玩家恒 true（这个方法名带 npe，但为调用方便对玩家短路返回 true，
// 避免调用方每次都要先判 Kind）。怪物只收系统消息；宠物/召唤/子弹
// 由主人驱动表现，全部接收。
func (DefaultBehavior) NpeRecvMsg(npe Brief, msgType int32) bool {
	switch npe.Kind {
	case scenemap.EntityKindPlayer:
		return true
	case scenemap.EntityKindNPE:
		return msgType <= 0
	default:
		return true
	}
}

// MateSyncForOwner 实现 Behavior：只有真正的主人能拿到直达同步。
func (DefaultBehavior) MateSyncForOwner(owner, mate Brief) bool {
	if !mate.MasterUUID.Valid() || owner.UUID == mate.UUID {
		return false
	}
	return mate.MasterUUID == owner.UUID
}

// Logger 是 AOI 需要的日志能力。
//
// 定义成本地最小接口而不是 import pkg/log：*log.Logger 的
// Debug/Info/Warn/Error(msg string, kv ...any) 天然满足它，
// 而测试可以塞一个记录器，不用构造 zap。
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// NopLogger 丢弃全部日志（默认实现）。
type NopLogger struct{}

// Debug 实现 Logger。
func (NopLogger) Debug(string, ...any) {}

// Info 实现 Logger。
func (NopLogger) Info(string, ...any) {}

// Warn 实现 Logger。
func (NopLogger) Warn(string, ...any) {}

// Error 实现 Logger。
func (NopLogger) Error(string, ...any) {}

// Config 是 AOI 的构造参数。
//
// 零值可用性：Geo/Behavior/Logger/MailboxSize 都有默认值；
// Key 与 Sink 必须提供（否则无从标识、无从投递）。
type Config struct {
	// Key/Route 只用于日志、注册与快照标识，不参与索引。
	Key   scenemap.MapKey
	Route scenemap.Route

	// Geo 是像素↔格换算。零值时取 grid.DefaultGeometry()。
	Geo grid.Geometry
	// Size 是行走格尺寸，用于把越界坐标夹回图内。零值表示不做边界裁剪。
	Size grid.Size

	// MailboxSize 有界邮箱容量，<=0 取 128。
	MailboxSize int
	// SameScreenLimit 进图时对"视野内玩家数"的上限，<=0 表示不限。
	//
	// 注意它限制的是进图，不是移动：移动途中撞上限不会把玩家传走，
	// 那会让客户端出现瞬移。真要控人数应该由 MGR 在分配出生点时处理。
	SameScreenLimit int
	// GridSize 覆盖 Geo.AoiGrid（每张图可以有不同视野倍率）。<=0 时用 Geo 的值。
	GridSize int32

	Behavior Behavior
	Sink     Sink
	Logger   Logger
}

// normalize 填充默认值并校验。
//
// 校验放在构造期而不是使用期：AOI 的错误坐标/空 Sink 只会在
// 第一条移动命令时暴露，而那时已经在压制图了。
func (c Config) normalize() (Config, error) {
	if c.GridSize <= 0 {
		c.GridSize = c.Geo.AoiGrid
	}
	if c.GridSize <= 0 {
		c.GridSize = AOIGridSize
	}
	if c.Geo == (grid.Geometry{}) {
		c.Geo = grid.DefaultGeometry()
	}
	c.Geo.AoiGrid = c.GridSize

	if c.MailboxSize <= 0 {
		c.MailboxSize = defaultMailboxSize
	}
	if c.Behavior == nil {
		c.Behavior = DefaultBehavior{}
	}
	if c.Logger == nil {
		c.Logger = NopLogger{}
	}
	if c.Sink == nil {
		return c, fmt.Errorf("%w: aoi.Sink 不能为空", scenemap.ErrInvalidArgument)
	}
	if err := c.Key.Validate(); err != nil {
		return c, err
	}
	if err := c.Geo.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Entity 是进图时交给 AOI 的实体描述。
//
// 只带"视野需要知道的东西"：位置、外观、归属。属性权威仍在 MGR/玩家
// Actor 里，AOI 拿到的是那一刻的拷贝。
type Entity struct {
	UUID  scenemap.UUID
	Kind  scenemap.EntityKind
	CfgID int32
	Name  string

	// PX/PY 是像素坐标（不是行走格）。AOI 用它算视野格与广播位置。
	PX int32
	PY int32

	Dir        scenemap.Dir
	HP         int32
	MaxHP      int32
	State      int32
	MasterUUID scenemap.UUID
	Ext        map[string]any
}

// brief 把 Entity 转成不可变外观（Ext 深拷贝一层）。
func (e Entity) brief() Brief {
	return Brief{
		UUID:       e.UUID,
		Kind:       e.Kind,
		CfgID:      e.CfgID,
		Name:       e.Name,
		X:          e.PX,
		Y:          e.PY,
		Dir:        e.Dir,
		HP:         e.HP,
		MaxHP:      e.MaxHP,
		State:      e.State,
		MasterUUID: e.MasterUUID,
		Ext:        cloneExt(e.Ext),
	}
}

// cloneExt 拷贝一层 Ext。nil 保持 nil。
func cloneExt(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// BroadcastKind 是广播范围。
type BroadcastKind int32

const (
	// BroadcastViewport 只发给发送者九宫格内的实体（排除发送者自己）。
	BroadcastViewport BroadcastKind = 1
	// BroadcastMap 发给全图实体（按 Behavior.NpeRecvMsg 过滤怪物）。
	BroadcastMap BroadcastKind = 2
	// BroadcastCircle 发给以某像素点为圆心、半径内的实体（技能范围）。
	// 这是 MGR 的 ScopeAround 对应物；用像素而不是格，因为客户端
	// 表现层按像素插值，格的圆心会随格大小抖动。
	BroadcastCircle BroadcastKind = 3
	// BroadcastUUIDs 发给指定收件人（纠偏包、单人同步）。
	BroadcastUUIDs BroadcastKind = 4
)

// BroadcastScope 描述广播范围。
//
// Viewport 用 Sender 定位九宫格；Map 忽略 Sender。
type BroadcastScope struct {
	Kind   BroadcastKind
	Sender scenemap.UUID

	// CenterPX/CenterPY/RadiusPX 仅 BroadcastCircle 使用（像素）。
	CenterPX int32
	CenterPY int32
	RadiusPX int32
	// UUIDs 仅 BroadcastUUIDs 使用。
	UUIDs []scenemap.UUID
}

// ViewportScope 构造视野内广播（按发送者的九宫格）。
func ViewportScope(sender scenemap.UUID) BroadcastScope {
	return BroadcastScope{Kind: BroadcastViewport, Sender: sender}
}

// MapScope 构造全图广播。
func MapScope() BroadcastScope { return BroadcastScope{Kind: BroadcastMap} }

// CircleScope 构造圆形范围广播（像素坐标）。
func CircleScope(cx, cy, radiusPX int32) BroadcastScope {
	return BroadcastScope{Kind: BroadcastCircle, CenterPX: cx, CenterPY: cy, RadiusPX: radiusPX}
}

// ToScope 构造指定收件人广播。
func ToScope(uuids ...scenemap.UUID) BroadcastScope {
	return BroadcastScope{Kind: BroadcastUUIDs, UUIDs: append([]scenemap.UUID(nil), uuids...)}
}

// command 是邮箱里的一条命令。
//
// done 非 nil 时表示同步调用：执行完把结果写回（容量 1，
// 即使调用方已超时，写回也不会阻塞 AOI 的 goroutine）。
type command struct {
	run  func(*actorState) error
	done chan error
}

// AOI 是对外句柄（并发安全）。
//
// 结构分两层：
//
//	AOI         生命周期（key/route/inbox/done）与快照发布点
//	actorState  全部可变的索引与外观表，**只由 loop goroutine 访问**
//
// 把可变状态收进 actorState 而不是摊在 AOI 上，是为了让"谁在写"
// 成为类型层面的事实：外部拿到 *AOI 根本碰不到 st。
type AOI struct {
	// life 保护 inbox/done/key/route/stopOnce/stopErr 这几个生命周期字段。
	// 热路径（post）只做一次 RLock，成本与读一个 atomic 同量级。
	life     sync.RWMutex
	key      scenemap.MapKey
	route    scenemap.Route
	inbox    *scenemap.Mailbox[command]
	done     chan struct{}
	stopOnce *sync.Once
	stopErr  error

	snap    Store
	dropped atomic.Uint64
}

// actorState 是 AOI goroutine 的私有状态。
type actorState struct {
	cfg    Config
	idx    *ViewGrid
	briefs map[scenemap.UUID]Brief
	// revision 每发布一次快照 +1。客户端靠它判断是否漏包。
	revision uint64
	// dirty 表示自上次发布以来索引/外观有变化。
	dirty bool
	// closed 由 stop 命令置位，loop 看到后 flush 并退出。
	closed bool

	dropped *atomic.Uint64
}

// newActorState 构造私有状态（含视野格边界）。
func newActorState(cfg Config, dropped *atomic.Uint64) *actorState {
	idx := NewViewGrid()
	if cfg.Size.Valid() {
		g := cfg.GridSize
		if g <= 0 {
			g = AOIGridSize
		}
		// 向上取整：地图 100×100、视野格 6 时最后一列只有 4 个行走格，
		// 但仍然是合法的一格。
		w := (cfg.Size.Width + g - 1) / g
		h := (cfg.Size.Height + g - 1) / g
		idx.SetBounds(w, h)
	}
	return &actorState{
		cfg:     cfg,
		idx:     idx,
		briefs:  make(map[scenemap.UUID]Brief, 64),
		dropped: dropped,
	}
}

// NewAOI 构造并启动一个 AOI Actor。
func NewAOI(cfg Config) (*AOI, error) {
	a := &AOI{}
	if err := a.restart(cfg); err != nil {
		return nil, err
	}
	return a, nil
}

// restart 用新配置重启（新建状态 + 新邮箱 + 新 goroutine）。
//
// 只能在"上一个 loop 已退出、且没有其他 goroutine 持有本实例"时调用——
// 也就是 Pool 内部。这也是房间池能复用 AOI 而不带脏状态的原因：
// 状态是整份重建的，不是逐字段清理。
func (a *AOI) restart(cfg Config) error {
	norm, err := cfg.normalize()
	if err != nil {
		return err
	}
	st := newActorState(norm, &a.dropped)
	inbox := scenemap.NewMailbox[command](norm.MailboxSize)
	done := make(chan struct{})

	a.life.Lock()
	a.key, a.route = norm.Key, norm.Route
	a.inbox, a.done = inbox, done
	a.stopOnce = &sync.Once{}
	a.stopErr = nil
	a.life.Unlock()

	a.snap.Store(EmptySnapshot())
	go a.run(st, inbox, done)
	return nil
}

// run 是 AOI 的唯一 goroutine。
//
// 循环形态：阻塞取一条 → 预算内连续 TryRecv → flush 一次快照。
// 批处理的收益是"一次拷贝服务多条命令"；不无限批处理是因为
// 快照要尽快发布，读方（玩家）看到的视野不能太旧。
func (a *AOI) run(st *actorState, inbox *scenemap.Mailbox[command], done chan struct{}) {
	defer close(done)
	for {
		c, ok := inbox.Recv()
		if !ok {
			// 邮箱被关闭（关房路径）：把最后的状态发出去再退出。
			a.flush(st)
			return
		}
		a.exec(c, st)
		for i := 1; i < batchBudget && !st.closed; i++ {
			c, ok = inbox.TryRecv()
			if !ok {
				break
			}
			a.exec(c, st)
		}
		a.flush(st)
		if st.closed {
			// 关房后不再处理积压命令：房间已经不属于任何人了，
			// 继续执行只会在旧状态上做无意义的广播。
			inbox.Close()
			return
		}
	}
}

// exec 执行一条命令。
//
// 单条命令 panic 只隔离这一条，不杀 goroutine：一个玩法的自定义 Behavior
// 越界不应该让几百个玩家一起掉线。代价是索引可能处于"改了一半"的状态，
// 所以这里同时把 dirty 置位，让下一次 flush 至少把当前状态如实发出去；
// 状态正确性的兜底仍是"关房重启"。
func (a *AOI) exec(c command, st *actorState) {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("scenemap/aoi: 命令 panic: %v", r)
				st.dirty = true
				st.cfg.Logger.Error("aoi 命令 panic，已隔离",
					"key", st.cfg.Key.Key(), "route", int32(st.cfg.Route), "panic", r)
			}
		}()
		err = c.run(st)
	}()
	if c.done != nil {
		// 同步调用先 flush 再回执：否则调用方拿到 nil 之后读 Snapshot 会
		// 看到"命令执行了但快照还是上一版"，测试与开房首批实体都会困惑。
		a.flush(st)
		// done 容量必须是 1（call 里保证）：即使调用方已经超时走人，
		// 这里的写回也不会阻塞，避免 AOI 被一个"已放弃"的调用拖住。
		c.done <- err
	}
}

// flush 在索引有变化时构建并原子发布一份新快照。
func (a *AOI) flush(st *actorState) {
	if !st.dirty {
		return
	}
	st.dirty = false
	a.snap.Store(st.build())
}

// build 构建快照。调用方必须是与 loop 同 goroutine。
//
// 只拷贝存在对应外观的实体：极端情况下（panic 打断 enter）索引里可能
// 残留一个没有 Brief 的 UUID，此时以 briefs 为准，保证快照自洽——
// "entities 与 views 数量一致"比"索引完全准确"更重要，读方据此渲染。
func (st *actorState) build() *Snapshot {
	st.revision++
	ents := make(map[scenemap.UUID]Brief, len(st.briefs))
	for u, b := range st.briefs {
		ents[u] = b.Clone()
	}
	cells := st.idx.Cells()
	views := make(map[CellKey][]scenemap.UUID, len(cells))
	cellOf := make(map[scenemap.UUID]CellKey, len(st.briefs))
	for _, c := range cells {
		list := st.idx.CellEntities(c)
		kept := make([]scenemap.UUID, 0, len(list))
		for _, u := range list {
			if _, ok := ents[u]; !ok {
				continue
			}
			kept = append(kept, u)
			cellOf[u] = c
		}
		if len(kept) > 0 {
			views[c] = kept
		}
	}
	return &Snapshot{Revision: st.revision, Entities: ents, Views: views, CellOf: cellOf}
}

// ---------------------------------------------------------------------------
// 对外 API（异步）
// ---------------------------------------------------------------------------

// post 非阻塞投递一条命令。
func (a *AOI) post(run func(*actorState) error) error {
	a.life.RLock()
	defer a.life.RUnlock()
	if a.inbox == nil {
		return scenemap.ErrClosed
	}
	return a.inbox.TrySend(command{run: run})
}

// call 同步投递一条命令并等待执行结果。
//
// 复用 scenemap.Call：它负责超时/ctx 取消，并强制 reply 有缓冲——
// 否则"超时方不再读，写回方永久阻塞"就是典型的 goroutine 泄漏。
func (a *AOI) call(ctx context.Context, run func(*actorState) error) error {
	done := make(chan error, 1)
	opErr, err := scenemap.Call[error](ctx, callTimeout, done, func() error {
		a.life.RLock()
		defer a.life.RUnlock()
		if a.inbox == nil {
			return scenemap.ErrClosed
		}
		return a.inbox.TrySend(command{run: run, done: done})
	})
	if err != nil {
		return err
	}
	return opErr
}

// Enter 异步投递"实体进图"。返回 error 只表示投递失败/过载。
func (a *AOI) Enter(ctx context.Context, e Entity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.post(func(st *actorState) error { return st.enter(e) })
}

// CallEnter 是 Enter 的同步变体。
func (a *AOI) CallEnter(ctx context.Context, e Entity) error {
	return a.call(ctx, func(st *actorState) error { return st.enter(e) })
}

// Sync 异步投递位置同步。
//
// 同名于 sync 包只是巧合：这里不涉及任何锁，方法是"同步位置"的意思。
func (a *AOI) Sync(ctx context.Context, uuid scenemap.UUID, px, py int32, dir scenemap.Dir, state int32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.post(func(st *actorState) error { return st.syncPos(uuid, px, py, dir, state) })
}

// CallSync 是 Sync 的同步变体。
func (a *AOI) CallSync(ctx context.Context, uuid scenemap.UUID, px, py int32, dir scenemap.Dir, state int32) error {
	return a.call(ctx, func(st *actorState) error { return st.syncPos(uuid, px, py, dir, state) })
}

// Leave 异步投递"实体离图"。
func (a *AOI) Leave(ctx context.Context, uuid scenemap.UUID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.post(func(st *actorState) error { return st.leave(uuid) })
}

// CallLeave 是 Leave 的同步变体。
func (a *AOI) CallLeave(ctx context.Context, uuid scenemap.UUID) error {
	return a.call(ctx, func(st *actorState) error { return st.leave(uuid) })
}

// Broadcast 异步投递一条业务广播。
func (a *AOI) Broadcast(msg any, scope BroadcastScope) error {
	return a.post(func(st *actorState) error { return st.broadcast(msg, scope) })
}

// CallBroadcast 是 Broadcast 的同步变体。
func (a *AOI) CallBroadcast(ctx context.Context, msg any, scope BroadcastScope) error {
	return a.call(ctx, func(st *actorState) error { return st.broadcast(msg, scope) })
}

// Close 优雅停止 AOI 并等待 goroutine 退出，幂等。
//
// 用阻塞 Send 而不是 TrySend：关房是控制面操作，必须送达；
// 但 Send 带 ctx，避免 AOI 已经卡死时把调用方一起拖住。
//
// 关房后：后续 Enter/Sync/Broadcast 返回 ErrClosed；Snapshot 仍可读
// （保留最后一帧，便于排障），Pool.Release 会清空它。
func (a *AOI) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.life.RLock()
	once := a.stopOnce
	a.life.RUnlock()
	if once == nil {
		return nil
	}
	once.Do(func() {
		err := a.requestStop(ctx)
		a.life.Lock()
		a.stopErr = err
		a.life.Unlock()
	})
	select {
	case <-a.Done():
		a.life.RLock()
		defer a.life.RUnlock()
		return a.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// requestStop 投递停止命令。
func (a *AOI) requestStop(ctx context.Context) error {
	a.life.RLock()
	inbox, done := a.inbox, a.done
	a.life.RUnlock()
	if inbox == nil || done == nil {
		return nil
	}
	select {
	case <-done:
		return nil // 已经退出
	default:
	}
	cmd := command{run: func(st *actorState) error {
		st.closed = true
		return nil
	}}
	if err := inbox.Send(ctx, cmd); err != nil {
		if errors.Is(err, scenemap.ErrClosed) {
			return nil
		}
		return err
	}
	return nil
}

// Done 返回一个在 goroutine 退出后关闭的 channel。
func (a *AOI) Done() <-chan struct{} {
	a.life.RLock()
	defer a.life.RUnlock()
	return a.done
}

// Stopped 报告 loop 是否已退出（非阻塞）。
func (a *AOI) Stopped() bool {
	d := a.Done()
	if d == nil {
		return true
	}
	select {
	case <-d:
		return true
	default:
		return false
	}
}

// Key 返回地图键。
func (a *AOI) Key() scenemap.MapKey {
	a.life.RLock()
	defer a.life.RUnlock()
	return a.key
}

// Route 返回分线号。
func (a *AOI) Route() scenemap.Route {
	a.life.RLock()
	defer a.life.RUnlock()
	return a.route
}

// Snapshot 返回当前快照（无锁读，任意 goroutine 可调）。
func (a *AOI) Snapshot() *Snapshot { return a.snap.Load() }

// Revision 返回当前快照版本。
func (a *AOI) Revision() uint64 { return a.snap.Load().Revision }

// Len 返回当前实体数（读快照，不是索引实时值）。
func (a *AOI) Len() int { return a.snap.Load().Len() }

// Dropped 返回累计被丢弃的投递数（Sink 报错）。
//
// 这是"慢消费者保护"的可观测点：值持续增长说明下游跟不上，
// 应该扩容或降频，而不是给 AOI 加重试。
func (a *AOI) Dropped() uint64 { return a.dropped.Load() }

// resetAfterStop 清空内部状态（Pool.Release 调用）。
//
// loop 已退出，actorState（格子索引 + 外观表）不再被任何 goroutine 引用，
// 会随下一次 GC 整份回收——这就是"清空索引"的实现方式：不逐条 delete，
// 而是让整份状态不可达。这里显式把对外快照换成空快照，让"已清空"在
// 外部可观测（Len()==0），也避免池里的空闲实例看起来还带着上一局的实体。
func (a *AOI) resetAfterStop() {
	a.snap.Store(EmptySnapshot())
}

// ---------------------------------------------------------------------------
// 命令实现（只在 loop goroutine 里执行）
// ---------------------------------------------------------------------------

// cellOf 像素 → 视野格（先夹到地图内）。
func (st *actorState) cellOf(px, py int32) CellKey {
	gp := st.cfg.Geo.CellOf(px, py)
	if st.cfg.Size.Valid() {
		gp = st.cfg.Size.Clamp(gp)
	}
	v := st.cfg.Geo.ViewOf(gp)
	return CellKey{X: v.X, Y: v.Y}
}

// visibleCells 返回视野格对应的九宫格（按地图边界裁剪）。
func (st *actorState) visibleCells(cell CellKey) []CellKey {
	all := Neighbors9(cell)
	out := make([]CellKey, 0, len(all))
	for _, c := range all {
		if st.idx.InBounds(c) {
			out = append(out, c)
		}
	}
	return out
}

// entitiesIn 返回这些格里的实体（去重排序，排除 exclude）。
func (st *actorState) entitiesIn(cells []CellKey, exclude scenemap.UUID) []scenemap.UUID {
	all := st.idx.EntitiesIn(cells)
	if !exclude.Valid() {
		return all
	}
	out := all[:0]
	for _, u := range all {
		if u != exclude {
			out = append(out, u)
		}
	}
	return out
}

// entitySet 返回这些格里的实体集合（差集运算用）。
func (st *actorState) entitySet(cells []CellKey) map[scenemap.UUID]struct{} {
	out := make(map[scenemap.UUID]struct{}, 16)
	for _, c := range cells {
		for u := range st.idx.cells[c] {
			out[u] = struct{}{}
		}
	}
	return out
}

// enter 处理进图。
//
// 顺序：加索引 → 算九宫格 → 给自己发全量出现包 → 给视野内其他实体发出现
// → （loop 稍后统一）发布快照。
//
// 先加索引再算视野，保证"自己"也在视野集合里（随后被排除），
// 同时让后进图的人能看到先进图的人——两条 enter 之间没有窗口期。
func (st *actorState) enter(e Entity) error {
	if !e.UUID.Valid() {
		return fmt.Errorf("%w: 实体 UUID 无效", scenemap.ErrInvalidArgument)
	}
	if _, exists := st.briefs[e.UUID]; exists {
		return fmt.Errorf("%w: 实体 %s 已在本图", scenemap.ErrAlreadyExists, e.UUID)
	}
	b := e.brief()
	cell := st.cellOf(e.PX, e.PY)

	// 同屏上限：只对玩家生效，且只在进图时判一次（移动途中不踢人）。
	if st.cfg.SameScreenLimit > 0 && b.IsPlayer() {
		if n := st.countPlayers(st.visibleCells(cell), e.UUID); n >= st.cfg.SameScreenLimit {
			return fmt.Errorf("%w: 视野内已有 %d 名玩家，上限 %d",
				scenemap.ErrRoomFull, n, st.cfg.SameScreenLimit)
		}
	}

	st.idx.Add(e.UUID, cell)
	st.briefs[e.UUID] = b
	st.dirty = true

	// 视野内已有实体：按 UUID 升序，保证同样的局面产生同样的包序。
	others := st.entitiesIn(st.visibleCells(cell), e.UUID)
	visible := make([]Brief, 0, len(others))
	for _, u := range others {
		ob, ok := st.briefs[u]
		if !ok {
			continue
		}
		if !st.cfg.Behavior.ASeeBForAOI(b, ob) {
			continue
		}
		visible = append(visible, ob)
	}

	// 1) 新实体：一条全量包（自己 + 一屏）。客户端只需构建一次场景。
	st.send(e.UUID, EnterViewMsg{
		Self:     b,
		Entities: visible,
		Revision: st.revision + 1,
	})

	// 2) 视野内其他人：逐条 Appear。反向可见性要再判一次——
	// Behavior 不保证对称（潜行、GM 隐身都是一边可见一边不可见）。
	for _, ob := range visible {
		if !st.cfg.Behavior.ASeeBForAOI(ob, b) {
			continue
		}
		st.send(ob.UUID, AppearMsg{Entity: b})
	}
	return nil
}

// syncPos 处理位置同步。
//
// 核心是**以实体集合为单位的差集**，不是格坐标差集：
//
//	oldView = 旧九宫格里的实体集合
//	newView = 新九宫格里的实体集合
//	进入视野 = newView - oldView
//	离开视野 = oldView - newView
//
// 用格坐标差集会误报：A 在格 5、B 在格 6 时相邻可见，A 移到格 6，
// 两份九宫格的格坐标差集非空，但 B 始终在视野里——客户端会看到
// B 闪一下（Leave 后立刻 Appear）。
func (st *actorState) syncPos(uuid scenemap.UUID, px, py int32, dir scenemap.Dir, state int32) error {
	b, ok := st.briefs[uuid]
	if !ok {
		return fmt.Errorf("%w: 实体 %s 不在本图", scenemap.ErrNotFound, uuid)
	}
	newCell := st.cellOf(px, py)
	b.X, b.Y, b.Dir, b.State = px, py, dir, state
	st.briefs[uuid] = b
	st.dirty = true

	oldCell, ok := st.idx.CellOf(uuid)
	if !ok {
		st.idx.Add(uuid, newCell)
		return nil
	}
	if oldCell == newCell {
		// 同格微调：只更新快照位置，一个包都不发。
		// 行走格级别的小位移在客户端靠插值平滑，广播反而会抖。
		return nil
	}
	st.idx.Move(uuid, newCell)

	oldCells := st.visibleCells(oldCell)
	newCells := st.visibleCells(newCell)
	oldView := st.entitySet(oldCells)
	newView := st.entitySet(newCells)

	entered := make([]scenemap.UUID, 0, 4)
	for u := range newView {
		if u == uuid {
			continue
		}
		if _, was := oldView[u]; !was {
			entered = append(entered, u)
		}
	}
	left := make([]scenemap.UUID, 0, 4)
	for u := range oldView {
		if u == uuid {
			continue
		}
		if _, still := newView[u]; !still {
			left = append(left, u)
		}
	}
	sortUUIDs(entered)
	sortUUIDs(left)

	// 自己这一侧：新看见的出现、看不见的消失。
	for _, u := range entered {
		ob, ok := st.briefs[u]
		if !ok || !st.cfg.Behavior.ASeeBForAOI(b, ob) {
			continue
		}
		st.send(uuid, AppearMsg{Entity: ob})
	}
	for _, u := range left {
		ob, ok := st.briefs[u]
		if !ok || !st.cfg.Behavior.ASeeBForAOI(b, ob) {
			continue
		}
		st.send(uuid, LeaveMsg{UUID: u, Kind: ob.Kind})
	}

	// 对端那一侧：自己出现/消失。必须与上面成对，否则对端会留下幽灵实体。
	for _, u := range entered {
		ob, ok := st.briefs[u]
		if !ok || !st.cfg.Behavior.ASeeBForAOI(ob, b) {
			continue
		}
		st.send(u, AppearMsg{Entity: b})
	}
	for _, u := range left {
		ob, ok := st.briefs[u]
		if !ok || !st.cfg.Behavior.ASeeBForAOI(ob, b) {
			continue
		}
		st.send(u, LeaveMsg{UUID: uuid, Kind: b.Kind})
	}

	// 移动包：新旧九宫格并集（去重、排除自己）。
	// 为什么是并集而不是新九宫格：刚离开视线的实体也要收到最后一条位置，
	// 否则它那边的最后位置停在旧格，客户端做"离开动画"时起点会跳。
	union := make([]CellKey, 0, len(oldCells)+len(newCells))
	union = append(union, oldCells...)
	union = append(union, newCells...)
	targets := st.entitiesIn(union, uuid)
	recipients := st.moveRecipients(b, targets)
	if len(recipients) > 0 {
		st.sendBatch(recipients, MoveMsg{
			UUID:       uuid,
			X:          px,
			Y:          py,
			Dir:        dir,
			State:      state,
			Cell:       newCell,
			MasterUUID: b.MasterUUID,
		})
	}
	return nil
}

// moveRecipients 过滤移动包的收件人。
func (st *actorState) moveRecipients(mover Brief, targets []scenemap.UUID) []scenemap.UUID {
	out := make([]scenemap.UUID, 0, len(targets))
	for _, u := range targets {
		v, ok := st.briefs[u]
		if !ok {
			continue
		}
		if !st.cfg.Behavior.ASeeBForMsg(v, mover, MsgTypeEntityMove) {
			continue
		}
		if !st.receives(v, MsgTypeEntityMove) {
			continue
		}
		out = append(out, u)
	}
	// 跟随型实体（宠物/召唤）：即使主人已经跑出九宫格，也要直达同步，
	// 否则主人视角里的宠物会停在上一次同步的位置。
	if mover.IsMate() {
		if owner, ok := st.briefs[mover.MasterUUID]; ok &&
			st.cfg.Behavior.MateSyncForOwner(owner, mover) {
			if !containsUUID(out, owner.UUID) {
				out = append(out, owner.UUID)
				sortUUIDs(out)
			}
		}
	}
	return out
}

// leave 处理离图。
func (st *actorState) leave(uuid scenemap.UUID) error {
	b, ok := st.briefs[uuid]
	if !ok {
		return fmt.Errorf("%w: 实体 %s 不在本图", scenemap.ErrNotFound, uuid)
	}
	cell, _ := st.idx.CellOf(uuid)
	viewers := st.entitiesIn(st.visibleCells(cell), uuid)
	for _, u := range viewers {
		v, ok := st.briefs[u]
		if !ok {
			continue
		}
		if !st.cfg.Behavior.ASeeBForAOI(v, b) {
			continue
		}
		st.send(u, LeaveMsg{UUID: uuid, Kind: b.Kind})
	}
	st.idx.Remove(uuid)
	delete(st.briefs, uuid)
	st.dirty = true
	return nil
}

// broadcast 处理业务广播。
func (st *actorState) broadcast(msg any, scope BroadcastScope) error {
	msgType := MessageTypeOf(msg)
	switch scope.Kind {
	case BroadcastViewport:
		sender, ok := st.briefs[scope.Sender]
		if !ok {
			return fmt.Errorf("%w: 广播发送者 %s 不在本图", scenemap.ErrNotFound, scope.Sender)
		}
		cell, _ := st.idx.CellOf(scope.Sender)
		targets := st.entitiesIn(st.visibleCells(cell), scope.Sender)
		out := make([]scenemap.UUID, 0, len(targets))
		for _, u := range targets {
			v, ok := st.briefs[u]
			if !ok {
				continue
			}
			if !st.cfg.Behavior.ASeeBForMsg(sender, v, msgType) {
				continue
			}
			if !st.receives(v, msgType) {
				continue
			}
			out = append(out, u)
		}
		st.sendBatch(out, msg)
		return nil

	case BroadcastCircle:
		// 圆形范围：像素距离平方比较，避免开方与浮点。
		// 目标必须排序：同一技能打中多人时，客户端按包序播放受击表现，
		// 顺序抖动会表现为"有时先弹 A 的伤害数字"。
		r2 := int64(scope.RadiusPX) * int64(scope.RadiusPX)
		var out []scenemap.UUID
		for u, b := range st.briefs {
			dx := int64(b.X - scope.CenterPX)
			dy := int64(b.Y - scope.CenterPY)
			if dx*dx+dy*dy > r2 {
				continue
			}
			if !st.receives(b, msgType) {
				continue
			}
			out = append(out, u)
		}
		sortUUIDs(out)
		st.sendBatch(out, msg)
		return nil

	case BroadcastUUIDs:
		out := make([]scenemap.UUID, 0, len(scope.UUIDs))
		for _, u := range scope.UUIDs {
			b, ok := st.briefs[u]
			if !ok || !st.receives(b, msgType) {
				continue
			}
			out = append(out, u)
		}
		st.sendBatch(out, msg)
		return nil

	case BroadcastMap:
		// 全图广播：目标必须排序，否则同一条消息在不同进程里
		// 顺序不同，压测与日志对比都会对不上。
		all := make([]scenemap.UUID, 0, len(st.briefs))
		for u := range st.briefs {
			all = append(all, u)
		}
		sortUUIDs(all)
		out := all[:0]
		for _, u := range all {
			if st.receives(st.briefs[u], msgType) {
				out = append(out, u)
			}
		}
		st.sendBatch(out, msg)
		return nil

	default:
		return fmt.Errorf("%w: 未知广播范围 %d", scenemap.ErrInvalidArgument, scope.Kind)
	}
}

// receives 报告实体是否接收某类消息。
//
// 玩家恒收；非玩家交给 Behavior（默认怪物只收系统消息）。
// 这是全图广播的性能保护：几百只怪每人一份包既浪费带宽，
// 也让 Sink 的批量投递失去意义。
func (st *actorState) receives(b Brief, msgType int32) bool {
	if b.IsPlayer() {
		return true
	}
	return st.cfg.Behavior.NpeRecvMsg(b, msgType)
}

// countPlayers 统计这些格里的玩家数（排除 exclude）。
func (st *actorState) countPlayers(cells []CellKey, exclude scenemap.UUID) int {
	n := 0
	for _, c := range cells {
		for u := range st.idx.cells[c] {
			if u == exclude {
				continue
			}
			if b, ok := st.briefs[u]; ok && b.IsPlayer() {
				n++
			}
		}
	}
	return n
}

// send 单条投递。Sink 报错只记日志并计数，不重试。
//
// 这是"慢消费者不拖死 AOI"的底线：AOI 在自己的 goroutine 里同步调
// Sink，如果这里为了一条投递去阻塞/重试，一整个房间的视野都会停。
func (st *actorState) send(uuid scenemap.UUID, msg any) {
	if err := st.cfg.Sink.Send(uuid, msg); err != nil {
		st.dropped.Add(1)
		st.cfg.Logger.Warn("aoi 投递失败，已丢弃",
			"key", st.cfg.Key.Key(), "uuid", uuid.String(),
			"msg_type", MessageTypeOf(msg), "err", err.Error())
	}
}

// sendBatch 批量投递（空列表直接跳过）。
func (st *actorState) sendBatch(uuids []scenemap.UUID, msg any) {
	if len(uuids) == 0 {
		return
	}
	if err := st.cfg.Sink.SendBatch(uuids, msg); err != nil {
		st.dropped.Add(uint64(len(uuids)))
		st.cfg.Logger.Warn("aoi 批量投递失败，已丢弃",
			"key", st.cfg.Key.Key(), "count", len(uuids),
			"msg_type", MessageTypeOf(msg), "err", err.Error())
	}
}

// containsUUID 线性查找（目标列表通常只有几十个，比建 set 便宜）。
func containsUUID(list []scenemap.UUID, u scenemap.UUID) bool {
	for _, v := range list {
		if v == u {
			return true
		}
	}
	return false
}

// 编译期断言：默认实现满足接口。
var (
	_ Behavior = DefaultBehavior{}
	_ Logger   = NopLogger{}
)
