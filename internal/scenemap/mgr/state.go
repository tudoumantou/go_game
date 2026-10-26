package mgr

import (
	"fmt"
	"sort"

	"game_study/internal/scenemap"
)

// 路径缓存的容量与时间窗（规格 5.5：记录最近 N 毫秒的格子路径）。
//
// 用固定窗口而不是"最近 N 个点"：位置同步是高频的，按个数限制时
// 网络抖动（一口气补发 100 个包）会把窗口挤爆、把更早的轨迹冲掉；
// 按时间窗才能稳定回答"过去 5 秒他走过哪些格"。
const (
	pathTraceCap = 64
	pathWindowMS = 5000
)

// PathPoint 是路径上的一个采样点。
type PathPoint struct {
	Pos scenemap.GridPos
	At  int64 // unix 毫秒
}

// PathTrace 是定长环形缓冲，保存玩家最近的格子轨迹。
//
// 为什么用环形缓冲：路径记录是"永远在写、偶尔回看"的热路径，append
// 到切片会让底层数组无限增长，而删除头部又是 O(n)。环形缓冲写入 O(1)、
// 零分配、天然覆盖旧数据。
//
// 只允许 MGR goroutine 访问（与 State 同一条单写者规则）。
type PathTrace struct {
	buf  []PathPoint
	head int // 下一个写入位置
	n    int // 已写入的有效条数（≤ cap）
}

func newPathTrace() *PathTrace {
	return &PathTrace{buf: make([]PathPoint, pathTraceCap)}
}

// Add 写入一个采样点，覆盖最旧的一条。
func (p *PathTrace) Add(pt PathPoint) {
	if len(p.buf) == 0 {
		p.buf = make([]PathPoint, pathTraceCap)
	}
	p.buf[p.head] = pt
	p.head = (p.head + 1) % len(p.buf)
	if p.n < len(p.buf) {
		p.n++
	}
}

// Points 返回按时间升序排列、且不早于 now-pathWindowMS 的轨迹副本。
//
// 返回副本而不是内部切片：调用方（玩法/AI/反外挂）不应该持有内部
// 缓冲的引用，否则它下一次 Add 会就地改写调用方正在读的数据。
func (p *PathTrace) Points(now int64) []PathPoint {
	if p.n == 0 {
		return nil
	}
	cut := now - pathWindowMS
	out := make([]PathPoint, 0, p.n)
	start := (p.head - p.n + len(p.buf)) % len(p.buf)
	for i := 0; i < p.n; i++ {
		pt := p.buf[(start+i)%len(p.buf)]
		if pt.At < cut {
			continue
		}
		out = append(out, pt)
	}
	return out
}

// State 是场景的权威状态（规格 3.3）。
//
// # 单写者（single-writer）纪律
//
// 本结构的**每一个字段**只允许 MGR 自己的 goroutine 读写。没有任何锁，
// 也不应该有：加锁会让"AOI 线程顺手改一下"这类越权变得合法，而单写者
// 模型的价值就在于越权会立刻被 -race 抓到。
//
// 跨 Actor 读状态的正确姿势：
//   - 通过 Mgr.Call(ctx, ...) 把读操作变成一条命令（测试与低频路径）；
//   - 或由 MGR 发布不可变快照（atomic.Pointer），读方只读快照。
//
// 绝不把 *State 交给另一个 goroutine 长期持有。
type State struct {
	// Key 地图唯一标识（MapKey + Route 才唯一确定一个 MGR 实例）。
	Key scenemap.MapKey
	// MapID 地图配置 ID（与 Key.MapID 一致，冗余存一份便于玩法直接读）。
	MapID int32
	// Route 分线号。0 表示"由平衡器决定"。
	Route scenemap.Route

	// Users 真实玩家。**只有玩家**——关图计时用 len(Users)==0 判断"图空了"，
	// 如果把宠物/召唤塞进来，一张只剩宠物的图会永远关不掉。
	Users map[scenemap.UUID]*UserState
	// NPES 怪物/NPC 的私有状态。没有独立 goroutine，全部活在 MGR 里。
	NPES map[scenemap.UUID]*NpeAIInfo
	// Pets 宠物/召唤/镜像的登记表：uuid → 主人 uuid。
	//
	// 规格 3.3 没有单列这个字段（原实现里它们混在 users 里），这里拆出来
	// 是为了让"玩家可重叠、怪物不可重叠"和关图计时都能拿到干净的玩家集合。
	Pets map[scenemap.UUID]scenemap.UUID
	// Bullets 飞行物。
	Bullets map[scenemap.UUID]*Bullet
	// Firewells 持续伤害区域。
	Firewells map[scenemap.UUID]*Firewell

	// MapStartTime 开图时间（unix 毫秒）。
	MapStartTime int64
	// RefreshData 刷怪器运行时状态（规格 7.2）。
	RefreshData *RefreshData
	// Self 地图自身的编号。0 表示由 RoomManager 在 MgrInit 里赋值。
	// 它与 MapKey 不重复：Key 是"名字"，Self 是"会话/实例编号"，
	// 跨节点重开同名图时 Key 相同而 Self 必须不同。
	Self scenemap.UUID
	// CloseTime 关图时间（unix 毫秒），0 表示不关图。
	// 语义是"到这个时刻如果还空着就关"，由每秒 loop 收割（规格 4.4）。
	CloseTime int64
	// Ext 框架预留的扩展槽（例如 GM 标记、活动 ID）。
	// 玩法私有状态放 Data，不要往 Ext 里塞业务字段——Ext 的键没有命名空间，
	// 多个玩法共用必然撞车。
	Ext map[string]any
	// Data 玩法私有状态，框架**不解释、不序列化、不校验**。
	// 谁写谁负责，MGR 只保证它的读写都在同一 goroutine 里。
	Data any

	// Revision 每次状态变更 +1（规格 11.2 的 Go 落地方式）。
	//
	// 用途：命令执行完比对前后 revision，判断"这条命令到底改没改状态"。
	// 依赖"返回新状态"来提交变更的方案，漏产出时容易被忽略，
	// 表现为状态静默不更新；这里用 revision 把它变成可断言的错误 ErrNoEffect。
	//
	// 纪律：任何改变可观察状态的代码路径都必须调用 Touch()。
	Revision uint64

	// Index 格子索引。与上面的 map 是同一份数据的两种视图：
	// Users/NPES 回答"这个实体是什么"，Index 回答"这一格有谁"。
	// 两者必须在同一个函数里同步更新，否则会出现"人站在墙上"的幽灵。
	Index *Index

	// ---- 以下为框架内部字段，玩法不要读写 ----

	// owner 反向指针，让 Command.Apply(s) 这种"只拿到 State"的签名
	// 仍能访问 Mgr 的依赖（AOI/Gameplay/配置）。只在 MGR goroutine 里读。
	owner *Mgr
	// closed 是否已进入关闭流程。
	closed bool
	// terminated 是否已调用过 Gameplay.OnTerminate（保证只调一次）。
	terminated bool
	// closeNotified 是否已发出关图通知（保证 OnClose 只发一次）。
	closeNotified bool
	// paths 玩家最近轨迹，按 UUID 索引。
	paths map[scenemap.UUID]*PathTrace
}

// newState 构造空状态。只允许 MGR goroutine 调用（Mgr.run 里）。
func newState(m *Mgr) *State {
	now := m.now()
	return &State{
		Key:          m.cfg.Key,
		MapID:        m.cfg.MapID,
		Route:        m.cfg.Route,
		Users:        make(map[scenemap.UUID]*UserState),
		NPES:         make(map[scenemap.UUID]*NpeAIInfo),
		Pets:         make(map[scenemap.UUID]scenemap.UUID),
		Bullets:      make(map[scenemap.UUID]*Bullet),
		Firewells:    make(map[scenemap.UUID]*Firewell),
		MapStartTime: now,
		RefreshData:  NewRefreshData(),
		Self:         m.cfg.Self,
		Ext:          make(map[string]any),
		Index:        NewIndex(m.cfg.Size),
		owner:        m,
		paths:        make(map[scenemap.UUID]*PathTrace),
	}
}

// MoveNPE 移动一个非玩家实体（怪物/召唤/宠物），走与玩家完全相同的
// 校验、索引迁移、AOI 同步与换格钩子。
//
// # 为什么直接调 syncMap 而不是 Post 命令
//
// AI tick 本身就在 MGR goroutine 里（handleBuffCheck → Deps.AITick）。
// 再 Post 一条命令会带来两个问题：移动延迟到下一轮（AI 看起来"卡半拍"），
// 以及邮箱满时命令被拒（怪物永远走不动，且没人报错）。
// 这一步是"当前状态的一部分"，必须同步完成。
//
// 调用约束：只能在 MGR goroutine 内调用。entity 包的 TickAll 正是
// 由 MGR 的 buff 定时器调用的，满足这个前提；玩家 Actor 调它会破坏单写者。
func (s *State) MoveNPE(uuid scenemap.UUID, to scenemap.GridPos, dir scenemap.Dir) error {
	if s.owner == nil {
		// 纯状态场景（单元测试、离线工具）：迁移索引 + 同步实体字段，
		// 但没有 AOI 可推。降级路径必须保留阻挡校验与字段更新——
		// 少了任何一条，"测试通过但线上怪物穿墙/位置不更新"就会发生。
		if s.Index.BlockedFor(uuid, to) {
			return fmt.Errorf("%w: %s 被动态阻挡占据", scenemap.ErrBlocked, to)
		}
		if _, err := s.Index.Move(uuid, to); err != nil {
			return err
		}
		if n := s.NPES[uuid]; n != nil {
			n.GridPos = to
			n.Dir = dir
		}
		if u := s.Users[uuid]; u != nil {
			u.GridPos = to
			u.Dir = dir
		}
		s.Touch()
		return nil
	}
	pixel := s.owner.cfg.Geo.CellCenter(to)
	_, err := s.owner.syncMap(s, CommandMove{UUID: uuid, Pixel: pixel, Dir: dir})
	return err
}

// Touch 自增 revision。**任何改变状态的路径都必须调用**。
//
// 代价是一次自增，收益是"命令没生效"能在开发期立刻炸出来，
// 而不是线上靠玩家反馈"我点了没反应"来猜。
func (s *State) Touch() { s.Revision++ }

// Now 返回当前时间（unix 毫秒）。走 Mgr 注入的时钟，测试可替换。
func (s *State) Now() int64 {
	if s.owner == nil {
		return 0
	}
	return s.owner.now()
}

// SpeedPermille 返回当前倍速（千分比，1000=1x）。
//
// 给 battle/entity 这类"挂在 MGR 上的算法包"用：它们需要知道倍速来缩放
// 技能前摇、子弹间隔、刷怪周期。无主状态返回 1000（不缩放）。
func (s *State) SpeedPermille() int32 {
	if s == nil || s.owner == nil {
		return 1000
	}
	return s.owner.Speed()
}

// Broadcast 在 MGR goroutine 内按范围广播（规格 §6.7 的 MGR.BcastMsg）。
//
// 为什么暴露这个方法：battle/玩法需要在"结算完成的那一刻"把表现包
// 发出去，而它们只拿得到 *State。走命令（CommandBcast）是异步的，
// 会让"结算顺序"与"广播顺序"之间插入其他命令；这里同步转发给 AOI，
// 保证"先扣血、再飘字"的顺序不被破坏。
//
// 无主状态（单元测试）直接丢弃：没有 AOI 可发，也不是错误。
func (s *State) Broadcast(msg any, scope BroadcastScope) error {
	if s == nil || s.owner == nil {
		return nil
	}
	_, err := s.owner.broadcast(msg, scope)
	return err
}

// Closed 报告是否已进入关闭流程。
func (s *State) Closed() bool { return s.closed }

// SetExt 写扩展槽（框架内部/玩法共用，键必须带命名空间前缀）。
func (s *State) SetExt(key string, v any) {
	if s.Ext == nil {
		s.Ext = make(map[string]any)
	}
	s.Ext[key] = v
	s.Touch()
}

// ExtValue 读扩展槽。
func (s *State) ExtValue(key string) (any, bool) {
	if s.Ext == nil {
		return nil, false
	}
	v, ok := s.Ext[key]
	return v, ok
}

// NpeUUIDs 返回按 UUID 升序排列的怪物 UUID 列表。
//
// 为什么必须排序：Go 的 map 遍历顺序是随机的（每次 range 的起点不同）。
// buff/召唤超时这类"逐个结算"的逻辑一旦依赖遍历顺序，两次运行会得到
// 不同结果（谁先死、谁先触发被动），线上问题无法复现。规格 7.2 要求
// 结算顺序稳定，所以这里统一提供排序后的视图，任何遍历 NPES 的地方
// 都应该用它，而不是直接 range map。
func (s *State) NpeUUIDs() []scenemap.UUID {
	out := make([]scenemap.UUID, 0, len(s.NPES))
	for id := range s.NPES {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// UserUUIDs 返回按 UUID 升序排列的玩家 UUID 列表（理由同上）。
func (s *State) UserUUIDs() []scenemap.UUID {
	out := make([]scenemap.UUID, 0, len(s.Users))
	for id := range s.Users {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// RecordPath 记录一次位置采样（进图/换格/同格微调都记）。
func (s *State) RecordPath(uuid scenemap.UUID, pos scenemap.GridPos, at int64) {
	if s.paths == nil {
		s.paths = make(map[scenemap.UUID]*PathTrace)
	}
	tr, ok := s.paths[uuid]
	if !ok {
		tr = newPathTrace()
		s.paths[uuid] = tr
	}
	tr.Add(PathPoint{Pos: pos, At: at})
}

// PathOf 返回最近路径的副本。now 用调用方传入的时间，避免多次读时钟。
func (s *State) PathOf(uuid scenemap.UUID, now int64) []PathPoint {
	tr, ok := s.paths[uuid]
	if !ok {
		return nil
	}
	return tr.Points(now)
}
