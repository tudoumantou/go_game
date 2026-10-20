package application

// planner.go 承载 DefaultPlanner 的装配、命令分派与公共计划工厂。
//
// 设计要点（文档 18.1/18.2）：
//   - 规划器是**无状态**的：所有可变输入（快照、now、随机种子、ID）
//     都由调用方注入，DefaultPlanner 自身只有构造后不再变化的配置字段。
//     因此同一个规划器可以被多个场景循环并发调用，不需要加锁。
//   - 时间与随机性通过**函数类型**注入（IDGen/ObjectIDGen/SeedGen）。
//     Go 的函数是一等公民，用函数类型而不是 interface 的好处是：
//     测试里可以直接传一个闭包（捕获计数器），生产里可以传 UUID
//     生成器或 SQL 序列，不需要为每种实现写一个类型。
//   - 对象 ID 与命令 ID 分开注入。命令 ID（UUID 字符串）可以由进程内
//     生成；对象 ID 必须由 SQL 自增分配（domain/ids.go 已说明原因），
//     M1 内存运行时用原子计数器实现，主进程会传自己的实现。

import (
	"encoding/json"
	"fmt"

	"game_study/internal/worldmap/domain"
)

// IDGen 生成命令/会话/战斗/事件 ID（UUID 语义，只需进程内唯一）。
type IDGen func() string

// ObjectIDGen 生成对象 ID（城池/部队/资源点）。
//
// 注释：对象 ID 的生产实现是 SQL 自增列（map_object.id），M1 内存运行时
// 用 atomic.Uint64 计数器替代。规划器不关心实现细节，只要求“同一个
// 运行时内不重复、单调递增优先”。之所以与 IDGen 分开，是因为两者
// 的失败模式不同：UUID 冲突概率可忽略，而对象 ID 冲突会直接写坏主键。
type ObjectIDGen func() domain.ObjectID

// SeedGen 按稳定刷新键 (block, generation, slot) 派生确定性种子。
//
// 为什么是“派生”而不是直接传一个随机数：刷新批处理可能中断重试，
// 重试时必须得到与首次完全相同的候选序列与位置序列（文档 10.5）。
// 用 (block, generation, slot) 做键，重试时自然复现；用 math/rand
// 全局源则无法从持久化数据重建序列。
type SeedGen func(block, generation, slot int64) int64

const (
	// unitWeight 是第一版的物品重量。文档 10.2 规定重量来自固定版本
	// 配置且必须为正；M1 所有物品重量恒为 1（units 即物品数），
	// 多物品重量表是 M2 的扩展点。写成常量而不是魔法数字，
	// 是为了让“重量从哪来”这个问题在代码里只有一个答案。
	unitWeight int64 = 1

	// m1RuleVersion 是本版规则版本字符串。它写入 Troop.Stats、
	// CollectSession.RuleVersion、BattleRecord.RuleVersion 与资源点
	// ConfigVersion，用于热更新时判断“这条历史记录是哪套规则产生的”
	// （文档 16.5：规则热更新不重算历史战斗）。
	m1RuleVersion = "m1"

	// m1RichBlockCells 是富饶块边长（格）。M1 没有富饶块配置表，
	// 用固定 50×50 的规则网格把世界切块；M2 由 gamedata 注入真实
	// 块矩形（并校验“一个富饶块完全落在一个逻辑场景内”）。
	// 50 能整除默认场景宽度 1000，因此块不会跨场景。
	m1RichBlockCells int64 = 50
)

// M1 部队默认战斗属性（文档 11.1：免费 MVP 由服务端固定配置生成）。
// 这些值不是运营数值，只是让“出征→共置→战斗→结果应用”链路可跑通。
const (
	m1TroopAttack  int64 = 100
	m1TroopDefense int64 = 100
	m1TroopHP      int64 = 1000
	m1GuardAttack  int64 = 80
	m1GuardDefense int64 = 120
	m1GuardHP      int64 = 1200
)

// DefaultPlanner 是 Planner 接口的具体实现。
//
// 注意类型名：接口叫 Planner，实现叫 DefaultPlanner——Go 允许接口与
// 类型同名吗？不允许同一包内重名，因此实现必须换名。这也顺带表达了
// “这是默认实现，测试可以另写一个桩”的意图。
type DefaultPlanner struct {
	// 以下字段构造后只读。Go 没有 const struct，只能靠纪律与注释：
	// 任何写这些字段的代码都在破坏“规划器无状态”的前提。
	topo      domain.Topology
	rules     Rules
	ids       IDGen
	objectIDs ObjectIDGen
	seeds     SeedGen
}

// 编译期断言：实现必须满足接口。接口变更时在这里就编译失败，
// 而不是等到 runtime 第一次调用 Plan 才发现。
var _ Planner = (*DefaultPlanner)(nil)

// NewPlanner 构造规划器。拓扑与规则在构造时各校验一次（文档 17.1：
// 启动校验，失败即拒绝启动）——运行中才发现速率为 0 会让所有计划
// 退化成除零或“瞬间到达”。
func NewPlanner(topo domain.Topology, rules Rules, ids IDGen, objectIDs ObjectIDGen, seeds SeedGen) (*DefaultPlanner, error) {
	if err := topo.Validate(); err != nil {
		return nil, fmt.Errorf("规划器构造失败(拓扑): %w", err)
	}
	if err := rules.Validate(); err != nil {
		return nil, fmt.Errorf("规划器构造失败(规则): %w", err)
	}
	if ids == nil {
		return nil, fmt.Errorf("%w: IDGen 不能为 nil（命令 ID 必须由调用方注入）", domain.ErrInvalidArgument)
	}
	if objectIDs == nil {
		return nil, fmt.Errorf("%w: ObjectIDGen 不能为 nil（对象 ID 必须由 SQL 自增或原子计数器分配）", domain.ErrInvalidArgument)
	}
	if seeds == nil {
		return nil, fmt.Errorf("%w: SeedGen 不能为 nil（刷新/战斗必须可回放）", domain.ErrInvalidArgument)
	}
	return &DefaultPlanner{topo: topo, rules: rules, ids: ids, objectIDs: objectIDs, seeds: seeds}, nil
}

// Plan 是规划器入口。行为顺序（文档 9.1/7.1）：
//
//  1. 信封校验失败直接返回错误——**不写回执**。只有“已接受但结果未知”
//     的命令才需要回执，格式错误是确定失败。
//  2. 命令种类非法 → 包装 ErrInvalidArgument。
//  3. 幂等：同幂等键且 PayloadHash 相同 → 返回只含首次回执的计划。
//     同键不同 hash → ErrIdempotencyConflict（不能把两次不同请求
//     的结果混成一个）。
//  4. 按 Kind 分派；Transfer 三兄弟在 M1 返回 ErrFeatureDisabled。
//
// 错误约定：所有拒绝都用 %w 包装 domain 的哨兵错误（ErrObjectNotFound、
// ErrSlotBusy 等），调用方用 errors.Is 判定并交给 domain.CodeOf 取业务码。
// 不要返回裸 errors.New：那样既丢失可判定性，也让 CodeOf 落到 44013。
func (p *DefaultPlanner) Plan(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	if err := env.Validate(); err != nil {
		return nil, err
	}
	if !knownKind(cmd.Kind) {
		return nil, fmt.Errorf("%w: 未知命令种类 %d", domain.ErrInvalidArgument, int32(cmd.Kind))
	}
	if snap == nil {
		return nil, fmt.Errorf("%w: 快照不能为空", domain.ErrInvalidArgument)
	}
	// 路由校验：TargetScene 由 runtime 填写，非 0 时必须等于快照场景。
	// 不一致说明命令投递到了错误的场景循环，属于路由 bug，按参数错误
	// 拒绝，绝不能“按当前场景凑合执行”。
	if cmd.TargetScene != 0 && cmd.TargetScene != snap.SceneID() {
		return nil, fmt.Errorf("%w: 命令目标场景 %d 与快照场景 %d 不符",
			domain.ErrInvalidArgument, cmd.TargetScene, snap.SceneID())
	}

	// 幂等重放。注意切片 ResultJSON 也要复制：快照内部的回执与
	// 交给 TxWorker 的计划必须互不共享底层数组（文档 4.2 深拷贝纪律）。
	if first, ok := snap.Receipt(env.Envelope()); ok {
		if first.PayloadHash != env.PayloadHash {
			return nil, fmt.Errorf("%w: request_id %q 已用于不同参数（payload_hash 不一致）",
				domain.ErrIdempotencyConflict, env.RequestID)
		}
		plan := newPlan(snap, env, p.ids, nowMS)
		// CommandID 复用首次回执的 ID：writer 以 (world,player,request)
		// 为主键写入，重复写同一行是幂等覆盖；如果这里新生成 ID，
		// 回执主键就会与首次不一致，对账时会出现两条“同一请求”的记录。
		// newPlan 刚分配的那个 ID 被随即覆盖、直接丢弃：浪费一个 UUID
		// 换取“所有计划都走同一个工厂”，比重写一份计划初始化更划算。
		plan.CommandID = first.CommandID
		replay := first
		replay.ResultJSON = append([]byte(nil), first.ResultJSON...)
		plan.CommandReceipt = &replay
		// MutationPlan 没有 Replayed 字段（不允许改 domain），调用方
		// 通过“计划无对象变更 + 回执已存在”识别重放。writer 侧对
		// 重复回执做 UPSERT 即可，不会产生第二次业务副作用。
		return plan, nil
	}

	switch cmd.Kind {
	case CmdMarchStart:
		return p.PlanMarchStart(snap, env, cmd, nowMS)
	case CmdRecall:
		return p.PlanRecall(snap, env, cmd, nowMS)
	case CmdArrive:
		return p.PlanArrive(snap, env, cmd, nowMS)
	case CmdCollectStart:
		return p.PlanCollectStart(snap, env, cmd, nowMS)
	case CmdCollectSettle:
		return p.PlanCollectSettle(snap, env, cmd, nowMS)
	case CmdRefresh:
		return p.PlanRefresh(snap, env, cmd, nowMS)
	case CmdBattleResult:
		return p.PlanBattleResult(snap, env, cmd, nowMS)
	case CmdBattleTimeout:
		return p.PlanBattleTimeout(snap, env, cmd, nowMS)
	case CmdTransferPrepare, CmdTransferActivate, CmdTransferRelease:
		return nil, fmt.Errorf("%w: 移交命令 %s 在 M1 未开放", domain.ErrFeatureDisabled, cmd.Kind)
	default:
		// knownKind 已拦截，这里是防御性兜底：将来新增枚举值但漏写
		// 分派分支时，返回可判定错误而不是 nil, nil。
		return nil, fmt.Errorf("%w: 命令种类 %s 未实现", domain.ErrInvalidArgument, cmd.Kind)
	}
}

// knownKind 报告枚举是否为已实现/已登记的命令种类。
func knownKind(k CommandKind) bool {
	switch k {
	case CmdMarchStart, CmdRecall, CmdArrive, CmdCollectStart, CmdCollectSettle,
		CmdRefresh, CmdBattleResult, CmdBattleTimeout,
		CmdTransferPrepare, CmdTransferActivate, CmdTransferRelease:
		return true
	default:
		return false
	}
}

// newPlan 是统一的计划工厂：一次填好命令 ID、幂等键、请求摘要、
// world epoch 与受影响场景 revision。
//
// nowMS 目前不落字段：计划里没有“创建时刻”，M1 由 writer 在事务提交时
// 统一取数据库时间；保留参数是为了 M2 增加 mutation_plan.created_at_ms
// 时不改所有调用点（与 SnapshotOptions 同样的取舍）。
func newPlan(snap *SceneSnapshot, env domain.CommandEnvelope, ids IDGen, nowMS int64) *domain.MutationPlan {
	_ = nowMS
	return &domain.MutationPlan{
		CommandID:   ids(),
		Key:         env.Envelope(),
		PayloadHash: env.PayloadHash,
		Epoch:       snap.Epoch(),
		// 第一版一条命令只影响当前场景。切片而不是单值：将来跨场景
		// 移交会变成两条，加锁顺序必须按 SceneID 升序（文档 7.2）。
		AffectedSceneRevisions: []domain.SceneRevision{
			{SceneID: snap.SceneID(), Revision: snap.Revision()},
		},
	}
}

// setReceipt 填命令回执。
//
// ResultJSON 用 map[string]any + json.Marshal（第一版），M5 换 proto：
//   - encoding/json 对 map 的键做字典序排序，输出稳定，同一输入
//     逐字节相同，便于测试与对账；
//   - map 的 value 只放 int64/uint64/string/bool，避免 float64
//     把 2^53 以上的 ID 悄悄截断（any 存 float 是经典陷阱）。
func setReceipt(plan *domain.MutationPlan, code int32, result map[string]any, nowMS int64) {
	plan.CommandReceipt = &domain.CommandReceipt{
		CommandID:     plan.CommandID,
		Key:           plan.Key,
		PayloadHash:   plan.PayloadHash,
		ResultCode:    code,
		ResultJSON:    receiptJSON(result),
		CommittedAtMS: nowMS,
	}
}

// receiptJSON 序列化回执。map 内只放基础类型，Marshal 实际不会失败；
// 万一失败也不能把 nil 写进回执（会被当成空 payload，客户端解析
// 失败后更难定位），返回一个固定可识别的 JSON。
func receiptJSON(result map[string]any) []byte {
	b, err := json.Marshal(result)
	if err != nil {
		return []byte(`{"error":"marshal_receipt"}`)
	}
	return b
}

// expectVersion 记录被更新对象的期望版本（乐观并发）。只对“已存在
// 且本计划会更新”的对象调用；新建对象没有期望版本。
func expectVersion(plan *domain.MutationPlan, key domain.ObjectKey, version uint64) {
	plan.ExpectedObjectVersions = append(plan.ExpectedObjectVersions,
		domain.ObjectVersion{Key: key, Version: version})
}

// upsertTroop 把部队放进 ObjectsUpsert。
func upsertTroop(plan *domain.MutationPlan, troop *domain.Troop, scene domain.SceneID) {
	plan.ObjectsUpsert = append(plan.ObjectsUpsert, domain.ObjectSnapshot{
		Key:        troop.Header.Key(),
		Kind:       domain.ObjectKindTroop,
		OwnerScene: scene,
		Version:    troop.Header.Version,
		PlayerID:   troop.PlayerID,
		Troop:      troop,
	})
}

// upsertResource 把资源点放进 ObjectsUpsert。
func upsertResource(plan *domain.MutationPlan, res *domain.ResourceNode, scene domain.SceneID) {
	plan.ObjectsUpsert = append(plan.ObjectsUpsert, domain.ObjectSnapshot{
		Key:        res.Header.Key(),
		Kind:       domain.ObjectKindResource,
		OwnerScene: scene,
		Version:    res.Header.Version,
		Resource:   res,
	})
}

// newEvent 构造一条 outbox 投影事件。payload 允许为空：消费者可以
// 只按 revision 变化重新拉快照（文档 7.3），事件体不是真源。
func (p *DefaultPlanner) newEvent(snap *SceneSnapshot, env domain.CommandEnvelope, kind string, payload []byte) domain.OutboxEvent {
	return domain.OutboxEvent{
		EventID:  p.ids(),
		WorldID:  env.WorldID,
		SceneID:  snap.SceneID(),
		Revision: snap.Revision(),
		Kind:     kind,
		Payload:  payload,
	}
}

// cargoUnits 按重量表计算当前负载。第一版重量恒为 unitWeight；
// 传闭包而不是传表，是因为 Troop.CargoUnits 的签名要求
// func(itemID int32) (int64, error)，闭包可以捕获配置而无需全局变量。
func (p *DefaultPlanner) cargoUnits(t *domain.Troop) (int64, error) {
	return t.CargoUnits(func(int32) (int64, error) { return unitWeight, nil })
}

// capacityLeft 返回剩余负载。
//
// 已超载（cargo > capacity）按 0 处理而不是报错：这种情况只可能来自
// 配置下调或数据损坏，此时部队本来就无法继续采集，让命令以“负载不足”
// 回城比整条命令报错、部队永久停在目标点更容易恢复。
func (p *DefaultPlanner) capacityLeft(t *domain.Troop) (int64, error) {
	used, err := p.cargoUnits(t)
	if err != nil {
		return 0, fmt.Errorf("部队 %d 负载计算失败: %w", t.Header.ID, err)
	}
	left := p.rules.CapacityUnits - used
	if left < 0 {
		return 0, nil
	}
	return left, nil
}

// newTroopStats 生成部队的默认战斗属性快照（文档 11.1）。
// SnapshotMS 取命令的 nowMS，保证同一命令重放出同一个快照。
func (p *DefaultPlanner) newTroopStats(nowMS int64) domain.CombatSnapshot {
	return domain.CombatSnapshot{
		ConfigID:    1,
		Level:       1,
		Attack:      m1TroopAttack,
		Defense:     m1TroopDefense,
		HP:          m1TroopHP,
		Speed:       p.rules.MarchSpeedUnitsPerSec,
		SnapshotMS:  nowMS,
		RuleVersion: m1RuleVersion,
	}
}

// m1GuardStats 是资源点/城池守军的默认属性（资源点自身没有 Troop 对象）。
func m1GuardStats(nowMS int64) domain.CombatSnapshot {
	return domain.CombatSnapshot{
		ConfigID:    2,
		Level:       1,
		Attack:      m1GuardAttack,
		Defense:     m1GuardDefense,
		HP:          m1GuardHP,
		SnapshotMS:  nowMS,
		RuleVersion: m1RuleVersion,
	}
}

// troopPosition 推导航行中部队在 nowMS 的权威位置。
func (p *DefaultPlanner) troopPosition(t *domain.Troop, nowMS int64) (domain.Pos, error) {
	if t.Move == nil {
		return domain.Pos{}, fmt.Errorf("%w: 部队 %d 没有移动段", domain.ErrInvalidState, t.Header.ID)
	}
	pos, err := t.Move.PositionAt(nowMS)
	if err != nil {
		return domain.Pos{}, fmt.Errorf("部队 %d 位置推导失败: %w", t.Header.ID, err)
	}
	return pos, nil
}

// targetPosition 返回目标对象的坐标。部队目标的坐标取它当前移动段
// 的终点（部队没有独立坐标字段，位置永远由 Move 推导）。
func (p *DefaultPlanner) targetPosition(snap *SceneSnapshot, env domain.CommandEnvelope, id domain.ObjectID) (domain.Pos, bool) {
	key := domain.ObjectKey{WorldID: env.WorldID, ID: id}
	obj, ok := snap.Object(key)
	if !ok || obj.Deleted {
		return domain.Pos{}, false
	}
	switch obj.Kind {
	case domain.ObjectKindCity:
		c, ok := snap.City(id)
		if !ok {
			return domain.Pos{}, false
		}
		return c.Position, true
	case domain.ObjectKindResource:
		r, ok := snap.Resource(id)
		if !ok {
			return domain.Pos{}, false
		}
		return r.Position, true
	case domain.ObjectKindTroop:
		t, ok := snap.Troop(id)
		if !ok || t.Move == nil {
			return domain.Pos{}, false
		}
		return t.Move.To, true
	default:
		return domain.Pos{}, false
	}
}

// deferredKindName 返回延迟动作的稳定名称（进回执与日志）。
func deferredKindName(k domain.DeferredKind) string {
	switch k {
	case domain.DeferredKindRecall:
		return "RECALL"
	case domain.DeferredKindForceReturn:
		return "FORCE_RETURN"
	default:
		return "NONE"
	}
}

// troopPhaseName 返回阶段名称。domain.TroopPhase 只有 Valid/Terminal，
// 没有 String 方法，回执里写数字对排查毫无帮助，这里补一个。
func troopPhaseName(p domain.TroopPhase) string {
	switch p {
	case domain.TroopPhaseMarching:
		return "MARCHING"
	case domain.TroopPhaseCollecting:
		return "COLLECTING"
	case domain.TroopPhaseQueued:
		return "QUEUED"
	case domain.TroopPhaseFighting:
		return "FIGHTING"
	case domain.TroopPhaseReturning:
		return "RETURNING"
	case domain.TroopPhaseSettling:
		return "SETTLING"
	case domain.TroopPhaseClosed:
		return "CLOSED"
	default:
		return "UNSPECIFIED"
	}
}

// sm64 是确定性伪随机流（SplitMix64）。
//
// 为什么不用 math/rand：
//   - 全局源从 Go 1.20 起自动随机播种，序列无法从持久化种子重建；
//   - rand.New(rand.NewSource(seed)) 虽然是确定性的，但它的算法
//     属于“实现细节”，跨 Go 版本不保证稳定；刷新批处理可能跨越
//     版本升级重试，序列变了就会出现“同一 slot 生成了不同位置”。
//
// SplitMix64 是无状态推进的纯函数，几行代码即可自证可回放。
type sm64 struct {
	state uint64
}

func newSM64(seed int64) *sm64 {
	// int64→uint64 按补码语义转换，负种子落在流的不同区段，
	// 与正种子不会互相污染，转换本身是确定的。
	return &sm64{state: uint64(seed) + 0x9E3779B97F4A7C15}
}

func (r *sm64) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// int63n 返回 [0,n) 内的确定性整数。n<=0 返回 0，调用方需保证 n>0。
// 取模偏差在“候选格抽签”这种用途下无业务意义，换取实现简单。
func (r *sm64) int63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(r.next() % uint64(n))
}
