package domain

import "fmt"

// TroopPhase 部队业务阶段。文档 6.1：表达“部队正在进行的业务活动”。
// 枚举值持久化，不可重排。
type TroopPhase int32

const (
	// TroopPhaseUnspecified 零值非法。
	TroopPhaseUnspecified TroopPhase = 0
	// TroopPhaseMarching 行军中（含跨场景边界推进）。
	TroopPhaseMarching TroopPhase = 1
	// TroopPhaseCollecting 采集中（必须存在活动 CollectSession）。
	TroopPhaseCollecting TroopPhase = 2
	// TroopPhaseQueued 在目标战斗队列排队中。
	TroopPhaseQueued TroopPhase = 3
	// TroopPhaseFighting 战斗中（必须有 battle_id）。
	TroopPhaseFighting TroopPhase = 4
	// TroopPhaseReturning 回城中。
	TroopPhaseReturning TroopPhase = 5
	// TroopPhaseSettling 已回城待交接（占用槽位，防止奖励未交接又复用）。
	TroopPhaseSettling TroopPhase = 6
	// TroopPhaseClosed 交接完成关闭。终态。
	TroopPhaseClosed TroopPhase = 7
)

func (p TroopPhase) Valid() bool {
	switch p {
	case TroopPhaseMarching, TroopPhaseCollecting, TroopPhaseQueued,
		TroopPhaseFighting, TroopPhaseReturning, TroopPhaseSettling, TroopPhaseClosed:
		return true
	default:
		return false
	}
}

// Terminal 报告阶段是否为终态。
func (p TroopPhase) Terminal() bool { return p == TroopPhaseClosed }

// BusyReason 描述“暂时不允许并发转换”的原因。文档 6.1：
// Phase 表达活动，Busy 表达临时阻塞，二者正交。
// 不用位组合表示占用原因：任意位组合无法校验，用显式枚举。
type BusyReason int32

const (
	// BusyReasonNone 无占用。
	BusyReasonNone BusyReason = 0
	// BusyReasonTransfer 跨场景所有权移交中。
	BusyReasonTransfer BusyReason = 1
	// BusyReasonDefendTransition 防守方为目标共置迁入防守场景中。
	BusyReasonDefendTransition BusyReason = 2
	// BusyReasonBattle 战斗裁决中。
	BusyReasonBattle BusyReason = 3
	// BusyReasonReturnTransition 脱离目标场景回城移交中。
	BusyReasonReturnTransition BusyReason = 4
)

func (r BusyReason) Valid() bool {
	switch r {
	case BusyReasonNone, BusyReasonTransfer, BusyReasonDefendTransition,
		BusyReasonBattle, BusyReasonReturnTransition:
		return true
	default:
		return false
	}
}

// BusyState 是业务锁快照。注意它不是 mutex：不因超时就一律清零，
// 解除必须来自对应持久事实的完成（transfer 提交、战斗终态等）。
// Watchdog 只按 OpID 查询事实，没有事实却长期 Busy 属于数据不变量
// 错误，处理是世界 NotReady 而不是“超过 15 秒自动清锁”（文档 11.5）。
type BusyState struct {
	Reason     BusyReason
	OpID       string
	SinceMS    int64
	DeadlineMS int64
}

// Idle 返回是否空闲。
func (b BusyState) Idle() bool { return b.Reason == BusyReasonNone }

// MoveSegment 是一段已持久化的移动。文档 9.2：权威位置按
// from + (to-from)*elapsed/duration 推导，绝不每帧累加，
// 因此重启后只需要 MoveSegment 本身就能重建任意时刻位置。
type MoveSegment struct {
	MoveID           string
	From             Pos
	To               Pos
	StartAtMS        int64
	EndAtMS          int64
	SpeedUnitsPerSec int64
	// Path 是本段之后剩余的路径点（不含 From/To），用于多段路线
	// 预计算各段起止时间；第一版直线路径 Path 为空。
	Path []Pos
	// SegmentIndex 当前处于第几段（0-based）。
	SegmentIndex int32
	// PathVersion 路径配置版本，过期 A* 结果按版本丢弃（9.2）。
	PathVersion uint64
}

// DurationMS 返回本段时长（毫秒）。零长度段时长为 0。
func (m *MoveSegment) DurationMS() int64 {
	d := m.EndAtMS - m.StartAtMS
	if d < 0 {
		return 0
	}
	return d
}

// PositionAt 推导给定时刻的位置。elapsed 先夹紧到 [0,duration]，
// 容忍时钟跳变与迟到事件；段内位移用 SafeMulDiv 防溢出。
func (m *MoveSegment) PositionAt(nowMS int64) (Pos, error) {
	if m.DurationMS() == 0 {
		return m.To, nil
	}
	elapsed := ClampDurationMS(nowMS-m.StartAtMS, m.DurationMS())
	dx, err := SafeMulDiv(m.To.X-m.From.X, elapsed, m.DurationMS())
	if err != nil {
		return Pos{}, fmt.Errorf("推导 x: %w", err)
	}
	dy, err := SafeMulDiv(m.To.Y-m.From.Y, elapsed, m.DurationMS())
	if err != nil {
		return Pos{}, fmt.Errorf("推导 y: %w", err)
	}
	return Pos{X: m.From.X + dx, Y: m.From.Y + dy}, nil
}

// ArrivedAt 报告给定时刻是否已到达本段终点。
func (m *MoveSegment) ArrivedAt(nowMS int64) bool { return nowMS >= m.EndAtMS }

// DeferredAction 是延迟动作。文档 9.4：FIGHTING 中收到召回不强制清锁，
// 保存一个延迟召回，战斗终态后执行。每支部队只保留一个有效延迟动作，
// 重复请求不叠加。
type DeferredAction struct {
	Kind      DeferredKind
	RequestID string
}

// DeferredKind 延迟动作类型。ForceReturn > Recall 优先级：
// 服务端强制召回可以顶替玩家召回，反向不可。
type DeferredKind int32

const (
	DeferredKindNone DeferredKind = 0
	// DeferredKindRecall 玩家召回。
	DeferredKindRecall DeferredKind = 1
	// DeferredKindForceReturn 服务端强制召回。
	DeferredKindForceReturn DeferredKind = 2
)

// CombatSnapshot 是战斗输入的不可变快照。文档 11.1：
// 免费 MVP 由服务端固定配置生成；真实玩家属性在出征时从 Game
// 可信快照取得，绝不信任客户端上报。
type CombatSnapshot struct {
	ConfigID   int32
	Level      int32
	Attack     int64
	Defense    int64
	HP         int64
	Speed      int64
	SnapshotMS int64
	// RuleVersion 规则版本；热更新规则不重算历史战斗。
	RuleVersion string
}

// Troop 是部队领域对象（文档第 6 章）。字段说明：
//
// 并发归属：Troop 的可变状态只由 OwnerScene 对应的 SceneLoop 修改，
// 其他 goroutine（TxWorker/AOI）只能拿到深拷贝快照。
// Cargo 用 map 是因为物品 ID 稀疏；对外/跨 goroutine 传递必须拷贝。
type Troop struct {
	Header     ObjectHeader
	PlayerID   int64
	SlotID     int32
	HomeCityID ObjectID
	Phase      TroopPhase
	Busy       BusyState
	Target     TargetRef
	Move       *MoveSegment
	Cargo      map[int32]int64
	// CapacityUnits 负载上限（units）。0 表示未配置，出征规划时拒绝。
	CapacityUnits int64
	Stats         CombatSnapshot
	Deferred      *DeferredAction
	// DispatchID 关联本次出征的许可单号（免费 MVP 也生成，
	// 便于将来切换到扣费流程时不改协议）。
	DispatchID string
	// SettlementID 回城交接单号；SETTLING 之后非空。
	SettlementID string
	// BattleID 仅在 FIGHTING 阶段非空，与 Busy=Battle 成对出现。
	BattleID string
}

// Clone 返回部队的深拷贝。跨 goroutine 传递（提交 TxWorker、
// 投影 AOI）必须用 Clone，禁止“复制结构体但共享内部 map”的伪快照
// （文档 4.2）。注意指针字段 Move/Deferred 也逐字段复制。
func (t *Troop) Clone() *Troop {
	if t == nil {
		return nil
	}
	cp := *t
	if t.Move != nil {
		m := *t.Move
		if t.Move.Path != nil {
			m.Path = append([]Pos(nil), t.Move.Path...)
		}
		cp.Move = &m
	}
	if t.Deferred != nil {
		d := *t.Deferred
		cp.Deferred = &d
	}
	if t.Cargo != nil {
		cargo := make(map[int32]int64, len(t.Cargo))
		for k, v := range t.Cargo {
			cargo[k] = v
		}
		cp.Cargo = cargo
	}
	return &cp
}

// CargoUnits 按物品重量表计算当前负载。weight 由调用方从
// 配置注入（domain 不读配置表），缺失物品返回错误而不是按 0 计——
// 静默按 0 计会让玩家塞满零重量物品刷爆经济。
func (t *Troop) CargoUnits(weight func(itemID int32) (int64, error)) (int64, error) {
	var total int64
	for itemID, qty := range t.Cargo {
		w, err := weight(itemID)
		if err != nil {
			return 0, err
		}
		if w <= 0 {
			return 0, fmt.Errorf("%w: 物品 %d 重量必须为正, got %d", ErrInvalidArgument, itemID, w)
		}
		// 溢出检查：单个物品数量上限由配置约束，但乘积仍显式检查。
		if qty > 0 && w > (1<<62)/qty {
			return 0, fmt.Errorf("%w: 部队 %d 物品 %d 负载溢出", ErrOverflow, t.Header.ID, itemID)
		}
		total += qty * w
	}
	return total, nil
}
