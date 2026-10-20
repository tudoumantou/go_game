// Package application 是 SLG 大地图的命令规划层。
//
// 职责边界（文档 18.1）：只做「已提交的只读快照 + 命令 → MutationPlan」的
// 纯计算。它不读时钟（nowMS 由调用方注入）、不读随机数（种子由调用方注入）、
// 不访问数据库与网络、不持有任何可变共享状态。这样做的原因有三个：
//
//  1. 可测试：给定快照与命令，规划结果是确定的，可用表驱动覆盖所有分支；
//  2. 可重放：崩溃恢复时能用同一份快照重算出同一个计划，不会因为
//     "规划时又读了一次库" 得出不同结果；
//  3. 可并发：规划器可以被多个场景循环同时调用（只要各自传自己的快照），
//     因为它没有字段被写。
//
// 依赖方向：application → domain、spatial。runtime 反过来 import 本包，
// 拿 SceneSnapshot/Command 类型并调用 Planner 接口。
package application

import (
	"fmt"

	"game_study/internal/worldmap/domain"
)

// CommandKind 命令种类。这些值会进入内部命令路由与回执，
// 一经发布不可重排（新增只能追加）。
//
// 为什么用枚举 + 集中式参数结构，而不是每种命令一个 interface：
// Go 的 interface 命令模式需要类型断言，规划器入口会变成
// switch cmd.(type)，一旦漏一个分支就是运行时 panic 而不是编译错误。
// 用 Kind 枚举 + 结构体字段，switch 漏分支时参数为零值，
// 校验函数会明确拒绝，错误可预测。
type CommandKind int32

const (
	// CmdMarchStart 出征（玩家命令）。
	CmdMarchStart CommandKind = 1
	// CmdRecall 召回（玩家命令）。
	CmdRecall CommandKind = 2
	// CmdArrive 到达（内部命令，由到期事件触发）。
	CmdArrive CommandKind = 3
	// CmdCollectStart 开始采集（内部命令，到达仲裁后触发）。
	CmdCollectStart CommandKind = 4
	// CmdCollectSettle 采集结算（内部命令，采集到期或召回触发）。
	CmdCollectSettle CommandKind = 5
	// CmdRefresh 富饶块刷新（内部命令）。
	CmdRefresh CommandKind = 6
	// CmdTransferPrepare/Activate/Release 跨场景移交（M4 实现，当前返回未开放）。
	CmdTransferPrepare  CommandKind = 7
	CmdTransferActivate CommandKind = 8
	CmdTransferRelease  CommandKind = 9
	// CmdBattleResult 战斗结果应用（内部命令，由 Resolver 回包触发，
	// 文档 11.3）。结果与超时共用 battle 维度的幂等键
	// （domain.BattleResultKey），谁先提交谁生效。
	CmdBattleResult CommandKind = 10
	// CmdBattleTimeout 战斗超时裁决（内部命令，由到期事件触发，
	// 文档 11.3：PENDING→TIMED_OUT，攻击无效、防守保留、无战损）。
	CmdBattleTimeout CommandKind = 11
)

func (k CommandKind) String() string {
	switch k {
	case CmdMarchStart:
		return "MARCH_START"
	case CmdRecall:
		return "RECALL"
	case CmdArrive:
		return "ARRIVE"
	case CmdCollectStart:
		return "COLLECT_START"
	case CmdCollectSettle:
		return "COLLECT_SETTLE"
	case CmdRefresh:
		return "REFRESH"
	case CmdTransferPrepare:
		return "TRANSFER_PREPARE"
	case CmdTransferActivate:
		return "TRANSFER_ACTIVATE"
	case CmdTransferRelease:
		return "TRANSFER_RELEASE"
	case CmdBattleResult:
		return "BATTLE_RESULT"
	case CmdBattleTimeout:
		return "BATTLE_TIMEOUT"
	default:
		return fmt.Sprintf("CommandKind(%d)", int32(k))
	}
}

// Internal 报告该命令是否由服务端自己产生（内部命令不写玩家幂等回执，
// 用确定性内部键做幂等，见 domain/collect.go 的 ArriveKey 等）。
func (k CommandKind) Internal() bool {
	switch k {
	case CmdArrive, CmdCollectStart, CmdCollectSettle, CmdRefresh,
		CmdTransferPrepare, CmdTransferActivate, CmdTransferRelease,
		CmdBattleResult, CmdBattleTimeout:
		return true
	default:
		return false
	}
}

// Command 是规划器的输入。所有字段都是「参数」而不是「权威状态」：
// 坐标、速度、奖励一律由服务端按快照与规则推导，客户端传来的
// 只有 ID 与意图（文档 9.1）。
type Command struct {
	Kind CommandKind

	// March 仅 CmdMarchStart 使用。
	March MarchParams
	// Recall 仅 CmdRecall 使用。
	Recall RecallParams
	// Arrive 仅 CmdArrive 使用。
	Arrive ArriveParams
	// CollectStart 仅 CmdCollectStart 使用。
	CollectStart CollectStartParams
	// CollectSettle 仅 CmdCollectSettle 使用。
	CollectSettle CollectSettleParams
	// Refresh 仅 CmdRefresh 使用。
	Refresh RefreshParams
	// BattleResult 仅 CmdBattleResult 使用。
	BattleResult BattleResultParams
	// BattleTimeout 仅 CmdBattleTimeout 使用。
	BattleTimeout BattleTimeoutParams
	// TargetScene 由 runtime 路由填写，规划器用它校验快照归属。
	TargetScene domain.SceneID
}

// MarchParams 出征参数。注意没有「速度」「携带量」「伤害」字段：
// 这些由服务端规则算，客户端指定就是作弊入口。
type MarchParams struct {
	SlotID     int32
	HomeCityID domain.ObjectID
	Target     domain.TargetRef
}

// RecallParams 召回参数。ExpectedVersion 用于乐观并发：
// 玩家看到的是旧界面时，召回可能基于过期版本，必须拒绝而不是
// "按当前状态猜测意图"。
type RecallParams struct {
	TroopID         domain.ObjectID
	ExpectedVersion uint64
	// Force 服务端管理命令使用；玩家协议永远传 false。
	Force bool
}

// ArriveParams 到达参数。MoveID 是过期判定的唯一依据：
// 改速或召回后 MoveID 会变，旧到达任务触发时必须判定过期并直接返回。
type ArriveParams struct {
	TroopID domain.ObjectID
	MoveID  string
}

// CollectStartParams 开始采集参数。
type CollectStartParams struct {
	TroopID    domain.ObjectID
	ResourceID domain.ObjectID
}

// CollectSettleParams 采集结算参数。SettleAtMS 由命令生成时固化：
// 重试不得重新取 now，否则会多算收益（文档 9.2 最后一句）。
type CollectSettleParams struct {
	SessionID  string
	SettleAtMS int64
}

// RefreshParams 富饶块刷新参数。Generation/Slot 构成稳定刷新键，
// 重试时同一个键必须得到同一个计划（幂等）。
type RefreshParams struct {
	RichBlockID int64
	Generation  int64
	Slot        int32
}

// BattleResultParams 战斗结果参数（文档 11.3）。
//
// 只有 battle_id 与胜者身份：伤害、战利品、阶段迁移一律由服务端按
// 规则推导，客户端上报的 BattleResultReport 不能直接操作 map_battle
// （11.2：外部 Resolver 需认证响应来源）。
//
// Winner 为 0 时退化为按 AttackerWon 推胜者（便于只关心"谁赢"的
// 内部调用方）；两者都给了且矛盾时以 Winner 为准——只有一个字段
// 能当权威，否则"胜负"会出现两个真源。
type BattleResultParams struct {
	BattleID    string
	Winner      domain.ObjectID
	AttackerWon bool
}

// BattleTimeoutParams 超时裁决参数。只带 battle_id：
// 超时时刻由到期事件决定，应用时取命令的 nowMS，不从客户端来。
type BattleTimeoutParams struct {
	BattleID string
}

// Rules 是规划器使用的服务端规则（来自配置，构造时注入）。
// 放在 application 而不是 domain，是因为 domain 必须保持纯类型与纯公式，
// 不携带"这一版运营参数是多少"的信息。
type Rules struct {
	// MarchSpeedUnitsPerSec 部队行军速度（units/秒）。第一版所有部队相同。
	MarchSpeedUnitsPerSec int64
	// CapacityUnits 部队负载上限（units）。
	CapacityUnits int64
	// CollectRateMilliPerSec 采集速率（千分之一物品/秒），必须为正。
	CollectRateMilliPerSec int64
	// MaxBattleQueuePerTarget 单个目标的战斗队列上限（文档 11.1，默认 100）。
	MaxBattleQueuePerTarget int
	// BattleTimeoutMS 战斗超时（文档 11.1，默认 30s）。
	BattleTimeoutMS int64
	// RefreshBatchSize 每批刷新对象数上限（文档 10.5，默认 20）。
	RefreshBatchSize int
	// ResourcePositionAttempts 单个资源点选址尝试次数（默认 100）。
	ResourcePositionAttempts int
	// RichBlock 富饶块刷新规则。
	RichBlock RichBlockRule
}

// Validate 校验规则自洽。启动时调用一次，失败即拒绝启动——
// 运行中才发现速率为 0 会让所有采集计划变成除零错误。
func (r Rules) Validate() error {
	if r.MarchSpeedUnitsPerSec <= 0 {
		return fmt.Errorf("%w: march_speed 必须为正, got %d", domain.ErrInvalidArgument, r.MarchSpeedUnitsPerSec)
	}
	if r.CapacityUnits <= 0 {
		return fmt.Errorf("%w: capacity_units 必须为正, got %d", domain.ErrInvalidArgument, r.CapacityUnits)
	}
	if r.CollectRateMilliPerSec <= 0 {
		return fmt.Errorf("%w: collect_rate_milli 必须为正, got %d", domain.ErrInvalidArgument, r.CollectRateMilliPerSec)
	}
	if r.MaxBattleQueuePerTarget <= 0 {
		return fmt.Errorf("%w: 战斗队列上限必须为正, got %d", domain.ErrInvalidArgument, r.MaxBattleQueuePerTarget)
	}
	if r.BattleTimeoutMS <= 0 {
		return fmt.Errorf("%w: 战斗超时必须为正, got %d", domain.ErrInvalidArgument, r.BattleTimeoutMS)
	}
	if r.RefreshBatchSize <= 0 {
		return fmt.Errorf("%w: 刷新批次必须为正, got %d", domain.ErrInvalidArgument, r.RefreshBatchSize)
	}
	if r.ResourcePositionAttempts <= 0 {
		return fmt.Errorf("%w: 选址尝试次数必须为正, got %d", domain.ErrInvalidArgument, r.ResourcePositionAttempts)
	}
	return r.RichBlock.Validate()
}

// RichBlockRule 富饶块刷新规则（文档 10.5）。
// 各档次 Minimum 之和不能超过 MaxCount —— 否则"先补最低数量"
// 这一步就不可能满足，刷新会陷入永远补不齐的状态。
type RichBlockRule struct {
	Tiers []RichTier
	// MaxCount 该富饶块最多资源点数。
	MaxCount int
}

// RichTier 一档资源：按权重抽取，先保证每档的 Minimum。
type RichTier struct {
	ResourceType int32
	ItemID       int32
	Weight       int64
	Minimum      int
	// TotalPerNode 单个资源点的总量（Total = Remaining）。
	TotalPerNode int64
}

// Validate 校验富饶块规则。
func (r RichBlockRule) Validate() error {
	if r.MaxCount <= 0 {
		return fmt.Errorf("%w: 富饶块 max_count 必须为正, got %d", domain.ErrInvalidArgument, r.MaxCount)
	}
	if len(r.Tiers) == 0 {
		return fmt.Errorf("%w: 富饶块至少要有一档资源", domain.ErrInvalidArgument)
	}
	minSum := 0
	var weightSum int64
	for i, t := range r.Tiers {
		if t.Weight <= 0 {
			return fmt.Errorf("%w: 档次 %d 权重必须为正", domain.ErrInvalidArgument, i)
		}
		if t.Minimum < 0 {
			return fmt.Errorf("%w: 档次 %d 最低数量不能为负", domain.ErrInvalidArgument, i)
		}
		if t.TotalPerNode <= 0 {
			return fmt.Errorf("%w: 档次 %d 单点总量必须为正", domain.ErrInvalidArgument, i)
		}
		minSum += t.Minimum
		weightSum += t.Weight
	}
	if minSum > r.MaxCount {
		return fmt.Errorf("%w: 各档最低数量之和 %d 超过上限 %d", domain.ErrInvalidArgument, minSum, r.MaxCount)
	}
	if weightSum <= 0 {
		return fmt.Errorf("%w: 权重总和必须为正", domain.ErrInvalidArgument)
	}
	return nil
}

// Planner 是 runtime.SceneLoop 调用的规划接口（文档 18.2）。
// 用接口而不是具体类型，是为了让 SceneLoop 的测试可以注入桩，
// 也让"重建规划器"（配置热更新）不必重建场景循环。
type Planner interface {
	// Plan 根据已提交快照与命令生成计划。返回的 error 表示
	// 「规划阶段拒绝」，必须带业务码语义（domain.CodeOf 可解析），
	// 由调用方转成稳定回执；不允许返回 (nil, nil)。
	Plan(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error)
}
