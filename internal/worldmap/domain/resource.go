package domain

import "fmt"

// CollectStatus 采集会话状态。
type CollectStatus int32

const (
	// CollectStatusUnspecified 零值非法。
	CollectStatusUnspecified CollectStatus = 0
	// CollectStatusActive 进行中。
	CollectStatusActive CollectStatus = 1
	// CollectStatusSettled 已结算。终态；重复结算返回原结果（幂等）。
	CollectStatusSettled CollectStatus = 2
)

func (s CollectStatus) Valid() bool { return s == CollectStatusActive || s == CollectStatusSettled }

// BattleStatus 战斗状态。文档 6.1：PENDING→RESOLVED/TIMED_OUT 单向，
// 迟到结果不可覆盖超时裁决（INV-06）。
type BattleStatus int32

const (
	BattleStatusUnspecified BattleStatus = 0
	// BattleStatusPending 等待裁决。
	BattleStatusPending BattleStatus = 1
	// BattleStatusResolved 已按 Resolver 结果裁决。
	BattleStatusResolved BattleStatus = 2
	// BattleStatusTimedOut 超时裁决（攻击无效、防守保留、无战损）。
	BattleStatusTimedOut BattleStatus = 3
)

func (s BattleStatus) Valid() bool {
	return s == BattleStatusPending || s == BattleStatusResolved || s == BattleStatusTimedOut
}

// Terminal 报告是否终态。
func (s BattleStatus) Terminal() bool { return s == BattleStatusResolved || s == BattleStatusTimedOut }

// ResourceNode 是资源点（文档第 6 章）。关键不变量：
// Total == Remaining + 已结算产量 + 刷新销毁审计余量（INV-03 守恒），
// Remaining 永不小于 0；同一时刻最多一个活动采集会话。
type ResourceNode struct {
	Header            ObjectHeader
	Position          Pos
	RichBlockID       int64
	ResourceType      int32
	ItemID            int32
	Total             int64
	Remaining         int64
	ActiveCollectID   string
	RefreshGeneration int64
	PendingDestroy    bool
	ConfigVersion     string
}

// City 是城池/主堡（文档第 6 章）。第一版主堡不可迁移和摧毁；
// GuildID 只是镜像，权限判断必须使用服务端更新的关系版本。
type City struct {
	Header        ObjectHeader
	PlayerID      int64
	GuildID       int64
	Position      Pos
	ConfigVersion string
}

// CollectSession 是活动采集会话（文档第 6、10 章）。
// 采集进度以 Session 为真源，属于资源点一侧；Troop 只持有已结算 Cargo，
// 不维护独立计数——两套计数必然冲突。
type CollectSession struct {
	ID                   string
	TroopID              ObjectID
	ResourceID           ObjectID
	StartAtMS            int64
	EndAtMS              int64
	RateMilliItemsPerSec int64
	PlannedQty           int64
	RuleVersion          string
	Status               CollectStatus
	// SettledQty 仅 SETTLED 后有效，记录最终结算数量（重复查询返回它）。
	SettledQty int64
}

// CollectPlan 是“开始采集”的规划输入（纯计算，文档 10.2）。
// 调用方提供当前资源余量、部队剩余负载与速率，本结构保存推导结果。
type CollectPlan struct {
	PlannedQty int64
	EndAtMS    int64
}

// PlanCollect 计算采集计划。文档 10.2 公式：
//
//	capacity_left = capacity_units - Σ(cargo_qty * weight)
//	load_qty      = floor(capacity_left / target_item_weight)
//	planned_qty   = min(resource_remaining, load_qty)
//	need_ms       = ceil(planned_qty * 1_000_000 / rate_milli)
//	end_at_ms     = start_at_ms + need_ms
//
// 返回错误而非截断：速率/重量为 0、负载为负都是配置或状态错误，
//
//	silently 截断会掩盖数据问题（文档 21.2 禁止“先加内存再补写”同类捷径）。
func PlanCollect(capacityUnits int64, cargoUnits int64, resourceRemaining int64, rateMilli int64, startAtMS int64) (CollectPlan, error) {
	if capacityUnits <= 0 {
		return CollectPlan{}, fmt.Errorf("%w: 负载上限必须为正, got %d", ErrInvalidArgument, capacityUnits)
	}
	if rateMilli <= 0 {
		return CollectPlan{}, fmt.Errorf("%w: 采集速率必须为正, got %d", ErrInvalidArgument, rateMilli)
	}
	if resourceRemaining < 0 {
		return CollectPlan{}, fmt.Errorf("%w: 资源余量不能为负, got %d", ErrInvalidArgument, resourceRemaining)
	}
	capacityLeft := capacityUnits - cargoUnits
	if capacityLeft < 0 {
		return CollectPlan{}, fmt.Errorf("%w: 部队负载 %d 超过上限 %d", ErrCapacityExhausted, cargoUnits, capacityUnits)
	}
	// 第一版所有资源点产出单一物品，weight=1（units 即物品数）。
	// 多物品资源点是扩展点：weight 表届时从配置注入。
	const targetItemWeight int64 = 1
	loadQty, err := FloorDiv(capacityLeft, targetItemWeight)
	if err != nil {
		return CollectPlan{}, err
	}
	planned := loadQty
	if resourceRemaining < planned {
		planned = resourceRemaining
	}
	if planned <= 0 {
		return CollectPlan{}, fmt.Errorf("%w: 无可采集数量(load=%d, remaining=%d)", ErrCapacityExhausted, loadQty, resourceRemaining)
	}
	needMS, err := CeilDiv(planned*1_000_000, rateMilli)
	if err != nil {
		return CollectPlan{}, err
	}
	return CollectPlan{PlannedQty: planned, EndAtMS: startAtMS + needMS}, nil
}

// SettleCollect 计算结算数量。文档 10.2：
//
//	elapsed  = clamp(settle_at - start, 0, end - start)
//	produced = floor(rate_milli * elapsed / 1_000_000)
//	qty      = min(planned, remaining, load, produced)
//
// 自然结束（elapsed == duration）时直接以 planned 结算，避免整数离散误差；
// 提前召回按 floor，重复相同 settle_at 始终得到相同结果（幂等）。
func SettleCollect(sess *CollectSession, settleAtMS, resourceRemaining, capacityLeft int64) (int64, error) {
	if sess == nil {
		return 0, fmt.Errorf("%w: 会话不能为空", ErrInvalidArgument)
	}
	if sess.RateMilliItemsPerSec <= 0 {
		return 0, fmt.Errorf("%w: 会话速率非法: %d", ErrInvalidArgument, sess.RateMilliItemsPerSec)
	}
	if sess.Status != CollectStatusActive {
		return 0, fmt.Errorf("%w: 会话 %s 已结算", ErrIdempotencyConflict, sess.ID)
	}
	duration := sess.EndAtMS - sess.StartAtMS
	if duration < 0 {
		return 0, fmt.Errorf("%w: 会话 %s 时间窗非法", ErrInvalidArgument, sess.ID)
	}
	elapsed := ClampDurationMS(settleAtMS-sess.StartAtMS, duration)
	produced, err := SafeMulDiv(sess.RateMilliItemsPerSec, elapsed, 1_000_000)
	if err != nil {
		return 0, err
	}
	qty := sess.PlannedQty
	if resourceRemaining < qty {
		qty = resourceRemaining
	}
	if capacityLeft < qty {
		qty = capacityLeft
	}
	if produced < qty {
		qty = produced
	}
	if qty < 0 {
		return 0, fmt.Errorf("%w: 结算数量为负", ErrOverflow)
	}
	return qty, nil
}
