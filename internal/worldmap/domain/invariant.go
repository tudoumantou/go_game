package domain

import "fmt"

// invariant.go 实现领域不变量校验（文档 6.1/10.3/5.3，
// 对应 18.1 目录里的 invariant.go：类型校验、合法状态
// 转换）。这些函数在两条路径上被调用：规划前（拒绝会把
// 系统拖入非法组合的命令）与恢复加载后（拒绝带着损坏
// 状态启动，世界转 NotReady）。
//
// 统一约定：
//   - 参数/结构缺失用 ErrInvalidArgument（格式类，不写回执；
//     CodeOf 把它映射到 CodeInvalidPosition 44001，占位
//     越界也归入此类，客户端处理一致）；
//   - 字段组合非法用 ErrInvalidState（状态机组合错误）；
//   - 全部哨兵错误经 %w 包装携带上下文，调用方用
//     errors.Is 判定，不做字符串匹配。
//
// 校验只读入参，不修改任何对象——加载校验要能对同一份
// 数据反复运行且结果一致。

// OccupancyLayer 占位层（文档 5.3）。
//
// 第一版只有静态层：静态地形、主堡、资源点互斥，都落在
// 同一层，靠 map_occupancy 唯一键
// (world_id, cell_x, cell_y, layer) 保证；部队移动不占
// 静态格，因此没有部队层。将来支持多层格、联盟建筑或
// 跨边界大建筑时向后追加层号，不重排已持久化的层值。
const (
	// OccupancyLayerUnspecified 零值非法。
	OccupancyLayerUnspecified int32 = 0
	// OccupancyLayerStatic 静态对象层（地形/主堡/资源点）。
	OccupancyLayerStatic int32 = 1
)

// ValidateTroop 校验部队的合法状态组合（文档 6.1）。
//
// hasActiveSession 由调用方传入“该部队当前是否存在活动
// CollectSession”。为什么是参数而不是字段：Troop 不持有
// 会话指针——采集进度以 CollectSession 为真源且属于资源
// 点一侧（10.1），部队只持有已结算 Cargo，两套计数必然
// 冲突。domain 无法从 Troop 自身回答“有没有会话”，只能
// 由掌握资源侧状态的调用方（资源 OwnerScene/加载器）回答。
//
// 检查的组合：
//   - Phase/BusyReason 枚举合法（6.1 必需枚举）；
//   - COLLECTING 必须存在活动会话，且不得携带 battle_id
//     （被攻击时先结算会话再进战斗，10.4，采集中与
//     战斗中互斥）；
//   - FIGHTING 必须 battle_id 非空且 Busy=Battle
//     （troop.go：二者成对出现）；
//   - FIGHTING 不可能同时 RETURNING：Phase 是单值枚举，
//     一个字段不可能既是 4 又是 5——这一点由类型系统保证，
//     此处显式说明意图；若将来改成位组合状态必须重新评估；
//   - SETTLING 必须 settlement_id 非空（回城交接单号，
//     SETTLING 仍占用槽位防复用，INV-05）；
//   - 反向约束：非 FIGHTING 阶段不得携带 battle_id 或
//     Busy=Battle——终态/回城中的部队挂着战斗态会让
//     恢复逻辑误判还有未解析战斗（19.6）；
//   - Cargo 无负值：数量守恒不允许负库存（INV-03 同类）。
func ValidateTroop(t *Troop, hasActiveSession bool) error {
	if t == nil {
		return fmt.Errorf("%w: 部队不能为空", ErrInvalidArgument)
	}
	if t.Header.Kind != ObjectKindTroop {
		return fmt.Errorf("%w: 对象 %s 种类 %s 不是部队", ErrInvalidState, t.Header.Key(), t.Header.Kind)
	}
	if !t.Phase.Valid() {
		return fmt.Errorf("%w: 部队 %s 阶段非法: %d", ErrInvalidState, t.Header.Key(), int32(t.Phase))
	}
	if !t.Busy.Reason.Valid() {
		return fmt.Errorf("%w: 部队 %s Busy 原因非法: %d", ErrInvalidState, t.Header.Key(), int32(t.Busy.Reason))
	}
	// Cargo 无负值。map 遍历顺序随机不影响结论：只要存在
	// 一个负值就拒绝，报哪一个由遍历决定，但错误稳定可判定。
	for itemID, qty := range t.Cargo {
		if qty < 0 {
			return fmt.Errorf("%w: 部队 %s 物品 %d 数量为负: %d",
				ErrInvalidState, t.Header.Key(), itemID, qty)
		}
	}

	switch t.Phase {
	case TroopPhaseCollecting:
		if !hasActiveSession {
			return fmt.Errorf("%w: 部队 %s COLLECTING 但无活动采集会话", ErrInvalidState, t.Header.Key())
		}
		if t.BattleID != "" {
			return fmt.Errorf("%w: 部队 %s COLLECTING 不应携带 battle_id %s",
				ErrInvalidState, t.Header.Key(), t.BattleID)
		}
	case TroopPhaseFighting:
		if t.BattleID == "" {
			return fmt.Errorf("%w: 部队 %s FIGHTING 缺少 battle_id", ErrInvalidState, t.Header.Key())
		}
		// battle_id 与 Busy=Battle 成对：只设其一说明状态
		// 机被外部直接改过字段，而不是走转换进来的。
		if t.Busy.Reason != BusyReasonBattle {
			return fmt.Errorf("%w: 部队 %s FIGHTING 但 Busy=%d（应为 BATTLE）",
				ErrInvalidState, t.Header.Key(), int32(t.Busy.Reason))
		}
	case TroopPhaseSettling:
		if t.SettlementID == "" {
			return fmt.Errorf("%w: 部队 %s SETTLING 缺少 settlement_id", ErrInvalidState, t.Header.Key())
		}
	}

	// 反向约束：战斗态只允许出现在 FIGHTING。
	if t.Phase != TroopPhaseFighting {
		if t.BattleID != "" {
			return fmt.Errorf("%w: 部队 %s 阶段 %d 携带 battle_id %s",
				ErrInvalidState, t.Header.Key(), int32(t.Phase), t.BattleID)
		}
		if t.Busy.Reason == BusyReasonBattle {
			return fmt.Errorf("%w: 部队 %s 阶段 %d 但 Busy=BATTLE",
				ErrInvalidState, t.Header.Key(), int32(t.Phase))
		}
	}
	return nil
}

// ValidateResource 校验资源点不变量（文档 10.3/INV-03）：
//
//   - Remaining ∈ [0, Total]：余量永不小于 0，且不超过
//     总量——Total == Remaining + 已结算产量 + 刷新销毁
//     审计余量（INV-03 守恒），Remaining > Total 说明
//     扣减与审计对不上；
//   - 有活动会话的资源不允许被标记 pending_destroy：
//     活跃采集/战斗目标不参与普通刷新删除（10.3）。
//     刷新流程必须跳过它们，否则会出现“采集中的资源点
//     被刷新删掉”的奖励丢失；
//   - 资源正在被攻击时的“战斗目标”不落在 ResourceNode
//     上（战斗挂在防守目标侧，见 11.1），本校验覆盖
//     采集这一半；战斗那一半由 battle/queue 记录保证。
func ValidateResource(r *ResourceNode) error {
	if r == nil {
		return fmt.Errorf("%w: 资源点不能为空", ErrInvalidArgument)
	}
	if r.Header.Kind != ObjectKindResource {
		return fmt.Errorf("%w: 对象 %s 种类 %s 不是资源点", ErrInvalidState, r.Header.Key(), r.Header.Kind)
	}
	if r.Total < 0 {
		return fmt.Errorf("%w: 资源点 %s 总量为负: %d", ErrInvalidState, r.Header.Key(), r.Total)
	}
	if r.Remaining < 0 {
		return fmt.Errorf("%w: 资源点 %s 余量为负: %d", ErrInvalidState, r.Header.Key(), r.Remaining)
	}
	if r.Remaining > r.Total {
		return fmt.Errorf("%w: 资源点 %s 余量 %d 超过总量 %d",
			ErrInvalidState, r.Header.Key(), r.Remaining, r.Total)
	}
	if r.ActiveCollectID != "" && r.PendingDestroy {
		return fmt.Errorf("%w: 资源点 %s 有活动会话 %s 不允许标记 pending_destroy",
			ErrInvalidState, r.Header.Key(), r.ActiveCollectID)
	}
	return nil
}

// ValidateOccupancyClaim 校验静态占位声明（文档 5.3）。
//
// 最终约束是 map_occupancy 唯一键
// (world, cell_x, cell_y, layer)：两个创建同时争格只有
// 一个事务能成功，另一个返回 PositionOccupied，不能先
// 广播再撤回。本函数是内存预检的领域侧部分——layer
// 合法、cell 在拓扑内、对象可识别；把明显非法的声明
// 挡在事务外，减少无谓的冲突回滚。
//
// topo 传入而不是包级常量：世界尺寸是拓扑级配置（5.1），
// 测试可以用小世界构造越界用例，生产按真实拓扑校验。
func ValidateOccupancyClaim(c OccupancyClaim, topo Topology) error {
	// 拓扑非法时“cell 是否在界内”没有意义，先拒绝拓扑。
	if err := topo.Validate(); err != nil {
		return err
	}
	if c.Layer != OccupancyLayerStatic {
		return fmt.Errorf("%w: 占位层非法: %d（第一版仅支持静态层 %d）",
			ErrInvalidArgument, c.Layer, OccupancyLayerStatic)
	}
	// 左闭右开：x == WidthCells 已越界。InBoundsCells 先拒
	// 负数再比较——Go 的整数除法向零取整，负数格坐标会
	// 算出负的场景号，不能依赖除法结果兜底。
	if !topo.InBoundsCells(c.CellX, c.CellY) {
		// 用 ErrInvalidArgument 而不是“位置非法”类错误：
		// errors.go 的哨兵集合里没有 ErrInvalidPosition，
		// 而 CodeOf 把 ErrInvalidArgument 映射到
		// CodeInvalidPosition(44001)，客户端处理一致。
		return fmt.Errorf("%w: 占位格 (%d,%d) 不在世界 %dx%d 内",
			ErrInvalidArgument, c.CellX, c.CellY, topo.WidthCells, topo.HeightCells)
	}
	if c.ObjectKey.WorldID == 0 || c.ObjectKey.ID == 0 {
		return fmt.Errorf("%w: 占位声明缺少对象标识: %s", ErrInvalidArgument, c.ObjectKey)
	}
	return nil
}
