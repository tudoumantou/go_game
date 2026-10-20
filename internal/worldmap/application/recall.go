package application

// recall.go 实现召回（文档 9.4 表 + 11.5 业务锁处理表）。
//
// 召回是“按当前阶段分桶”的命令：同一个请求在 MARCHING/COLLECTING/
// QUEUED/FIGHTING/RETURNING/SETTLING 下的语义完全不同，因此用
// switch troop.Phase 分派，每个分支各自返回一个完整计划。
//
// 统一原则：
//   - 任何会改变持久状态的计划都必须把 Troop.Header.Version +1，
//     并写 ExpectedObjectVersions（乐观并发）；
//   - 幂等分支（RETURNING、已存在同 kind Deferred）不推进版本，
//     也不写 ObjectsUpsert —— 重复请求不能制造版本抖动；
//   - 采集结算的资源扣减与 Cargo 增加必须在**同一个计划**里（INV-04）。

import (
	"fmt"

	"game_study/internal/worldmap/domain"
)

// PlanRecall 处理玩家/服务端召回。
func (p *DefaultPlanner) PlanRecall(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	troop, ok := snap.Troop(cmd.Recall.TroopID)
	if !ok {
		return nil, fmt.Errorf("%w: 部队 %d 不存在", domain.ErrObjectNotFound, cmd.Recall.TroopID)
	}
	if troop.PlayerID != env.PlayerID {
		return nil, fmt.Errorf("%w: 部队 %d 属于玩家 %d，与请求玩家 %d 不符",
			domain.ErrNotOwner, troop.Header.ID, troop.PlayerID, env.PlayerID)
	}
	// 乐观并发：ExpectedVersion 非 0 时必须与当前版本一致。玩家看到的是
	// 旧界面时不能“按当前状态猜意图”（比如界面显示的行军中、实际已到
	// 目标开始采集，两者召回语义不同）。
	if v := cmd.Recall.ExpectedVersion; v != 0 && v != troop.Header.Version {
		return nil, fmt.Errorf("%w: 部队 %d 期望版本 %d，当前 %d",
			domain.ErrStaleVersion, troop.Header.ID, v, troop.Header.Version)
	}

	// Busy 与 Phase 正交（troop.go）。先判 Busy，再判 Phase：
	// TRANSFER/DEFEND_TRANSITION/RETURN_TRANSITION 下无论 Phase 是什么
	// 都不能规划回城，必须先等对应持久事实完成（11.5）。
	switch troop.Busy.Reason {
	case domain.BusyReasonNone:
		// 继续按 Phase 分派。
	case domain.BusyReasonBattle:
		if troop.Phase != domain.TroopPhaseFighting {
			return nil, fmt.Errorf("%w: 部队 %d Busy=BATTLE 但阶段是 %s（状态不一致）",
				domain.ErrTroopBusy, troop.Header.ID, troopPhaseName(troop.Phase))
		}
	case domain.BusyReasonTransfer:
		// 移交中的部队所有权可能正在切换，此时规划回城会写到错误的
		// 场景。返回 Pending，让客户端稍后重试（文档 9.4）。
		return nil, fmt.Errorf("%w: 部队 %d 正在移交（op_id=%s），请稍后重试",
			domain.ErrOperationPending, troop.Header.ID, troop.Busy.OpID)
	default:
		return nil, fmt.Errorf("%w: 部队 %d Busy=%d，暂不允许召回",
			domain.ErrTroopBusy, troop.Header.ID, int32(troop.Busy.Reason))
	}

	switch troop.Phase {
	case domain.TroopPhaseMarching:
		return p.recallMarching(snap, env, troop, nowMS)
	case domain.TroopPhaseCollecting:
		return p.recallCollecting(snap, env, troop, nowMS)
	case domain.TroopPhaseQueued:
		return p.recallQueued(snap, env, troop, nowMS)
	case domain.TroopPhaseFighting:
		return p.recallFighting(snap, env, troop, cmd.Recall.Force, nowMS)
	case domain.TroopPhaseReturning:
		return p.recallReturning(snap, env, troop, nowMS)
	case domain.TroopPhaseSettling, domain.TroopPhaseClosed:
		return p.recallSettled(snap, env, troop, nowMS)
	default:
		return nil, fmt.Errorf("%w: 部队 %d 阶段非法: %d",
			domain.ErrInvalidState, troop.Header.ID, int32(troop.Phase))
	}
}

// recallMarching：在当前权威位置截断路线，生成回城 MoveSegment。
//
// 位置必须按 MoveSegment.PositionAt 重算，不能取 from 或 to：
// "现在在哪"只有时间推导能回答（文档 9.2）。
func (p *DefaultPlanner) recallMarching(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, nowMS int64) (*domain.MutationPlan, error) {
	pos, err := p.troopPosition(troop, nowMS)
	if err != nil {
		return nil, err
	}
	plan := newPlan(snap, env, p.ids, nowMS)
	moveID, arriveAt, err := p.appendReturnMove(snap, troop, pos, nowMS)
	if err != nil {
		return nil, err
	}
	old := p.markTroopUpdated(plan, troop, snap.SceneID())

	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":     uint64(troop.Header.ID),
		"move_id":      moveID,
		"arrive_at_ms": arriveAt,
		"from_x":       pos.X,
		"from_y":       pos.Y,
		"old_version":  old,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "TroopMoved", nil))
	return plan, nil
}

// recallCollecting：先按会话结算已采部分，再转回城。
//
// 文档 9.4 明确要求“结算和路线变更一个事务提交”，因此资源扣减、
// Cargo 增加、会话 SETTLED、Troop→RETURNING、新 Move 全部写进同一个
// MutationPlan（INV-04：不允许资源减了而 Cargo 没加）。
func (p *DefaultPlanner) recallCollecting(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, nowMS int64) (*domain.MutationPlan, error) {
	sess, ok := snap.SessionOnAnyResource(troop.Header.ID)
	if !ok {
		// COLLECTING 但没有活动会话：状态机被外部改坏，或会话已结算
		// 但部队还没转阶段。宁可报错也不凭空扣加（文档 10.3）。
		return nil, fmt.Errorf("%w: 部队 %d 处于 COLLECTING 但没有活动采集会话",
			domain.ErrInvalidState, troop.Header.ID)
	}
	res, ok := snap.Resource(sess.ResourceID)
	if !ok {
		return nil, fmt.Errorf("%w: 采集会话 %s 关联的资源点 %d 不存在",
			domain.ErrObjectNotFound, sess.ID, sess.ResourceID)
	}

	capacityLeft, err := p.capacityLeft(troop)
	if err != nil {
		return nil, err
	}
	// 提前召回按 floor 结算（SettleCollect 内部已实现），
	// 重复相同 nowMS 始终得到相同 qty。
	qty, err := domain.SettleCollect(sess, nowMS, res.Remaining, capacityLeft)
	if err != nil {
		return nil, fmt.Errorf("会话 %s 召回结算: %w", sess.ID, err)
	}

	plan := newPlan(snap, env, p.ids, nowMS)

	// 1) 资源扣减（INV-03：Remaining 永不小于 0）。
	res.Remaining -= qty
	if res.Remaining == 0 {
		// 余量为 0 打删除标；真正删除由刷新/清理流程做（文档 10.3），
		// 这里只标记，避免同一事务里既结算又物理删除导致审计断链。
		res.PendingDestroy = true
	}
	// 会话置 SETTLED 后资源不再有活动会话，必须清 active_collect_id，
	// 否则新会话永远建不起来（ValidateResource 也会拒绝 pending_destroy
	// 与 active_collect_id 并存）。
	res.ActiveCollectID = ""
	res.Header.Version++

	// 2) Cargo 增加。qty 为 0 时不写 0 值条目，避免 Cargo 里堆积
	// 一堆“数量 0”的物品（对账时全是噪音）。
	if qty > 0 {
		if troop.Cargo == nil {
			troop.Cargo = map[int32]int64{}
		}
		troop.Cargo[res.ItemID] += qty
	}

	// 3) 会话结算变更。MutationPlan 没有“会话更新”字段，SettleCollect
	// 变化本身就是 writer 更新会话状态（SETTLED + settled_qty）的依据。
	plan.CollectSettle = append(plan.CollectSettle, domain.CollectSettleChange{
		SessionID:  sess.ID,
		SettledQty: qty,
		SettleAtMS: nowMS,
	})

	// 4) 部队转回城。采集期间部队共置在资源点，回城起点就是资源点坐标
	// （不依赖 Troop.Move，采集阶段 Move 已被清空）。
	pos := res.Position
	if troop.Move != nil {
		if p2, err := troop.Move.PositionAt(nowMS); err == nil {
			pos = p2
		}
	}
	moveID, arriveAt, err := p.appendReturnMove(snap, troop, pos, nowMS)
	if err != nil {
		return nil, err
	}

	upsertResource(plan, res, snap.SceneID())
	p.markTroopUpdated(plan, troop, snap.SceneID())

	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":     uint64(troop.Header.ID),
		"session_id":   sess.ID,
		"settled_qty":  qty,
		"item_id":      int64(res.ItemID),
		"resource_id":  uint64(res.Header.ID),
		"remaining":    res.Remaining,
		"move_id":      moveID,
		"arrive_at_ms": arriveAt,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "TroopMoved", nil))
	return plan, nil
}

// recallQueued：退出目标战斗队列后回城。
//
// 【M1 契约缺口处理——必读】
// MutationPlan 没有队列字段，而“从队列删除”必须持久化，否则重启后
// 队列会把已经回城的部队又拉进战斗。M1 的替代约定是：
//
//   - 队列成员在持久层表现为 map_battle 里 Status=PENDING 且
//     AppliedOperationID=m1QueuedBattleMarker 的记录（见 arrive.go）；
//   - 队首（真正在打的战斗）是 Status=PENDING 且不带该标记的记录；
//   - 退出队列 = 把该成员的 BattleRecord 置为 TIMED_OUT。
//
// TIMED_OUT 在文档 11.3 的语义是“攻击无效、防守保留、无战损”，
// 恰好等同于“该攻击方退出”，不会误伤防守方。runtime 的内存队列
// 实现按 battle 状态重建：TIMED_OUT/RESOLVED 的记录不再是队列成员。
//
// 这是 M1 的权宜表示，有两个已知缺陷，M2 必须修：
//  1. 同一目标的多条排队记录无法用 ActiveBattleOn（快照按目标去重）
//     精确表达，只能靠 runtime 按状态过滤；
//  2. 排队没有显式 enqueue_seq，顺序只能靠 DeadlineMS + BattleID
//     近似，同一毫秒入队的顺序不严格保证。
//
// M2 增加 map_battle_queue(world_id,target_id,troop_id,enqueue_seq)
// 后，QueueChange{TargetID, Leaving} 由 writer 直接消费，不再借用
// BattleRecord。
func (p *DefaultPlanner) recallQueued(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, nowMS int64) (*domain.MutationPlan, error) {
	plan := newPlan(snap, env, p.ids, nowMS)
	targetID := troop.Target.TargetID

	// 找到目标上的 PENDING 战斗。快照按目标只保留一条“进行中”记录，
	// 因此它可能是别人正在打的战斗；M1 仍按“置 TIMED_OUT 表示本攻击方
	// 退出”处理（见上面的说明）。
	var battleID string
	if rec, ok := snap.ActiveBattleOn(targetID); ok && rec.Status == domain.BattleStatusPending {
		rec.Status = domain.BattleStatusTimedOut
		plan.BattleUpsert = append(plan.BattleUpsert, *rec)
		battleID = rec.BattleID
	}

	// 回城起点：优先用 Move 推导的位置；排队中的部队通常没有 Move
	// （到达后已清空），退化为目标坐标；目标也消失时退化为主堡坐标
	// （零长度移动段，等价于“已在城里”，由到达命令收尾）。
	pos, ok := p.targetPosition(snap, env, targetID)
	if troop.Move != nil {
		if p2, err := troop.Move.PositionAt(nowMS); err == nil {
			pos, ok = p2, true
		}
	}
	if !ok {
		home, homeOK := snap.City(troop.HomeCityID)
		if !homeOK {
			return nil, fmt.Errorf("%w: 部队 %d 的主堡 %d 不存在",
				domain.ErrObjectNotFound, troop.Header.ID, troop.HomeCityID)
		}
		pos = home.Position
	}

	moveID, arriveAt, err := p.appendReturnMove(snap, troop, pos, nowMS)
	if err != nil {
		return nil, err
	}
	troop.Target = domain.TargetRef{} // 退出队列后不再指向目标
	troop.BattleID = ""
	p.markTroopUpdated(plan, troop, snap.SceneID())

	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":     uint64(troop.Header.ID),
		"battle_id":    battleID,
		"queued_left":  true,
		"move_id":      moveID,
		"arrive_at_ms": arriveAt,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "TroopMoved", nil))
	return plan, nil
}

// recallFighting：不强制清锁，只保存一个延迟动作（文档 9.4/11.5）。
//
// 为什么不能直接改 Phase：战斗正在别的执行单元里裁决，硬改 Phase 会
// 让战斗结果回来时找不到攻守双方，或者把结果应用到已经回城的部队上。
// 正确做法是写 Deferred，由战斗终态命令在同一个事务里执行召回。
func (p *DefaultPlanner) recallFighting(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, force bool, nowMS int64) (*domain.MutationPlan, error) {
	kind := domain.DeferredKindRecall
	if force {
		kind = domain.DeferredKindForceReturn
	}
	plan := newPlan(snap, env, p.ids, nowMS)

	if troop.Deferred != nil {
		switch {
		case troop.Deferred.Kind == kind:
			// 同 Kind 幂等：不叠加、不推进版本。客户端重复点击召回
			// 应该拿到同样的 AcceptedDeferred，而不是两个延迟动作。
			setReceipt(plan, domain.CodeOK, map[string]any{
				"accepted":      true,
				"deferred":      true,
				"idempotent":    true,
				"troop_id":      uint64(troop.Header.ID),
				"battle_id":     troop.BattleID,
				"deferred_kind": deferredKindName(troop.Deferred.Kind),
			}, nowMS)
			return plan, nil
		case kind == domain.DeferredKindForceReturn:
			// 优先级 ForceReturn > Recall：服务端强制召回顶替玩家召回。
			// 这是**升级**，要推进版本让 writer 覆盖旧延迟动作。
			troop.Deferred = &domain.DeferredAction{Kind: kind, RequestID: env.RequestID}
			p.markTroopUpdated(plan, troop, snap.SceneID())
		default:
			// 现有是 ForceReturn，新请求是玩家 Recall：保留强制召回，
			// 回执仍报 deferred，但不改任何状态。
			setReceipt(plan, domain.CodeOK, map[string]any{
				"accepted":      true,
				"deferred":      true,
				"kept":          deferredKindName(troop.Deferred.Kind),
				"troop_id":      uint64(troop.Header.ID),
				"battle_id":     troop.BattleID,
				"deferred_kind": deferredKindName(troop.Deferred.Kind),
			}, nowMS)
			return plan, nil
		}
	} else {
		troop.Deferred = &domain.DeferredAction{Kind: kind, RequestID: env.RequestID}
		p.markTroopUpdated(plan, troop, snap.SceneID())
	}

	// Phase 与 Busy 一律不动：FIGHTING 的战斗锁只能由战斗终态解除
	// （troop.go 的 BusyState 注释：不因超时一律清零）。
	setReceipt(plan, domain.CodeOK, map[string]any{
		"accepted":      true,
		"deferred":      true,
		"troop_id":      uint64(troop.Header.ID),
		"battle_id":     troop.BattleID,
		"deferred_kind": deferredKindName(troop.Deferred.Kind),
	}, nowMS)
	return plan, nil
}

// recallReturning：幂等返回当前回城路线，不重复创建 Move、不推进版本。
func (p *DefaultPlanner) recallReturning(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, nowMS int64) (*domain.MutationPlan, error) {
	if troop.Move == nil {
		return nil, fmt.Errorf("%w: 部队 %d 处于 RETURNING 但没有移动段",
			domain.ErrInvalidState, troop.Header.ID)
	}
	plan := newPlan(snap, env, p.ids, nowMS)
	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":     uint64(troop.Header.ID),
		"move_id":      troop.Move.MoveID,
		"arrive_at_ms": troop.Move.EndAtMS,
		"idempotent":   true,
	}, nowMS)
	return plan, nil
}

// recallSettled：SETTLING/CLOSED 返回既有交接状态，不做任何修改。
func (p *DefaultPlanner) recallSettled(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, nowMS int64) (*domain.MutationPlan, error) {
	plan := newPlan(snap, env, p.ids, nowMS)
	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":      uint64(troop.Header.ID),
		"settlement_id": troop.SettlementID,
		"phase":         troopPhaseName(troop.Phase),
		"idempotent":    true,
	}, nowMS)
	return plan, nil
}

// appendReturnMove 生成“从 from 回主堡”的移动段，并把 Phase 置 RETURNING。
//
// 【MVP 简化】回城路径**不做阻挡检测**。风险：部队可能直线穿过后来
// 出现的静态建筑（例如玩家在出兵后建了墙），表现为穿墙。第一版接受
// 这个偏差，因为回城是系统强制动作，拒绝回城会让部队永久卡在目标点，
// 比穿墙更难恢复。M2 应改为复用出征时保存的原路（PathVersion 相同
// 时反向走原路），只有原路失效才重新寻路。
func (p *DefaultPlanner) appendReturnMove(snap *SceneSnapshot, troop *domain.Troop, from domain.Pos, nowMS int64) (moveID string, arriveAtMS int64, err error) {
	home, ok := snap.City(troop.HomeCityID)
	if !ok {
		return "", 0, fmt.Errorf("%w: 部队 %d 的主堡 %d 不存在",
			domain.ErrObjectNotFound, troop.Header.ID, troop.HomeCityID)
	}
	moveID = p.ids()
	// 速度同样只来自规则；intent 传 Unspecified 让 PlanMarchTo 置 RETURNING。
	seg, err := p.PlanMarchTo(troop, from, home.Position, p.rules.MarchSpeedUnitsPerSec, nowMS, moveID, domain.MarchIntentUnspecified)
	if err != nil {
		return "", 0, err
	}
	return moveID, seg.EndAtMS, nil
}

// markTroopUpdated 统一处理“更新已有部队”的三件事：推进版本、
// 记录期望版本、放进 ObjectsUpsert。返回推进前的版本，供回执对账。
//
// 为什么集中到一个函数：漏掉 ExpectedObjectVersions 只会让 CAS 失效
// （并发写悄悄覆盖），不会编译报错，属于最难查的一类 bug。
func (p *DefaultPlanner) markTroopUpdated(plan *domain.MutationPlan, troop *domain.Troop, scene domain.SceneID) uint64 {
	old := troop.Header.Version
	troop.Header.Version = old + 1
	expectVersion(plan, troop.Header.Key(), old)
	upsertTroop(plan, troop, scene)
	return old
}
