package application

// collect.go 实现开始采集与采集结算（文档 10.1/10.2/10.3）。
//
// 两个命令共享一条铁律：**资源扣减与 Cargo 增加必须同生共死**（INV-04）。
// 允许出现“资源减了 Cargo 没加”或反之的中间态，等价于凭空创造/销毁物品。
// 因此两处都把两边的写入放进同一个 MutationPlan，由 TxWorker 一个事务提交。

import (
	"fmt"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/spatial"
)

// PlanCollectStart 开始采集（内部命令，到达仲裁后触发）。
func (p *DefaultPlanner) PlanCollectStart(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	troop, ok := snap.Troop(cmd.CollectStart.TroopID)
	if !ok {
		return nil, fmt.Errorf("%w: 部队 %d 不存在", domain.ErrObjectNotFound, cmd.CollectStart.TroopID)
	}
	if troop.PlayerID != env.PlayerID {
		return nil, fmt.Errorf("%w: 部队 %d 属于玩家 %d，与请求玩家 %d 不符",
			domain.ErrNotOwner, troop.Header.ID, troop.PlayerID, env.PlayerID)
	}
	res, ok := snap.Resource(cmd.CollectStart.ResourceID)
	if !ok {
		return nil, fmt.Errorf("%w: 资源点 %d 不存在", domain.ErrObjectNotFound, cmd.CollectStart.ResourceID)
	}

	plan := newPlan(snap, env, p.ids, nowMS)
	sess, err := p.applyCollectStart(plan, snap, env, troop, res, nowMS)
	if err != nil {
		return nil, err
	}
	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":    uint64(troop.Header.ID),
		"resource_id": uint64(res.Header.ID),
		"session_id":  sess.ID,
		"planned_qty": sess.PlannedQty,
		"end_at_ms":   sess.EndAtMS,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "CollectStarted", nil))
	return plan, nil
}

// applyCollectStart 把“建会话 + 资源占位 + 部队转 COLLECTING”写进 plan。
// 到达仲裁与独立的 CollectStart 命令共用它，避免两条路径各写一套规则
// 后逐渐漂移（例如一边校验共置、一边忘校验）。
func (p *DefaultPlanner) applyCollectStart(plan *domain.MutationPlan, snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, res *domain.ResourceNode, nowMS int64) (*domain.CollectSession, error) {
	switch troop.Phase {
	case domain.TroopPhaseFighting, domain.TroopPhaseQueued,
		domain.TroopPhaseSettling, domain.TroopPhaseClosed:
		return nil, fmt.Errorf("%w: 部队 %d 阶段 %s 不能开始采集",
			domain.ErrInvalidState, troop.Header.ID, troopPhaseName(troop.Phase))
	}

	// 共置校验：第一版只要求“相距不超过 1 格”（文档 9.3：资源目标无移动，
	// 只需同格到达）。用 SegmentLengthUnits 而不是 manhattan 距离：
	// 斜向贴格也是 1 格，曼哈顿会算成 2。
	//
	// Move 为 nil 表示部队已经停在目标点（到达仲裁建会话时会清空 Move），
	// 此时直接用资源点坐标，距离为 0。
	from := res.Position
	if troop.Move != nil {
		pos, err := p.troopPosition(troop, nowMS)
		if err != nil {
			return nil, err
		}
		from = pos
	}
	dist, err := spatial.SegmentLengthUnits(from, res.Position)
	if err != nil {
		return nil, fmt.Errorf("部队 %d 与资源点 %d 距离计算失败: %w", troop.Header.ID, res.Header.ID, err)
	}
	if dist > p.topo.PositionUnitsCell {
		return nil, fmt.Errorf("%w: 部队 %d 与资源点 %d 相距 %d units（>%d，未共置）",
			domain.ErrInvalidArgument, troop.Header.ID, res.Header.ID, dist, p.topo.PositionUnitsCell)
	}

	// 每资源最多一个活动会话（10.1）。资源占用不是“先检查空闲再异步占用”，
	// 而是在同一事务里写 active_collect_id；这里的内存检查只是预检。
	if sess, ok := snap.ActiveSessionOn(res.Header.ID); ok && sess.Status == domain.CollectStatusActive {
		return nil, fmt.Errorf("%w: 资源点 %d 已被会话 %s（部队 %d）占用",
			domain.ErrTargetOccupied, res.Header.ID, sess.ID, sess.TroopID)
	}
	if res.Remaining <= 0 {
		return nil, fmt.Errorf("%w: 资源点 %d 已枯竭", domain.ErrCapacityExhausted, res.Header.ID)
	}

	cargo, err := p.cargoUnits(troop)
	if err != nil {
		return nil, err
	}
	// PlanCollect 内部完成 capacity_left/load_qty/planned_qty/need_ms
	// 的整数推导（文档 10.2），规划器不重复实现公式。
	cp, err := domain.PlanCollect(p.rules.CapacityUnits, cargo, res.Remaining, p.rules.CollectRateMilliPerSec, nowMS)
	if err != nil {
		return nil, fmt.Errorf("部队 %d 采集计划: %w", troop.Header.ID, err)
	}

	sess := domain.CollectSession{
		ID:                   p.ids(),
		TroopID:              troop.Header.ID,
		ResourceID:           res.Header.ID,
		StartAtMS:            nowMS,
		EndAtMS:              cp.EndAtMS,
		RateMilliItemsPerSec: p.rules.CollectRateMilliPerSec,
		PlannedQty:           cp.PlannedQty,
		RuleVersion:          m1RuleVersion,
		Status:               domain.CollectStatusActive,
	}
	res.ActiveCollectID = sess.ID
	res.Header.Version++

	// 采集进度以 Session 为真源，部队不带独立计数（10.1）；
	// 转 COLLECTING 后 Move 清空，避免旧的到达任务再次触发仲裁。
	troop.Phase = domain.TroopPhaseCollecting
	troop.Move = nil

	plan.CollectStart = append(plan.CollectStart, sess)
	// 资源点版本也推进：它从“无主”变成“有活动会话”。
	expectVersion(plan, res.Header.Key(), res.Header.Version-1)
	upsertResource(plan, res, snap.SceneID())
	p.markTroopUpdated(plan, troop, snap.SceneID())
	return &sess, nil
}

// PlanCollectSettle 采集结算（自然结束或召回触发）。
//
// 与召回的区别：召回把“结算 + 回城”组合在一条命令里；本命令**只结算**，
// 不改变 Troop.Phase。Troop 从 COLLECTING 转出由后续的召回/回城命令负责。
// 这会产生一个短暂的中间态（COLLECTING 且无活动会话），因此 writer 在
// 应用本命令时不应以 hasActiveSession=false 跑 ValidateTroop——那里要
// 用 true（本事务刚把会话结算掉，语义上仍是同一采集流程）。
func (p *DefaultPlanner) PlanCollectSettle(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	sess, ok := snap.SessionByID(cmd.CollectSettle.SessionID)
	if !ok {
		return nil, fmt.Errorf("%w: 采集会话 %s 不存在", domain.ErrObjectNotFound, cmd.CollectSettle.SessionID)
	}

	// 已结算 → 幂等返回首次数量。绝不能重新按当前时间算一遍：
	// 那样重复结算会凭空多产（文档 10.2 最后一句）。
	if sess.Status == domain.CollectStatusSettled {
		plan := newPlan(snap, env, p.ids, nowMS)
		setReceipt(plan, domain.CodeOK, map[string]any{
			"idempotent":  true,
			"session_id":  sess.ID,
			"settled_qty": sess.SettledQty,
			"resource_id": uint64(sess.ResourceID),
		}, nowMS)
		return plan, nil
	}
	if sess.Status != domain.CollectStatusActive {
		return nil, fmt.Errorf("%w: 会话 %s 状态非法: %d",
			domain.ErrInvalidState, sess.ID, int32(sess.Status))
	}

	troop, ok := snap.Troop(sess.TroopID)
	if !ok {
		return nil, fmt.Errorf("%w: 会话 %s 关联的部队 %d 不存在",
			domain.ErrObjectNotFound, sess.ID, sess.TroopID)
	}
	res, ok := snap.Resource(sess.ResourceID)
	if !ok {
		return nil, fmt.Errorf("%w: 会话 %s 关联的资源点 %d 不存在",
			domain.ErrObjectNotFound, sess.ID, sess.ResourceID)
	}

	// SettleAtMS 由命令生成时固化（重试不重新取 now）。小于等于 0 视为
	// 调用方没填，退化为命令的 nowMS（仅内部命令允许）。
	settleAt := cmd.CollectSettle.SettleAtMS
	if settleAt <= 0 {
		settleAt = nowMS
	}

	capacityLeft, err := p.capacityLeft(troop)
	if err != nil {
		return nil, err
	}
	// 提前结算按 floor（SettleCollect 内部），避免反复中断套利。
	qty, err := domain.SettleCollect(sess, settleAt, res.Remaining, capacityLeft)
	if err != nil {
		return nil, fmt.Errorf("会话 %s 结算: %w", sess.ID, err)
	}

	plan := newPlan(snap, env, p.ids, nowMS)

	res.Remaining -= qty
	if res.Remaining == 0 {
		res.PendingDestroy = true
	}
	res.ActiveCollectID = ""
	res.Header.Version++

	if qty > 0 {
		if troop.Cargo == nil {
			troop.Cargo = map[int32]int64{}
		}
		troop.Cargo[res.ItemID] += qty
	}

	plan.CollectSettle = append(plan.CollectSettle, domain.CollectSettleChange{
		SessionID:  sess.ID,
		SettledQty: qty,
		SettleAtMS: settleAt,
	})

	expectVersion(plan, res.Header.Key(), res.Header.Version-1)
	upsertResource(plan, res, snap.SceneID())
	p.markTroopUpdated(plan, troop, snap.SceneID())

	setReceipt(plan, domain.CodeOK, map[string]any{
		"session_id":  sess.ID,
		"troop_id":    uint64(troop.Header.ID),
		"resource_id": uint64(res.Header.ID),
		"item_id":     int64(res.ItemID),
		"settled_qty": qty,
		"remaining":   res.Remaining,
		"settle_at":   settleAt,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "CollectSettled", nil))
	return plan, nil
}
