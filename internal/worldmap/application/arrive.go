package application

// arrive.go 实现到达仲裁（文档 9.3 表）。
//
// 表格必须**按顺序**判断，先命中先返回：顺序本身是业务规则。
// 例如“资源点无剩余”要先于“同一玩家另一支部队已采集”，否则一个被采空
// 的资源点会先报 Occupied 再回城，回执原因就答非所问了。
//
// 另一个关键点是“过期任务”：改速/召回会换 MoveID，旧到达任务触发时
// 必须判定过期并**零副作用**返回。这是文档 9.2 明文规定的正常路径，
// 不是异常——因此它返回成功回执而不是错误。

import (
	"fmt"
	"sort"
	"strings"

	"game_study/internal/worldmap/domain"
)

// m1QueuedBattleMarker 写在 BattleRecord.AppliedOperationID 上，用于把
// “排队成员”与“正在裁决的战斗”区分开。这是 M1 的权宜表示（MutationPlan
// 没有队列字段，见 recall.go 的详细说明）；M2 增加 map_battle_queue 后删除。
const m1QueuedBattleMarker = "m1:queued"

// PlanArrive 处理移动到达。
func (p *DefaultPlanner) PlanArrive(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	troop, ok := snap.Troop(cmd.Arrive.TroopID)
	if !ok {
		return nil, fmt.Errorf("%w: 部队 %d 不存在", domain.ErrObjectNotFound, cmd.Arrive.TroopID)
	}

	// 过期判定：MoveID 是唯一依据（ArriveKey 已把 move_id 放进幂等键）。
	// 部队没有 Move（采集中/战斗中）时旧到达任务同样视为过期。
	if troop.Move == nil || cmd.Arrive.MoveID != troop.Move.MoveID {
		plan := newPlan(snap, env, p.ids, nowMS)
		setReceipt(plan, domain.CodeOK, map[string]any{
			"stale":    true,
			"troop_id": uint64(troop.Header.ID),
		}, nowMS)
		return plan, nil
	}

	arrivePos := troop.Move.To

	// 回城到达：Troop→SETTLING + 生成 delivery（文档 9.5）。
	// 位置不等于主堡属于数据不一致（例如主堡迁移留下的旧路线），MVP
	// 仍按回城收尾：拒绝收尾会让部队停在半路且没有新的到期事件。
	if troop.Phase == domain.TroopPhaseReturning {
		return p.planHomeArrival(snap, env, troop, arrivePos, nowMS)
	}

	// 重新校验目标（9.3 第一行）：目标可能在行军途中被销毁。
	key := domain.ObjectKey{WorldID: env.WorldID, ID: troop.Target.TargetID}
	obj, ok := snap.Object(key)
	if !ok || obj.Deleted {
		return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
			domain.CodeOf(domain.ErrObjectNotFound), "目标已消失", nil)
	}

	switch obj.Kind {
	case domain.ObjectKindResource:
		return p.arriveResource(snap, env, troop, obj.Key.ID, arrivePos, nowMS)
	case domain.ObjectKindTroop, domain.ObjectKindCity:
		return p.arriveCombatTarget(snap, env, troop, obj.Kind, obj.Key.ID, arrivePos, nowMS)
	default:
		return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
			domain.CodeOf(domain.ErrInvalidArgument), "目标种类不支持", nil)
	}
}

// arriveResource 按文档 9.3 表格处理资源目标。
func (p *DefaultPlanner) arriveResource(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, resourceID domain.ObjectID, arrivePos domain.Pos, nowMS int64) (*domain.MutationPlan, error) {
	res, ok := snap.Resource(resourceID)
	if !ok {
		return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
			domain.CodeOf(domain.ErrObjectNotFound), "资源点已消失", nil)
	}
	// 无剩余资源 → 回城，不发采集奖励。
	if res.Remaining <= 0 {
		return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
			domain.CodeOf(domain.ErrCapacityExhausted), "资源点已枯竭", map[string]any{
				"resource_id": uint64(res.Header.ID),
			})
	}
	capacityLeft, err := p.capacityLeft(troop)
	if err != nil {
		return nil, err
	}
	if capacityLeft <= 0 {
		return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
			domain.CodeOf(domain.ErrCapacityExhausted), "部队负载已满", map[string]any{
				"resource_id": uint64(res.Header.ID),
			})
	}

	sess, hasSession := snap.ActiveSessionOn(res.Header.ID)
	if hasSession && sess.Status != domain.CollectStatusActive {
		// 快照里残留已结算会话：视同没有活动会话（快照约定只传活动会话，
		// 这里防御性处理，避免把历史会话当成占用者）。
		hasSession = false
	}
	if hasSession {
		// 同一支部队重复到达：幂等返回已有会话，不建第二个。
		if sess.TroopID == troop.Header.ID {
			plan := newPlan(snap, env, p.ids, nowMS)
			setReceipt(plan, domain.CodeOK, map[string]any{
				"idempotent":  true,
				"troop_id":    uint64(troop.Header.ID),
				"resource_id": uint64(res.Header.ID),
				"session_id":  sess.ID,
				"end_at_ms":   sess.EndAtMS,
			}, nowMS)
			return plan, nil
		}
		// 同玩家另一支部队已采集 → Occupied 并回城；采集没有 FIFO
		// （文档 11.4 最后一段：排队的只有敌对攻击）。
		if p.troopBelongsTo(snap, sess.TroopID, troop.PlayerID) {
			return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
				domain.CodeOf(domain.ErrTargetOccupied), "同玩家另一支部队正在采集", map[string]any{
					"resource_id": uint64(res.Header.ID),
					"session_id":  sess.ID,
					"occupied_by": uint64(sess.TroopID),
				})
		}

		// 敌方占用：无战斗则开战，有战斗则排队。
		defStats := m1GuardStats(nowMS)
		if defender, ok := snap.Troop(sess.TroopID); ok {
			defStats = defender.Stats
		}
		if battle, ok := snap.ActiveBattleOn(res.Header.ID); ok && battle.Status == domain.BattleStatusPending {
			if len(snap.QueuedOn(res.Header.ID)) >= p.rules.MaxBattleQueuePerTarget {
				return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
					domain.CodeOf(domain.ErrQueueFull), "目标战斗队列已满", map[string]any{
						"resource_id": uint64(res.Header.ID),
						"battle_id":   battle.BattleID,
					})
			}
			return p.enqueueBattle(snap, env, troop, battle.BattleID, res.Header.ID, sess.TroopID, defStats, nowMS)
		}

		return p.startBattle(snap, env, troop, res.Header.ID, sess.TroopID, defStats, nowMS)
	}

	// 空资源点 + 负载可用 → 建会话。三件套（CollectStart + 资源 active +
	// Troop→COLLECTING）由 applyCollectStart 写进同一个计划（文档 10.1）。
	plan := newPlan(snap, env, p.ids, nowMS)
	created, err := p.applyCollectStart(plan, snap, env, troop, res, nowMS)
	if err != nil {
		return nil, err
	}
	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":     uint64(troop.Header.ID),
		"resource_id":  uint64(res.Header.ID),
		"session_id":   created.ID,
		"planned_qty":  created.PlannedQty,
		"end_at_ms":    created.EndAtMS,
		"arrive_at_ms": nowMS,
	}, nowMS)
	return plan, nil
}

// arriveCombatTarget 处理攻击部队/城池（不涉及采集）。
func (p *DefaultPlanner) arriveCombatTarget(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, kind domain.ObjectKind, targetID domain.ObjectID, arrivePos domain.Pos, nowMS int64) (*domain.MutationPlan, error) {
	var defenderID domain.ObjectID
	defStats := m1GuardStats(nowMS)

	switch kind {
	case domain.ObjectKindCity:
		city, ok := snap.City(targetID)
		if !ok {
			return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
				domain.CodeOf(domain.ErrObjectNotFound), "目标城池已消失", nil)
		}
		if city.PlayerID == troop.PlayerID {
			return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
				domain.CodeOf(domain.ErrTargetOccupied), "不能攻击自己的城池", nil)
		}
		defenderID = city.Header.ID
	case domain.ObjectKindTroop:
		defender, ok := snap.Troop(targetID)
		if !ok || defender.Phase == domain.TroopPhaseClosed {
			return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
				domain.CodeOf(domain.ErrObjectNotFound), "目标部队已消失", nil)
		}
		if defender.PlayerID == troop.PlayerID {
			return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
				domain.CodeOf(domain.ErrTargetOccupied), "不能攻击自己的部队", nil)
		}
		defenderID = defender.Header.ID
		defStats = defender.Stats
	default:
		return nil, fmt.Errorf("%w: 不支持的目标种类 %s", domain.ErrInvalidArgument, kind)
	}

	if battle, ok := snap.ActiveBattleOn(targetID); ok && battle.Status == domain.BattleStatusPending {
		if len(snap.QueuedOn(targetID)) >= p.rules.MaxBattleQueuePerTarget {
			return p.planReturnAfterFailure(snap, env, troop, arrivePos, nowMS,
				domain.CodeOf(domain.ErrQueueFull), "目标战斗队列已满", map[string]any{
					"target_id": uint64(targetID),
					"battle_id": battle.BattleID,
				})
		}
		return p.enqueueBattle(snap, env, troop, battle.BattleID, targetID, defenderID, defStats, nowMS)
	}
	return p.startBattle(snap, env, troop, targetID, defenderID, defStats, nowMS)
}

// startBattle 创建 PENDING 战斗，并把攻击方转入 FIGHTING。
//
// input_hash = sha256(attacker || defender || seed)：相同输入→相同结果
// 的可判定依据（文档 11.2）。random_seed 由 SeedGen 按
// (target, now, attacker) 派生，保证可回放且与刷新流互不干扰。
func (p *DefaultPlanner) startBattle(snap *SceneSnapshot, env domain.CommandEnvelope, attacker *domain.Troop, targetID, defenderID domain.ObjectID, defenderStats domain.CombatSnapshot, nowMS int64) (*domain.MutationPlan, error) {
	plan := newPlan(snap, env, p.ids, nowMS)
	battleID, record := p.newBattleRecord(env, attacker, targetID, defenderID, defenderStats, "", nowMS)
	plan.BattleUpsert = append(plan.BattleUpsert, record)

	// FIGHTING 必须三件套一致：Phase=FIGHTING、BattleID 非空、Busy=BATTLE
	// （ValidateTroop 会校验，写错组合会让世界恢复时转 NotReady）。
	attacker.Phase = domain.TroopPhaseFighting
	attacker.Move = nil
	attacker.BattleID = battleID
	attacker.Busy = domain.BusyState{
		Reason:     domain.BusyReasonBattle,
		OpID:       battleID,
		SinceMS:    nowMS,
		DeadlineMS: record.DeadlineMS,
	}
	p.markTroopUpdated(plan, attacker, snap.SceneID())

	setReceipt(plan, domain.CodeOK, map[string]any{
		"battle_id":   battleID,
		"target_id":   uint64(targetID),
		"attacker_id": uint64(attacker.Header.ID),
		"defender_id": uint64(defenderID),
		"deadline_ms": record.DeadlineMS,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "BattleStarted", nil))
	return plan, nil
}

// enqueueBattle 把攻击方追加到目标战斗队列。
//
// 【M1 表示】队列成员写成一条 Status=PENDING 的 BattleRecord，并用
// AppliedOperationID=m1QueuedBattleMarker 与队首区分。DeadlineMS 取
// 入队时间 + 超时，作为 M1 的近似入队序；M2 用 map_battle_queue.enqueue_seq。
func (p *DefaultPlanner) enqueueBattle(snap *SceneSnapshot, env domain.CommandEnvelope, attacker *domain.Troop, activeBattleID string, targetID, defenderID domain.ObjectID, defenderStats domain.CombatSnapshot, nowMS int64) (*domain.MutationPlan, error) {
	if activeBattleID == "" {
		activeBattleID = "unknown"
	}
	plan := newPlan(snap, env, p.ids, nowMS)
	queuedID, record := p.newBattleRecord(env, attacker, targetID, defenderID, defenderStats, m1QueuedBattleMarker, nowMS)
	plan.BattleUpsert = append(plan.BattleUpsert, record)

	attacker.Phase = domain.TroopPhaseQueued
	attacker.Move = nil
	attacker.BattleID = "" // 非 FIGHTING 不得携带 battle_id
	attacker.Busy = domain.BusyState{}
	p.markTroopUpdated(plan, attacker, snap.SceneID())

	setReceipt(plan, domain.CodeOK, map[string]any{
		"queued":    true,
		"troop_id":  uint64(attacker.Header.ID),
		"battle_id": activeBattleID,
		"queue_id":  queuedID,
		"target_id": uint64(targetID),
		"queue_len": int64(len(snap.QueuedOn(targetID)) + 1),
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "BattleQueued", nil))
	return plan, nil
}

// newBattleRecord 构造战斗记录（创建与排队共用）。
// marker 为空表示队首（真正参与裁决），非空表示排队成员。
func (p *DefaultPlanner) newBattleRecord(env domain.CommandEnvelope, attacker *domain.Troop, targetID, defenderID domain.ObjectID, defenderStats domain.CombatSnapshot, marker string, nowMS int64) (string, domain.BattleRecord) {
	battleID := p.ids()
	seed := p.seeds(int64(targetID), nowMS, int64(attacker.Header.ID))
	// 手工拼接规范化字节串：字段顺序固定、无分隔符歧义，
	// 不依赖 map 遍历或 protobuf 编码顺序（文档 7.1 的同类要求）。
	canonical := fmt.Sprintf("%d|%d|%d", attacker.Header.ID, defenderID, seed)
	return battleID, domain.BattleRecord{
		BattleID:      battleID,
		WorldID:       env.WorldID,
		TargetID:      targetID,
		AttackerID:    attacker.Header.ID,
		DefenderID:    defenderID,
		InputHash:     domain.PayloadHashOf([]byte(canonical)),
		AttackerStats: attacker.Stats,
		DefenderStats: defenderStats,
		RuleVersion:   m1RuleVersion,
		RandomSeed:    seed,
		Status:        domain.BattleStatusPending,
		DeadlineMS:    nowMS + p.rules.BattleTimeoutMS,
		// marker 只用于 M1 队列区分；队首留空，避免与“结果已应用”的
		// 语义混淆（结果应用会写真实 operation id）。
		AppliedOperationID: marker,
	}
}

// planHomeArrival 回城收尾（文档 9.5）：
// Troop→SETTLING、固化 Cargo、创建唯一 delivery/outbox、清除 Move。
//
// **关键**：Cargo 不在这里清空。文档明确“先保留货物及交接单，不能先
// 把 Cargo 清零再尝试发奖”。Cargo 要等玩家 Actor 确认资产提交后，
// 由 delivery→APPLIED 的事务清空并把 Troop→CLOSED（M3 的确认流程）。
func (p *DefaultPlanner) planHomeArrival(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, arrivePos domain.Pos, nowMS int64) (*domain.MutationPlan, error) {
	home, ok := snap.City(troop.HomeCityID)
	if !ok {
		return nil, fmt.Errorf("%w: 部队 %d 的主堡 %d 不存在",
			domain.ErrObjectNotFound, troop.Header.ID, troop.HomeCityID)
	}
	plan := newPlan(snap, env, p.ids, nowMS)

	// SettlementNo 取到达时的对象版本：同一部队多次回城时版本单调递增，
	// 配合 UNIQUE(world,troop,no) 可以挡住重复创建交接单。
	settlementNo := int64(troop.Header.Version)
	deliveryID := p.ids()

	troop.Phase = domain.TroopPhaseSettling
	troop.Move = nil
	troop.SettlementID = deliveryID
	troop.Deferred = nil // 回城后不再需要延迟召回
	p.markTroopUpdated(plan, troop, snap.SceneID())

	// Delivery.Cargo 必须是深拷贝：map 是引用类型，直接赋 troop.Cargo
	// 会让计划与快照共享底层哈希表（文档 4.2 深拷贝纪律）。
	deliveryCargo := make(map[int32]int64, len(troop.Cargo))
	for k, v := range troop.Cargo {
		deliveryCargo[k] = v
	}
	plan.DeliveryCreate = append(plan.DeliveryCreate, domain.Delivery{
		DeliveryID:   deliveryID,
		WorldID:      env.WorldID,
		TroopID:      troop.Header.ID,
		PlayerID:     troop.PlayerID,
		Cargo:        deliveryCargo,
		PayloadHash:  cargoHash(deliveryCargo),
		CreatedAtMS:  nowMS,
		Status:       domain.DeliveryStatusPending,
		Attempts:     0,
		NextRetryMS:  0,
		SettlementNo: settlementNo,
	})

	extra := map[string]any{
		"troop_id":      uint64(troop.Header.ID),
		"delivery_id":   deliveryID,
		"settlement_no": settlementNo,
		"cargo_hash":    domain.HashString(cargoHash(deliveryCargo)),
		"phase":         troopPhaseName(troop.Phase),
	}
	// 位置与主堡不一致时把偏差写进回执，便于对账发现问题路线。
	if arrivePos != home.Position {
		extra["arrive_mismatch"] = true
		extra["arrive_x"] = arrivePos.X
		extra["arrive_y"] = arrivePos.Y
	}
	setReceipt(plan, domain.CodeOK, extra, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "DeliveryCreated", nil))
	return plan, nil
}

// planReturnAfterFailure 是“目标不可用 → 回城”的统一实现：生成回城
// 路线、写失败原因回执。
//
// 注意它返回的是**成功提交的计划**而不是 error：部队确实要回城，
// 业务失败体现在 ResultCode 上（文档 7.2：业务失败也写回执，
// 否则同一请求稍后重试会突然变成另一个结果）。
func (p *DefaultPlanner) planReturnAfterFailure(snap *SceneSnapshot, env domain.CommandEnvelope, troop *domain.Troop, from domain.Pos, nowMS int64, code int32, reason string, extra map[string]any) (*domain.MutationPlan, error) {
	plan := newPlan(snap, env, p.ids, nowMS)
	moveID, arriveAt, err := p.appendReturnMove(snap, troop, from, nowMS)
	if err != nil {
		return nil, err
	}
	troop.Target = domain.TargetRef{}
	p.markTroopUpdated(plan, troop, snap.SceneID())

	result := map[string]any{
		"troop_id":     uint64(troop.Header.ID),
		"reason":       reason,
		"move_id":      moveID,
		"arrive_at_ms": arriveAt,
	}
	for k, v := range extra {
		result[k] = v
	}
	setReceipt(plan, code, result, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "TroopMoved", nil))
	return plan, nil
}

// troopBelongsTo 判断部队是否属于指定玩家。查不到视为“不是”，
// 让调用方按敌方处理：宁可打一场空战斗，也不要把归属不明的目标
// 当成自己人而拒绝。
func (p *DefaultPlanner) troopBelongsTo(snap *SceneSnapshot, troopID domain.ObjectID, playerID int64) bool {
	t, ok := snap.Troop(troopID)
	return ok && t.PlayerID == playerID
}

// cargoHash 计算交接单 payload 摘要。
//
// 必须先把物品 ID 排序再拼接：Go 的 map 遍历顺序是随机的（每次 range
// 的起点不同），直接遍历会导致同一份 Cargo 每次算出不同的 hash，
// 玩家侧去重（slg:delivery:{id} + payload_hash）会误判为冲突（13.5）。
func cargoHash(cargo map[int32]int64) [32]byte {
	keys := make([]int, 0, len(cargo))
	for k := range cargo {
		keys = append(keys, int(k))
	}
	sort.Ints(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%d:%d;", k, cargo[int32(k)])
	}
	return domain.PayloadHashOf([]byte(b.String()))
}
