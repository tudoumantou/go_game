package application

// battle.go 实现战斗结果的落地与超时裁决（文档 11.3 / 11.4 / 10.4）。
//
// 这一层只做三件事，不重新裁决：
//  1. 把 Resolver（或超时命令）给出的胜负**应用到状态机**：战斗记录
//     PENDING→RESOLVED/TIMED_OUT，双方部队换阶段、队列前进；
//  2. 保证**只应用一次**：终态战斗收到任何重复/迟到消息都只回执，
//     不重算、不改部队（INV-06）；
//  3. 把"队首结束→下一名开战"写进同一个计划（11.4），由 writer
//     在一个事务里完成出队、建新战斗、改部队，避免"队首已结束但队列
//     没前进"的悬挂状态。
//
// 为什么胜负参数不在这里重新计算：Resolver 在工作池执行（11.2），
// 它的结果通过内部命令回来；规划层如果再用当前属性算一遍，等于制造
// 第二个真源——Resolver 说的是 A 赢，规划层算出 B 赢，以谁为准都会
// 与已持久化的 input_hash 对不上。因此这里只信 WinnerID，并校验它
// 必须是本场战斗的攻守之一。
//
// 幂等设计（状态机幂等）：终态战斗的重复命令返回"只含回执的计划"，
// 不写对象、不写战斗、不推进 revision。这样客户端重试、MQ 重投、
// 超时与结果同时到达三种场景共用一条路径，不需要额外的去重表。

import (
	"fmt"

	"game_study/internal/worldmap/domain"
)

// maxQueueSkipPerCommand 是单条命令最多跳过的失效队列成员数（文档 11.4）。
//
// 为什么要设上限：队列里可能有大量已回城/已阵亡的成员（客户端断线、
// 强制召回），一次全部跳过会让单个事务耗时不可控，还会把后续事件
// 挤在同一个事务里。超过上限的剩余成员留到下一次触发（例如下一个
// 超时/结果命令）继续清理。
const maxQueueSkipPerCommand = 20

// PlanBattleResult 应用一次战斗裁决结果（文档 11.3）。
//
// 行为顺序：
//  1. 战斗不存在 → ErrObjectNotFound（不写回执的稳定失败）；
//  2. 已终态 → 只含回执的幂等计划（status 取自记录），绝不重算；
//  3. PENDING → 校验胜者身份，写 RESOLVED + Result + AppliedOperationID；
//  4. 部队路由：胜方攻方打资源点且资源可用 → 准备采集；其余回城；
//  5. 队列前进：队首出队并新开一场 PENDING 战斗。
func (p *DefaultPlanner) PlanBattleResult(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	battleID := cmd.BattleResult.BattleID
	if battleID == "" {
		return nil, fmt.Errorf("%w: battle_id 不能为空", domain.ErrInvalidArgument)
	}
	rec, ok := findBattleByID(snap, battleID)
	if !ok {
		return nil, fmt.Errorf("%w: 战斗 %s 不存在", domain.ErrObjectNotFound, battleID)
	}
	// 状态值非法说明数据损坏，按状态错误拒绝：不能把 99 当成 PENDING
	// 继续往下走，否则会写出一个"既不是终态也不是进行中"的记录。
	if !rec.Status.Valid() {
		return nil, fmt.Errorf("%w: 战斗 %s 状态非法: %d", domain.ErrInvalidState, battleID, int32(rec.Status))
	}
	// INV-06：终态不可被迟到结果覆盖。返回首次结果的只读回执，
	// 不回写任何对象——这正是"重复结果返回首次结果"的落地方式。
	if rec.Status.Terminal() {
		return p.battleTerminalPlan(snap, env, rec, nowMS), nil
	}

	winner, attackerWon, err := resolveWinner(rec, cmd.BattleResult)
	if err != nil {
		return nil, err
	}

	attacker, ok := snap.Troop(rec.AttackerID)
	if !ok {
		return nil, fmt.Errorf("%w: 战斗 %s 的攻击方部队 %d 不存在",
			domain.ErrObjectNotFound, battleID, rec.AttackerID)
	}
	// 守方可能是部队，也可能是资源点/城池（守军属性快照在记录里）。
	// 后者没有 Troop 对象，用占位快照承载状态机，但**不写回计划**：
	// 把一个凭空造出来的"部队"落库，等于给资源点生成了一个假对象。
	defender, defenderIsTroop := snap.Troop(rec.DefenderID)
	if !defenderIsTroop {
		defender = battleGuardPlaceholder(rec)
	}

	result := domain.BattleResult{
		WinnerID:    winner,
		LoserID:     battleLoser(rec, winner),
		AppliedAtMS: nowMS,
		AttackerWon: attackerWon,
	}
	opKey := battleOperationKey(env, rec.BattleID)
	// 复用 domain 的状态机：PENDING→RESOLVED 的比较更新、双方解除战斗态、
	// 败者保留 Cargo 都在那里实现。application 再写一遍会逐渐漂移。
	applied, err := domain.ApplyBattleResult(rec, attacker, defender, result, opKey)
	if err != nil {
		return nil, fmt.Errorf("战斗 %s 应用裁决: %w", rec.BattleID, err)
	}

	plan := newPlan(snap, env, p.ids, nowMS)
	plan.BattleUpsert = append(plan.BattleUpsert, *rec)

	// 胜方攻方 + 资源目标 → 直接开新采集会话（10.4：胜者按剩余资源
	// 与空负载新建会话）。失败（资源枯竭/被占）退回回城，不阻断裁决。
	attackerRoute, err := p.routeAfterBattle(plan, snap, env, rec, attacker, attackerWon, nowMS)
	if err != nil {
		return nil, err
	}
	defenderRoute := battleRouteNone
	if defenderIsTroop {
		defenderRoute, err = p.routeAfterBattle(plan, snap, env, rec, defender, !attackerWon, nowMS)
		if err != nil {
			return nil, err
		}
	}

	nextBattleID, err := p.advanceBattleQueue(plan, snap, env, rec, nowMS)
	if err != nil {
		return nil, err
	}

	setReceipt(plan, domain.CodeOK, map[string]any{
		"battle_id":      rec.BattleID,
		"status":         battleStatusName(rec.Status),
		"winner":         uint64(applied.WinnerID),
		"loser":          uint64(applied.LoserID),
		"attacker_won":   applied.AttackerWon,
		"target_id":      uint64(rec.TargetID),
		"attacker_route": attackerRoute,
		"defender_route": defenderRoute,
		"queue_next":     nextBattleID,
		"applied_at_ms":  nowMS,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "BattleResolved", nil))
	return plan, nil
}

// PlanBattleTimeout 执行超时裁决（文档 11.3）。
//
// 语义固定："攻击无效、防守保留、无战损"——攻方回城，防守方（资源点
// 上的采集部队/守军）状态不变，谁也不掉血、不掉货。因此这里不走
// domain.ApplyBattleResult（它会把双方都置为 RETURNING），而是直接
// 写终态并只路由攻方。
func (p *DefaultPlanner) PlanBattleTimeout(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	battleID := cmd.BattleTimeout.BattleID
	if battleID == "" {
		return nil, fmt.Errorf("%w: battle_id 不能为空", domain.ErrInvalidArgument)
	}
	rec, ok := findBattleByID(snap, battleID)
	if !ok {
		return nil, fmt.Errorf("%w: 战斗 %s 不存在", domain.ErrObjectNotFound, battleID)
	}
	if !rec.Status.Valid() {
		return nil, fmt.Errorf("%w: 战斗 %s 状态非法: %d", domain.ErrInvalidState, battleID, int32(rec.Status))
	}
	// 结果与超时谁先提交谁生效（11.3）：已 RESOLVED 再收到超时，
	// 只回执既有终态，不能再改成 TIMED_OUT。
	if rec.Status.Terminal() {
		return p.battleTerminalPlan(snap, env, rec, nowMS), nil
	}

	plan := newPlan(snap, env, p.ids, nowMS)
	rec.Status = domain.BattleStatusTimedOut
	// 超时没有 Resolver 结果：留空比编一个假结果更诚实。
	// domain.ApplyBattleResult 在读到 TIMED_OUT 且 Result=nil 时会
	// 合成"防守胜"的只读视图，迟到结果因此永远无法反转它。
	rec.Result = nil
	rec.AppliedOperationID = battleOperationKey(env, rec.BattleID)
	plan.BattleUpsert = append(plan.BattleUpsert, *rec)

	attackerRoute := battleRouteNone
	attacker, ok := snap.Troop(rec.AttackerID)
	if ok {
		// 解除战斗锁：与 FIGHTING 阶段的三件套（BattleID/Busy/Phase）
		// 必须一起清，只清一半会让恢复时校验失败。
		attacker.BattleID = ""
		if attacker.Busy.Reason == domain.BusyReasonBattle {
			attacker.Busy = domain.BusyState{}
		}
		pos := p.battleSitePosition(snap, env, rec.TargetID, attacker, nowMS)
		if err := p.sendBattleTroopHome(plan, snap, attacker, pos, nowMS); err != nil {
			return nil, err
		}
		attackerRoute = battleRouteReturning
	}

	nextBattleID, err := p.advanceBattleQueue(plan, snap, env, rec, nowMS)
	if err != nil {
		return nil, err
	}

	setReceipt(plan, domain.CodeOK, map[string]any{
		"battle_id":      rec.BattleID,
		"status":         battleStatusName(rec.Status),
		"timed_out":      true,
		"winner":         uint64(rec.DefenderID),
		"loser":          uint64(rec.AttackerID),
		"target_id":      uint64(rec.TargetID),
		"attacker_route": attackerRoute,
		"queue_next":     nextBattleID,
		"applied_at_ms":  nowMS,
	}, nowMS)
	plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "BattleTimedOut", nil))
	return plan, nil
}

// battleTerminalPlan 构造"战斗已终态"的只含回执计划。
//
// 没有任何对象/战斗/队列写入：终态是既成事实，重复命令不能推进
// 版本（会制造版本抖动），也不能再次触发队列前进（会重复开战）。
func (p *DefaultPlanner) battleTerminalPlan(snap *SceneSnapshot, env domain.CommandEnvelope, rec *domain.BattleRecord, nowMS int64) *domain.MutationPlan {
	plan := newPlan(snap, env, p.ids, nowMS)
	setReceipt(plan, domain.CodeOK, map[string]any{
		"idempotent": true,
		"battle_id":  rec.BattleID,
		"status":     battleStatusName(rec.Status),
		"target_id":  uint64(rec.TargetID),
	}, nowMS)
	return plan
}

// resolveWinner 从命令参数推出胜者并校验它不是第三方。
//
// 两个字段都可能被填充，必须指定唯一权威：Winner 非 0 时以它为准，
// AttackerWon 只作为"没给身份"时的退化输入。为什么不要求两者严格
// 一致：bool 没有"未设置"状态，false 既可能是"守方胜"也可能是
// "调用方没填"，强行要求一致会把只写 Winner 的合法调用方挡在门外。
// 唯一被拒绝的矛盾是"明确声明攻方获胜（true）却把胜者写成守方"——
// 那是真的自相矛盾，猜任何一个都会污染审计。
func resolveWinner(rec *domain.BattleRecord, params BattleResultParams) (domain.ObjectID, bool, error) {
	if rec == nil {
		return 0, false, fmt.Errorf("%w: 战斗记录不能为空", domain.ErrInvalidArgument)
	}
	winner := params.Winner
	if winner == 0 {
		// 没给身份时退化为按布尔标志推：内部调用方只关心"谁赢"。
		winner = rec.DefenderID
		if params.AttackerWon {
			winner = rec.AttackerID
		}
	} else if params.AttackerWon && winner == rec.DefenderID {
		return 0, false, fmt.Errorf("%w: 战斗 %s 胜者 %d 与 attacker_won=true 矛盾",
			domain.ErrInvalidArgument, rec.BattleID, winner)
	}
	if winner != rec.AttackerID && winner != rec.DefenderID {
		return 0, false, fmt.Errorf("%w: 战斗 %s 的胜者 %d 不是攻守任何一方(%d/%d)",
			domain.ErrInvalidArgument, rec.BattleID, winner, rec.AttackerID, rec.DefenderID)
	}
	return winner, winner == rec.AttackerID, nil
}

// battleLoser 返回败者 ID。
func battleLoser(rec *domain.BattleRecord, winner domain.ObjectID) domain.ObjectID {
	if winner == rec.AttackerID {
		return rec.DefenderID
	}
	return rec.AttackerID
}

// battleGuardPlaceholder 为"守方不是部队"的目标（资源点/城池）造一个
// 只存在于内存里的占位快照。
//
// 为什么需要它：domain.ApplyBattleResult 要按 Header.ID 校验胜者身份，
// 并解除双方的战斗态；没有守方对象就无法走同一个状态机。占位对象
// 携带 ID/WorldID 且处于 FIGHTING+BATTLE 锁，与真实守军的形态一致，
// 但**绝不进 ObjectsUpsert**——它只是计算载体。
func battleGuardPlaceholder(rec *domain.BattleRecord) *domain.Troop {
	return &domain.Troop{
		Header: domain.ObjectHeader{
			WorldID: rec.WorldID,
			ID:      rec.DefenderID,
			Kind:    domain.ObjectKindTroop,
		},
		Phase:    domain.TroopPhaseFighting,
		BattleID: rec.BattleID,
		Busy:     domain.BusyState{Reason: domain.BusyReasonBattle, OpID: rec.BattleID},
	}
}

// 部队在战斗结束后的去向，写进回执便于对账。
const (
	battleRouteNone       = "NONE"
	battleRouteReturning  = "RETURNING"
	battleRouteCollecting = "COLLECTING"
)

// routeAfterBattle 把一支部队从战斗态送到下一步。
//
// 规则（文档 11.3/10.4）：
//   - 胜方是攻方且目标是资源点 → 准备采集（建会话，三件套同一计划）；
//   - 其余一律 RETURNING，Cargo 原样保留（第一版不允许掠夺）。
//
// 注意 domain.ApplyBattleResult 已经清掉 BattleID/Busy 并把 Phase 置
// RETURNING，这里只负责"回城路线"或"改去采集"，不重复改版本。
func (p *DefaultPlanner) routeAfterBattle(plan *domain.MutationPlan, snap *SceneSnapshot, env domain.CommandEnvelope, rec *domain.BattleRecord, troop *domain.Troop, won bool, nowMS int64) (string, error) {
	// 只有攻方获胜才可能留下采集；守方获胜按 11.3 仍是脱离接触回城。
	if won && troop.Header.ID == rec.AttackerID {
		if res, ok := snap.Resource(rec.TargetID); ok && res.Remaining > 0 {
			// 先改 Target，再调 applyCollectStart：它会立刻把部队写进
			// ObjectsUpsert，晚改字段虽然因指针共享也能生效，但依赖
			// "计划里存的是指针"这种隐式约定，读代码时容易被误判。
			troop.Target = domain.TargetRef{TargetID: res.Header.ID, Intent: domain.MarchIntentCollect}
			if _, err := p.applyCollectStart(plan, snap, env, troop, res, nowMS); err == nil {
				return battleRouteCollecting, nil
			}
			// 建会话失败（枯竭/被占/负载满）不改变裁决结果：部队回城。
			// 这里吞掉错误是业务决策，不是遗漏——战斗已经打赢了，
			// 不能因为抢不到采集权就把整场裁决回滚。
		}
	}
	pos := p.battleSitePosition(snap, env, rec.TargetID, troop, nowMS)
	if err := p.sendBattleTroopHome(plan, snap, troop, pos, nowMS); err != nil {
		return "", err
	}
	return battleRouteReturning, nil
}

// sendBattleTroopHome 生成回城路线并落计划：清目标、置 RETURNING、推进版本。
//
// 与 planReturnAfterFailure 的区别是不写回执/不改 ResultCode——战斗的
// 业务码由裁决命令本身决定，回城只是副作用。
func (p *DefaultPlanner) sendBattleTroopHome(plan *domain.MutationPlan, snap *SceneSnapshot, troop *domain.Troop, from domain.Pos, nowMS int64) error {
	troop.Target = domain.TargetRef{}
	// appendReturnMove 内部走 PlanMarchTo，intent 传 Unspecified 会把
	// Phase 置 RETURNING 并生成新 MoveID（旧到达任务因此自动过期）。
	if _, _, err := p.appendReturnMove(snap, troop, from, nowMS); err != nil {
		return fmt.Errorf("部队 %d 战斗后回城路线: %w", troop.Header.ID, err)
	}
	troop.BattleID = ""
	if troop.Busy.Reason == domain.BusyReasonBattle {
		troop.Busy = domain.BusyState{}
	}
	p.markTroopUpdated(plan, troop, snap.SceneID())
	return nil
}

// battleSitePosition 推导部队战斗结束时所处的位置。
//
// 优先级：移动段推导（最权威，9.2）→ 目标坐标 → 主堡坐标。
// 最后一级是兜底：守方部队没有 Move 也没有独立坐标字段，只能退回
// 主堡（零长度回城段，到达命令会直接收尾），总比生成一条起点为
// (0,0) 的错误路线好。
func (p *DefaultPlanner) battleSitePosition(snap *SceneSnapshot, env domain.CommandEnvelope, targetID domain.ObjectID, troop *domain.Troop, nowMS int64) domain.Pos {
	if troop != nil && troop.Move != nil {
		if pos, err := troop.Move.PositionAt(nowMS); err == nil {
			return pos
		}
	}
	if pos, ok := p.targetPosition(snap, env, targetID); ok {
		return pos
	}
	if troop != nil {
		if home, ok := snap.City(troop.HomeCityID); ok {
			return home.Position
		}
	}
	return domain.Pos{}
}

// advanceBattleQueue 执行"队首结束 → 下一名开始"（文档 11.4）。
//
// 返回新战斗的 BattleID（没有下一名时为空）。要点：
//   - 每个被考察的成员都写 QueueDequeue：失效成员也要在事务里删除，
//     否则队列会永久堵着一个永远不会开战的部队；
//   - 失效判定（部队已回城/目标已消失）用**快照内的当前状态**，不用
//     入队时的状态；
//   - 新战斗沿用原记录的目标与守军属性快照：资源点守军没有独立对象，
//     属性只能来自战斗记录（11.1）。
func (p *DefaultPlanner) advanceBattleQueue(plan *domain.MutationPlan, snap *SceneSnapshot, env domain.CommandEnvelope, rec *domain.BattleRecord, nowMS int64) (string, error) {
	queue := snap.QueuedOn(rec.TargetID)
	for i, troopID := range queue {
		if i >= maxQueueSkipPerCommand {
			// 剩余成员留待后续命令处理（11.4 的"剩余发后续任务"）。
			break
		}
		plan.QueueDequeue = append(plan.QueueDequeue, domain.QueueEntry{
			TargetID: rec.TargetID,
			TroopID:  troopID,
		})

		troop, ok := snap.Troop(troopID)
		if !ok || troop.Phase != domain.TroopPhaseQueued {
			continue // 已回城/已关闭：事务删掉即可
		}
		obj, ok := snap.Object(domain.ObjectKey{WorldID: rec.WorldID, ID: rec.TargetID})
		if !ok || obj.Deleted {
			// 目标已消失：队列里所有成员都没有意义，但仍要逐个出队。
			continue
		}

		battleID, record := p.newBattleRecord(env, troop, rec.TargetID, rec.DefenderID, rec.DefenderStats, nowMS)
		plan.BattleUpsert = append(plan.BattleUpsert, record)

		// 与 startBattle 保持同一套三件套：Phase/BattleID/Busy 必须
		// 同时成立，等价于"FIGHTING 才有战斗锁"的不变量。
		troop.Phase = domain.TroopPhaseFighting
		troop.Move = nil
		troop.Target = domain.TargetRef{TargetID: rec.TargetID, Intent: domain.MarchIntentAttack}
		troop.BattleID = battleID
		troop.Busy = domain.BusyState{
			Reason:     domain.BusyReasonBattle,
			OpID:       battleID,
			SinceMS:    nowMS,
			DeadlineMS: record.DeadlineMS,
		}
		p.markTroopUpdated(plan, troop, snap.SceneID())
		return battleID, nil
	}
	return "", nil
}

// findBattleByID 按 battle_id 在快照里查战斗记录。
//
// 快照的 battles 以**目标**为键（每目标同时最多一场进行中战斗），
// 而命令只带 battle_id，因此这里做一次线性扫描。三点说明：
//  1. 同一个包内可以直接读私有字段，不需要为查询另开导出 API
//     （导出 AllBattles 会把内部索引形状变成对外契约）；
//  2. Go 的 map 遍历顺序随机，但 battle_id 全局唯一，命中项唯一，
//     因此顺序不影响结果——这是"随机序无害"的前提，写代码时必须
//     自己确认，而不是默认 map 有序；
//  3. 万一数据损坏出现重复 battle_id，取 TargetID 最小的那条，
//     让结果仍然确定（不依赖遍历顺序）。
func findBattleByID(snap *SceneSnapshot, battleID string) (*domain.BattleRecord, bool) {
	if snap == nil || battleID == "" {
		return nil, false
	}
	var found *domain.BattleRecord
	var foundTarget domain.ObjectID
	for target, rec := range snap.battles {
		if rec.BattleID != battleID {
			continue
		}
		if found == nil || target < foundTarget {
			cp := *rec
			found = &cp
			foundTarget = target
		}
	}
	if found == nil {
		return nil, false
	}
	return found, true
}

// battleOperationKey 返回写入 AppliedOperationID 的幂等键。
//
// 优先用命令的 RequestID（runtime 用 domain.BattleResultKey 生成），
// 这样结果命令与超时命令共享同一个键：谁先提交，谁把这个键写进
// 记录，后来者读到终态直接返回，形成"首次事务定终态"。
func battleOperationKey(env domain.CommandEnvelope, battleID string) string {
	if env.RequestID != "" {
		return env.RequestID
	}
	return domain.BattleResultKey(battleID)
}

// battleStatusName 返回战斗状态的稳定名称（进回执与日志）。
func battleStatusName(s domain.BattleStatus) string {
	switch s {
	case domain.BattleStatusPending:
		return "PENDING"
	case domain.BattleStatusResolved:
		return "RESOLVED"
	case domain.BattleStatusTimedOut:
		return "TIMED_OUT"
	default:
		return fmt.Sprintf("BattleStatus(%d)", int32(s))
	}
}
