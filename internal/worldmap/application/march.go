package application

// march.go 实现出征规划（文档 9.1 六步）。
//
// 出征是唯一“无中生有”创建部队的命令，因此它承担了三类校验：
// 归属（主堡是不是你的）、资源（槽位是否空闲）、空间（目标存在且路径可达）。
// 所有坐标、速度、负载、战斗属性都由服务端按规则推导，客户端只给
// (slot_id, target_id, intent)。

import (
	"fmt"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/spatial"
)

// marchTarget 是解析后的出征目标：对象种类、坐标与对象 ID。
type marchTarget struct {
	id       domain.ObjectID
	kind     domain.ObjectKind
	position domain.Pos
}

// PlanMarchStart 实现文档 9.1 的顺序：
//
//  1. 主堡存在、归属玩家、归属本场景；
//  2. (player,slot) 活动槽位唯一（SETTLING 也算占用）；
//  3. 目标存在且种类与 intent 匹配；
//  4. 直线路径不穿静态阻挡；
//  5. 按规则速度生成 MoveSegment；
//  6. 生成 Troop + 回执 + 投影事件的计划。
//
// 第 5 步“按规则速度”是本函数最重要的性质：MarchParams 里根本没有
// 速度字段，速度只能来自 p.rules，客户端无从干预。
func (p *DefaultPlanner) PlanMarchStart(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	m := cmd.March

	// 1) 主堡。主堡是起点也是未来回城的目标，必须属于本场景的写权限
	// 范围（OwnerScene）。位置不一致说明对象已经不该由本场景处理。
	city, ok := snap.City(m.HomeCityID)
	if !ok {
		return nil, fmt.Errorf("%w: 主堡 %d 不存在", domain.ErrObjectNotFound, m.HomeCityID)
	}
	if city.PlayerID != env.PlayerID {
		return nil, fmt.Errorf("%w: 主堡 %d 属于玩家 %d，与请求玩家 %d 不符",
			domain.ErrNotOwner, m.HomeCityID, city.PlayerID, env.PlayerID)
	}
	if city.Header.OwnerScene != snap.SceneID() {
		return nil, fmt.Errorf("%w: 主堡 %d 归属场景 %d，不在当前场景 %d",
			domain.ErrInvalidArgument, m.HomeCityID, city.Header.OwnerScene, snap.SceneID())
	}

	// 2) 槽位唯一。INV-05：同一 (world,player,slot) 同时只能有一支部队
	// “活着”。SETTLING 仍占槽位——奖励还没交接完就复用槽位，会出现
	// 两支同槽位部队各自持有 Cargo 的混乱（文档 9.1 第 3 条）。
	for _, t := range snap.TroopsOfPlayer(env.PlayerID) {
		if t.SlotID == m.SlotID && t.Phase != domain.TroopPhaseClosed {
			return nil, fmt.Errorf("%w: 玩家 %d 槽位 %d 已被部队 %d(%s) 占用",
				domain.ErrSlotBusy, env.PlayerID, m.SlotID, t.Header.ID, troopPhaseName(t.Phase))
		}
	}

	// 3) 目标存在且种类匹配 intent。
	target, err := p.resolveMarchTarget(snap, env, m.Target)
	if err != nil {
		return nil, err
	}

	from := city.Position
	to := target.position
	if !p.topo.InBounds(from) || !p.topo.InBounds(to) {
		return nil, fmt.Errorf("%w: 出征起终点越界: from=(%d,%d) to=(%d,%d)",
			domain.ErrInvalidArgument, from.X, from.Y, to.X, to.Y)
	}

	// 4) 路径检测。第一版只有直线，Bresenham 逐格判断静态阻挡
	// （部队之间不互相阻挡，文档 5.3）。受阻就拒绝，绝不偷偷穿墙。
	if blockedAt, hit := spatial.RaycastBlocked(from, to, snap.Blocked); hit {
		cx := blockedAt.X / p.topo.PositionUnitsCell
		cy := blockedAt.Y / p.topo.PositionUnitsCell
		return nil, fmt.Errorf("%w: 从 (%d,%d) 到 (%d,%d) 的路径在格 (%d,%d) 受阻",
			domain.ErrPathBlocked, from.X, from.Y, to.X, to.Y, cx, cy)
	}

	plan := newPlan(snap, env, p.ids, nowMS)

	// 6) 对象装配。先建 Troop 再建 Move，是因为 Troop 的 Header 版本
	// 在创建时即为 1（新建对象没有“上一版”，不需要 CAS 期望值）。
	troop := &domain.Troop{
		Header: domain.ObjectHeader{
			WorldID:    env.WorldID,
			ID:         p.objectIDs(),
			Kind:       domain.ObjectKindTroop,
			OwnerScene: snap.SceneID(),
			Version:    1,
		},
		PlayerID:   env.PlayerID,
		SlotID:     m.SlotID,
		HomeCityID: m.HomeCityID,
		// Cargo 必须是非 nil 空 map：nil map 读安全、写 panic，
		// 后续“Cargo[item] += qty”会直接炸；这里一次性给出可写空表。
		Cargo:         map[int32]int64{},
		CapacityUnits: p.rules.CapacityUnits,
		Stats:         p.newTroopStats(nowMS),
		Target:        m.Target,
		DispatchID:    "", // 免费 MVP 不产生计费许可单，字段留给付费流程。
	}

	moveID := p.ids()
	// 5) 生成 MoveSegment。速度来自规则，nowMS 由调用方注入；
	// BuildMove 内部用 ceil(len/speed) 秒——取整方向是向上：宁可晚到，
	// 也不能让到达任务早于真实位置，否则部队会“提前”开始采集。
	seg, err := p.PlanMarchTo(troop, from, to, p.rules.MarchSpeedUnitsPerSec, nowMS, moveID, m.Target.Intent)
	if err != nil {
		return nil, err
	}

	upsertTroop(plan, troop, snap.SceneID())

	// 不写 OccupancyClaims：部队移动不占静态格（文档 5.3 只有静态层），
	// 兵线是投影（LineIndex），不进入 map_occupancy。给部队占格会让
	// 两条并行的行军路线互相阻塞，并且部队走后还要记得释放，得不偿失。

	// 回执字段与文档 9.1 第 6 步一致：troop_id、move_id、arrive_at_ms、
	// target_id。object_version 由 writer 根据计划里的 Version 回填。
	setReceipt(plan, domain.CodeOK, map[string]any{
		"troop_id":     uint64(troop.Header.ID),
		"move_id":      seg.MoveID,
		"arrive_at_ms": seg.EndAtMS,
		"target_id":    uint64(target.id),
		"slot_id":      int64(m.SlotID),
	}, nowMS)

	// 投影事件：payload 可以为空，消费者按 revision 重拉快照即可
	// （文档 7.3）。kind 用稳定字符串，客户端 AOI 侧按它分发。
	plan.OutboxEvents = append(plan.OutboxEvents,
		p.newEvent(snap, env, "TroopMoved", nil))

	return plan, nil
}

// resolveMarchTarget 校验目标存在、种类与 intent 匹配，并返回坐标。
//
// 目标预检**不承诺**到达时仍然可采集（文档 9.1 第 4 条）：从出征到
// 到达之间目标可能被采空或进入战斗，那是 PlanArrive 的仲裁职责。
func (p *DefaultPlanner) resolveMarchTarget(snap *SceneSnapshot, env domain.CommandEnvelope, ref domain.TargetRef) (marchTarget, error) {
	if ref.TargetID == 0 {
		return marchTarget{}, fmt.Errorf("%w: target_id 不能为 0", domain.ErrInvalidArgument)
	}
	if !ref.Intent.Valid() {
		return marchTarget{}, fmt.Errorf("%w: intent 非法: %d（必须显式传采集或攻击）",
			domain.ErrInvalidArgument, int32(ref.Intent))
	}
	key := domain.ObjectKey{WorldID: env.WorldID, ID: ref.TargetID}
	obj, ok := snap.Object(key)
	if !ok || obj.Deleted {
		return marchTarget{}, fmt.Errorf("%w: 目标 %d 不存在或已删除", domain.ErrObjectNotFound, ref.TargetID)
	}

	switch ref.Intent {
	case domain.MarchIntentCollect:
		if obj.Kind != domain.ObjectKindResource {
			return marchTarget{}, fmt.Errorf("%w: 采集意图的目标 %d 种类是 %s，不是资源点",
				domain.ErrInvalidArgument, ref.TargetID, obj.Kind)
		}
	case domain.MarchIntentAttack:
		if obj.Kind != domain.ObjectKindTroop && obj.Kind != domain.ObjectKindCity {
			return marchTarget{}, fmt.Errorf("%w: 攻击意图的目标 %d 种类是 %s，只允许部队或城池",
				domain.ErrInvalidArgument, ref.TargetID, obj.Kind)
		}
	}

	switch obj.Kind {
	case domain.ObjectKindResource:
		res, ok := snap.Resource(ref.TargetID)
		if !ok {
			return marchTarget{}, fmt.Errorf("%w: 资源点 %d 详情缺失", domain.ErrObjectNotFound, ref.TargetID)
		}
		return marchTarget{id: ref.TargetID, kind: obj.Kind, position: res.Position}, nil
	case domain.ObjectKindCity:
		city, ok := snap.City(ref.TargetID)
		if !ok {
			return marchTarget{}, fmt.Errorf("%w: 城池 %d 详情缺失", domain.ErrObjectNotFound, ref.TargetID)
		}
		if city.PlayerID == env.PlayerID {
			return marchTarget{}, fmt.Errorf("%w: 不能攻击自己的城池 %d", domain.ErrInvalidArgument, ref.TargetID)
		}
		return marchTarget{id: ref.TargetID, kind: obj.Kind, position: city.Position}, nil
	case domain.ObjectKindTroop:
		t, ok := snap.Troop(ref.TargetID)
		if !ok {
			return marchTarget{}, fmt.Errorf("%w: 部队 %d 详情缺失", domain.ErrObjectNotFound, ref.TargetID)
		}
		if t.PlayerID == env.PlayerID {
			return marchTarget{}, fmt.Errorf("%w: 不能攻击自己的部队 %d", domain.ErrInvalidArgument, ref.TargetID)
		}
		if t.Move == nil {
			// 部队位置只能由 Move 推导；没有 Move 的目标无法确定落点，
			// 按参数错误拒绝而不是猜一个坐标。
			return marchTarget{}, fmt.Errorf("%w: 目标部队 %d 没有移动段，无法确定位置",
				domain.ErrInvalidArgument, ref.TargetID)
		}
		return marchTarget{id: ref.TargetID, kind: obj.Kind, position: t.Move.To}, nil
	default:
		return marchTarget{}, fmt.Errorf("%w: 不支持的目标种类 %s", domain.ErrInvalidArgument, obj.Kind)
	}
}

// PlanMarchTo 生成一段直线移动并写回 troop.Move，供出征、召回回城、
// 到达仲裁回城复用。
//
// 参数 intent 决定 Phase：
//   - 采集/攻击意图 → MARCHING；
//   - 其他（回城调用传 Unspecified）→ RETURNING。
//
// 返回 MoveSegment 方便调用方直接读 EndAtMS 写回执，避免再去 troop.Move
// 里翻（虽然结果一样，但显式返回值让“这段移动是刚生成的”一目了然）。
func (p *DefaultPlanner) PlanMarchTo(troop *domain.Troop, from, to domain.Pos, speedUnitsPerSec, nowMS int64, moveID string, intent domain.MarchIntent) (*domain.MoveSegment, error) {
	if troop == nil {
		return nil, fmt.Errorf("%w: 部队不能为空", domain.ErrInvalidArgument)
	}
	// inBounds 由规划器注入：spatial 不依赖 Topology，边界规则由上层给。
	seg, err := spatial.BuildMove(from, to, speedUnitsPerSec, nowMS, moveID, p.topo.InBounds)
	if err != nil {
		return nil, fmt.Errorf("部队 %d 生成移动段: %w", troop.Header.ID, err)
	}
	troop.Move = seg
	if intent.Valid() {
		troop.Phase = domain.TroopPhaseMarching
	} else {
		troop.Phase = domain.TroopPhaseReturning
	}
	return seg, nil
}
