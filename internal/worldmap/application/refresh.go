package application

// refresh.go 实现富饶块刷新（文档 10.5）。
//
// 关键设计：
//   - 当前数量从**已提交对象重算**（snap.ResourcesOfBlock），不读独立
//     计数器——独立计数是第二真源，迟早与对象表对不上；
//   - 候选位置与档位抽签全部来自 (block,generation,slot) 派生的确定性
//     种子，重试得到同一序列（重试不能重新抽奖，10.5 第 5 条）；
//   - 选址失败计入 skipped 而不无限循环（10.5 第 4 条）；
//   - 幂等以“该 generation 已生成数量达到目标”判定（M1 近似），
//     M2 用 map_refresh_slot 表按 slot 精确判重。

import (
	"fmt"

	"game_study/internal/worldmap/domain"
)

// PlanRefresh 生成一批资源点创建计划。
func (p *DefaultPlanner) PlanRefresh(snap *SceneSnapshot, env domain.CommandEnvelope, cmd Command, nowMS int64) (*domain.MutationPlan, error) {
	rp := cmd.Refresh
	plan := newPlan(snap, env, p.ids, nowMS)

	// 规则自洽校验。NewPlanner 已经校验过一次，这里是纵深防御：
	// 规划器也可能由别的构造函数（M2 的热更新路径）重新装配，
	// “Σmin <= max” 不成立时“先补最低数量”永远补不完。
	if err := p.rules.RichBlock.Validate(); err != nil {
		return nil, fmt.Errorf("富饶块 %d 刷新规则非法: %w", rp.RichBlockID, err)
	}

	existing := snap.ResourcesOfBlock(rp.RichBlockID)
	maxCount := p.rules.RichBlock.MaxCount

	// 幂等判定（M1 近似）：资源 Header 没有 slot 字段，无法逐 slot
	// 判重；用“同一 generation 已生成的数量 >= 目标”作为“这批已经做过”。
	// M2 增加 map_refresh_slot(world,block,generation,slot) 后可精确到
	// 单个 slot，支持中断后从下一个 slot 继续而不是整批重来。
	createdInGen := 0
	for _, r := range existing {
		if r.RefreshGeneration == rp.Generation {
			createdInGen++
		}
	}
	if createdInGen >= maxCount || len(existing) >= maxCount {
		setReceipt(plan, domain.CodeOK, map[string]any{
			"idempotent": true,
			"block_id":   rp.RichBlockID,
			"generation": rp.Generation,
			"existing":   int64(len(existing)),
			"target":     int64(maxCount),
		}, nowMS)
		return plan, nil
	}

	// 按 resourceType 统计现有数量。只统计本块内的资源点；其他块的
	// 同类型资源不影响本块配额。
	counts := make(map[int32]int, len(p.rules.RichBlock.Tiers))
	for _, r := range existing {
		counts[r.ResourceType]++
	}

	deficit := maxCount - len(existing)
	budget := deficit
	if budget > p.rules.RefreshBatchSize {
		// 每批最多 RefreshBatchSize 个：空间不足或写入过大都会把
		// 一个刷新命令拖成几分钟的长事务（10.5 第 4 条）。
		budget = p.rules.RefreshBatchSize
	}

	// 先补每档最低数量，再按权重补剩余额度。
	picks := make([]RichTier, 0, budget)
	for _, tier := range p.rules.RichBlock.Tiers {
		need := tier.Minimum - counts[tier.ResourceType]
		for i := 0; i < need && len(picks) < budget; i++ {
			picks = append(picks, tier)
		}
	}
	// 种子只派生一次，后续所有抽签共用同一条流：序列的位置是稳定的
	// （第 n 次抽签永远消耗第 n 个随机数），因此同 seed 必然同结果。
	seed := p.seeds(rp.RichBlockID, rp.Generation, int64(rp.Slot))
	rng := newSM64(seed)
	for len(picks) < budget {
		picks = append(picks, pickTierByWeight(rng, p.rules.RichBlock.Tiers))
	}

	minX, minY, maxX, maxY, ok := p.m1BlockRect(rp.RichBlockID)
	if !ok {
		return nil, fmt.Errorf("%w: 富饶块 %d 不在世界 %dx%d 范围内",
			domain.ErrInvalidArgument, rp.RichBlockID, p.topo.WidthCells, p.topo.HeightCells)
	}
	occupied := p.occupiedCells(snap)
	placed := make(map[cellKey]struct{}, len(picks))

	created := 0
	skipped := 0
	for _, tier := range picks {
		pos, cell, ok := p.pickResourcePosition(snap, rng, minX, minY, maxX, maxY, occupied, placed)
		if !ok {
			// 尝试次数用尽：记录 skipped 继续下一个对象，不回头重试，
			// 避免“空间不足时无限循环”。
			skipped++
			continue
		}
		placed[cell] = struct{}{}

		id := p.objectIDs()
		node := &domain.ResourceNode{
			Header: domain.ObjectHeader{
				WorldID:    env.WorldID,
				ID:         id,
				Kind:       domain.ObjectKindResource,
				OwnerScene: snap.SceneID(),
				Version:    1,
			},
			Position:          pos,
			RichBlockID:       rp.RichBlockID,
			ResourceType:      tier.ResourceType,
			ItemID:            tier.ItemID,
			Total:             tier.TotalPerNode,
			Remaining:         tier.TotalPerNode,
			RefreshGeneration: rp.Generation,
			ConfigVersion:     m1RuleVersion,
		}
		plan.ObjectsUpsert = append(plan.ObjectsUpsert, domain.ObjectSnapshot{
			Key:        node.Header.Key(),
			Kind:       domain.ObjectKindResource,
			OwnerScene: snap.SceneID(),
			Version:    node.Header.Version,
			Resource:   node,
		})
		// 静态占位：资源点占格，最终唯一约束在 map_occupancy
		// (world,cell_x,cell_y,layer)（文档 5.3）。
		plan.OccupancyClaims = append(plan.OccupancyClaims, domain.OccupancyClaim{
			CellX: cell[0], CellY: cell[1],
			Layer:     domain.OccupancyLayerStatic,
			ObjectKey: node.Header.Key(),
		})
		created++
	}

	setReceipt(plan, domain.CodeOK, map[string]any{
		"block_id":   rp.RichBlockID,
		"generation": rp.Generation,
		"slot":       int64(rp.Slot),
		"created":    int64(created),
		"skipped":    int64(skipped),
		"deficit":    int64(deficit),
		"batch_size": int64(p.rules.RefreshBatchSize),
	}, nowMS)
	if created > 0 {
		plan.OutboxEvents = append(plan.OutboxEvents, p.newEvent(snap, env, "RichBlockRefreshed", nil))
	}
	return plan, nil
}

// pickTierByWeight 按权重抽一档。权重总和在 Validate 中保证为正，
// 因此这里不会出现除零或空返回（兜底返回最后一档只为防御）。
func pickTierByWeight(rng *sm64, tiers []RichTier) RichTier {
	var total int64
	for _, t := range tiers {
		total += t.Weight
	}
	pick := rng.int63n(total)
	for _, t := range tiers {
		if pick < t.Weight {
			return t
		}
		pick -= t.Weight
	}
	return tiers[len(tiers)-1]
}

// m1BlockRect 返回富饶块的格矩形（左下闭、右上开）。
//
// M1 没有富饶块配置表：按固定边长把世界切成规则网格，blockID 按行优先
// 编号。M2 由 gamedata 注入真实矩形，并在启动时校验“不跨场景”。
func (p *DefaultPlanner) m1BlockRect(blockID int64) (minX, minY, maxX, maxY int64, ok bool) {
	if blockID < 0 {
		return 0, 0, 0, 0, false
	}
	perRow := (p.topo.WidthCells + m1RichBlockCells - 1) / m1RichBlockCells
	if perRow <= 0 {
		return 0, 0, 0, 0, false
	}
	bx := blockID % perRow
	by := blockID / perRow
	minX = bx * m1RichBlockCells
	minY = by * m1RichBlockCells
	if minX >= p.topo.WidthCells || minY >= p.topo.HeightCells {
		return 0, 0, 0, 0, false
	}
	maxX = minX + m1RichBlockCells
	if maxX > p.topo.WidthCells {
		maxX = p.topo.WidthCells
	}
	maxY = minY + m1RichBlockCells
	if maxY > p.topo.HeightCells {
		maxY = p.topo.HeightCells
	}
	return minX, minY, maxX, maxY, true
}

// occupiedCells 收集“已被静态对象占用”的格：现有资源点 + 主堡。
//
// 快照没有 AllCities（只有按 ID 查的 City），主堡位置只能从部队的
// HomeCityID 反查补齐。M2 应在 SceneSnapshot 增加 AllCities，否则
// “没有任何部队的主堡”只能依赖 BlockedCells 提供。这里不查全局
// occupancy 表：M1 由 map_occupancy 唯一键在 SQL 侧兜底，内存预检
// 只负责把明显冲突的候选格过滤掉。
func (p *DefaultPlanner) occupiedCells(snap *SceneSnapshot) map[cellKey]struct{} {
	occ := make(map[cellKey]struct{})
	for _, r := range snap.AllResources() {
		x, y := p.topo.UnitsToCells(r.Position)
		occ[cellKey{x, y}] = struct{}{}
	}
	for _, t := range snap.AllTroops() {
		if t.HomeCityID == 0 {
			continue
		}
		if c, ok := snap.City(t.HomeCityID); ok {
			x, y := p.topo.UnitsToCells(c.Position)
			occ[cellKey{x, y}] = struct{}{}
		}
	}
	return occ
}

// pickResourcePosition 在富饶块内抽一个合法格。
//
// 每个候选对象最多尝试 ResourcePositionAttempts 次；全部失败返回
// ok=false（调用方计 skipped）。候选格要求：
//   - 未被静态阻挡（地形/建筑）；
//   - 未被现有资源点或主堡占用；
//   - 未被本批已放置的对象占用（placed 是批内去重，不能只靠 occupied）；
//   - 落在当前场景（富饶块不跨场景是配置约束，这里做最后一道检查）。
func (p *DefaultPlanner) pickResourcePosition(snap *SceneSnapshot, rng *sm64, minX, minY, maxX, maxY int64, occupied map[cellKey]struct{}, placed map[cellKey]struct{}) (domain.Pos, cellKey, bool) {
	width := maxX - minX
	height := maxY - minY
	if width <= 0 || height <= 0 {
		return domain.Pos{}, cellKey{}, false
	}
	for attempt := 0; attempt < p.rules.ResourcePositionAttempts; attempt++ {
		cx := minX + rng.int63n(width)
		cy := minY + rng.int63n(height)
		cell := cellKey{cx, cy}

		if _, dup := occupied[cell]; dup {
			continue
		}
		if _, dup := placed[cell]; dup {
			continue
		}
		if snap.Blocked(cx, cy) {
			continue
		}
		// 落在格中心：格角坐标会让资源点贴在格子边界上，客户端
		// 渲染与“同格判定”都容易出现半个格的歧义。
		pos := p.topo.CellsToUnits(cx, cy)
		pos.X += p.topo.PositionUnitsCell / 2
		pos.Y += p.topo.PositionUnitsCell / 2
		if p.topo.SceneAt(pos) != snap.SceneID() {
			continue
		}
		return pos, cell, true
	}
	return domain.Pos{}, cellKey{}, false
}
