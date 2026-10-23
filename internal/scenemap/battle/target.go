package battle

import (
	"sort"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// selectTargets 选出本次 CAST 的全部命中目标（规格 §10.4 的四分支）。
//
//	1 只对自己：自己必中，再按范围补其他人；
//	2 指定落点：以落点为圆心/朝向选人；
//	3 没点目标：从自己位置动态选；
//	4 指定目标：先校验射程与阵营，失败降级为动态寻敌。
//
// 返回顺序：主目标在前，其余按"距离 → UUID"升序——同一份场景状态
// 必须得到同一份命中列表，否则伤害榜/掉落归属无法复现。
func (rt *Runtime) selectTargets(s *mgr.State, caster *Sprite, cfg *SkillConfig, sc *SkillContext) []*Sprite {
	if s == nil || caster == nil || cfg == nil {
		return nil
	}
	if cfg.TargetType == TargetSelf {
		// 只对自己：单体直接返回自己；范围技在下面把自己放进列表首位。
		if cfg.Area == AreaSingle {
			return []*Sprite{caster}
		}
	}

	var primary *Sprite
	// 分支 4：客户端点了目标。
	if sc.Target.Valid() && sc.Target != caster.UUID {
		if t := rt.spriteOf(s, sc.Target); t != nil && t.Alive() && rt.validTarget(caster, t, cfg) {
			// 射程校验：超出射程不是"打空"，而是非法目标——由调用方
			// 决定是回错误还是降级。这里降级为动态寻敌（规格 §10.4）。
			if grid.Chebyshev(caster.Pos, t.Pos) <= cfg.Range {
				primary = t
			}
		}
	}
	// 分支 2：指定落点（以落点为中心，不依赖某个具体目标）。
	center := caster.Pos
	if sc.SpecPos != nil {
		center = *sc.SpecPos
	} else if primary != nil {
		center = primary.Pos
	}

	// 友方单体：点名了友军就打友军，没点就给自己（治疗/增益的默认目标）。
	if cfg.Area == AreaSingle && cfg.TargetType == TargetAlly {
		if primary != nil {
			return []*Sprite{primary}
		}
		return []*Sprite{caster}
	}

	if cfg.Area == AreaSingle && cfg.TargetType != TargetSelf && cfg.TargetType != TargetAlly {
		// 分支 3：没点目标 → 取射程内最近的可攻击目标。
		if primary == nil {
			primary = rt.nearest(s, caster, cfg, caster.Pos)
		}
		if primary == nil {
			return nil
		}
		if sc.PrimaryTarget == 0 {
			sc.PrimaryTarget = primary.UUID
		}
		return []*Sprite{primary}
	}

	// 范围技：以 center 为圆心/朝向，收集全部合法目标。
	out := make([]*Sprite, 0, cfg.TargetNum+1)
	if cfg.TargetType == TargetSelf || cfg.TargetType == TargetAlly {
		out = append(out, caster)
	}
	for _, cand := range rt.candidates(s) {
		if cand.UUID == caster.UUID {
			continue
		}
		if !cand.Alive() || !rt.validTarget(caster, cand, cfg) {
			continue
		}
		if !rt.inArea(caster, cand, center, cfg, sc.Dir) {
			continue
		}
		out = append(out, cand)
	}
	// 主目标优先：点名的目标哪怕在范围边缘也要排在第一个（伤害榜归因）。
	if primary != nil {
		out = append([]*Sprite{primary}, filterOut(out, primary.UUID)...)
	}
	// 截断人数：主目标之后按距离取最近的。
	if n := int(cfg.TargetNum); n > 0 && len(out) > n {
		head := out[:1]
		rest := out[1:]
		sortSprites(rest, caster.Pos)
		out = append(head, rest[:n-1]...)
	}
	if len(out) == 0 {
		return nil
	}
	if sc.PrimaryTarget == 0 {
		sc.PrimaryTarget = out[0].UUID
	}
	return out
}

// candidates 收集场景里所有可能的战斗单位（按 UUID 升序）。
//
// 为什么不用 Index.EntitiesAt 逐格取：技能范围最大也就十几格，
// 全量扫描几十个实体比"按 9 宫格取"更简单，也不会漏掉刚换格的实体。
// 实体上千时再换空间索引——那是优化，不是正确性。
func (rt *Runtime) candidates(s *mgr.State) []*Sprite {
	out := make([]*Sprite, 0, len(s.NPES)+len(s.Users)+len(s.Pets))
	for _, id := range s.NpeUUIDs() {
		if info := s.NPES[id]; info != nil && info.Alive() {
			if sp := rt.spriteOf(s, id); sp != nil {
				out = append(out, sp)
			}
		}
	}
	for _, id := range s.UserUUIDs() {
		if sp := rt.spriteOf(s, id); sp != nil {
			out = append(out, sp)
		}
	}
	for id := range s.Pets {
		if sp := rt.spriteOf(s, id); sp != nil {
			out = append(out, sp)
		}
	}
	return out
}

// validTarget 判断阵营/类型是否可选中（规格 ttype_can_atk 的简化）。
//
// PK 模式（和平/全体/帮派/阵营）在演示框架里收敛成"阵营是否相同"：
// 真实项目应在这里接 PK 模式表，接口保持不变。
func (rt *Runtime) validTarget(caster, t *Sprite, cfg *SkillConfig) bool {
	if caster == nil || t == nil || !t.Alive() {
		return false
	}
	switch cfg.TargetType {
	case TargetSelf:
		return t.UUID == caster.UUID
	case TargetAlly:
		return t.Camp == caster.Camp
	case TargetEnemy:
		if t.UUID == caster.UUID {
			return false
		}
		// 同主人（宠物与主人）永远不是敌人。
		if caster.Owner != 0 && caster.Owner == t.Owner {
			return false
		}
		return t.Camp != caster.Camp
	default: // TargetAll
		return true
	}
}

// inArea 判断目标是否落在技能的范围内。
func (rt *Runtime) inArea(caster, t *Sprite, center scenemap.GridPos, cfg *SkillConfig, dir scenemap.Dir) bool {
	switch cfg.Area {
	case AreaCircle:
		r := cfg.AreaRadius
		if r <= 0 {
			r = 1
		}
		return grid.Chebyshev(center, t.Pos) <= r
	case AreaSector:
		deg := cfg.SectorDeg
		if deg <= 0 {
			deg = 60
		}
		r := cfg.AreaRadius
		if r <= 0 {
			r = cfg.Range
		}
		d := dir
		if d == scenemap.DirNone {
			d = caster.Dir
		}
		return grid.InSector(caster.Pos, t.Pos, d, deg, r)
	case AreaRect:
		r := cfg.AreaRadius
		if r <= 0 {
			r = 2
		}
		return grid.Square(center, r).Contains(t.Pos)
	default:
		return t.UUID == caster.UUID
	}
}

// nearest 返回射程内最近的可攻击目标（同距离取 UUID 小的）。
func (rt *Runtime) nearest(s *mgr.State, caster *Sprite, cfg *SkillConfig, from scenemap.GridPos) *Sprite {
	var best *Sprite
	bestD := int32(1 << 30)
	for _, cand := range rt.candidates(s) {
		if cand.UUID == caster.UUID || !rt.validTarget(caster, cand, cfg) {
			continue
		}
		d := grid.Chebyshev(from, cand.Pos)
		if d > cfg.Range {
			continue
		}
		if d < bestD || (d == bestD && best != nil && cand.UUID < best.UUID) {
			best, bestD = cand, d
		}
	}
	return best
}

// sortSprites 按"到 origin 的距离 → UUID"升序排序。
func sortSprites(list []*Sprite, origin scenemap.GridPos) {
	sort.Slice(list, func(i, j int) bool {
		di := grid.Chebyshev(origin, list[i].Pos)
		dj := grid.Chebyshev(origin, list[j].Pos)
		if di != dj {
			return di < dj
		}
		return list[i].UUID < list[j].UUID
	})
}

// filterOut 去掉指定 UUID（主目标已经单独放前面）。
func filterOut(list []*Sprite, uuid scenemap.UUID) []*Sprite {
	out := list[:0]
	for _, sp := range list {
		if sp.UUID != uuid {
			out = append(out, sp)
		}
	}
	return out
}
