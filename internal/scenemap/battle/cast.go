package battle

import (
	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// prefixPerGrid 是"每格射程增加的前摇比例"（万分比）。
//
// 规格 §10.2 的公式：base_prefix / (speed_coef * (1 + distance * per_grid_ms))。
// 这里把 per_grid_ms 收敛成固定比例 3%/格：前摇是表现参数，逐技能配置
// 会让配置表膨胀，而"距离越远飞得越久"的体感只取决于量级。
const prefixPerGrid int64 = 300

// CastCommand 是"客户端报技能"的命令（规格 §12.1 的 16000）。
//
// 为什么是 Command 而不是直接函数：出手必须与移动/进图/刷怪在同一条
// 串行队列里执行，否则会出现"怪已经被别处删了，技能还在按旧目标结算"。
type CastCommand struct {
	// Cast 客户端参数。
	Cast Cast
}

// NewCastCommand 构造施法命令。
func NewCastCommand(c Cast) mgr.Command { return &CastCommand{Cast: c} }

// Name 实现 mgr.Command。
func (c *CastCommand) Name() string { return "battle.cast" }

// MayNoop 实现 noopCommand：被拒绝的出手（冷却/禁攻）不改状态。
func (c *CastCommand) MayNoop() bool { return true }

// Apply 实现 mgr.Command。
func (c *CastCommand) Apply(s *mgr.State) (any, error) {
	rt, ok := From(s)
	if !ok {
		// 运行时没装配：按"本图不结算"处理，而不是 panic。
		return CastResult{SkillID: c.Cast.SkillID, Code: ErrCodeNoBattle}, nil
	}
	return rt.cast(s, c.Cast), nil
}

// cast 校验并启动一次出手（LAUNCH 立即，CAST 延后）。
func (rt *Runtime) cast(s *mgr.State, in Cast) CastResult {
	res := CastResult{SkillID: in.SkillID}
	now := rt.now(s)

	// ① 主城拦截（规格 §13.9：玩家侧 + MGR 侧双重拦截，这里是在 MGR 侧）。
	if rt.opts.NoBattle {
		res.Code = ErrCodeNoBattle
		return res
	}
	// ② 配置必须存在：查不到显式报错，不默认成普攻（规格 §13.13）。
	cfg, err := rt.table.Skill(in.SkillID)
	if err != nil {
		res.Code = ErrCodeMissingSkill
		return res
	}
	// ③ 施法者存在且可行动。
	A := rt.spriteOf(s, in.Caster)
	if A == nil {
		res.Code = ErrCodeNoSprite
		return res
	}
	if !A.Alive() {
		res.Code = ErrCodeDead
		return res
	}
	if A.Pause {
		res.Code = ErrCodeBanAttack
		return res
	}
	if A.HasBuffKind(BuffBanAttack, now) || A.HasBuffKind(BuffStun, now) {
		res.Code = ErrCodeBanAttack
		return res
	}
	// ④ 镜像白名单（规格 §13.15）：移动与施法两个入口必须同时维护。
	if A.Kind == scenemap.EntityKindMirror && !rt.MirrorAllowed() {
		res.Code = ErrCodeMirrorForbidden
		return res
	}
	// ⑤ 冷却。
	if left := rt.cooldownLeft(A.UUID, cfg.ID, now); left > 0 {
		res.Code = ErrCodeCooldown
		return res
	}
	rt.setCooldown(A.UUID, cfg.ID, now, rt.scaleMS(s, cfg.CooldownMS))
	A.Fight = true
	A.LastFightAt = now

	dir := in.Dir
	if dir == scenemap.DirNone {
		dir = A.Dir
	}

	// ⑥ LAUNCH：只处理释放 Buff/被动/时停，不产生伤害与飘字。
	launchSC := &SkillContext{
		SkillID: cfg.ID, Caster: A.UUID, Target: in.Target,
		TargetPos: in.TargetPos, Action: ActionLaunch, Dir: dir,
	}
	rt.runLaunch(s, A, cfg, launchSC, now)

	// ⑦ CAST：按前摇 + 弹道延迟投递。
	dist := rt.castDistance(s, A, in)
	prefix := rt.prefixMS(s, A, cfg, dist)
	res.PrefixMS = prefix
	res.Accepted = true
	res.Target = in.Target
	// 表现：告诉客户端"开始前摇"（可选协议，不影响结算）。
	_ = s.Broadcast(CastStart{
		Caster: A.UUID, SkillID: cfg.ID, Target: in.Target,
		TargetPos: in.TargetPos, PrefixMS: prefix,
	}, mgr.BroadcastAround(A.Pos, 8))

	plan := castPlan{caster: A.UUID, skillID: cfg.ID, target: in.Target, targetPos: in.TargetPos, dir: dir, part: 0}
	rt.Schedule(s, prefix, "battle.cast.hit", func(st *mgr.State) {
		rt.runCast(s, plan)
	})
	return res
}

// castPlan 是一次已受理出手的后续参数（多段/弹道延迟都基于它继续排）。
type castPlan struct {
	caster    scenemap.UUID
	skillID   int32
	target    scenemap.UUID
	targetPos *scenemap.GridPos
	dir       scenemap.Dir
	// part 当前段号（0 起）。
	part int32
}

// runLaunch 执行前摇（规格 §10.6.2）。
//
// 有目标就对目标跑一次 LAUNCH 公式（挂释放 Buff）；没目标时只跑被动，
// 不凭空造一个目标——"前摇阶段就把范围里最近的怪锁死"会让玩家在
// 前摇期间无法通过走位躲开。
func (rt *Runtime) runLaunch(s *mgr.State, A *Sprite, cfg *SkillConfig, sc *SkillContext, now int64) {
	if len(cfg.LaunchBuffs) == 0 && len(cfg.PassiveIDs) == 0 {
		return
	}
	if sc.Target.Valid() {
		if T := rt.spriteOf(s, sc.Target); T != nil && T.Alive() {
			rt.calcSkill(s, A, T, cfg, sc, now)
			return
		}
	}
	// 没有目标：只登记被动，不产生任何目标侧效果。
	sc.Action = ActionLaunch
	sc.TriggerSkills = append(sc.TriggerSkills, cfg.PassiveIDs)
}

// runCast 执行一次命中（规格 §10.3 / §10.5）。
func (rt *Runtime) runCast(s *mgr.State, plan castPlan) {
	if rt == nil || s == nil {
		return
	}
	now := rt.now(s)
	cfg, err := rt.table.Skill(plan.skillID)
	if err != nil {
		return
	}
	A := rt.spriteOf(s, plan.caster)
	if A == nil || !A.Alive() {
		return
	}
	// 远程弹道：CAST 到达后再按格子距离延迟一次才真正命中（规格 §13.22）。
	// 只对第一段做，后续段不再叠加（否则多段技会越打越慢）。
	if plan.part == 0 && cfg.RemoteDelayPerGridMS > 0 {
		dist := rt.castDistance(s, A, Cast{Target: plan.target, TargetPos: plan.targetPos, Caster: plan.caster})
		if dist > 0 {
			delay := rt.scaleMS(s, int64(dist)*cfg.RemoteDelayPerGridMS)
			next := plan
			next.part = -1 // 标记"弹道延迟已加过"
			rt.Schedule(s, delay, "battle.cast.remote", func(st *mgr.State) {
				next.part = 0
				rt.runCast(st, next)
			})
			return
		}
	}

	sc := &SkillContext{
		SkillID: cfg.ID, Caster: A.UUID, Target: plan.target,
		TargetPos: plan.targetPos, Action: ActionCast, Dir: plan.dir,
		HarmIndex: plan.part,
	}
	targets := rt.selectTargets(s, A, cfg, sc)
	if len(targets) == 0 {
		// 没有目标：技能仍然消耗了冷却，但没有飘字（客户端自己处理空放）。
		return
	}

	// 逐个目标结算，合并结果。
	var deads []scenemap.UUID
	events := make(map[scenemap.UUID]*SkillResultEvent)
	var suck int64
	for _, T := range targets {
		beforeHP := T.HP
		beforeBuffs := buffIDs(T)
		one := &SkillContext{
			SkillID: cfg.ID, Caster: A.UUID, Target: T.UUID,
			TargetPos: plan.targetPos, Action: ActionCast,
			Dir: plan.dir, HarmIndex: plan.part,
		}
		rt.calcSkill(s, A, T, cfg, one, now)
		if one.ErrorCode != 0 && one.ErrorCode != ErrCodeBadTarget {
			continue
		}
		suck += one.SuckBlood

		// 表现回路：飘字按"每个目标一条"广播（规格 §10.5 的 16000 S2C）。
		if len(one.Harms) > 0 {
			crit, bok := hasTag(one.Harms, HarmCrit), hasTag(one.Harms, HarmBok)
			_ = s.Broadcast(HarmBroadcast{
				SkillID: cfg.ID, Caster: A.UUID, Target: T.UUID,
				HP: T.HP, MaxHP: T.MaxHP, Harms: one.Harms,
				Crit: crit, Bok: bok, Aoe: cfg.Area != AreaSingle,
				DebugInfo: one.DebugInfo,
			}, mgr.BroadcastAround(T.Pos, 8))
		}
		// Buff 变化广播。
		added := diffIDs(buffIDs(T), beforeBuffs)
		if len(added) > 0 {
			rt.broadcastBuffSync(s, T, added, nil)
		}
		// 结果回路：只发给"目标的主人"（玩家侧实体才有主人）。
		owner := rt.ownerOf(s, T.UUID)
		if owner.Valid() {
			ev := events[owner]
			if ev == nil {
				ev = &SkillResultEvent{SkillID: cfg.ID, Caster: A.UUID}
				events[owner] = ev
			}
			ev.Targets = append(ev.Targets, TargetResult{
				UUID: T.UUID, HP: T.HP, MaxHP: T.MaxHP,
				Delta: beforeHP - T.HP, Dead: T.Dead,
				Harms: one.Harms, BuffsAdded: added,
			})
		}
		if T.Dead {
			deads = append(deads, T.UUID)
		}
		// 进战：受击者也要标记战斗状态（脱战计时从这刻起算）。
		if T.Alive() {
			T.Fight = true
			T.LastFightAt = now
		}
		// 仇恨：伤害即仇恨（规格 §10.5 step 7）。
		if T.IsNpc() {
			rt.addHate(s, T.UUID, A.UUID, maxInt64(beforeHP-T.HP, 1), now)
		}
	}

	// 吸血合并发一次（规格 §10.5：多目标吸血不逐个发包）。
	if suck > 0 {
		owner := rt.ownerOf(s, A.UUID)
		if owner.Valid() {
			ev := events[owner]
			if ev == nil {
				ev = &SkillResultEvent{SkillID: cfg.ID, Caster: A.UUID}
				events[owner] = ev
			}
			ev.SuckBlood = suck
		}
	}

	// 死亡链：所有目标结算完之后统一处理，避免"边遍历边删 NPES"。
	for _, id := range deads {
		if sp := rt.sprites[id]; sp != nil {
			rt.handleDead(s, sp, A.UUID, now)
		}
	}

	// 事件投递（在 MGR goroutine 内调用 Notify，由装配层决定异步化）。
	for owner, ev := range events {
		if rt.opts.Notify != nil {
			rt.opts.Notify(owner, *ev)
		}
	}

	// 子弹 / 火墙：一次出手只生成一个（不是每个目标一个）。
	rt.spawnEffect(s, A, cfg, sc, now)

	// 多段：第 0 段打完后按攻速缩放间隔排下一段（规格 §10.3 step 3）。
	parts := cfg.Parts
	if parts < 1 {
		parts = 1
	}
	if plan.part >= 0 && plan.part+1 < parts {
		next := plan
		next.part = plan.part + 1
		interval := rt.scaleMS(s, cfg.PartIntervalMS)
		if interval <= 0 {
			interval = 100
		}
		// 段间隔吃攻速：攻速越高，多段越紧凑。
		if A.SpeedAtk > 0 {
			interval = interval * PermilleOne / A.SpeedAtk
		}
		rt.Schedule(s, interval, "battle.cast.part", func(st *mgr.State) {
			rt.runCast(st, next)
		})
	}
}

// castDistance 计算施法者到目标（或目标格）的格子距离。
func (rt *Runtime) castDistance(s *mgr.State, A *Sprite, in Cast) int32 {
	if in.TargetPos != nil {
		return grid.Chebyshev(A.Pos, *in.TargetPos)
	}
	if in.Target.Valid() {
		if T := rt.spriteOf(s, in.Target); T != nil {
			return grid.Chebyshev(A.Pos, T.Pos)
		}
	}
	return 0
}

// prefixMS 计算前摇（规格 §10.2 / §13.21）。
//
//	prefix = base / (speed_atk * (1 + distance * per_grid))
//
// 全程整数：分母用万分比表示，最后再按地图倍速缩放（倍速统一在外层处理）。
func (rt *Runtime) prefixMS(s *mgr.State, A *Sprite, cfg *SkillConfig, dist int32) int64 {
	base := cfg.PrefixMS
	if base <= 0 {
		return 0
	}
	spd := A.SpeedAtk
	if spd <= 0 {
		spd = PermilleOne
	}
	denom := spd * (PermilleOne + int64(dist)*prefixPerGrid) / PermilleOne
	if denom <= 0 {
		denom = PermilleOne
	}
	out := base * PermilleOne / denom
	return rt.scaleMS(s, out)
}

// scaleMS 按地图倍速缩放一个毫秒值。
//
// 倍速是"玩法节奏"：200% 档下 1000ms 前摇变 500ms。触发器秒表不吃倍速，
// 但技能/子弹/刷怪吃——按语义选择，不能一刀切。
func (rt *Runtime) scaleMS(s *mgr.State, ms int64) int64 {
	if ms <= 0 {
		return 0
	}
	speed := int64(1000)
	if s != nil {
		if p := s.SpeedPermille(); p > 0 {
			speed = int64(p)
		}
	}
	out := ms * 1000 / speed
	if out < 1 {
		out = 1
	}
	return out
}

// spawnEffect 生成子弹/火墙（规格 §10.6.7 step 8）。
func (rt *Runtime) spawnEffect(s *mgr.State, A *Sprite, cfg *SkillConfig, sc *SkillContext, now int64) {
	switch cfg.Effect {
	case EffectBullet:
		rt.seq++
		id := scenemap.UUID(-1_000_000 - rt.seq)
		if s.Bullets == nil {
			s.Bullets = make(map[scenemap.UUID]*mgr.Bullet)
		}
		target := A.Pos
		if sc.PrimaryTarget.Valid() {
			if T := rt.sprites[sc.PrimaryTarget]; T != nil {
				target = T.Pos
			}
		}
		if sc.TargetPos != nil {
			target = *sc.TargetPos
		}
		dir := grid.DirOf(A.Pos, target)
		if dir == scenemap.DirNone {
			dir = A.Dir
		}
		speed := cfg.BulletSpeed
		if speed <= 0 {
			speed = 8
		}
		s.Bullets[id] = &mgr.Bullet{
			UUID: id, Owner: A.UUID, SkillID: cfg.ID,
			Pos: A.Pos, Pixel: grid.DefaultGeometry().CellCenter(A.Pos),
			Dir: dir, Speed: speed,
			ExpireAt:   now + 3000,
			Target:     target,
			SubSkillID: cfg.SubSkillID,
			NextAt:     now,
		}
		s.Touch()
	case EffectFirewell:
		rt.seq++
		id := scenemap.UUID(-2_000_000 - rt.seq)
		if s.Firewells == nil {
			s.Firewells = make(map[scenemap.UUID]*mgr.Firewell)
		}
		r := cfg.FirewellRadius
		if r <= 0 {
			r = 1
		}
		center := A.Pos
		if sc.TargetPos != nil {
			center = *sc.TargetPos
		}
		dmg := cfg.FirewellDamage
		if dmg <= 0 {
			dmg = 10
		}
		interval := cfg.FirewellTickMS
		if interval <= 0 {
			interval = 500
		}
		s.Firewells[id] = &mgr.Firewell{
			UUID: id, Owner: A.UUID, GridPos: center, Radius: r,
			Damage: dmg, ExpireAt: now + 5000, NextTickAt: now + interval,
		}
		s.Touch()
	}
}

// tickBullets 推进子弹（每 500ms 一跳，按 Speed 走格）。
func (rt *Runtime) tickBullets(s *mgr.State, now int64) {
	if len(s.Bullets) == 0 {
		return
	}
	for id, b := range s.Bullets {
		if b == nil {
			continue
		}
		if b.ExpireAt > 0 && now >= b.ExpireAt {
			delete(s.Bullets, id)
			continue
		}
		if b.NextAt > 0 && now < b.NextAt {
			continue
		}
		// 每跳走 1 格（Speed 只决定跳多久一次，简化实现见设计文档）。
		b.Pos = stepToward(b.Pos, b.Target)
		b.NextAt = now + 120
		if b.Pos == b.Target || grid.Chebyshev(b.Pos, b.Target) <= 0 {
			// 命中：以落点为指定位置结算子技能（规格 §10.11）。
			owner := b.Owner
			sub := b.SubSkillID
			pos := b.Pos
			delete(s.Bullets, id)
			s.Touch()
			if sub > 0 {
				rt.runSubSkill(s, owner, sub, pos, now)
			}
			continue
		}
		s.Touch()
	}
}

// tickFirewells 结算火墙周期伤害。
func (rt *Runtime) tickFirewells(s *mgr.State, now int64) {
	if len(s.Firewells) == 0 {
		return
	}
	for id, fw := range s.Firewells {
		if fw == nil {
			continue
		}
		if fw.ExpireAt > 0 && now >= fw.ExpireAt {
			delete(s.Firewells, id)
			continue
		}
		if fw.NextTickAt > 0 && now < fw.NextTickAt {
			continue
		}
		fw.NextTickAt = now + 500
		for _, sp := range rt.candidates(s) {
			if sp == nil || !sp.Alive() || !sp.IsNpc() {
				continue
			}
			if grid.Chebyshev(fw.GridPos, sp.Pos) > fw.Radius {
				continue
			}
			real, dead, _ := rt.applyRealHarm(s, sp, fw.Damage, fw.Owner, now)
			if real <= 0 {
				continue
			}
			_ = s.Broadcast(HarmBroadcast{
				SkillID: 0, Caster: fw.Owner, Target: sp.UUID,
				HP: sp.HP, MaxHP: sp.MaxHP,
				Harms: []HarmEntry{{Tag: HarmAOE, Val: real}},
			}, mgr.BroadcastAround(sp.Pos, 6))
			if dead {
				rt.handleDead(s, sp, fw.Owner, now)
			}
		}
	}
}

// runSubSkill 以指定落点结算子技能（子弹命中/火墙触发）。
func (rt *Runtime) runSubSkill(s *mgr.State, caster scenemap.UUID, skillID int32, pos scenemap.GridPos, now int64) {
	cfg, err := rt.table.Skill(skillID)
	if err != nil {
		return
	}
	rt.Schedule(s, 0, "battle.subskill", func(st *mgr.State) {
		A := rt.spriteOf(st, caster)
		if A == nil || !A.Alive() {
			return
		}
		p := pos
		sc := &SkillContext{
			SkillID: cfg.ID, Caster: caster, Action: ActionCast,
			SpecPos: &p, Dir: A.Dir,
		}
		targets := rt.selectTargets(st, A, cfg, sc)
		for _, T := range targets {
			one := &SkillContext{SkillID: cfg.ID, Caster: caster, Target: T.UUID, Action: ActionCast, SpecPos: &p}
			t := rt.now(st)
			rt.calcSkill(st, A, T, cfg, one, t)
			if len(one.Harms) > 0 {
				_ = st.Broadcast(HarmBroadcast{
					SkillID: cfg.ID, Caster: caster, Target: T.UUID,
					HP: T.HP, MaxHP: T.MaxHP, Harms: one.Harms, Aoe: true,
				}, mgr.BroadcastAround(T.Pos, 8))
			}
			if T.Dead {
				rt.handleDead(st, T, caster, t)
			} else if T.IsNpc() {
				rt.addHate(st, T.UUID, caster, 1, t)
			}
		}
	})
}

// stepToward 朝目标格走一步（8 向，先对角后直线）。
func stepToward(from, to scenemap.GridPos) scenemap.GridPos {
	dx, dy := to.X-from.X, to.Y-from.Y
	if dx == 0 && dy == 0 {
		return from
	}
	nx, ny := from.X, from.Y
	if dx != 0 {
		if dx > 0 {
			nx++
		} else {
			nx--
		}
	}
	if dy != 0 {
		if dy > 0 {
			ny++
		} else {
			ny--
		}
	}
	return scenemap.GridPos{X: nx, Y: ny}
}

// buffIDs 返回当前 Buff ID 列表（顺序稳定）。
func buffIDs(sp *Sprite) []int32 {
	if sp == nil {
		return nil
	}
	out := make([]int32, 0, len(sp.Buffs))
	for _, b := range sp.Buffs {
		if b != nil {
			out = append(out, b.ID)
		}
	}
	return out
}

// diffIDs 返回 after 中新增的 ID。
func diffIDs(after, before []int32) []int32 {
	seen := make(map[int32]bool, len(before))
	for _, id := range before {
		seen[id] = true
	}
	var out []int32
	for _, id := range after {
		if !seen[id] {
			out = append(out, id)
		}
	}
	return out
}

// hasTag 判断飘字里是否有某标签。
func hasTag(list []HarmEntry, tag HarmTag) bool {
	for _, h := range list {
		if h.Tag == tag {
			return true
		}
	}
	return false
}

// maxInt64 返回较大值。
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
