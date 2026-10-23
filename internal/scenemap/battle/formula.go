package battle

import (
	"fmt"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/mgr"
)

// calcCtx 是"一个攻击者打一个目标"的结算上下文。
//
// 拆成结构体而不是一路传 8 个参数：公式链有 10+ 步，每步都可能改写
// 伤害值/飘字/错误码，参数列表会随着功能增加无限膨胀。
type calcCtx struct {
	rt  *Runtime
	s   *mgr.State
	A   *Sprite
	T   *Sprite
	cfg *SkillConfig
	sc  *SkillContext
	now int64
	// debug 记录实际执行过的步骤（写入 SkillContext.DebugInfo）。
	debug []string
}

// calcSkill 执行"一次出手对一个目标"的完整公式链（规格 §10.6）。
//
// # 流水线而不是一坨 if
//
// 每一步只做一件事并把"下一步是谁"写在注释里；掉序（例如先吸血后扣血）
// 是这类系统最隐蔽的 bug，按规格顺序排列能让 review 只对照文档即可。
//
// 返回的 SkillContext 是**本目标的视图**，调用方负责把 harms/deads
// 合并到整次出手的总上下文里（多目标时每个目标各算一遍）。
func (rt *Runtime) calcSkill(s *mgr.State, A, T *Sprite, cfg *SkillConfig, sc *SkillContext, now int64) *SkillContext {
	c := &calcCtx{rt: rt, s: s, A: A, T: T, cfg: cfg, sc: sc, now: now}

	// ---- step 1: 前置拦截（规格 §10.6.1）----
	if A == nil || T == nil {
		sc.ErrorCode = ErrCodeBadTarget
		return sc
	}
	if !A.Alive() {
		sc.ErrorCode = ErrCodeDead
		return sc
	}
	if A.HasBuffKind(BuffBanAttack, now) {
		// 禁攻是"整条链取消"，不是"打 0 伤害"：客户端不该看到飘字。
		sc.ErrorCode = ErrCodeBanAttack
		c.trace("ban_attack")
		sc.DebugInfo = c.join()
		return sc
	}
	if !T.Alive() {
		sc.ErrorCode = ErrCodeBadTarget
		c.trace("target_dead")
		sc.DebugInfo = c.join()
		return sc
	}
	// 无敌：伤害 0，但保留一次 SUPER 飘字（客户端据此播"免疫"）。
	if T.HasBuffKind(BuffSuper, now) {
		sc.Harms = append(sc.Harms, HarmEntry{Tag: HarmSuper, Val: 0})
		c.trace("target_super")
		sc.DebugInfo = c.join()
		return sc
	}

	// ---- step 2: LAUNCH 只处理释放 Buff/被动，不产生伤害（§10.6.2）----
	if sc.Action == ActionLaunch {
		c.trace("launch")
		for _, id := range cfg.LaunchBuffs {
			rt.applyBuff(T, id, A.UUID, cfg.ID, now)
		}
		// LAUNCH 的被动只是"登记"，真正释放由 CAST 阶段统一触发，
		// 避免同一发技能在前摇与命中各触发一次同样的被动。
		sc.TriggerSkills = append(sc.TriggerSkills, cfg.PassiveIDs)
		sc.DebugInfo = c.join()
		return sc
	}

	// ---- step 3: CAST 的伤害类型分流（§10.6.3）----
	switch cfg.HarmType {
	case HarmTypeNotDamage:
		c.trace("not_damage")
		c.applyHitBuffs()
		sc.DebugInfo = c.join()
		return sc
	case HarmTypeOnlyAOE:
		c.trace("only_aoe")
		c.dealAoe()
		sc.DebugInfo = c.join()
		return sc
	case HarmTypeSuckBlood:
		c.trace("only_suck")
		c.lifeSteal(T.HP * cfg.SuckPermille / PermilleOne)
		sc.DebugInfo = c.join()
		return sc
	}
	// 同主人（宠物打自己人）按纯效果处理。
	if A.Owner != 0 && A.Owner == T.Owner && A.UUID != T.UUID {
		c.trace("same_owner")
		c.applyHitBuffs()
		sc.DebugInfo = c.join()
		return sc
	}

	// ---- step 4: 斩杀（只对小怪，且目标没有免死）----
	if cfg.SeckillPermille > 0 && T.IsNpc() && !T.HasBuffKind(BuffUndie, now) && rt.chance(cfg.SeckillPermille) {
		c.trace("seckill")
		sc.Harms = append(sc.Harms, HarmEntry{Tag: HarmSeckill, Val: T.HP})
		real, dead, _ := rt.applyRealHarm(s, T, T.HP, A.UUID, now)
		sc.RealHarm = real
		if dead {
			sc.Deads = append(sc.Deads, T.UUID)
		}
		sc.DebugInfo = c.join()
		return sc
	}

	// ---- step 5: 闪避（§10.6.4）----
	if rt.dodged(A, T) {
		c.trace("miss")
		sc.Harms = append(sc.Harms, HarmEntry{Tag: HarmMiss, Val: 0})
		sc.DebugInfo = c.join()
		return sc
	}

	// ---- step 6: 伤害公式（§10.6.5）----
	val, crit, bok := c.damage()
	c.trace(fmt.Sprintf("damage=%d crit=%v bok=%v", val, crit, bok))

	// ---- step 7: 落地扣血（§10.6.6）----
	tag := HarmNormal
	if crit {
		tag = HarmCrit
	}
	if bok {
		// 格挡是"额外标记 + 减伤"，与普通/暴击并存：先插 BOK 再插主伤害。
		sc.Harms = append(sc.Harms, HarmEntry{Tag: HarmBok, Val: 0})
	}
	real, dead, absorbed := rt.applyRealHarm(s, T, val, A.UUID, now)
	sc.Harms = append(sc.Harms, HarmEntry{Tag: HarmAbsorb, Val: absorbed})
	sc.Harms = append(sc.Harms, HarmEntry{Tag: tag, Val: val})
	sc.RealHarm = real
	if dead {
		sc.Deads = append(sc.Deads, T.UUID)
	}

	// ---- step 8: 吸血 → 反伤（§10.6.7 顺序不可变）----
	if cfg.SuckPermille > 0 {
		steal := val * cfg.SuckPermille / PermilleOne
		if steal > 0 {
			c.lifeSteal(steal)
			c.trace("life_steal")
		}
	}
	rt.thorns(s, A, T, cfg, sc, now)
	c.trace("thorns")

	// ---- step 9: 命中 Buff/被动 ----
	c.applyHitBuffs()

	// ---- step 10: 收尾（§10.6.8）----
	// 临时属性（skill_step_over 的 del_extends）在当前实现里没有独立的
	// 临时属性槽：Buff 的增删本身就是成对的，无需额外清理。
	sc.DebugInfo = c.join()
	return sc
}

// damage 计算伤害数值（规格 §10.6.5 的赋值顺序）。
//
// 每一步都显式 /PermilleOne（整除截断）：顺序与截断点一起构成"可复现"
// 的定义。把多步乘一起再除一次，结果会与规格不一致，调数值时对不上。
func (c *calcCtx) damage() (val int64, crit, bok bool) {
	A, T, cfg := c.A, c.T, c.cfg

	atk := A.EffAtk()
	def := T.EffDef()

	// 1) 基础攻防
	val = atk - def*(PermilleOne-clamp(A.IgnoreDef, 0, PermilleOne))/PermilleOne
	if val < 0 {
		val = 0
	}
	// 2) 技能系数 + 固定加伤
	val = val*cfg.Coeff/PermilleOne + cfg.Harm
	if val < 1 {
		val = 1
	}

	// 3) 暴击：暴伤底数 1.3（规格里代码是 max(1.3+inc-dec, 1)）
	crit = c.rt.chance(clamp(A.Crit-T.ReCrit, 0, PermilleOne))
	if crit {
		m := 13000 + A.CritDmgInc - T.CritDmgDec
		if m < PermilleOne {
			m = PermilleOne
		}
		val = val * m / PermilleOne
	}

	// 4) 会心
	if c.rt.chance(clamp(A.LuckyHit-T.ReLuckyHit, 0, PermilleOne)) {
		m := 12000 + A.LuckyHitInc - T.LuckyHitDec
		if m < PermilleOne {
			m = PermilleOne
		}
		val = val * m / PermilleOne
	}

	// 5) 元素克制
	if A.ElementCoe > 0 {
		val = val * A.ElementCoe / PermilleOne
	}

	// 6) 格挡：概率后减伤，减伤上限 90%
	bok = c.rt.chance(clamp((PermilleOne-A.IgnoreBok)*T.Bok/PermilleOne, 0, PermilleOne))
	if bok {
		cut := clamp(T.BokDmgInc-T.BokDmgDec, 0, 9000)
		val = val * (PermilleOne - cut) / PermilleOne
	}

	// 7) 增伤 / 终伤（终伤下限 0.1）
	if A.AddDmgCoe > 0 {
		val = val * A.AddDmgCoe / PermilleOne
	}
	final := A.FinalAddDmgCoe
	if final < 1000 {
		final = 1000
	}
	val = val * final / PermilleOne

	// 8) 歧视
	if A.Discriminate > 0 {
		val = val * A.Discriminate / PermilleOne
	}

	// 9) 宠物/召唤增伤（对宠物类攻击者生效）
	if A.Kind == scenemap.EntityKindPet || A.Kind == scenemap.EntityKindSummon {
		m := PermilleOne + T.AddPetDmg - T.DecPetDmg
		if m < 1000 {
			m = 1000
		}
		val = val * m / PermilleOne
	}

	// 10) 地图类型增伤
	if A.MapTypeDmg > 0 {
		val = val * A.MapTypeDmg / PermilleOne
	}

	// 11) PK 压制（只对玩家/宠物来源生效）
	if T.PkPress > 0 && (A.Kind == scenemap.EntityKindPlayer ||
		A.Kind == scenemap.EntityKindPet || A.Kind == scenemap.EntityKindSummon) {
		m := PermilleOne - T.PkPress
		if m < 1000 {
			m = 1000
		}
		val = val * m / PermilleOne
	}

	// 12) 真实攻防
	val = val + A.RealAtk - T.RealDef

	// 13) 百分比伤害
	if cfg.PercentDmg > 0 && T.MaxHP > 0 {
		val += T.HP * cfg.PercentDmg / PermilleOne
	}

	// 14) 保底 1 点：命中就该有反馈，0 伤害会让玩家以为技能没生效
	if val < 1 {
		val = 1
	}
	return val, crit, bok
}

// dodged 判定闪避（规格 §10.6.4 的 HitRate）。
//
// 单位统一到万分比：命中率 = 命中 - 抗闪 + (1-无视闪避)*目标闪避。
// 原实现里的 "+1" 是"基础命中 1.0"，这里直接写进默认 Hit=10000，
// 不再对每个来源做 ±1 的浮点校正（有意偏离，见设计文档 §5）。
func (rt *Runtime) dodged(A, T *Sprite) bool {
	rate := (A.Hit - T.IgMonHit) - (PermilleOne-clamp(A.IgnoreDod, 0, PermilleOne))*T.Dod/PermilleOne
	rate = clamp(rate, 0, PermilleOne)
	return !rt.chance(rate)
}

// dealAoe 处理"只 AOE"伤害：不判闪避/暴击，直接按系数扣血。
func (c *calcCtx) dealAoe() {
	val := c.A.EffAtk()*c.cfg.Coeff/PermilleOne + c.cfg.Harm
	if val < 1 {
		val = 1
	}
	real, dead, absorbed := c.rt.applyRealHarm(c.s, c.T, val, c.A.UUID, c.now)
	c.sc.Harms = append(c.sc.Harms, HarmEntry{Tag: HarmAOE, Val: val})
	if absorbed > 0 {
		c.sc.Harms = append(c.sc.Harms, HarmEntry{Tag: HarmAbsorb, Val: absorbed})
	}
	c.sc.RealHarm = real
	if dead {
		c.sc.Deads = append(c.sc.Deads, c.T.UUID)
	}
}

// applyHitBuffs 给目标挂命中 Buff（HitBuffs）。
func (c *calcCtx) applyHitBuffs() {
	for _, id := range c.cfg.HitBuffs {
		c.rt.applyBuff(c.T, id, c.A.UUID, c.cfg.ID, c.now)
	}
}

// lifeSteal 给攻击者回血（受 MaxHP 上限约束）。
func (c *calcCtx) lifeSteal(v int64) {
	if v <= 0 {
		return
	}
	before := c.A.HP
	c.A.HP += v
	if c.A.HP > c.A.MaxHP {
		c.A.HP = c.A.MaxHP
	}
	real := c.A.HP - before
	if real <= 0 {
		return
	}
	c.sc.SuckBlood += real
	c.sc.Harms = append(c.sc.Harms, HarmEntry{Tag: HarmSuck, Val: real})
}

// thorns 处理反伤（规格 §10.6.7 step 2）。
//
// 只对"非宠物攻击者"按攻击力×系数反伤；宠物来源直接按固定比例，
// 避免宠物高攻把主人反死这类反直觉结果。
func (rt *Runtime) thorns(s *mgr.State, A, T *Sprite, cfg *SkillConfig, sc *SkillContext, now int64) {
	if T.ThornsRate <= 0 {
		return
	}
	if !rt.chance(clamp(T.ThornsRate-A.ReThornsRate, 0, PermilleOne)) {
		return
	}
	dmg := T.Atk * T.ThornsCoe / PermilleOne
	if A.Kind == scenemap.EntityKindPet || A.Kind == scenemap.EntityKindSummon {
		dmg = dmg / 2
	}
	if dmg <= 0 {
		return
	}
	real, dead, _ := rt.applyRealHarm(s, A, dmg, T.UUID, now)
	sc.Harms = append(sc.Harms, HarmEntry{Tag: HarmThorns, Val: real})
	if dead {
		sc.Deads = append(sc.Deads, A.UUID)
	}
}

// applyRealHarm 把伤害落到目标身上：灵力 → 护盾 → 免死 → 扣血（§10.6.6）。
//
// 返回：实际扣血、是否致死、被吸收量。**不处理死亡链**——调用方拿到
// dead=true 后统一走 killNpc/killPlayer，避免"每个伤害来源各写一套死亡逻辑"。
func (rt *Runtime) applyRealHarm(s *mgr.State, T *Sprite, amount int64, from scenemap.UUID, now int64) (real int64, dead bool, absorbed int64) {
	if T == nil || amount <= 0 || !T.Alive() {
		return 0, false, 0
	}
	left := amount

	// 1) 灵力：1 点抵 2 伤（向上取整消耗，避免 1 点伤害白嫖 1 点灵力）
	if T.Spirit > 0 {
		use := left * 2
		if use > T.Spirit*2 {
			use = T.Spirit * 2
		}
		spent := (use + 1) / 2
		if spent > T.Spirit {
			spent = T.Spirit
		}
		T.Spirit -= spent
		left -= spent * 2
		absorbed += spent * 2
		if left < 0 {
			absorbed += left
			left = 0
		}
	}

	// 2) 护盾：按条扣 Remain（Sprite.Shield 只是汇总缓存）
	if left > 0 {
		for _, b := range T.Buffs {
			if b == nil || b.Kind != BuffShield || b.Remain <= 0 {
				continue
			}
			use := left
			if use > b.Remain {
				use = b.Remain
			}
			b.Remain -= use
			left -= use
			absorbed += use
			if left <= 0 {
				break
			}
		}
		rt.refreshShield(T)
	}

	// 3) 免死：本应致死时压到 1 点
	if left > 0 && T.HP-left <= 0 && T.HasBuffKind(BuffUndie, now) {
		left = T.HP - 1
		if left < 0 {
			left = 0
		}
	}

	T.HP -= left
	real = left
	if T.HP <= 0 {
		T.HP = 0
		T.Dead = true
		dead = true
	}

	// NPC 的 HP 必须回写场景权威（NpeAIInfo），否则场景里的血条/死亡判定
	// 与战斗完全脱节——这是"双写"里唯一允许的写方向（battle → 场景）。
	rt.syncNpcHP(s, T)
	return real, dead, absorbed
}

// syncNpcHP 把战斗投影的 HP 回写 mgr.NpeAIInfo。
//
// 只写 NPC：玩家的属性权威在玩家 Actor，battle 写回去就变成双写。
func (rt *Runtime) syncNpcHP(s *mgr.State, sp *Sprite) {
	if s == nil || sp == nil || !sp.IsNpc() {
		return
	}
	info := s.NPES[sp.UUID]
	if info == nil {
		return
	}
	info.HP = sp.HP
	info.GridPos = sp.Pos
}

// trace 记录一步（DebugInfo 用）。
func (c *calcCtx) trace(step string) { c.debug = append(c.debug, step) }

// join 拼接调试信息。
func (c *calcCtx) join() string {
	out := ""
	for i, s := range c.debug {
		if i > 0 {
			out += " > "
		}
		out += s
	}
	return out
}
