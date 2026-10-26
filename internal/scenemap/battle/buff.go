package battle

import (
	"sort"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/mgr"
)

// applyBuff 给目标施加一条 Buff，返回是否生效。
//
// # 叠加规则（规格 §10.9）
//
//   - Replace=true 或 MaxStacks<=1：刷新时长并把层数重置为 1；
//   - 否则层数 +1，封顶 MaxStacks；
//   - 配置不存在：不施加、不 panic——一条配错的 Buff 不该让整次结算崩掉，
//     但调用方要能从 DebugInfo 看出来（trace 里会记 buff_missing）。
func (rt *Runtime) applyBuff(target *Sprite, cfgID int32, from scenemap.UUID, skillID int32, now int64) bool {
	if target == nil || !target.Alive() || cfgID <= 0 {
		return false
	}
	cfg, err := rt.table.Buff(cfgID)
	if err != nil {
		return false
	}
	// 已有同类：按配置决定顶替还是叠层。
	for _, b := range target.Buffs {
		if b == nil || b.ID != cfgID {
			continue
		}
		if cfg.Replace || cfg.MaxStacks <= 1 {
			b.ExpireAt = now + cfg.DurationMS
			b.NextTickAt = now + cfg.TickMS
			b.Stacks = 1
			b.Value = cfg.Value
			if cfg.Kind == BuffShield {
				// 顶替护盾：先把旧盾撤掉，再加新盾，避免"刷新时白赚一层"。
				rt.removeShield(target, b)
				b.Remain = cfg.Value
				rt.refreshShield(target)
			}
			return true
		}
		if b.Stacks < cfg.MaxStacks {
			b.Stacks++
			if cfg.Kind == BuffShield {
				b.Remain += cfg.Value
				rt.refreshShield(target)
			}
		}
		b.ExpireAt = now + cfg.DurationMS
		return true
	}
	b := &Buff{
		ID: cfg.ID, Kind: cfg.Kind, AttrField: cfg.AttrField,
		Value: cfg.Value, Stacks: 1, From: from, SkillID: skillID,
		AppliedAt: now, ExpireAt: now + cfg.DurationMS,
		TickMS: cfg.TickMS, MaxStacks: cfg.MaxStacks,
	}
	if cfg.TickMS > 0 {
		b.NextTickAt = now + cfg.TickMS
	}
	if cfg.Kind == BuffShield {
		b.Remain = cfg.Value
	}
	target.Buffs = append(target.Buffs, b)
	if cfg.Kind == BuffShield {
		rt.refreshShield(target)
	}
	return true
}

// removeBuff 移除指定 ID 的 Buff（全部层），返回是否真的移除。
//
//nolint:unused // P4 预留：Buff 按 ID 精确移除的接线入口（当前只整组重算）
func (rt *Runtime) removeBuff(target *Sprite, cfgID int32) bool {
	if target == nil {
		return false
	}
	out := target.Buffs[:0]
	removed := false
	for _, b := range target.Buffs {
		if b != nil && b.ID == cfgID {
			if b.Kind == BuffShield {
				rt.removeShield(target, b)
			}
			removed = true
			continue
		}
		out = append(out, b)
	}
	target.Buffs = out
	if removed {
		rt.refreshShield(target)
	}
	return removed
}

// removeShield 撤掉一条护盾 Buff 的剩余量。
func (rt *Runtime) removeShield(target *Sprite, b *Buff) {
	if target == nil || b == nil || b.Remain <= 0 {
		return
	}
	b.Remain = 0
}

// refreshShield 重算 Sprite.Shield（对外可见的护盾值）。
//
// 护盾的权威存在 Buff.Remain 里（受击要按条扣减），Sprite.Shield 只是
// 汇总缓存：直接改 Sprite.Shield 而不改 Buff，会出现"盾破了 Buff 还在，
// 到期时又凭空多出一层盾"。
func (rt *Runtime) refreshShield(target *Sprite) {
	if target == nil {
		return
	}
	var sum int64
	for _, b := range target.Buffs {
		if b != nil && b.Kind == BuffShield && b.Remain > 0 {
			sum += b.Remain
		}
	}
	target.Shield = sum
}

// tickBuffs 推进所有实体的 Buff（500ms，吃倍速）。
//
// 遍历顺序按 UUID 升序：Dot 致死会触发死亡链（掉落/通关），顺序不同
// 会让"同一拍两只怪互致死"的击杀归属在两次运行间不同（规格 §7.1 的
// 可复现性要求）。
func (rt *Runtime) tickBuffs(s *mgr.State, now int64) {
	for _, uuid := range rt.spriteUUIDs() {
		sp := rt.sprites[uuid]
		if sp == nil {
			continue
		}
		var removed []int32
		keep := sp.Buffs[:0]
		for _, b := range sp.Buffs {
			if b == nil {
				continue
			}
			if b.Expired(now) {
				if b.Kind == BuffShield {
					rt.removeShield(sp, b)
				}
				if b.Kind == BuffStun {
					rt.resumeAfterStun(s, sp)
				}
				removed = append(removed, b.ID)
				continue
			}
			// 周期结算：Dot 扣血 / Hot 回血。
			if b.TickMS > 0 && b.NextTickAt > 0 && now >= b.NextTickAt {
				b.NextTickAt = now + b.TickMS
				switch b.Kind {
				case BuffDot:
					rt.dotDamage(s, sp, b, now)
				case BuffHot:
					rt.hotHeal(s, sp, b, now)
				}
			}
			keep = append(keep, b)
		}
		sp.Buffs = keep
		if len(removed) > 0 {
			rt.broadcastBuffSync(s, sp, nil, removed)
		}
	}
}

// dotDamage 结算一跳持续伤害。
//
// Dot 不走闪避/暴击（它没有"出手"这个动作），但要走护盾/灵力/免死：
// 否则"上盾后免疫灼烧"会成为一个稳定可复现的数值漏洞。
func (rt *Runtime) dotDamage(s *mgr.State, sp *Sprite, b *Buff, now int64) {
	if !sp.Alive() {
		return
	}
	dmg := b.Value * int64(maxInt32(b.Stacks, 1))
	if dmg <= 0 {
		return
	}
	real, dead, _ := rt.applyRealHarm(s, sp, dmg, b.From, now)
	if real <= 0 {
		return
	}
	// 表现回路：Dot 也是"看得见的伤害"，客户端要飘字。
	_ = s.Broadcast(HarmBroadcast{
		SkillID: b.SkillID, Caster: b.From, Target: sp.UUID,
		HP: sp.HP, MaxHP: sp.MaxHP,
		Harms: []HarmEntry{{Tag: HarmNormal, Val: real}},
	}, mgr.BroadcastAround(sp.Pos, 6))
	if dead {
		rt.handleDead(s, sp, b.From, now)
	}
}

// hotHeal 结算一跳持续回血。
func (rt *Runtime) hotHeal(s *mgr.State, sp *Sprite, b *Buff, now int64) {
	if sp == nil || sp.Dead {
		return
	}
	v := b.Value * int64(maxInt32(b.Stacks, 1))
	if v <= 0 {
		return
	}
	sp.HP += v
	if sp.HP > sp.MaxHP {
		sp.HP = sp.MaxHP
	}
	rt.syncNpcHP(s, sp)
}

// resumeAfterStun 时停结束后把 AI 从 Stopped 拉回待机。
//
// 只处理"因为我们时停而 Stopped"的情况：如果 AI 本来就是 Stopped
// （剧情演出），这里会误伤——所以只在没有其他 Stun Buff 时才恢复。
func (rt *Runtime) resumeAfterStun(s *mgr.State, sp *Sprite) {
	if !sp.IsNpc() || s == nil {
		return
	}
	if sp.HasBuffKind(BuffStun, rt.now(s)) {
		return
	}
	if info := s.NPES[sp.UUID]; info != nil && info.AI == mgr.AIStateStopped {
		info.AI = mgr.AIStateIdle
		info.NextThinkAt = rt.now(s)
	}
}

// broadcastBuffSync 广播 Buff 变化（表现回路）。
func (rt *Runtime) broadcastBuffSync(s *mgr.State, sp *Sprite, added, removed []int32) {
	if s == nil || sp == nil || (len(added) == 0 && len(removed) == 0) {
		return
	}
	_ = s.Broadcast(BuffSync{UUID: sp.UUID, Added: added, Removed: removed},
		mgr.BroadcastAround(sp.Pos, 6))
}

// spriteUUIDs 返回按 UUID 升序的投影列表（确定性遍历）。
func (rt *Runtime) spriteUUIDs() []scenemap.UUID {
	out := make([]scenemap.UUID, 0, len(rt.sprites))
	for id := range rt.sprites {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
