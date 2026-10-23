package battle

import (
	"game_study/internal/scenemap"
	"game_study/internal/scenemap/mgr"
)

// handleDead 是所有死亡路径的统一入口（规格 §10.8 / §13.5）。
//
// # 为什么必须统一 + 幂等
//
// 一只怪可能在同一拍被三类来源打到：技能命中、Dot 跳伤、火墙 tick。
// 每个来源各自"扣血到 0 → 发放掉落"会出双份掉落、重复通关。
// Runtime.dead 是 MGR 私有的进程内标记（等价于原实现的
// `{DeadNpcUUID, Uuid}`），第二次进入直接返回。
//
// # 双写顺序
//
// 先写场景权威（NpeAIInfo：HP/AI/DeadAt/清仇恨），再发事件/调钩子：
// 反过来会出现"玩家已收到击杀事件，但场景里怪还活着"的窗口——玩法在
// 事件里立刻清场/推进波次时，会看到一只不该存在的怪。
func (rt *Runtime) handleDead(s *mgr.State, sp *Sprite, killer scenemap.UUID, now int64) {
	if rt == nil || s == nil || sp == nil {
		return
	}
	if rt.dead[sp.UUID] {
		return // 幂等：同一只怪只结算一次死亡
	}
	rt.dead[sp.UUID] = true

	sp.Dead = true
	sp.HP = 0
	sp.Fight = false
	// 死亡清 Buff（规格 §10.9 的解除来源之一）。
	for _, b := range sp.Buffs {
		if b != nil && b.Kind == BuffShield {
			rt.removeShield(sp, b)
		}
	}
	sp.Buffs = nil
	rt.refreshShield(sp)

	if sp.IsNpc() {
		if info := s.NPES[sp.UUID]; info != nil {
			info.HP = 0
			info.AI = mgr.AIStateDead
			info.DeadAt = now
			info.Target = 0
			info.Hate = info.Hate[:0]
		}
		// 结果回路：击杀者的主人收 MonDeadEvent（掉落主流程在那里）。
		owner := rt.ownerOf(s, killer)
		if owner.Valid() && rt.opts.Notify != nil {
			rt.opts.Notify(owner, MonDeadEvent{
				Monster: sp.UUID, NpcID: sp.CfgID, Killer: killer,
				X: sp.Pos.X, Y: sp.Pos.Y,
			})
		}
		// 装配层钩子：Despawn + 玩法 HandleNpcDead（推进波次/通关）。
		if rt.opts.OnNpcDead != nil {
			rt.opts.OnNpcDead(s, sp.UUID, sp.CfgID)
		}
		return
	}

	// 玩家/宠物/召唤：只通知主人，复活与掉落在玩家 Actor 侧处理。
	owner := sp.Owner
	if !owner.Valid() {
		owner = sp.UUID
	}
	if rt.opts.Notify != nil {
		rt.opts.Notify(owner, PlayerDeadEvent{UUID: sp.UUID, Killer: killer})
	}
}

// ownerOf 返回"结果事件该发给谁"：宠物/召唤/镜像 → 主人，玩家 → 自己。
//
// 为什么击杀者可能是怪物：反伤/环境伤害也能致死。这时没有主人可发，
// 返回 0 让调用方跳过通知（掉落由场景的"无主击杀"规则处理）。
func (rt *Runtime) ownerOf(s *mgr.State, uuid scenemap.UUID) scenemap.UUID {
	if !uuid.Valid() {
		return 0
	}
	if sp, ok := rt.sprites[uuid]; ok && sp != nil && sp.Owner.Valid() {
		return sp.Owner
	}
	if master, ok := s.Pets[uuid]; ok && master.Valid() {
		return master
	}
	// 没有投影时按场景表判断：玩家自己就是主人。
	if s.Users[uuid] != nil {
		return uuid
	}
	return 0
}

// AddHate 给怪物加仇恨并维护目标（规格 §10.5 step 7 的 npc_be_attacked）。
//
// 为什么放在 battle 而不是复用 entity：entity 的仇恨入口是私有函数且
// 与"扣血"耦合（Damage 里顺带 addHate）。战斗需要"结算前先写仇恨"
// （伤害可能被闪避，但仇恨照加），所以这里对 NpeAIInfo 的导出字段
// 直接操作——Hate 是场景数据，battle 作为 MGR 内的算法有权写。
func (rt *Runtime) addHate(s *mgr.State, npc scenemap.UUID, from scenemap.UUID, amount int64, now int64) {
	if s == nil || !from.Valid() || from == npc {
		return
	}
	info := s.NPES[npc]
	if info == nil || !info.Alive() {
		return
	}
	if amount <= 0 {
		amount = 1
	}
	for i := range info.Hate {
		if info.Hate[i].UUID == from {
			info.Hate[i].Hate += amount
			info.Hate[i].UpdatedAt = now
			info.Target = topHate(info.Hate)
			if info.AI == mgr.AIStateIdle || info.AI == mgr.AIStatePatrol {
				info.AI = mgr.AIStateChase
			}
			return
		}
	}
	info.Hate = append(info.Hate, mgr.HateEntry{UUID: from, Hate: amount, UpdatedAt: now})
	info.Target = topHate(info.Hate)
	if info.AI == mgr.AIStateIdle || info.AI == mgr.AIStatePatrol {
		info.AI = mgr.AIStateChase
	}
}

// topHate 返回仇恨最高者（同值取先出现，保证可复现）。
func topHate(list []mgr.HateEntry) scenemap.UUID {
	var best scenemap.UUID
	var bestVal int64
	for _, e := range list {
		if e.UUID == best {
			continue
		}
		if e.Hate > bestVal {
			best, bestVal = e.UUID, e.Hate
		}
	}
	return best
}
