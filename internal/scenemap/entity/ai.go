package entity

import (
	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// AIState 是 mgr.AIState 的**类型别名**（注意是 `=`，不是新类型）。
//
// 为什么用别名：状态最终要写回 NpeAIInfo.AI（mgr 的类型）。定义新类型
// 会让每一处赋值都要转换，而别名让 entity.StateGuard 与 mgr.AIStatePatrol
// 完全等价，两边可以混用，调用方也不必 import 两个名字。
type AIState = mgr.AIState

// 规格 3.7 的四个战术状态在 mgr 枚举上的映射。
//
// mgr 的枚举是"通用状态机"（Idle/Patrol/Chase/Attack/Return/Dead/Stopped），
// 规格 3.7 是"战术角色"（FIXED/GUARD/FIGHT/RETURN）。这里做一对一映射，
// 而不是往 mgr 里加枚举值：mgr 不能反向依赖 entity，而且枚举会进快照与
// 协议、只能追加不能重排。
//
//	FIXED  → Idle    原地不动、不索敌（木桩/装饰 NPC）
//	GUARD  → Patrol  警戒半径内索敌；没有目标就在出生点 1 格内游走
//	FIGHT  → Chase   追击与攻击（Attack 只是表现，状态仍是 FIGHT）
//	RETURN → Return  脱战归位 + 回血
const (
	StateFixed   AIState = mgr.AIStateIdle
	StateGuard   AIState = mgr.AIStatePatrol
	StateFight   AIState = mgr.AIStateChase
	StateReturn  AIState = mgr.AIStateReturn
	StateDead    AIState = mgr.AIStateDead
	StateStopped AIState = mgr.AIStateStopped
)

// 第一版的 AI 调参。全部是常量而不是配置项：等玩法真的需要差异化
// （不同怪的警戒半径/攻击间隔）再提升成 Spec 字段，现在先保证行为可复现。
const (
	// GuardRadius 是 GUARD 的索敌半径（切比雪夫格）。
	GuardRadius int32 = 6
	// ChaseGiveUp 是 FIGHT 的脱战距离：目标跑出这个范围就放弃。
	ChaseGiveUp int32 = 12
	// MoveIntervalMS 是移动节流：同一只怪最快 500ms 走一格。
	// 不节流的话 500 只怪会在每个 AI tick 里各走一步，MGR 同一条
	// goroutine 上直接卡死（这是"AI 不要用全局轮询"的另一面：
	// 轮询可以，但每次轮询的代价必须有上限）。
	MoveIntervalMS int64 = 500
	// AttackIntervalMS 是攻击节流：相邻时 1 秒一刀。
	AttackIntervalMS int64 = 1000
	// HateIdleClearMS 是脱战清仇恨的时间。
	HateIdleClearMS int64 = 5000
	// ReturnHealPerSec 是 RETURN 状态的每秒回血（千分比，50 = 5%）。
	ReturnHealPerSec int64 = 50
	// WanderRadius 是 GUARD 游走半径（1 格）。
	WanderRadius int32 = 1
	// maxPendingEvents 是事件队列上限：没人消费时不能无界增长。
	maxPendingEvents = 1024
)

// AttackEvent 是"怪打到了谁"的结果事件。
//
// 为什么只产生事件、不直接扣玩家血：玩家属性权威在玩家 Actor（规格 3.6），
// MGR 这边扣血会造出第二份 HP，两份一起改必然漂移。事件由装配层投递给
// 目标主人，由玩家 Actor 走战斗结算（规格 8.1 的双回路）。
type AttackEvent struct {
	// UUID 出手的怪。
	UUID scenemap.UUID
	// Target 挨打的目标（玩家侧）。
	Target scenemap.UUID
	// Damage 伤害数值（来自 Spec.Attack；防御公式在战斗层）。
	Damage int64
	// At 事件产生时间（unix 毫秒）。
	At int64
}

// TickAll 推进全图 AI 一轮。
//
// 遍历顺序用 s.NpeUUIDs()（升序）：Go 的 map range 顺序是随机的，
// 如果"谁先动"依赖遍历顺序，两次运行就会出现不同的卡位结果，
// 线上问题无法复现。升序让结算顺序成为状态的一部分。
func TickAll(s *mgr.State, nowMS int64) {
	if s == nil {
		return
	}
	st := storeOf(s)
	if st == nil {
		return
	}
	st.ticks++
	seq := st.ticks

	// 清理"已经不在 NPES 里"的运行时：mgr 的 entityLeave 可以直接删怪
	// （不经过 Despawn），不清会留下永久泄漏的节点表。
	for id := range st.nodes {
		if s.NPES[id] == nil {
			delete(st.nodes, id)
		}
	}

	for _, id := range s.NpeUUIDs() {
		info := s.NPES[id]
		if info == nil {
			continue
		}
		tickOne(s, info, nowMS, seq)
	}
}

// tickOne 推进一步单只怪的状态机。
func tickOne(s *mgr.State, info *mgr.NpeAIInfo, nowMS int64, seq uint64) {
	if info == nil {
		return
	}
	switch info.AI {
	case StateDead, StateStopped:
		// 尸体与"时停"都不推进：时停期间连死亡计时都要冻结（规格 7.2）。
		return
	}

	rt := ensureRuntime(s, info)
	if rt == nil {
		return
	}

	// 脱战清仇恨：FIGHT 期间不算脱战（正在打或正在追），其余状态超过
	// 5 秒没有任何加仇就清空，避免"仇恨表跟着怪一辈子"。
	if !isFight(info.AI) && len(info.Hate) > 0 &&
		nowMS-rt.lastHateMs >= HateIdleClearMS {
		clearHate(info, rt)
		s.Touch()
	}

	switch info.AI {
	case StateFixed:
		// FIXED：不动、不索敌。木桩也会被打，但不会还手。
	case StateGuard:
		tickGuard(s, info, rt, nowMS, seq)
	case StateFight, mgr.AIStateAttack:
		// Attack 是"表现上的攻击态"，行为与 FIGHT 完全一致：走到身边再打。
		tickFight(s, info, rt, nowMS)
	case StateReturn:
		tickReturn(s, info, rt, nowMS)
	default:
		// 未知状态按 FIXED 处理：宁可不动，也不要在未知语义下乱走。
	}
}

// tickGuard 是 GUARD：先索敌，索不到就在出生点附近游走。
func tickGuard(s *mgr.State, info *mgr.NpeAIInfo, rt *npeRuntime, nowMS int64, seq uint64) {
	if target, ok := nearestPlayer(s, info.GridPos, GuardRadius); ok {
		info.AI = StateFight
		info.Target = target
		ensureHate(info, rt, target, nowMS)
		s.Touch()
		return
	}

	// 游走节流：每 500ms 最多挪一格。
	if nowMS < rt.nextMoveMs {
		return
	}
	rt.nextMoveMs = nowMS + MoveIntervalMS

	// 确定性伪随机：种子 = UUID ^ tick 序号（规格 3.7 的要求）。
	// 同一只怪在同一拍永远选同一格，回放能复现；不同拍的序列不同。
	r := newRNG(uint64(info.UUID) ^ seq)
	dx := int32(r.Intn(int(2*WanderRadius)+1)) - WanderRadius
	dy := int32(r.Intn(int(2*WanderRadius)+1)) - WanderRadius
	dst := rt.home().Add(dx, dy)
	if dst == info.GridPos {
		return
	}
	if !canStep(s, info.UUID, dst, info.Radius) {
		return
	}
	_ = MoveOne(s, info.UUID, dst)
}

// tickFight 是 FIGHT：目标有效就贴上去打，无效就归位。
func tickFight(s *mgr.State, info *mgr.NpeAIInfo, rt *npeRuntime, nowMS int64) {
	tgt := info.Target
	if !tgt.Valid() {
		toReturn(s, info, rt, nowMS)
		return
	}
	u := s.Users[tgt]
	if u == nil {
		// 目标丢失（离图/切图/死亡）。这里只认玩家侧：宠物/召唤的
		// 位置没有存在 Users 里，第一版不作为 AI 目标。
		toReturn(s, info, rt, nowMS)
		return
	}
	dist := grid.Chebyshev(info.GridPos, u.GridPos)
	if dist > ChaseGiveUp {
		toReturn(s, info, rt, nowMS)
		return
	}

	if dist <= 1 {
		// 相邻：攻击。节流 1 秒，且**只产生事件**，不改玩家 HP。
		if nowMS-rt.lastAtkMs < AttackIntervalMS {
			return
		}
		rt.lastAtkMs = nowMS
		dmg := rt.spec.Attack
		if dmg <= 0 {
			dmg = 1
		}
		// 砍人也算加仇：否则"只挨打不还手"的怪会在 5 秒后把目标忘了。
		addHate(info, rt, tgt, dmg, nowMS)
		pushEvent(s, AttackEvent{UUID: info.UUID, Target: tgt, Damage: dmg, At: nowMS})
		s.Touch()
		return
	}

	if nowMS < rt.nextMoveMs {
		return
	}
	rt.nextMoveMs = nowMS + MoveIntervalMS
	next := stepToward(s, info, u.GridPos)
	if next != info.GridPos {
		_ = MoveOne(s, info.UUID, next)
	}
}

// tickReturn 是 RETURN：往出生点走，边走边回血，到家转 GUARD。
func tickReturn(s *mgr.State, info *mgr.NpeAIInfo, rt *npeRuntime, nowMS int64) {
	// 每秒回 5%（MaxHP * 50 / 1000）。整数千分比，不引入浮点：
	// 浮点会让"回满血"的时刻依赖精度，测试与回放都不好断言。
	if info.MaxHP > 0 && info.HP < info.MaxHP && nowMS >= rt.nextHealMs {
		heal := info.MaxHP * ReturnHealPerSec / 1000
		if heal < 1 {
			heal = 1
		}
		info.HP += heal
		if info.HP > info.MaxHP {
			info.HP = info.MaxHP
		}
		rt.nextHealMs = nowMS + 1000
		s.Touch()
	}

	home := rt.home()
	if info.GridPos == home {
		info.AI = StateGuard
		clearHate(info, rt)
		s.Touch()
		return
	}
	if nowMS < rt.nextMoveMs {
		return
	}
	rt.nextMoveMs = nowMS + MoveIntervalMS
	next := stepToward(s, info, home)
	if next != info.GridPos {
		_ = MoveOne(s, info.UUID, next)
	}
}

// toReturn 切到 RETURN 并清空目标/仇恨。
func toReturn(s *mgr.State, info *mgr.NpeAIInfo, rt *npeRuntime, nowMS int64) {
	info.AI = StateReturn
	clearHate(info, rt)
	// 立刻开始归位与回血，不等下一个 500ms/1s 窗口。
	rt.nextMoveMs = nowMS
	rt.nextHealMs = nowMS
	s.Touch()
}

// MoveOne 把一只怪挪到指定格。
//
// 这是 entity 里唯一的"移动入口"：校验 + 改索引 + 改状态 + Touch。
// AOI 同步不在这里做——见包注释与交付说明：mgr 目前没有暴露
// "NPE 位置 → AOI Sync" 的入口（CommandMove 需要像素坐标，而 entity
// 拿不到 Geometry 之外的换算依据），所以这一步由装配层补。
func MoveOne(s *mgr.State, uuid scenemap.UUID, to scenemap.GridPos) error {
	cur, ok := s.Index.At(uuid)
	if !ok {
		return scenemap.ErrNotFound
	}
	if cur == to {
		return nil
	}
	// 走 State.MoveNPE：它复用玩家的 syncMap 路径，因此索引迁移、
	// AOI 同步（客户端能看到怪移动）、换格钩子都是同一套逻辑。
	// 直接改 Index 会让怪"在服务端动了、客户端看不见"——这是实测踩过的坑。
	return s.MoveNPE(uuid, to, grid.DirOf(cur, to))
}

// stepToward 返回"朝 target 走一步"的落点（走不了返回原地）。
//
// # 为什么不用 grid.AStar
//
// grid.AStar 的签名要求 *grid.Blocker，而 entity 拿不到地图的静态矩阵
// （State 上没有 Walkable/Blocker 出口），构造不出 Blocker。这里退化成
// "8 邻域贪心一步"——与 grid.StraightStep 同语义，但用格子索引做占位
// 校验（StraightStep 只查 Blocker 的动态层）。
//
// 邻域顺序由 grid.Neighbors8 固定（右→右下→下→…），同距离取先出现者，
// 因此同样的局面必然走同一格；mgr 补上 Blocker 出口后换成 AStar 即可。
func stepToward(s *mgr.State, info *mgr.NpeAIInfo, target scenemap.GridPos) scenemap.GridPos {
	from := info.GridPos
	best := from
	bestD := grid.Chebyshev(from, target)
	for _, nb := range grid.Neighbors8(from) {
		if !canStep(s, info.UUID, nb, info.Radius) {
			continue
		}
		if d := grid.Chebyshev(nb, target); d < bestD {
			best, bestD = nb, d
		}
	}
	return best
}

// canStep 报告一只怪能否迈到 p。
//
// 半径 0 用 Index.CanPlace（含边界判断 + 占位）；半径 >0 时它自己的
// footprint 会覆盖邻格，CanPlace 必然为 false，只能退化为"排除别人的占位"，
// 边界交给 Index.Move 的 ErrOutOfRange 兜底。
func canStep(s *mgr.State, uuid scenemap.UUID, p scenemap.GridPos, radius int32) bool {
	if s == nil || s.Index == nil {
		return false
	}
	if radius <= 0 {
		return s.Index.CanPlace(p, 0)
	}
	return !s.Index.BlockedFor(uuid, p)
}

// nearestPlayer 找半径内最近的玩家侧实体（切比雪夫）。
//
// 只遍历 Users：宠物/召唤在 State.Pets 里，没有位置字段（位置在 Index 的
// 私有 meta 里），第一版不作为 AI 的索敌目标。
//
// 遍历用 UserUUIDs()（升序），距离相同时天然取 UUID 较小的一个——
// 又是"结果可复现"的一条：map range 的随机顺序会让两只等距的玩家
// 在两次运行里被不同地选中。
func nearestPlayer(s *mgr.State, from scenemap.GridPos, radius int32) (scenemap.UUID, bool) {
	best := scenemap.UUID(0)
	bestD := radius + 1
	for _, id := range s.UserUUIDs() {
		u := s.Users[id]
		if u == nil {
			continue
		}
		d := grid.Chebyshev(from, u.GridPos)
		if d <= radius && d < bestD {
			best, bestD = id, d
		}
	}
	return best, best.Valid()
}

// isFight 报告是否处于战斗态。
func isFight(ai AIState) bool {
	return ai == StateFight || ai == mgr.AIStateAttack
}

// pushEvent 把事件放进队列（满了丢最旧的一条）。
func pushEvent(s *mgr.State, ev AttackEvent) {
	st := storeOf(s)
	if st == nil {
		return
	}
	if len(st.events) >= maxPendingEvents {
		// 队列满说明装配层漏了 DrainEvents——丢最旧的一条，保证 MGR 的
		// 内存不被"没人消费"拖垮。丢一条表现事件不影响权威状态。
		copy(st.events, st.events[1:])
		st.events = st.events[:len(st.events)-1]
	}
	st.events = append(st.events, ev)
}

// DrainEvents 取走当前累积的攻击事件（取走即清空）。
//
// 取走而不是"读后保留"：事件队列是"恰好消费一次"的语义，保留会让同一次
// 攻击被下游结算两次（双倍伤害），而双倍伤害是最难在测试里发现的 bug。
func DrainEvents(s *mgr.State) []AttackEvent {
	st := peekStore(s)
	if st == nil || len(st.events) == 0 {
		return nil
	}
	out := st.events
	st.events = nil
	return out
}
