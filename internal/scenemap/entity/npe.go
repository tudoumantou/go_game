// Package entity 实现"活在地图里的实体"：怪物创建/销毁、AI 状态机、
// 刷怪器推进。
//
// # 依赖方向
//
//	entity → mgr → scenemap / grid
//
// mgr 提供数据结构（NpeAIInfo/HateEntry）与格子索引，entity 只写算法，
// 绝不反向被 mgr import（否则成环）。所以 mgr 用 Deps.AITick 这种函数钩子
// 把 entity 挂进来，装配点（examples）负责接线。
//
// # 单写者纪律（红线）
//
// 本包所有函数都要求调用方**已经处在 MGR 的 goroutine 里**（AITick 钩子、
// 命令的 Apply、玩法的 MgrInit/Loop）。它们直接读写 *mgr.State，没有任何锁；
// 在别的 goroutine 里调用会立刻被 -race 抓到——这是设计意图，不是疏漏。
//
// # 与规格 3.7 的差异
//
// 规格的 NpeAIInfo 有 InitX/InitY、NextMoveMs/LastAtkMs、DeadHandled 等
// 字段，mgr.NpeAIInfo 目前没有。本包把这几项放进 State.Ext 的私有运行时表
// （见 store.go 的 npeRuntime），随 Spawn/Despawn 增删。等 mgr 补齐字段后
// 搬迁即可，调用点不变。
package entity

import (
	"fmt"
	"math"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// spawnSearchR 是落点被占时向外搜索的切比雪夫半径（规格：3 格内）。
const spawnSearchR int32 = 3

// ringSearchSize 是 grid.RandomCellInRing 的边界参数。
//
// 真正的边界判定在 Index.CanPlace（它用地图的真实尺寸），这里给一个足够
// 大的 Size 让 Contains 恒真即可。为什么不直接传地图 Size：mgr.State 没有
// 暴露 Index 的尺寸，entity 也拿不到地图配置——属于「mgr 缺失 API」，
// 见交付说明（建议 mgr 增加 State.Size() 或 Index.Size()）。
var ringSearchSize = grid.Size{Width: math.MaxInt32, Height: math.MaxInt32}

// Spec 是刷一只怪需要的全部配置。
//
// 字段与 mgr.NpeAIInfo 能承接的部分一一对应；对不上的两项（ModelID、
// RefreshID）留在 Spec 里，由运行时表保存，Despawn 时原样返回给玩法。
type Spec struct {
	// ConfigID 怪物配置表 ID（写进 NpeAIInfo.TemplateID，死亡回调按它查表）。
	ConfigID int32
	// ModelID 外观/模型 ID。NpeAIInfo 没有这个字段，只存在于 Spec。
	ModelID int32
	// AI 初始状态（StateFixed/StateGuard/StateFight/StateReturn）。
	AI AIState
	// HP 初始血量（同时作为 MaxHP；<=0 按 1 处理，避免"出生即尸体"）。
	HP int64
	// Attack 攻击力：伤害事件的数值来源。玩家 HP 的扣减在玩家 Actor 侧做。
	Attack int64
	// Defense 防御力。第一版不参与伤害公式，留给战斗层。
	Defense int64
	// Speed 移动速度（格/秒 ×1000，与 NpeAIInfo.Speed 同口径）。
	// 第一版 AI 用固定节拍（500ms 一步），Speed 只做存储。
	Speed int32
	// Radius 体积半径：0=占 1 格；r>0 占 (2r+1)² 格并登记动态阻挡。
	Radius int32
	// Skills 技能 ID 列表（按优先级）。mgr 只存 SkillEntry，等级默认 1。
	Skills []int32
	// RefreshID 刷怪器 ID（"这只怪属于哪个刷怪器"的存活统计口径）。
	RefreshID int32
}

// Spawn 在地图上创建一只怪，返回分配到的 UUID。
//
// 写入顺序：分配 UUID → 选落点 → 占格子索引 → 写 NPES → 记运行时。
//
// # 为什么这里不调 AOI
//
// AOI 同步必须只有一个写入口（MGR 的 EntityEnter / 移动命令），否则
// "权威状态写了、AOI 没写"或两次写顺序颠倒都会让客户端看到幽灵。
// 本函数只负责权威状态；AOI 由装配层（MGR 的命令/钩子路径）补。
// 代价是"刷怪结果要回传给 MGR 做同步"，所以 TickRefresh 会把新 UUID
// 返回出去。
func Spawn(s *mgr.State, spec Spec, pos scenemap.GridPos, geometry grid.Geometry) (scenemap.UUID, error) {
	if s == nil {
		return 0, fmt.Errorf("%w: state 为 nil", scenemap.ErrInvalidArgument)
	}
	if s.Index == nil {
		return 0, fmt.Errorf("%w: state 没有格子索引", scenemap.ErrInvalidState)
	}
	if !pos.Valid() {
		return 0, fmt.Errorf("%w: 刷怪落点 %s 非法", scenemap.ErrInvalidArgument, pos)
	}
	if geometry.CellW <= 0 || geometry.CellH <= 0 {
		geometry = grid.DefaultGeometry()
	}
	radius := spec.Radius
	if radius < 0 {
		radius = 0
	}

	uuid := allocUUID(s)

	cell := pos
	if !s.Index.CanPlace(cell, radius) {
		// 落点不可走（越界）或被占：在 3 格内用确定性伪随机找位。
		// 从 r=1 开始（r=0 就是刚刚判失败的原点），先内后外。
		r := newRNG(uint64(uuid) ^ (uint64(uint32(spec.ConfigID)) << 32))
		found, err := grid.RandomCellInRing(cell, 1, spawnSearchR, ringSearchSize,
			func(p scenemap.GridPos) bool { return s.Index.CanPlace(p, radius) }, r.Intn)
		if err != nil {
			return 0, fmt.Errorf("entity: 落点 %s 附近 %d 格内无空位: %w", pos, spawnSearchR, err)
		}
		cell = found
	}

	actual, err := s.Index.Enter(uuid, cell, mgr.EnterOption{
		Kind:         scenemap.EntityKindNPE,
		Radius:       radius,
		AllowOverlap: false,
		// 上面已经找好落点，这里只允许原地放置；若中途被抢（单写者下不会），
		// 宁可报错也不要静默挪走——那会让刷怪器的落点与实际位置对不上。
		MaxSearchR: 1,
	})
	if err != nil {
		return 0, fmt.Errorf("entity: 怪物落格失败 %s: %w", cell, err)
	}

	hp := spec.HP
	if hp <= 0 {
		hp = 1
	}
	ai := spec.AI
	switch ai {
	case mgr.AIStateIdle, mgr.AIStatePatrol, mgr.AIStateChase, mgr.AIStateAttack,
		mgr.AIStateReturn, mgr.AIStateStopped:
	default:
		ai = mgr.AIStateIdle
	}

	skills := make([]mgr.SkillEntry, 0, len(spec.Skills))
	for _, id := range spec.Skills {
		if id <= 0 {
			continue
		}
		skills = append(skills, mgr.SkillEntry{SkillID: id, Level: 1})
	}

	now := s.Now()
	info := &mgr.NpeAIInfo{
		UUID:       uuid,
		TemplateID: spec.ConfigID,
		Kind:       scenemap.EntityKindNPE,
		GridPos:    actual,
		Pixel:      geometry.CellCenter(actual),
		Dir:        scenemap.DirNone,
		Radius:     radius,
		HP:         hp,
		MaxHP:      hp,
		Speed:      spec.Speed,
		AI:         ai,
		// 仇恨列表初始化为空切片而不是 nil：下面的 append 与 [:0] 都不必
		// 再判空，且"有仇恨表但没人"和"没初始化"在调试时能区分。
		Hate:        make([]mgr.HateEntry, 0, 4),
		Skills:      skills,
		BornAt:      now,
		NextThinkAt: now,
	}
	if s.NPES == nil {
		s.NPES = make(map[scenemap.UUID]*mgr.NpeAIInfo)
	}
	s.NPES[uuid] = info

	setRuntime(s, uuid, &npeRuntime{
		spec:       spec,
		geometry:   geometry,
		initX:      actual.X,
		initY:      actual.Y,
		nextHealMs: now,
	})
	s.Touch()
	return uuid, nil
}

// Despawn 把一只怪移出地图，返回它的出生模板与被删标记。
//
// 为什么返回 Spec：玩法结算掉落需要"死的是什么怪、什么配置"，而
// NpeAIInfo 只留了 TemplateID。返回模板让玩法不必再查一次配置表。
//
// 动态阻挡的回收由 Index.Leave 完成（体积怪的 footprint 在 place 时登记、
// 在 unplace 时按 meta.radius 重新算一遍全部回收）。
func Despawn(s *mgr.State, uuid scenemap.UUID) (Spec, bool) {
	if s == nil {
		return Spec{}, false
	}
	info, ok := s.NPES[uuid]
	if !ok || info == nil {
		return Spec{}, false
	}

	// 没有运行时（怪是 mgr 的 enterNPE 直接建的）时只能从当前状态回填：
	// 这时拿到的 AI 是"死时的状态"，调用方应把它当成"未知"。
	spec := Spec{
		ConfigID: info.TemplateID,
		AI:       info.AI,
		HP:       info.MaxHP,
		Speed:    info.Speed,
		Radius:   info.Radius,
	}
	if rt := runtimeOf(s, uuid); rt != nil {
		spec = rt.spec
		// HP 回填成 MaxHP 而不是"死时的 0"：Spec 的语义是"出生模板"，
		// 掉落/经验按模板血量算，而不是按被打了多少算。
		spec.HP = info.MaxHP
	}

	if s.Index != nil {
		s.Index.Leave(uuid)
	}
	delete(s.NPES, uuid)
	deleteRuntime(s, uuid)
	s.Touch()
	return spec, true
}

// Damage 对怪物造成伤害，返回"本次是否真正致死"。
//
// # 幂等
//
// 死亡结算必须只发生一次：同一只怪可能被多个技能、火墙、结算补刀同时
// 打到（多来源累加），HP 会连着几次 <=0。用 deadHandled 标记兜住，
// 第二次起一律返回 false——调用方据此决定"要不要走掉落/通关回调"，
// 避免双份掉落、重复通关（规格 8.2 / 11.5）。
//
// # 不写玩家 HP
//
// 这里只处理怪物自己的 HP。怪打玩家的结果走 AttackEvent，由玩家 Actor
// 结算（规格 3.6：玩家属性权威在玩家侧）。
func Damage(s *mgr.State, uuid scenemap.UUID, amount int64, from scenemap.UUID) (dead bool) {
	if s == nil {
		return false
	}
	info, ok := s.NPES[uuid]
	if !ok || info == nil || !info.Alive() {
		return false
	}
	if amount < 0 {
		amount = 0
	}
	rt := runtimeOf(s, uuid)

	now := s.Now()
	info.HP -= amount
	// 伤害即仇恨：谁打得多，目标就是谁（规格 3.7 的 HateList 累加）。
	if from.Valid() {
		addHate(info, rt, from, amount, now)
		if info.AI == StateGuard {
			// 守卫被打了要还手；FIXED 是木桩，按设计不索敌。
			info.AI = StateFight
			info.Target = from
		}
	}

	if info.HP > 0 {
		s.Touch()
		return false
	}

	// HP<=0：进入死亡态。DeadAt 与 AI 都要写，Alive() 才为 false。
	if rt != nil && rt.deadHandled {
		return false
	}
	if rt == nil {
		// 没有运行时（mgr 自己创建的怪）：用 DeadAt 兜幂等。
		if info.DeadAt != 0 || info.AI == StateDead {
			return false
		}
	}
	if rt != nil {
		rt.deadHandled = true
	}
	info.HP = 0
	info.AI = StateDead
	info.DeadAt = now
	info.Target = 0
	info.Hate = info.Hate[:0]
	s.Touch()
	return true
}

// addHate 累加一条仇恨并刷新目标。
//
// 目标取"仇恨最高者"，同值时取先出现的（切片顺序固定 → 结果可复现；
// 用 map 存仇恨的话，同分怪物的目标每次运行都可能不同）。
func addHate(info *mgr.NpeAIInfo, rt *npeRuntime, from scenemap.UUID, amount int64, now int64) {
	if info == nil || !from.Valid() || from == info.UUID {
		return
	}
	if amount <= 0 {
		amount = 1
	}
	hit := false
	for i := range info.Hate {
		if info.Hate[i].UUID == from {
			info.Hate[i].Hate += amount
			info.Hate[i].UpdatedAt = now
			hit = true
			break
		}
	}
	if !hit {
		info.Hate = append(info.Hate, mgr.HateEntry{UUID: from, Hate: amount, UpdatedAt: now})
	}
	if rt != nil {
		rt.lastHateMs = now
	}
	info.Target = topHate(info.Hate)
}

// ensureHate 保证某个目标至少有一条仇恨记录（索敌时用，不累加数值）。
func ensureHate(info *mgr.NpeAIInfo, rt *npeRuntime, target scenemap.UUID, now int64) {
	if info == nil {
		return
	}
	for i := range info.Hate {
		if info.Hate[i].UUID == target {
			info.Hate[i].UpdatedAt = now
			if rt != nil {
				rt.lastHateMs = now
			}
			return
		}
	}
	addHate(info, rt, target, 1, now)
}

// topHate 返回仇恨最高者；没有仇恨返回 0。
func topHate(list []mgr.HateEntry) scenemap.UUID {
	best := scenemap.UUID(0)
	var bestHate int64
	for _, e := range list {
		if e.UUID == best {
			continue
		}
		if e.Hate > bestHate {
			best, bestHate = e.UUID, e.Hate
		}
	}
	return best
}

// clearHate 清空仇恨表与目标（脱战 / 归位时调用）。
func clearHate(info *mgr.NpeAIInfo, rt *npeRuntime) {
	if info == nil {
		return
	}
	info.Hate = info.Hate[:0]
	info.Target = 0
	if rt != nil {
		rt.lastHateMs = 0
	}
}

// allocUUID 从负数段分配怪物 UUID。
//
// mgr 目前没有 NextNpeUUID，这里用确定性规则：-1 起递减。
//
// 为什么不用随机/时间戳：同一份状态（同样的 NPES 数量）必须得到同一个
// UUID，回放与单测才能复现；负数段与玩家 UUID（正数，来自角色 ID）
// 天然不冲突（见 scenemap.UUID 的注释）。
//
// 为什么要探测：len(s.NPES) 在 Despawn 后会变小，编号会被复用；
// 如果复用到一只还活着的怪的 UUID，索引会直接报 ErrAlreadyExists。
// 探测顺延一位即可，且仍然完全确定。
func allocUUID(s *mgr.State) scenemap.UUID {
	id := scenemap.UUID(-int64(len(s.NPES) + 1))
	for {
		if _, exists := s.NPES[id]; !exists {
			return id
		}
		id--
	}
}
