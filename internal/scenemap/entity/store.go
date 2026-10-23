package entity

import (
	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// storeKey 是 entity 在 State.Ext 里的命名空间。
//
// 为什么不放 State.Data：Data 是"玩法私有状态"的约定槽位（一张图只能有
// 一份，谁写谁负责）。entity 是框架扩展，同一张图上可能同时挂着主城/
// 活动等多个玩法，塞进 Data 就会和玩法抢槽位。用带冒号前缀的 Ext 键，
// 是框架内部模块之间约定的做法（Ext 的键必须自带命名空间）。
const storeKey = "entity:npe:state"

// store 是 entity 挂在 MGR 状态里的私有运行时表。
//
// 所有字段只在 MGR goroutine 内读写（与 State 同一条单写者纪律），
// 所以没有锁。跨 goroutine 想读，只能通过 Mgr.Call 把读变成命令。
type store struct {
	// nodes 按 UUID 存"mgr.NpeAIInfo 装不下的那部分状态"。
	nodes map[scenemap.UUID]*npeRuntime
	// refreshers 刷怪器，按 ID 索引（遍历时按 ID 升序，结果可复现）。
	refreshers map[int32]*Refresher
	// events 待消费的战斗事件（第一版只有攻击）。
	events []AttackEvent
	// ticks TickAll 的调用序号，给确定性伪随机提供种子。
	ticks uint64
}

// newStore 构造空表。
func newStore() *store {
	return &store{
		nodes:      make(map[scenemap.UUID]*npeRuntime),
		refreshers: make(map[int32]*Refresher),
	}
}

// storeOf 取（必要时创建）私有表。
//
// 注意它会写 State.Ext（进而 Touch 一次 revision），所以只在"确实要改
// 状态"的路径调用；只读路径用 peekStore。
func storeOf(s *mgr.State) *store {
	if s == nil {
		return nil
	}
	if st := peekStore(s); st != nil {
		return st
	}
	st := newStore()
	s.SetExt(storeKey, st)
	return st
}

// peekStore 只读地取私有表，不存在时返回 nil（不会创建、不会 Touch）。
func peekStore(s *mgr.State) *store {
	if s == nil {
		return nil
	}
	v, ok := s.ExtValue(storeKey)
	if !ok {
		return nil
	}
	st, _ := v.(*store)
	return st
}

// npeRuntime 是 NpeAIInfo 放不下的运行时字段。
//
// 对应规格 3.7 的 InitX/InitY、NextMoveMs/LastAtkMs、TargetUUID、
// DeadHandled。mgr.NpeAIInfo 目前没有这些字段（也不该为了 entity 反向
// 依赖而加），所以放在这里，随 Spawn/Despawn 生命周期增删。
//
// 等 mgr 补齐字段后，把这里的字段搬进 NpeAIInfo 即可，调用点不用改。
type npeRuntime struct {
	// spec 是出生模板的快照：Despawn 要把它还给玩法做掉落结算。
	spec Spec
	// geometry 用于把格坐标换算成像素（NpeAIInfo.Pixel 的更新）。
	geometry grid.Geometry
	// initX/initY 是出生点，RETURN 状态往这里走，GUARD 状态围着它游走。
	initX, initY int32
	// nextMoveMs 下一次允许移动的时间（节流，500ms 一步）。
	nextMoveMs int64
	// lastAtkMs 上一次攻击的时间（节流，1000ms 一刀）。
	lastAtkMs int64
	// lastHateMs 最近一次加仇时间：脱战 5 秒后清空仇恨。
	lastHateMs int64
	// nextHealMs 下一次回血的时间（RETURN 状态每秒 5%）。
	nextHealMs int64
	// deadHandled 死亡结算的幂等标记：HP<=0 只结算一次。
	deadHandled bool
}

// home 返回出生点。
func (rt *npeRuntime) home() scenemap.GridPos {
	return scenemap.GridPos{X: rt.initX, Y: rt.initY}
}

// runtimeOf 读取一只怪的运行时（不存在返回 nil）。
//
// 为什么允许 nil：mgr 自己的 enterNPE 也能创建怪（不经过 entity.Spawn），
// 这类怪的 InitX/InitY 没有记录，由 ensureRuntime 按当前位置补一份。
func runtimeOf(s *mgr.State, uuid scenemap.UUID) *npeRuntime {
	st := peekStore(s)
	if st == nil {
		return nil
	}
	return st.nodes[uuid]
}

// ensureRuntime 取运行时，缺失时按 NpeAIInfo 的现状补一份。
func ensureRuntime(s *mgr.State, info *mgr.NpeAIInfo) *npeRuntime {
	if info == nil {
		return nil
	}
	if rt := runtimeOf(s, info.UUID); rt != nil {
		return rt
	}
	st := storeOf(s)
	if st == nil {
		return nil
	}
	rt := &npeRuntime{
		spec:     Spec{ConfigID: info.TemplateID, AI: info.AI, HP: info.MaxHP, Speed: info.Speed, Radius: info.Radius},
		geometry: grid.DefaultGeometry(),
		initX:    info.GridPos.X,
		initY:    info.GridPos.Y,
	}
	st.nodes[info.UUID] = rt
	return rt
}

// setRuntime 写入运行时。
func setRuntime(s *mgr.State, uuid scenemap.UUID, rt *npeRuntime) {
	if st := storeOf(s); st != nil && rt != nil {
		st.nodes[uuid] = rt
	}
}

// deleteRuntime 删除运行时（Despawn 时调用，避免 map 随击杀无限增长）。
func deleteRuntime(s *mgr.State, uuid scenemap.UUID) {
	if st := peekStore(s); st != nil {
		delete(st.nodes, uuid)
	}
}

// rng 是 splitmix64：确定性、零分配、跨平台一致。
//
// 为什么不用 math/rand：包级 rand 的默认种子来自时钟，同一份输入两次
// 运行结果不同。AI 游走、刷怪落点这类"看起来随机"的逻辑一旦不可复现，
// 线上问题（"这只怪为什么刷在墙里"）就只能靠猜。这里把种子做成显式
// 参数——同一局面必然得到同一结果，测试也能断言具体落点。
type rng struct{ state uint64 }

// newRNG 由种子构造；0 会换成一个非零常数（splitmix 要求状态非零）。
func newRNG(seed uint64) *rng {
	if seed == 0 {
		seed = 0x9E3779B97F4A7C15
	}
	return &rng{state: seed}
}

// next 产出一个 64 位伪随机数。
func (r *rng) next() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// Intn 返回 [0,n) 的伪随机数；n<=0 返回 0（避免除零 panic）。
func (r *rng) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}
