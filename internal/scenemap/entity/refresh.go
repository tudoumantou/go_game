package entity

import (
	"fmt"
	"sort"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// RefreshKind 是刷怪器类型（规格 3.8 的 NUM/TIME）。
type RefreshKind int32

const (
	// RefreshNUM 一次性刷怪：到期补足一次，之后永不再刷（副本波次）。
	RefreshNUM RefreshKind = 1
	// RefreshTIME 周期刷怪：每次到期补足到 Count，然后排下一次（野外密度）。
	RefreshTIME RefreshKind = 2
)

// String 便于日志。
func (k RefreshKind) String() string {
	switch k {
	case RefreshNUM:
		return "NUM"
	case RefreshTIME:
		return "TIME"
	default:
		return fmt.Sprintf("RefreshKind(%d)", int32(k))
	}
}

// Refresher 是一个刷怪器的配置与运行时。
//
// 前 8 个字段对应规格 3.8 的 NpcRefresh；后两个是"落地必需但规格没列"的
// 生成参数：刷怪器知道自己"刷几只/刷在哪/多久刷"，但不知道"刷出来的怪
// 长什么样"，所以模板与几何必须由创建者（玩法）给。
type Refresher struct {
	// ID 刷怪器编号（同一张图内唯一）。
	ID int32
	// Kind 一次性 / 周期。
	Kind RefreshKind
	// CfgID 刷出来的怪物配置 ID。
	CfgID int32
	// Count 目标存活数（补足到这个数）。
	Count int32
	// CycleMS 周期（仅 TIME 使用）。
	CycleMS int64
	// MaxAlive 存活上限（<=0 表示等于 Count）。
	MaxAlive int32
	// NextAtMS 下次触发时间（unix 毫秒）。
	NextAtMS int64
	// Positions 候选落点（按顺序尝试，落点被占时由 Spawn 在附近找位）。
	Positions []scenemap.GridPos

	// Template 怪物属性模板（ConfigID 为空时用 CfgID 覆盖）。
	Template Spec
	// Geometry 格↔像素换算（由玩法/装配层给，entity 没有全局配置）。
	Geometry grid.Geometry

	// done 记录 NUM 型已经触发过（一次性语义）。
	//
	// 为什么不用 NextAtMS=0 当哨兵：0 是合法的"立刻触发"时间，
	// 一个字段不能同时表达"立即"和"已结束"，必须分开。
	done bool
}

// AddRefresher 登记一个刷怪器。
//
// 同时往 State.RefreshData.Spawners 镜像一份：规格 7.2 要求刷怪进度是
// "可快照、可回放"的场景状态，Spawners 就是那份对外可见的账本；
// entity 的私有表只是"带 Positions/Template 的完整配置"。
func AddRefresher(s *mgr.State, r *Refresher) error {
	if s == nil || r == nil {
		return fmt.Errorf("%w: state/refresher 为 nil", scenemap.ErrInvalidArgument)
	}
	if r.Kind != RefreshNUM && r.Kind != RefreshTIME {
		return fmt.Errorf("%w: 刷怪器 %d 的类型 %d 非法", scenemap.ErrInvalidArgument, r.ID, r.Kind)
	}
	if r.Count <= 0 {
		return fmt.Errorf("%w: 刷怪器 %d 的 Count 必须为正", scenemap.ErrInvalidArgument, r.ID)
	}
	if len(r.Positions) == 0 {
		return fmt.Errorf("%w: 刷怪器 %d 没有落点", scenemap.ErrInvalidArgument, r.ID)
	}
	st := storeOf(s)
	if st == nil {
		return fmt.Errorf("%w: state 不可用", scenemap.ErrInvalidState)
	}
	if _, dup := st.refreshers[r.ID]; dup {
		return fmt.Errorf("%w: 刷怪器 %d", scenemap.ErrAlreadyExists, r.ID)
	}
	st.refreshers[r.ID] = r

	if s.RefreshData == nil {
		s.RefreshData = mgr.NewRefreshData()
	}
	s.RefreshData.Spawners[r.ID] = mirrorSpawner(r, 0)
	s.Touch()
	return nil
}

// RemoveRefresher 注销一个刷怪器（已刷出的怪不受影响）。
func RemoveRefresher(s *mgr.State, id int32) {
	st := peekStore(s)
	if st != nil {
		delete(st.refreshers, id)
	}
	if s != nil && s.RefreshData != nil {
		delete(s.RefreshData.Spawners, id)
	}
	if s != nil {
		s.Touch()
	}
}

// RefresherOf 按 ID 取刷怪器（只读，不存在返回 nil）。
func RefresherOf(s *mgr.State, id int32) *Refresher {
	st := peekStore(s)
	if st == nil {
		return nil
	}
	return st.refreshers[id]
}

// TickRefresh 推进全部刷怪器，返回本次新刷出的 UUID。
//
// # 调用点
//
// 只在 MGR 的 loop 定时器里调用（由装配层塞进 Mgr.SetRefresh 或
// Deps 的钩子）。entity 绝不自己起 goroutine / 定时器——一刷怪器一
// goroutine 会在几百张图时变成几百个并发写 State 的写者。
//
// # 返回值的用途
//
// 新怪要进 AOI。entity 不碰 AOI（单一写入口在 MGR），所以把 UUID 交回
// 装配层，由它决定如何同步（当前 mgr 缺这个入口，见交付说明）。
func TickRefresh(s *mgr.State, nowMS int64) []scenemap.UUID {
	st := peekStore(s)
	if st == nil || len(st.refreshers) == 0 {
		return nil
	}

	// 按 ID 升序：多个刷怪器同拍到期时，"先刷谁"必须稳定，
	// 否则先刷的占了好落点、后刷的找不到位置，两次运行结果不同。
	ids := make([]int32, 0, len(st.refreshers))
	for id := range st.refreshers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var out []scenemap.UUID
	for _, id := range ids {
		r := st.refreshers[id]
		// done 表示一次性刷怪器已经完成；NextAtMS 的 0 值是"立即到期"
		// （合法的"马上刷"），所以只判 done，不能用 <=0 当哨兵。
		if r == nil || r.done {
			continue
		}
		if nowMS < r.NextAtMS {
			continue
		}

		target := int(r.Count)
		if r.MaxAlive > 0 && int(r.MaxAlive) < target {
			target = int(r.MaxAlive)
		}
		alive := countAlive(s, r)
		need := target - alive
		spawned := 0
		for i := 0; i < need; i++ {
			pos := r.pick(nowMS, i)
			uuid, err := Spawn(s, r.spec(), pos, r.geometry())
			if err != nil {
				// 单只刷失败不影响同一批的其他怪：一张挤爆的图不应该
				// 让整个刷怪器停摆（下一次 tick 会继续补）。
				continue
			}
			spawned++
			out = append(out, uuid)
		}

		if r.Kind == RefreshNUM {
			// 一次性：置 done，NextAtMS 归零（不再参与到期判断）。
			r.done = true
			r.NextAtMS = 0
		} else {
			cycle := r.CycleMS
			if cycle <= 0 {
				cycle = 1000
			}
			r.NextAtMS = nowMS + cycle
		}

		// 账本同步：Alive 用"刷完之后"的存活数（含之前就活着的）。
		if s.RefreshData != nil {
			s.RefreshData.Spawners[r.ID] = mirrorSpawner(r, int32(alive+spawned))
		}
	}
	return out
}

// spec 返回刷怪模板，补齐 CfgID/RefreshID 两个"由刷怪器决定"的字段。
func (r *Refresher) spec() Spec {
	sp := r.Template
	if sp.ConfigID == 0 {
		sp.ConfigID = r.CfgID
	}
	// 用刷怪器 ID 覆盖 RefreshID：存活统计按"属于哪个刷怪器"精确匹配，
	// 同一模板的两个刷怪器也不会互相把对方刷的怪算成自己的。
	sp.RefreshID = r.ID
	return sp
}

// geometry 返回几何参数，零值时用默认值。
func (r *Refresher) geometry() grid.Geometry {
	g := r.Geometry
	if g.CellW <= 0 || g.CellH <= 0 {
		return grid.DefaultGeometry()
	}
	return g
}

// pick 选第 i 只怪的落点。
//
// 种子 = 刷怪器 ID ^ 时间 ^ 序号：同一个刷怪器在同一毫秒刷出的一批怪
// 落点分布稳定，回放能复现；不同批次之间有变化。
func (r *Refresher) pick(nowMS int64, i int) scenemap.GridPos {
	if len(r.Positions) == 0 {
		return scenemap.GridPos{}
	}
	rnd := newRNG(uint64(uint32(r.ID))<<32 ^ uint64(nowMS) ^ uint64(i))
	return r.Positions[rnd.Intn(len(r.Positions))]
}

// countAlive 统计刷怪器名下还活着的怪。
func countAlive(s *mgr.State, r *Refresher) int {
	n := 0
	for id, info := range s.NPES {
		if info == nil || !info.Alive() {
			continue
		}
		if ownedBy(s, id, info, r) {
			n++
		}
	}
	return n
}

// ownedBy 报告这只怪是否属于该刷怪器。
//
// 优先用运行时的 Spec.RefreshID（精确）；没有运行时（怪是 mgr 的
// enterNPE 直接建的）就退化为"同模板 ID"。
//
// 局限：mgr.NpeAIInfo 没有 RefreshID 字段，所以"外部创建的、同模板的怪"
// 会被算进存活数——这属于 mgr 缺失 API，见交付说明。
func ownedBy(s *mgr.State, uuid scenemap.UUID, info *mgr.NpeAIInfo, r *Refresher) bool {
	if rt := runtimeOf(s, uuid); rt != nil && rt.spec.RefreshID != 0 {
		return rt.spec.RefreshID == r.ID
	}
	return info.TemplateID == r.CfgID
}

// mirrorSpawner 构造 RefreshData 里的账本条目。
func mirrorSpawner(r *Refresher, alive int32) *mgr.Spawner {
	return &mgr.Spawner{
		ID:         r.ID,
		TemplateID: r.CfgID,
		MaxAlive:   r.MaxAlive,
		Alive:      alive,
		IntervalMS: r.CycleMS,
		NextAt:     r.NextAtMS,
		Enabled:    !r.done,
	}
}
