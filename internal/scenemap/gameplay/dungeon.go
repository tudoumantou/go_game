package gameplay

import (
	"log"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/entity"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
)

// Dungeon 是最小可玩的副本玩法：若干波怪，清完通关。
//
// # 状态机
//
//	MgrInit → 第 1 波（CurrentWave=1）
//	本波全灭 → 第 2 波 … → 最后一波全灭 → Cleared=true + OnClear（只调一次）
//	关图 → OnTerminate（结算点，交给玩法上层发奖/写库）
//
// # 为什么波次推进有两处触发
//
// 正常路径是 HandleNpcDead（"最后一只怪死了"最接近事件本身）；Loop 里的
// 检查是兜底：怪可能因为"召唤到期/被 GM 清场/掉线重连刷图"等非死亡路径
// 消失，此时没有死亡回调，只能靠每秒扫描一次 NPES 发现"本波已经空了"。
// 两处都走 tryAdvance，靠"当前波已经没有存活怪"这个条件天然幂等。
type Dungeon struct {
	mgr.Unimplemented

	// TotalWaves 总波数。
	TotalWaves int32
	// CurrentWave 当前波次（1 起；0=未开始）。
	CurrentWave int32
	// Killed 累计击杀数（跨波累加）。
	Killed int32
	// Cleared 是否已通关。通关后 Loop/HandleNpcDead 全部短路。
	Cleared bool
	// OnClear 通关回调（发奖/广播/写战绩）。
	//
	// 必须**幂等**：框架只保证调用一次，但玩法自己也可能从别处触发结算，
	// 这里用 cleared 标记兜住，重复调用不会重复发奖。
	OnClear func(*mgr.State)

	// OnEnd 关图结算回调（掉落/通关奖励交给玩法上层）。
	OnEnd func(*mgr.State)

	// PerWave 每波刷几只。
	PerWave int32
	// CfgID 本副本的怪配置 ID（同时用于"这只怪是不是我的"的判定）。
	CfgID int32
	// Positions 刷怪落点（副本通常是固定刷怪点）。
	Positions []scenemap.GridPos
	// Geo 格↔像素换算。
	Geo grid.Geometry

	// cleared 是 OnClear 的幂等标记（与 Cleared 分开：Cleared 是业务状态，
	// 可能被外部读；cleared 只表示"回调已经发过"，避免语义纠缠）。
	cleared bool
}

// 编译期断言：副本必须满足玩法接口。
var _ mgr.Gameplay = (*Dungeon)(nil)

// 默认刷怪落点：地图左上角的五个格。
//
// 只用在小地图/测试；真实副本必须由配置给出刷怪点，否则"刷在墙里"。
var defaultPositions = []scenemap.GridPos{
	{X: 1, Y: 1}, {X: 2, Y: 1}, {X: 3, Y: 1}, {X: 1, Y: 2}, {X: 3, Y: 2},
}

// NewDungeon 构造副本玩法。
//
// 参数用位置参数而不是配置结构体：这是示例，字段就是这几个；
// 真正的项目里应该用 configs 里的地图参数表构造。
func NewDungeon(totalWaves, perWave int32, cfgID int32, positions []scenemap.GridPos) *Dungeon {
	if totalWaves <= 0 {
		totalWaves = 1
	}
	if perWave <= 0 {
		perWave = 1
	}
	if len(positions) == 0 {
		positions = defaultPositions
	}
	return &Dungeon{
		TotalWaves: totalWaves,
		PerWave:    perWave,
		CfgID:      cfgID,
		Positions:  positions,
		Geo:        grid.DefaultGeometry(),
	}
}

// MgrInit 初始化副本：把 Dungeon 存进 State.Data，并刷出第一波。
//
// # 为什么把 Dungeon 自己存进 s.Data
//
// 框架"不解释、不序列化"Data，玩法把实例放进去之后，凡是从命令/钩子里
// 拿到 *State 的地方都能取回来（例如 GM 命令查进度），不必再维护一张
// "State → 玩法实例"的全局表。Data 一张图只有一份，正好对应"一张图
// 一个玩法实例"。
func (d *Dungeon) MgrInit(s *mgr.State, args ...any) error {
	if s == nil {
		return scenemap.ErrInvalidArgument
	}
	s.Data = d
	d.CurrentWave = 0
	d.Killed = 0
	d.Cleared = false
	d.cleared = false
	return d.spawnWave(s, 1)
}

// Loop 是每秒（吃倍速）的波次检查。
//
// 规格 7.3：Loop 只做阶段推进，不做重活。这里只数一次"本波还剩几只"，
// 空波就是推进，全清就是通关。
func (d *Dungeon) Loop(s *mgr.State) error {
	if s == nil || d.Cleared || d.CurrentWave == 0 {
		return nil
	}
	if d.aliveCount(s) > 0 {
		return nil
	}
	d.tryAdvance(s)
	return nil
}

// PlayerEnter 副本不区分个人进度，只保证"有人进来就重置关图计时"
// ——关图计时由 MGR 的 onPlayerEnter 负责，这里无需额外动作。
func (d *Dungeon) PlayerEnter(scenemap.UUID, *mgr.State) error { return nil }

// PlayerLeave 单人副本可以在人走空时直接结算（由 MGR 根据
// CanCheckStopMap 决定关图，关图时 OnTerminate 统一结算）。
func (d *Dungeon) PlayerLeave(scenemap.UUID, scenemap.LeaveReason, *mgr.State) error {
	return nil
}

// HandleNpcDead 记录击杀，并在本波全灭时推进波次。
//
// 只认自己的模板 ID：副本图里可能有召唤物/宠物死亡，不能算进副本进度。
func (d *Dungeon) HandleNpcDead(npcID int32, uuid scenemap.UUID, x, y int32, s *mgr.State) error {
	_ = uuid
	_ = x
	_ = y
	if s == nil || d.Cleared || npcID != d.CfgID {
		return nil
	}
	d.Killed++
	if d.aliveCount(s) == 0 {
		d.tryAdvance(s)
	}
	return nil
}

// CanCheckStopMap 只在"已通关"或"图里没人"时允许关图。
//
// 副本进行中即使一个人都没有也不允许关？——不，那样会让"开错难度、
// 玩家秒退"的房间永远占着池子。这里按规格给的表达式实现：
//
//	Cleared || len(Users)==0
//
// 也就是"有玩家在进行中 → 不许关；打完了或者本来就空着 → 随便关"。
// MGR 只在 Users 为空时才会问这个问题（见 onPlayerLeave），所以实际
// 效果是：空副本会被关，有人的副本由玩家自己决定何时离开。
func (d *Dungeon) CanCheckStopMap(s *mgr.State) bool {
	if s == nil {
		return true
	}
	return d.Cleared || len(s.Users) == 0
}

// OnTerminate 是关图前的结算点。
//
// 框架在这里只做"告诉玩法要关了"：掉落、通关奖励、战绩落库都由玩法上层
// 在 OnEnd 里完成（有些项目还要走发奖中心/邮件，不适合塞进 MGR 的
// goroutine，实现方应把它投递出去而不是同步 RPC）。
func (d *Dungeon) OnTerminate(s *mgr.State) error {
	if s == nil {
		return nil
	}
	log.Printf("[scenemap/gameplay] 副本结算: key=%s wave=%d/%d kills=%d cleared=%v",
		s.Key.Key(), d.CurrentWave, d.TotalWaves, d.Killed, d.Cleared)
	if d.OnEnd != nil {
		d.OnEnd(s)
	}
	return nil
}

// ---- 内部 ----

// spawnWave 刷第 n 波：注销上一波的刷怪器、登记新的、立刻补满。
func (d *Dungeon) spawnWave(s *mgr.State, n int32) error {
	if n > d.TotalWaves {
		d.finish(s)
		return nil
	}
	d.CurrentWave = n

	// 上一波的刷怪器已经 done，但仍占着注册表（Num=1 用一条新 ID 更直观），
	// 显式摘掉，避免 RefreshData 里堆一串历史刷怪器。
	if n > 1 {
		entity.RemoveRefresher(s, d.refresherID(n-1))
	}

	now := s.Now()
	r := &entity.Refresher{
		ID:        d.refresherID(n),
		Kind:      entity.RefreshNUM,
		CfgID:     d.CfgID,
		Count:     d.PerWave,
		MaxAlive:  d.PerWave,
		NextAtMS:  now,
		Positions: d.Positions,
		Template: entity.Spec{
			// FIXED：副本的小怪只负责挨打，走位由客户端表现；AI 打开时
			// 把它们改成 StateGuard 即可（本包已实现完整状态机）。
			AI:     entity.StateFixed,
			HP:     100,
			Attack: 10,
			Speed:  1000,
		},
		Geometry: d.Geo,
	}
	if err := entity.AddRefresher(s, r); err != nil {
		return err
	}
	// 立刻补满第一波：规格 7.4 要求 MgrInit 就能"刷初始怪"，不能等
	// 下一秒 loop 才出现——玩家进副本的第一帧就该看到怪。
	spawned := entity.TickRefresh(s, now)
	if len(spawned) == 0 {
		log.Printf("[scenemap/gameplay] 副本第 %d 波刷怪失败（落点被占/无可用格）", n)
	}
	return nil
}

// tryAdvance 在"当前波没有存活怪"时推进到下一波或通关。
func (d *Dungeon) tryAdvance(s *mgr.State) {
	if d.Cleared || d.CurrentWave == 0 {
		return
	}
	if d.CurrentWave >= d.TotalWaves {
		d.finish(s)
		return
	}
	if err := d.spawnWave(s, d.CurrentWave+1); err != nil {
		log.Printf("[scenemap/gameplay] 副本推进到第 %d 波失败: %v", d.CurrentWave+1, err)
	}
}

// finish 通关：置标记并**只调一次** OnClear。
func (d *Dungeon) finish(s *mgr.State) {
	if d.Cleared {
		return
	}
	d.Cleared = true
	if d.cleared {
		return
	}
	d.cleared = true
	if d.OnClear != nil {
		d.OnClear(s)
	}
}

// aliveCount 数本副本还活着的怪（按模板 ID）。
//
// 只数 Alive()：尸体（AI=Dead）已经不算"本波未清"，否则 HandleNpcDead
// 在"扣血致死"和"移出 NPES"之间会看到一只还在图里的尸体，永远推进不了。
func (d *Dungeon) aliveCount(s *mgr.State) int {
	if s == nil {
		return 0
	}
	n := 0
	for _, info := range s.NPES {
		if info == nil || !info.Alive() {
			continue
		}
		if info.TemplateID == d.CfgID {
			n++
		}
	}
	return n
}

// refresherID 给每一波一个独立的刷怪器 ID（1000+波次），避免复用同一
// 条记录时"上一波 done 标记"被带进下一波。
func (d *Dungeon) refresherID(wave int32) int32 { return 1000 + wave }
