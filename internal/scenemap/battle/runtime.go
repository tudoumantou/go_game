package battle

import (
	"math/rand"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/mgr"
)

// extKey 是 battle 运行时在 State.Ext 里的键。
//
// 用带命名空间前缀的键（规格 §3.3 对 Ext 的要求）：Ext 是多模块共用的
// 无类型槽位，裸键 "battle" 早晚会和玩法/活动模块撞车。
const extKey = "battle.runtime"

// Options 是战斗运行时的构造参数。
//
// 全部零值可用：Table 为 nil 用 DefaultTable，Now 为 nil 用系统时钟。
// 这让"只想测公式"的单元测试不必构造整个场景。
type Options struct {
	// Table 配置表。
	Table *Table
	// NoBattle 禁止结算（主城/安全区，规格 §13.9）。
	NoBattle bool
	// MapType 地图类型（写进结算上下文，供玩法记伤）。
	MapType int32
	// MirrorMaps 允许镜像施法的地图类型白名单（规格 §13.15）。
	//
	// 缺省为空 = 一律拒绝。白名单为空比"默认放行"安全：
	// 漏配的后果是"镜像不能打"（可见、可测），而不是"任意图都能用
	// 客户端代控协议施法"（不可见、可被利用）。
	MirrorMaps []int32
	// Now 时钟（unix 毫秒）。nil 用 time.Now。
	Now func() int64
	// Submit 把一条命令投回 MGR（用于延时 CAST / 子弹命中）。
	//
	// 装配层传 Mgr.Post；不传时延时任务进 Runtime 的本地队列，
	// 由 Tick/RunDue 手动推进——这是测试可以完全确定性的前提。
	Submit func(mgr.Command) error
	// Notify 把结果事件投递给"目标主人"的玩家 Actor（规格 §10.7 结果回路）。
	Notify func(uuid scenemap.UUID, evt any)
	// OnNpcDead 怪物死亡回调（装配层接 entity.Despawn + 玩法 HandleNpcDead）。
	OnNpcDead func(s *mgr.State, uuid scenemap.UUID, npcID int32)
	// RandSeed 随机种子（0 用固定种子：默认必须可复现）。
	RandSeed int64
	// FightTimeoutMS 脱战超时（毫秒，0 用 5000）。
	FightTimeoutMS int64
}

// Runtime 是一张图的战斗私有状态。
//
// # 单写者
//
// 与 mgr.State 同一条纪律：只在 MGR goroutine 内读写。挂在 State.Ext 上，
// 随 State 一起被 MGR 独占，不额外加锁。
type Runtime struct {
	table *Table
	opts  Options
	clock func() int64

	// sprites 战斗属性投影：NPC 权威、玩家只读镜像。
	sprites map[scenemap.UUID]*Sprite
	// cd 技能冷却：uuid → skillID → 可用时间（unix 毫秒）。
	cd map[scenemap.UUID]map[int32]int64
	// passiveCD 被动内部冷却：key = "uuid:passiveID"。
	passiveCD map[string]int64
	// dead 死亡幂等标记（规格 §13.5）。
	//
	// 必须与 sprites 同生命周期：怪被 Despawn 后 UUID 会被复用
	// （entity.allocUUID 从 len(NPES) 倒推），残留标记会把新怪判成尸体。
	dead map[scenemap.UUID]bool
	// delayed 本地延时队列（Options.Submit 为 nil 时使用）。
	delayed []delayJob
	// seq 子弹/火墙的 UUID 序号（负数高位段，与怪物的 -1..-N 隔开）。
	seq int64
	// rng 确定性随机源（暴击/闪避/格挡全部走它）。
	rng *rand.Rand
	// fightTimeout 脱战超时（毫秒）。
	fightTimeout int64
	// mirrorOK 镜像白名单（构造时展开成 set）。
	mirrorOK map[int32]bool
}

// delayJob 是本地延时任务。
type delayJob struct {
	at int64
	fn func(*mgr.State)
}

// jobCommand 把闭包适配成 mgr.Command（延时投递用）。
//
// 为什么允许"闭包进 MGR"：延时任务本质是"未来的自己给现在的自己发一条
// 消息"，闭包捕获的都是不可变参数（UUID/技能 ID），不跨 Actor 传状态。
type jobCommand struct {
	name string
	fn   func(*mgr.State)
}

// Name 实现 mgr.Command。
func (c jobCommand) Name() string { return c.name }

// Apply 实现 mgr.Command。
func (c jobCommand) Apply(s *mgr.State) (any, error) {
	if c.fn != nil {
		c.fn(s)
	}
	return nil, nil
}

// MayNoop 实现 mgr.noopCommand：延时任务可能"什么都没做"（目标已消失）。
func (c jobCommand) MayNoop() bool { return true }

// Ensure 取得（必要时创建）一张图的战斗运行时。
//
// # 幂等（规格 §4.2）
//
// 房间图可能被"框架回调 + 玩法显式调用"重复拉起，第二次必须复用同一份
// 运行时——否则冷却表/死亡标记会被重置，表现为"技能突然能连放"。
func Ensure(s *mgr.State, opts Options) *Runtime {
	if s == nil {
		return nil
	}
	if s.Ext == nil {
		s.Ext = make(map[string]any)
	}
	if rt, ok := s.Ext[extKey].(*Runtime); ok && rt != nil {
		return rt
	}
	rt := newRuntime(opts)
	s.Ext[extKey] = rt
	s.Touch()
	return rt
}

// From 读取已有运行时（未初始化返回 false）。
func From(s *mgr.State) (*Runtime, bool) {
	if s == nil || s.Ext == nil {
		return nil, false
	}
	rt, ok := s.Ext[extKey].(*Runtime)
	return rt, ok && rt != nil
}

// newRuntime 构造运行时并补齐默认值。
func newRuntime(opts Options) *Runtime {
	if opts.Table == nil {
		opts.Table = DefaultTable()
	}
	if opts.Now == nil {
		opts.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if opts.RandSeed == 0 {
		// 固定种子：默认行为必须可复现，线上要随机由装配层显式传种子。
		opts.RandSeed = 20261023
	}
	if opts.FightTimeoutMS <= 0 {
		opts.FightTimeoutMS = 5000
	}
	rt := &Runtime{
		table:        opts.Table,
		opts:         opts,
		clock:        opts.Now,
		sprites:      make(map[scenemap.UUID]*Sprite),
		cd:           make(map[scenemap.UUID]map[int32]int64),
		passiveCD:    make(map[string]int64),
		dead:         make(map[scenemap.UUID]bool),
		rng:          rand.New(rand.NewSource(opts.RandSeed)),
		fightTimeout: opts.FightTimeoutMS,
		mirrorOK:     make(map[int32]bool, len(opts.MirrorMaps)),
	}
	for _, t := range opts.MirrorMaps {
		rt.mirrorOK[t] = true
	}
	return rt
}

// Table 返回配置表（只读）。
func (rt *Runtime) Table() *Table { return rt.table }

// NoBattle 报告本图是否禁止结算。
func (rt *Runtime) NoBattle() bool { return rt.opts.NoBattle }

// MirrorAllowed 报告当前地图是否允许镜像施法。
func (rt *Runtime) MirrorAllowed() bool { return rt.mirrorOK[rt.opts.MapType] }

// now 返回当前时间：优先用 State 注入的时钟（便于测试整体对齐），
// 无主状态（单元测试）退回 Runtime 自己的时钟。
func (rt *Runtime) now(s *mgr.State) int64 {
	if s != nil {
		if n := s.Now(); n > 0 {
			return n
		}
	}
	return rt.clock()
}

// chance 掷一次万分比概率。
func (rt *Runtime) chance(permille int64) bool {
	if permille <= 0 {
		return false
	}
	if permille >= PermilleOne {
		return true
	}
	return int64(rt.rng.Intn(int(PermilleOne))) < permille
}

// spriteOf 取得实体的战斗投影；不存在时按场景状态懒创建。
//
// # 为什么懒创建而不是进图时注册
//
// 怪物由 entity.Spawn / mgr.enterNPE 创建，它们不认识 battle。让 battle
// 在第一次被攻击/出手时从 NPES 里"投影"出来，可以做到**零装配**接入：
// 场景照常刷怪，战斗只读现成数据。
func (rt *Runtime) spriteOf(s *mgr.State, uuid scenemap.UUID) *Sprite {
	if rt == nil || s == nil || !uuid.Valid() {
		return nil
	}
	if sp, ok := rt.sprites[uuid]; ok && sp != nil {
		// 死亡标记与怪的重生：Despawn 后 UUID 会被复用，而 rt.dead 里
		// 还留着上一只的标记。检测到"场景里这只怪活着但战斗记录是死的"
		// 就重建投影并清标记——漏了这一步，新怪第一下必然打不出伤害。
		if info := s.NPES[uuid]; info != nil && info.Alive() && sp.Dead {
			delete(rt.dead, uuid)
			delete(rt.sprites, uuid)
		} else {
			// NPC 的 HP 以场景为准（可能被 entity.Damage 直接改过）。
			if info := s.NPES[uuid]; info != nil {
				sp.HP = info.HP
				sp.MaxHP = info.MaxHP
				sp.Pos = info.GridPos
				sp.Dir = info.Dir
			}
			if u := s.Users[uuid]; u != nil {
				sp.Pos = u.GridPos
				sp.Dir = u.Dir
			}
			return sp
		}
	}
	if info := s.NPES[uuid]; info != nil {
		return rt.newNpcSprite(info)
	}
	if u := s.Users[uuid]; u != nil {
		return rt.newPlayerSprite(u)
	}
	if p := s.Pets[uuid]; p.Valid() {
		// 宠物/召唤暂按"主人模板"投影：真实项目应从宠物表读。
		sp := rt.newPlayerSprite(&mgr.UserState{UUID: uuid, Kind: scenemap.EntityKindPet, MasterUUID: p})
		sp.Owner = p
		rt.sprites[uuid] = sp
		return sp
	}
	return nil
}

// newNpcSprite 从怪物模板 + 场景状态构造投影。
func (rt *Runtime) newNpcSprite(info *mgr.NpeAIInfo) *Sprite {
	cfg, err := rt.table.Npc(info.TemplateID)
	if err != nil {
		// 配置缺失也允许打：用默认值并保持 HP 与场景一致。
		// 这里不返回 nil——"打不动"比"属性不对"更难排查。
		cfg = &NpcConfig{CfgID: info.TemplateID, MaxHP: info.MaxHP, Camp: 2}
	}
	sp := &Sprite{
		UUID: info.UUID, Kind: info.Kind, CfgID: info.TemplateID,
		Camp: cfg.Camp,
		HP:   info.HP, MaxHP: info.MaxHP,
		Atk: cfg.Atk, Def: cfg.Def, Hit: cfg.Hit, Dod: cfg.Dod, Crit: cfg.Crit,
		SpeedAtk: PermilleOne, ElementCoe: PermilleOne,
		ThornsCoe: PermilleOne, FinalAddDmgCoe: PermilleOne,
		Pos: info.GridPos, Dir: info.Dir,
	}
	if sp.MaxHP <= 0 {
		sp.MaxHP = cfg.MaxHP
	}
	if sp.MaxHP <= 0 {
		sp.MaxHP = 1
	}
	if sp.HP <= 0 {
		sp.HP = sp.MaxHP
	}
	rt.sprites[info.UUID] = sp
	return sp
}

// newPlayerSprite 从玩家模板构造投影。
func (rt *Runtime) newPlayerSprite(u *mgr.UserState) *Sprite {
	t := rt.table.Player
	sp := &Sprite{
		UUID: u.UUID, Kind: u.Kind, Owner: u.UUID,
		Camp: t.Camp, HP: t.MaxHP, MaxHP: t.MaxHP,
		Atk: t.Atk, Def: t.Def, Hit: t.Hit, Dod: t.Dod, Crit: t.Crit,
		SpeedAtk: PermilleOne, ElementCoe: PermilleOne,
		ThornsCoe: PermilleOne, FinalAddDmgCoe: PermilleOne,
		Pos: u.GridPos, Dir: u.Dir,
	}
	if sp.MaxHP <= 0 {
		sp.MaxHP = 1000
		sp.HP = 1000
	}
	rt.sprites[u.UUID] = sp
	return sp
}

// UpsertPlayer 覆盖/同步一个玩家侧的战斗投影。
//
// 由玩家 Actor 在进图/属性变化时投递（SyncPlayerCommand）。只合并战斗
// 相关字段，位置始终以场景为准（避免客户端/场景两侧位置漂移）。
func (rt *Runtime) UpsertPlayer(s *mgr.State, sp Sprite) {
	if rt == nil || !sp.UUID.Valid() {
		return
	}
	old, ok := rt.sprites[sp.UUID]
	if !ok || old == nil {
		c := sp
		if c.Kind == scenemap.EntityKindNone {
			c.Kind = scenemap.EntityKindPlayer
		}
		if c.Owner == 0 {
			c.Owner = c.UUID
		}
		if c.MaxHP <= 0 {
			c.MaxHP = rt.table.Player.MaxHP
		}
		if c.HP <= 0 {
			c.HP = c.MaxHP
		}
		rt.sprites[c.UUID] = &c
	} else {
		// 保留结算侧状态（Buffs/位置/死亡），覆盖属性字段。
		old.MaxHP, old.HP = sp.MaxHP, sp.HP
		old.Atk, old.Def, old.RealAtk, old.RealDef = sp.Atk, sp.Def, sp.RealAtk, sp.RealDef
		old.Hit, old.Dod, old.Crit, old.ReCrit = sp.Hit, sp.Dod, sp.Crit, sp.ReCrit
		old.CritDmgInc, old.CritDmgDec = sp.CritDmgInc, sp.CritDmgDec
		old.LuckyHit, old.ReLuckyHit = sp.LuckyHit, sp.ReLuckyHit
		old.Bok, old.IgnoreBok, old.BokDmgInc, old.BokDmgDec = sp.Bok, sp.IgnoreBok, sp.BokDmgInc, sp.BokDmgDec
		old.AddDmgCoe, old.FinalAddDmgCoe, old.Discriminate = sp.AddDmgCoe, sp.FinalAddDmgCoe, sp.Discriminate
		old.AddPetDmg, old.DecPetDmg, old.MapTypeDmg, old.PkPress = sp.AddPetDmg, sp.DecPetDmg, sp.MapTypeDmg, sp.PkPress
		old.ThornsRate, old.ReThornsRate, old.ThornsCoe = sp.ThornsRate, sp.ReThornsRate, sp.ThornsCoe
		old.IgnoreDef, old.SpeedAtk, old.ElementCoe = sp.IgnoreDef, sp.SpeedAtk, sp.ElementCoe
		old.Camp = sp.Camp
	}
	if u := s.Users[sp.UUID]; u != nil {
		old := rt.sprites[sp.UUID]
		old.Pos, old.Dir = u.GridPos, u.Dir
	}
}

// Schedule 安排一条延时任务（毫秒，已按倍速缩放）。
//
// 两条路径：
//   - 生产（Options.Submit 非空）：time.AfterFunc 到点后把命令 Post 回
//     MGR，投递与执行严格分离，符合"状态只由 MGR goroutine 写"；
//   - 测试（Submit 为空）：进本地队列，由 RunDue 在指定时间点执行，
//     不依赖真实时钟，结果完全确定。
func (rt *Runtime) Schedule(s *mgr.State, delayMS int64, name string, fn func(*mgr.State)) {
	if rt == nil || fn == nil {
		return
	}
	if delayMS <= 0 {
		fn(s)
		return
	}
	if rt.opts.Submit != nil {
		time.AfterFunc(time.Duration(delayMS)*time.Millisecond, func() {
			_ = rt.opts.Submit(jobCommand{name: name, fn: fn})
		})
		return
	}
	rt.delayed = append(rt.delayed, delayJob{at: rt.now(s) + delayMS, fn: fn})
}

// RunDue 执行所有到期任务（测试与 Tick 共用）。
//
// 边遍历边执行：任务里可能会再 Schedule（多段伤害），追加到切片尾部，
// 用下标循环自然把"本轮新到期的任务"也跑掉；新任务若在未来则留到下一轮。
func (rt *Runtime) RunDue(s *mgr.State, now int64) {
	if rt == nil || len(rt.delayed) == 0 {
		return
	}
	for i := 0; i < len(rt.delayed); i++ {
		job := rt.delayed[i]
		if job.at > now {
			continue
		}
		rt.delayed = append(rt.delayed[:i], rt.delayed[i+1:]...)
		i--
		job.fn(s)
	}
}

// PendingDelayed 返回未执行的延时任务数（测试/监控用）。
func (rt *Runtime) PendingDelayed() int {
	if rt == nil {
		return 0
	}
	return len(rt.delayed)
}

// cooldownLeft 返回技能剩余冷却（毫秒，0 = 可用）。
func (rt *Runtime) cooldownLeft(uuid scenemap.UUID, skillID int32, now int64) int64 {
	m := rt.cd[uuid]
	if m == nil {
		return 0
	}
	until := m[skillID]
	if until <= now {
		return 0
	}
	return until - now
}

// setCooldown 记录技能冷却。
func (rt *Runtime) setCooldown(uuid scenemap.UUID, skillID int32, now, cdMS int64) {
	if cdMS <= 0 {
		return
	}
	m := rt.cd[uuid]
	if m == nil {
		m = make(map[int32]int64)
		rt.cd[uuid] = m
	}
	m[skillID] = now + cdMS
}

// passiveReady 检查并占用被动内部冷却。
func (rt *Runtime) passiveReady(uuid scenemap.UUID, passiveID int32, now, cdMS int64) bool {
	key := uuid.String() + ":" + itoa(passiveID)
	if until := rt.passiveCD[key]; until > now {
		return false
	}
	if cdMS > 0 {
		rt.passiveCD[key] = now + cdMS
	}
	return true
}

// Tick 是 500ms（吃倍速）的战斗推进：延时任务 → Buff 周期 → 子弹 → 火墙 → 脱战。
//
// 由装配层挂进 mgr.Deps.BuffTick。放在同一个入口里，是为了让"一拍的
// 结算顺序"成为确定的事实：先到期的技能，再持续伤害，再飞行物命中，
// 最后脱战判定——反过来会出现"刚被 Dot 打死又打出一发子弹"的鬼故事。
func Tick(s *mgr.State, now int64) {
	rt, ok := From(s)
	if !ok {
		return
	}
	if now <= 0 {
		now = rt.now(s)
	}
	rt.RunDue(s, now)
	rt.tickBuffs(s, now)
	rt.tickBullets(s, now)
	rt.tickFirewells(s, now)
	rt.tickFightState(s, now)
}

// tickFightState 处理脱战：超时无出手/受击就把 Fight 置回 false。
func (rt *Runtime) tickFightState(s *mgr.State, now int64) {
	for _, sp := range rt.sprites {
		if sp == nil || !sp.Fight {
			continue
		}
		if sp.LastFightAt > 0 && now-sp.LastFightAt >= rt.fightTimeout {
			sp.Fight = false
		}
	}
}

// itoa 是包内小工具（避免为几个字符串拼接引入 strconv 的完整语义）。
func itoa(v int32) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [12]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
