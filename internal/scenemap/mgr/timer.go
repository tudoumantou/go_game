package mgr

import (
	"time"

	"game_study/internal/scenemap"
)

// 定时器的基础间隔（规格 7.1）。
//
//	loop / buffCheck 吃倍速：倍速是玩法节奏（刷怪、buff、AI），
//	                       加速必须同时加快它们，否则"3 倍速"只有怪走得快；
//	oneSec / halfSec 不吃倍速：触发器与轻量维护是"墙上时间"语义，
//	                       加速会让"每 5 秒开门"变成"每 1.6 秒开门"，
//	                       与玩家的时间直觉冲突。
const (
	oneSecInterval  = time.Second
	halfSecInterval = 500 * time.Millisecond
	// minTimerInterval 是间隔下限。1ms 是 Go 定时器在高负载下的实际粒度，
	// 再小只会让 MGR 空转、把 CPU 烧在 select 上。
	minTimerInterval = time.Millisecond
)

// CondMove 是"换格"触发器条件号（规格 6.3 的 COND_2）。
//
// 条件号来自配置表，只追加不复用。0/1 在触发器系统里另有含义
// （进图/离图），这里只落地移动这一条。
const CondMove int32 = 2

// TriggerFunc 是场景触发器（每秒调用一次，不吃倍速）。
//
// 第一版只留接口：真正的触发器表（踩点开机关、区域喊话）由玩法/触发器
// 模块实现，MGR 只负责"按时调用"。fn 为 nil 时 oneSec 定时器自动停表——
// 空转一张没有触发器的图是纯浪费（几百张图就是几百个每秒唤醒）。
type TriggerFunc func(s *State, now int64)

// TriggerCondFunc 是条件触发器（事件驱动，例如换格触发 COND_2）。
type TriggerCondFunc func(s *State, uuid scenemap.UUID, oldPos, newPos scenemap.GridPos, cond int32)

// RefreshFunc 是刷怪器推进（遗留接口，规格 7.2）。
//
// 为什么保留"外部函数"而不是在 MGR 里写刷怪逻辑：刷怪规则与玩法强相关
// （副本按波次、野外按密度、活动按时间表），且它需要随机数与配置。
// MGR 只提供"每秒一次、串行、带 State"的执行点。
type RefreshFunc func(s *State, now int64)

// repeatingTimer 是自续期的单次定时器。
//
// # 为什么不用 time.Ticker
//
// Ticker 的间隔在创建后不可修改。倍速一切换（1000→2000）就得 Stop 旧的、
// New 一个新的，而新旧交替之间必然丢/多一拍，且 Ticker 会丢弃"来不及处理"
// 的 tick（对游戏循环来说，丢一拍意味着 buff 少结算一次）。Timer 每次
// 触发后由我们自己 Reset(新间隔)，语义精确：**下一拍的间隔就是当前倍速**。
//
// Reset 必须 Stop + 排空：如果定时器已经触发但还没被 select 取走，
// 直接 Reset 会让旧值留在 channel 里，下一次 select 立刻又触发一次
// （典型的"越跑越快"bug）。Go 1.23 之前这个坑必须手动处理，本包保守
// 按老规则写，兼容所有 1.22+ 运行时。
type repeatingTimer struct {
	t        *time.Timer
	interval time.Duration
}

func newRepeating(iv time.Duration) *repeatingTimer {
	if iv < minTimerInterval {
		iv = minTimerInterval
	}
	return &repeatingTimer{t: time.NewTimer(iv), interval: iv}
}

// C 返回触发通道。
func (r *repeatingTimer) C() <-chan time.Time { return r.t.C }

// SetInterval 只改间隔，不重置当前这一拍。
func (r *repeatingTimer) SetInterval(iv time.Duration) {
	if iv < minTimerInterval {
		iv = minTimerInterval
	}
	r.interval = iv
}

// Reset 按当前间隔重新计时（用于"已触发，排下一拍"）。
func (r *repeatingTimer) Reset() {
	if !r.t.Stop() {
		// 已触发但值还没被取走：排空，避免下一拍立即触发。
		select {
		case <-r.t.C:
		default:
		}
	}
	r.t.Reset(r.interval)
}

// Stop 停止并排空。
func (r *repeatingTimer) Stop() {
	if !r.t.Stop() {
		select {
		case <-r.t.C:
		default:
		}
	}
}

// scale 把基础间隔换算成当前倍速下的实际间隔。
//
// 倍速用千分比：1000=1x，2000=2x。间隔 = 基础 × 1000 / 倍速，
// 即 2 倍速时 1s 变 500ms。全程整数运算，不引入浮点误差。
func (m *Mgr) scale(d time.Duration) time.Duration {
	p := m.speed.Load()
	if p <= 0 {
		p = 1000
	}
	out := d * 1000 / time.Duration(p)
	if out < minTimerInterval {
		out = minTimerInterval
	}
	return out
}

// loopInterval / buffInterval 返回吃倍速的间隔。
func (m *Mgr) loopInterval() time.Duration { return m.scale(m.cfg.TickInterval) }
func (m *Mgr) buffInterval() time.Duration { return m.scale(m.cfg.TickInterval / 2) }

// startTimers 创建并启动四个定时器（只在 run goroutine 内调用）。
func (m *Mgr) startTimers() {
	m.loopT = newRepeating(m.loopInterval())
	m.buffT = newRepeating(m.buffInterval())
	m.oneSecT = newRepeating(oneSecInterval)
	m.halfSecT = newRepeating(halfSecInterval)
	m.setOneSecArmed(m.getTrigger() != nil)
	m.setHalfArmed(m.cfg.HalfSecEnabled)
}

// stopTimers 停掉全部定时器。
func (m *Mgr) stopTimers() {
	for _, t := range []*repeatingTimer{m.loopT, m.buffT, m.oneSecT, m.halfSecT} {
		if t != nil {
			t.Stop()
		}
	}
}

// rearmTimers 在倍速/触发器变化后重建间隔（只在 run goroutine 内调用）。
func (m *Mgr) rearmTimers() {
	if m.loopT == nil {
		return // 还没启动，startTimers 会用新倍速创建
	}
	m.loopT.SetInterval(m.loopInterval())
	m.loopT.Reset()
	m.buffT.SetInterval(m.buffInterval())
	m.buffT.Reset()
	// oneSec/halfSec 不吃倍速，只在"启用状态"变化时开关，避免白白重置相位。
	m.setOneSecArmed(m.getTrigger() != nil)
	m.setHalfArmed(m.cfg.HalfSecEnabled)
}

// setOneSecArmed 开关 oneSec 定时器。
//
// 停表用 nil channel 的技巧：select 在 nil channel 上永远阻塞，
// 等价于"这个 case 不存在"，比每次 select 前判断 bool 更省事，
// 也不会因为忘记判断而空跑。
func (m *Mgr) setOneSecArmed(on bool) {
	if on == m.oneSecArmed {
		return
	}
	m.oneSecArmed = on
	if on {
		m.oneSecT.SetInterval(oneSecInterval)
		m.oneSecT.Reset()
		m.oneSecC = m.oneSecT.C()
		return
	}
	m.oneSecT.Stop()
	m.oneSecC = nil
}

// setHalfArmed 开关 halfSec 定时器（默认关闭）。
func (m *Mgr) setHalfArmed(on bool) {
	if on == m.halfArmed {
		return
	}
	m.halfArmed = on
	if on {
		m.halfSecT.SetInterval(halfSecInterval)
		m.halfSecT.Reset()
		m.halfSecC = m.halfSecT.C()
		return
	}
	m.halfSecT.Stop()
	m.halfSecC = nil
}

// ---- 触发器/hook 读锁封装 ----

func (m *Mgr) getTrigger() TriggerFunc {
	m.hooksMu.RLock()
	defer m.hooksMu.RUnlock()
	return m.trigger
}

func (m *Mgr) getCondTrigger() TriggerCondFunc {
	m.hooksMu.RLock()
	defer m.hooksMu.RUnlock()
	return m.condTrigger
}

func (m *Mgr) getRefresh() RefreshFunc {
	m.hooksMu.RLock()
	defer m.hooksMu.RUnlock()
	return m.refresh
}

// ---- 四个定时器的处理函数 ----

// handleLoop 是每秒（吃倍速）的主逻辑。抽成普通方法，让 CommandTick
// 可以"手动跑一拍"，测试不必等真实定时器。
func (m *Mgr) handleLoop(s *State, now int64) {
	m.loopTicks++

	// 1) 推进刷怪器（遗留接口）。
	m.tickRefresh(s, now)

	// 2) 玩法自己的每秒逻辑。
	if err := m.gameplay.Loop(s); err != nil {
		m.deps.Logger.Error("scenemap/mgr: Gameplay.Loop 失败",
			"key", m.cfg.Key.Key(), "err", err)
	}

	// 3) 收割关图。放在最后：让玩法有机会在这一拍里"救人"（例如
	//    最后一秒有人复活、玩法把 CanCheckStopMap 改成 false）。
	m.collectClose(s, now)
}

// tickRefresh 调用刷怪推进钩子。
func (m *Mgr) tickRefresh(s *State, now int64) {
	fn := m.getRefresh()
	if fn == nil {
		return
	}
	fn(s, now)
}

// handleBuffCheck 是 500ms（吃倍速）的 buff/召唤结算。
//
// # 为什么一定要排序
//
// NPES 是 map，Go 的 range 顺序每次运行都不同（随机起点 + 随机步长）。
// buff 结算是有副作用的（扣血、死亡、触发被动），顺序不同会导致：
//   - 两只互相致死的怪，谁先死取决于遍历顺序 → 掉落归属不稳定；
//   - 复现线上问题时回放顺序对不上，日志再全也没用。
//
// 所以统一走 NpeUUIDs()（升序），让结算顺序成为状态的一部分。
func (m *Mgr) handleBuffCheck(s *State, now int64) {
	// AI 每轮推进一次（不是每只怪一次）：AI 自己遍历 NPES，
	// 放在这里保证"每 500ms 恰好一轮"，与怪的数量无关。
	if m.deps.AITick != nil {
		m.deps.AITick(s, now)
	}
	// 战斗结算（DoT/子弹/火墙/延时施法）与 AI 同一拍、同一 goroutine：
	// 两条时间线合并成一条，结算顺序才是可回放的。
	if m.deps.BuffTick != nil {
		m.deps.BuffTick(s, now)
	}
	for _, id := range s.NpeUUIDs() {
		info := s.NPES[id]
		if info == nil {
			continue // 结算过程中可能被删（理论上单写者不会，防御）
		}
		m.buffTickOne(s, info, now)
	}
}

// buffTickOne 是单只怪的 buff 结算。
//
// 第一版只落地"按 UUID 升序遍历"这条可复现性约束，**结算内容尚未实现**，
// 下面三条是接入点，刻意不写假实现：
//
//	TODO(规格 7.2 时停延迟死亡): AIStateStopped 的怪要延迟 DeadAt 的结算，
//	     时停期间不推进死亡计时。
//	TODO(规格 7.2 召唤超时): SummonExpireAt 到期的召唤物走死亡流程
//	     （HandleNpcDead + 删 NPES + 回收阻挡）。
//	TODO(规格 7.2 脱战恢复): 长时间无仇恨的怪回血、回出生点。
//	TODO(规格 7.2 buff 周期): Buffs 列表按 duration 扣血/加状态。
func (m *Mgr) buffTickOne(s *State, info *NpeAIInfo, now int64) {
	_ = info
}

// handleOneSec 是每秒的触发器（不吃倍速）。
//
// 无触发器时由 onOneSecTick 自动停表，这里只负责调用。
func (m *Mgr) handleOneSec(s *State, now int64) {
	fn := m.getTrigger()
	if fn == nil {
		return
	}
	fn(s, now)
}

// onLoopTick / onBuffTick / onOneSecTick / onHalfSecTick 由 run 的 select 调用，
// 负责"处理 + 排下一拍"。
//
// 注意 Reset 放在处理之后：这样间隔是"处理结束 → 下一拍开始"，
// 如果处理偶发超时，下一拍会被自然推迟而不是追赶（追赶会造成 tick 风暴，
// 一张卡过的图会在恢复瞬间连跑十几次结算）。代价是长时间高负载下
// 实际周期略大于标称值，对游戏循环可以接受。

func (m *Mgr) onLoopTick(s *State) {
	m.handleLoop(s, m.now())
	m.loopT.Reset()
}

func (m *Mgr) onBuffTick(s *State) {
	m.handleBuffCheck(s, m.now())
	m.buffT.Reset()
}

func (m *Mgr) onOneSecTick(s *State) {
	if m.getTrigger() == nil {
		// 触发器被清空：自动停表，不再占用每秒一次唤醒。
		m.setOneSecArmed(false)
		return
	}
	m.handleOneSec(s, m.now())
	m.oneSecT.Reset()
}

func (m *Mgr) onHalfSecTick(s *State) {
	// 第一版没有必须在 500ms 粒度上做的事（子弹推进由玩法在 Loop 里做）。
	// 保留定时器骨架是为了让"默认关闭 + 需要时开启"成为配置项，
	// 而不是以后再加一个定时器（那会改变 select 的形状）。
	_ = s
	m.halfSecT.Reset()
}
