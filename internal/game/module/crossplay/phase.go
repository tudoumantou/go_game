// phase.go —— 阶段计算：把「现在几点」翻译成「活动走到哪一步了」。
//
// 为什么各服自己算而不等中心服下发期次：与 pkg/activity 的设计一致——
// 给定任意时刻，中心服和每个游戏服都能用同一套算法算到同一个窗口，
// 消息丢了只是少了确认，不会算错。代价是两台机器的时钟必须大致同步
// （NTP 常态下误差毫秒级），换来的是「中心服一条消息都不发，游戏服
// 照样知道自己该不该收报名」。
package crossplay

import (
	"game_study/pkg/activity"
)

// secondsPerHour 一小时的秒数。
//
// 不在本包之外传播：阶段偏移是「小时」这个业务单位，配置写的也是小时，
// 换成秒只在这一层发生。写成常量而不是散落的 3600，是为了让「小时的
// 定义」只有一处（timeutil 里只有天的定义，没有小时的）。
const secondsPerHour int64 = 3600

// 阶段边界（相对本周起点，单位小时）。集中在这里，PhaseAt 与 phaseBounds
// 共用，避免两处各写一遍偏移表达式——它们一旦不同步，界面倒计时就会
// 和实际切期时刻差一截。
type bounds struct {
	enrollEnd   int64 // 报名期结束 = 分组期开始
	activeStart int64 // 分组期结束 = 活动期开始
	settleStart int64 // 活动期结束 = 结束期开始
	weekEnd     int64 // 结束期结束 = 下一周开始
}

// boundsOf 计算 now 所在这一周的四条边界。
func (c Config) boundsOf(now int64) bounds {
	cfg := c.normalized()
	weekStart, weekEnd, _ := activity.Week(now)
	enrollEnd := weekStart + cfg.EnrollHours*secondsPerHour
	return bounds{
		enrollEnd:   enrollEnd,
		activeStart: enrollEnd + cfg.GroupHours*secondsPerHour,
		settleStart: weekEnd - cfg.SettleHours*secondsPerHour,
		weekEnd:     weekEnd,
	}
}

// PhaseAt 返回 now 所处阶段，以及该阶段的起止时刻（unix 秒）。
//
// 阶段全部是「本周起点 + 偏移」：
//
//	报名期  [weekStart,   enrollEnd)
//	分组期  [enrollEnd,   activeStart)
//	活动期  [activeStart, settleStart)
//	结束期  [settleStart, weekEnd)
//
// 用 activity.Week 而不是 activity.Window(7)：后者的锚点是 2020-01-01，
// 那是**周三**，算出来的一周从周三开始。这个坑已经在 pkg/activity 的
// 测试里固定下来了（TestWindowUnchanged）。
func (c Config) PhaseAt(now int64) (phase Phase, start, end int64) {
	b := c.boundsOf(now)
	weekStart, _, _ := activity.Week(now)

	switch {
	case now < b.enrollEnd:
		return PhaseEnroll, weekStart, b.enrollEnd
	case now < b.activeStart:
		return PhaseGrouping, b.enrollEnd, b.activeStart
	case now < b.settleStart:
		return PhaseActive, b.activeStart, b.settleStart
	default:
		return PhaseSettle, b.settleStart, b.weekEnd
	}
}

// inEnroll 是否处于报名期（报名与取消的唯一判据）。
func (c Config) inEnroll(now int64) bool {
	p, _, _ := c.PhaseAt(now)
	return p == PhaseEnroll
}

// inActive 是否处于活动期（可攻击、可改分）。
func (c Config) inActive(now int64) bool {
	p, _, _ := c.PhaseAt(now)
	return p == PhaseActive
}
