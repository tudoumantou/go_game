// Package activity 活动周期计算（R7.1）。
//
// 周期定义：自固定基准起，按活动配置的持续天数等分。
// 好处是**不需要存「活动什么时候开的」**：给定任意时刻，中心服和每个游戏服
// 都能独立算出同一个周期，消息丢了也不会算错，只是少了「已下发」的确认。
//
// 时间全部按北京时间：与签到、商店刷新保持一致，否则同一活动在不同服
// 会出现跨天边界不一致。
//
// 架构位置：center 服配置下发 duration_days，各 game 服从本地时钟算窗口；
// 输入输出都是 unix 秒（int64），不依赖时区数据库——容器镜像常缺 tzdata，
// FixedZone 固定 +8 让计算结果在任何机器上确定。
package activity

import (
	"time"

	"game_study/pkg/timeutil"
)

// 一天的秒数用 timeutil.SecondsPerDay（int64）：活动计算全部走 int64 秒，
// 避免与 32 位平台/int32 运算时的位宽差异（时间戳乘天数很容易超 int32）。
// 不在本包重复声明常量——那样「一天的秒数」就有两个定义。

// epoch 周期基准（北京时间 2020-01-01 00:00，**星期三**）。
//
// 选 2020：保证任意生产时刻减 epoch 都是正数且量级适中，给「基准之前」
// 的哨兵判断（见 Window）留出空间。FixedZone 而非 LoadLocation：
// 不依赖系统 tzdata，跨机器结果确定；8*3600 秒 = +08:00，无 DST。
//
// 注意它落在星期三：按 7 天等分时周期起点必然是周三 00:00。
// 要与自然周（周一 00:00）对齐必须换锚点，见 weekAnchor / Week。
var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, timeutil.BusinessZone).Unix()

// weekAnchor 自然周锚点（北京时间 2019-12-30 00:00，星期一）。
//
// epoch 是周三，直接拿它按 7 天等分会得到「周三 → 周三」的周期，
// 与「周一 00:00 开始报名」这类按周设计的玩法差两天。往前推 2 天到
// 2019-12-30 即可让每个周期起点都落在周一 00:00。
//
// 两个锚点相差整数天，同一时刻在两个基准下的周期序号只差一个常数，
// 不改变任何已有调用的结果（Window 仍用 epoch）。
var weekAnchor = time.Date(2019, 12, 30, 0, 0, 0, 0, timeutil.BusinessZone).Unix()

// Window 返回 now 所在周期的起止与周期序号。
//
// durationDays <= 0 时按 1 天处理：配置写错不能让活动永不结束或永不开始。
// now < epoch 归入第 0 期：避免负数周期导致 instance_id 形态异常
// （下游用 period 拼实例标识，负号会破坏格式）。
//
// Go 细节：命名返回值 (start, end, period int64) 让三个分支都能裸 return，
// 文档即签名；整数除法天然向下取整，因此 start 永远对齐到
// 「anchor + k*span」，不依赖本地时区/日历——没有月份概念，不需要
// 处理大小月与 DST，这是「按天等分」相对「自然月」的核心简化。
func Window(durationDays int32, now int64) (start, end, period int64) {
	return WindowFrom(epoch, durationDays, now)
}

// WindowFrom 同 Window，但周期基准由调用方指定。
//
// 存在的理由：**周期起点落在星期几完全由锚点决定**，而不同玩法要求的起点
// 不同。活动实例按 duration_days 等分用 epoch 就够；跨服玩法要「每周一
// 00:00 开始报名」，就必须把锚点前移到 weekAnchor。
//
// 把锚点做成参数而不是让调用方自己减偏移，是因为「周期起点」只应该有一个
// 算法：调用方各自算偏移，很容易在「now 恰好等于锚点」「now 小于锚点」
// 这些边界上分叉（负数除法的取整方向尤其容易写错）。
//
// anchor <= 0 时退回 epoch：零值不是合法锚点（1970-01-01 在业务上无意义，
// 更可能是忘了传），静默兜底比让整条时间线错位好。
func WindowFrom(anchor int64, durationDays int32, now int64) (start, end, period int64) {
	if anchor <= 0 {
		anchor = epoch
	}
	span := int64(durationDays) * timeutil.SecondsPerDay
	if span <= 0 {
		span = timeutil.SecondsPerDay
	}
	if now < anchor {
		// 基准之前的时刻归到第 0 期，避免负数周期导致 instance_id 形态异常。
		return anchor, anchor + span, 0
	}
	period = (now - anchor) / span
	start = anchor + period*span
	return start, start + span, period
}

// Week 返回 now 所在自然周的起止与周序号，周起点固定为周一 00:00。
//
// 与 Window(7, now) 的唯一区别是锚点：那个得到的是「周三 00:00 起的一周」。
// 需要「周一报名、周二分组」这类按周玩法时用这个，不要用 Window(7)。
//
// 返回的 period 是「自 2019-12-30 起的第几周」，只作周期标识用
// （拼 instance_id、日志排查），不保证与 ISO 周号一致，也不要拿它做
// 跨年推算。
func Week(now int64) (start, end, period int64) {
	return WindowFrom(weekAnchor, timeutil.DaysPerWeek, now)
}

// Active 判断 now 是否落在 [start, end) 内。
//
// 左闭右开：end 恰是下一周期的 start，右闭会让边界时刻同时属于两个
// 周期（重复发奖）；配合 Window 的等分定义，相邻周期无缝且不重叠。
func Active(start, end, now int64) bool {
	return now >= start && now < end
}
