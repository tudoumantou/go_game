// Package activity 活动周期计算（R7.1）。
//
// 周期定义：自固定基准（2020-01-01 00:00 +08:00）起，按活动配置的持续天数等分。
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

import "time"

// daySeconds 一天的秒数（86400）。
//
// 显式声明为 int64：活动计算全部走 int64 秒，避免与 32 位平台/int32
// 运算时的位宽差异（时间戳乘天数很容易超 int32）。
const daySeconds int64 = 86400

// epoch 周期基准（北京时间 2020-01-01 00:00）。
//
// 选 2020：保证任意生产时刻减 epoch 都是正数且量级适中，给「基准之前」
// 的哨兵判断（见 Window）留出空间。FixedZone 而非 LoadLocation：
// 不依赖系统 tzdata，跨机器结果确定；8*3600 秒 = +08:00，无 DST。
var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.FixedZone("CST", 8*3600)).Unix()

// Window 返回 now 所在周期的起止与周期序号。
//
// durationDays <= 0 时按 1 天处理：配置写错不能让活动永不结束或永不开始。
// now < epoch 归入第 0 期：避免负数周期导致 instance_id 形态异常
// （下游用 period 拼实例标识，负号会破坏格式）。
//
// Go 细节：命名返回值 (start, end, period int64) 让三个分支都能裸 return，
// 文档即签名；整数除法天然向下取整，因此 start 永远对齐到
// 「epoch + k*span」，不依赖本地时区/日历——没有月份概念，不需要
// 处理大小月与 DST，这是「按天等分」相对「自然月」的核心简化。
func Window(durationDays int32, now int64) (start, end, period int64) {
	span := int64(durationDays) * daySeconds
	if span <= 0 {
		span = daySeconds
	}
	if now < epoch {
		// 基准之前的时刻归到第 0 期，避免负数周期导致 instance_id 形态异常。
		return epoch, epoch + span, 0
	}
	period = (now - epoch) / span
	start = epoch + period*span
	return start, start + span, period
}

// Active 判断 now 是否落在 [start, end) 内。
//
// 左闭右开：end 恰是下一周期的 start，右闭会让边界时刻同时属于两个
// 周期（重复发奖）；配合 Window 的等分定义，相邻周期无缝且不重叠。
func Active(start, end, now int64) bool {
	return now >= start && now < end
}
