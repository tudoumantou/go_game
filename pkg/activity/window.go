// Package activity 活动周期计算（R7.1）。
//
// 周期定义：自固定基准（2020-01-01 00:00 +08:00）起，按活动配置的持续天数等分。
// 好处是**不需要存「活动什么时候开的」**：给定任意时刻，中心服和每个游戏服
// 都能独立算出同一个周期，消息丢了也不会算错，只是少了「已下发」的确认。
//
// 时间全部按北京时间：与签到、商店刷新保持一致，否则同一活动在不同服
// 会出现跨天边界不一致。
package activity

import "time"

const daySeconds int64 = 86400

// epoch 周期基准（北京时间 2020-01-01 00:00）。
var epoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.FixedZone("CST", 8*3600)).Unix()

// Window 返回 now 所在周期的起止与周期序号。
//
// durationDays <= 0 时按 1 天处理：配置写错不能让活动永不结束或永不开始。
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
func Active(start, end, now int64) bool {
	return now >= start && now < end
}
