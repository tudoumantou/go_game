// Package timeutil 业务时间口径：固定时区、周几映射、一周天数、自然日序号。
//
// 为什么要收口：签到/登录礼包/活动/商店刷新都按北京时间算「今天」，
// 各自 new 一个 FixedZone 或写 7、86400 这类数字，口径一旦漂移，
// 同一个玩家在不同玩法里会跨不同的天。这里只放「全服共用」的时间常量，
// 具体玩法规则（几点重置、按周还是按日）仍留在模块里。
//
// 用 FixedZone 而不是 LoadLocation：不依赖镜像里的 tzdata，
// 跨机器结果确定；北京时间无夏令时，8*3600 秒即可。
//
// 「今天是第几天」这条口径曾经有三份实现（商店、登录礼包、活动），
// 其中活动那份漏了偏移、按 UTC 日界算，于是限购在北京 0 点重置、
// 活动实例要等到北京 8 点才换。三份都不长，但「同一个判断写两遍
// 就一定会分叉」——现在只剩 BusinessDay 一个答案。
package timeutil

import "time"

// DaysPerWeek 一周天数，同时是「星期序号」的上界：周一=1 … 周日=7。
const DaysPerWeek int32 = 7

// BusinessZone 业务时区（UTC+8）。
var BusinessZone = time.FixedZone("Asia/Shanghai", 8*3600)

// businessOffsetSeconds 业务时区相对 UTC 的偏移。
const businessOffsetSeconds int64 = 8 * 3600

// secondsPerDay 一天的秒数。
//
// 写成常量而不是在多处写字面量：位数写错不会报错，只会让「天」这个
// 单位悄悄变长或变短，表现为「某天多出一小时」这种极难复现的 bug。
const secondsPerDay int64 = 86400

// Weekday 返回业务时区下的周几：1=周一 … 7=周日。
//
// Go 的 time.Weekday 零值是 Sunday(0)，与「周一=1」差一天，
// 所以周日单独映射成 7；先 In(zone) 再取 Weekday，避免服务器用 UTC 时
// 把北京时间的周一算成周日。
func Weekday(t time.Time) int32 {
	wd := t.In(BusinessZone).Weekday()
	if wd == time.Sunday {
		return DaysPerWeek
	}
	return int32(wd)
}

// BusinessDay 返回业务时区下的自然日序号：1970-01-01 为第 0 天。
//
// 用途是「跨天清零」类玩法（商店限购、登录礼包、活动实例）：把时间
// 归一成一个整数，比较相等即同一天，不需要判断「几点重置」。
//
// 零值语义：0 是 1970-01-01，早于任何真实数据，所以状态结构里
// Day=0 可以安全表示「从未发生过」（见 loginreward.Data.LastDay）。
//
// 为什么用固定偏移而不是 t.In(BusinessZone) 再取年月日：存量数据
// 是按 (Unix+8h)/86400 落的，换算法会让所有玩家在同一刻「多跨一天」。
// 北京时间无夏令时，两种算法等价，选与数据一致的那个。
func BusinessDay(t time.Time) int64 { return BusinessDayUnix(t.Unix()) }

// BusinessDayUnix 同 BusinessDay，入参是 Unix 秒。
//
// 单独提供一个 Unix 版本：协议、日志上报与中心服下发的时刻都是 int64 秒，
// 调用方拿到秒再包一层 time.Unix 只是为了马上取回秒，白绕一圈。
func BusinessDayUnix(sec int64) int64 {
	return (sec + businessOffsetSeconds) / secondsPerDay
}
