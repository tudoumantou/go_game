// Package timeutil 业务时间口径：固定时区、周几映射、一周天数。
//
// 为什么要收口：签到/登录礼包/活动/商店刷新都按北京时间算「今天」，
// 各自 new 一个 FixedZone 或写 7、86400 这类数字，口径一旦漂移，
// 同一个玩家在不同玩法里会跨不同的天。这里只放「全服共用」的时间常量，
// 具体玩法规则（几点重置、按周还是按日）仍留在模块里。
//
// 用 FixedZone 而不是 LoadLocation：不依赖镜像里的 tzdata，
// 跨机器结果确定；北京时间无夏令时，8*3600 秒即可。
package timeutil

import "time"

// DaysPerWeek 一周天数，同时是「星期序号」的上界：周一=1 … 周日=7。
const DaysPerWeek int32 = 7

// BusinessZone 业务时区（UTC+8）。
var BusinessZone = time.FixedZone("Asia/Shanghai", 8*3600)

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
