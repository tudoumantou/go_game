package signin

import "time"

// 使用固定 UTC+8，避免服务进程或容器的 Local 时区不同造成重复领取。
var businessZone = time.FixedZone("Asia/Shanghai", 8*60*60)

func businessDay(now time.Time) int64 { return (now.Unix() + 8*60*60) / 86400 }

func dayTime(day int64) time.Time { return time.Unix(day*86400-8*60*60, 0).In(businessZone) }

// cycleFor 按自然日推进，离线多个周期后仍保留原来的七日边界。
func cycleFor(anchor, today int64) (int64, int32) {
	if today < anchor {
		return anchor, 0
	} // 服务器时间回退时禁止提前领取。
	start := anchor + (today-anchor)/7*7
	return start, int32(today - start + 1)
}
