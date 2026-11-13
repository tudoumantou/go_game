package activity

import (
	"testing"
	"time"

	"game_study/pkg/timeutil"
)

// bjTime 把「北京时间」字面量转成 unix 秒，测试里用可读的日期而不是裸数字。
func bjTime(t *testing.T, s string) int64 {
	t.Helper()
	tm, err := time.ParseInLocation("2006-01-02 15:04:05", s, timeutil.BusinessZone)
	if err != nil {
		t.Fatalf("解析时间 %q: %v", s, err)
	}
	return tm.Unix()
}

// TestWeekStartsOnMonday 是 Week 的核心契约：每个周期起点都是周一 00:00。
//
// 扫 400 周（约 7 年半）而不是抽查几天：锚点算错时往往是「整体平移」，
// 抽查容易正好落在对的日期上。
func TestWeekStartsOnMonday(t *testing.T) {
	base := bjTime(t, "2026-03-01 00:00:00") // 周日，故意取在周界附近
	_, _, p0 := Week(base)

	for k := int64(-200); k <= 200; k++ {
		// 用 base 那周的周序号平移，避免自己重算锚点等于自证。
		start, end, period := WeekFromPeriod(p0 + k)
		if period != p0+k {
			t.Fatalf("period = %d, 期望 %d", period, p0+k)
		}
		tm := time.Unix(start, 0).In(timeutil.BusinessZone)
		if wd := timeutil.Weekday(tm); wd != 1 {
			t.Fatalf("第 %d 周起点 %s 是周%d，期望周一", period, tm.Format("2006-01-02 15:04"), wd)
		}
		if h, m, s := tm.Hour(), tm.Minute(), tm.Second(); h != 0 || m != 0 || s != 0 {
			t.Fatalf("第 %d 周起点 %s 不是 00:00:00", period, tm.Format("2006-01-02 15:04:05"))
		}
		if end-start != 7*timeutil.SecondsPerDay {
			t.Fatalf("第 %d 周跨度 = %d 秒, 期望 7 天", period, end-start)
		}
	}
}

// WeekFromPeriod 是测试辅助：由周序号反推窗口。
// 生产代码不需要「从序号求窗口」的能力（所有调用都从 now 出发），
// 所以不放进 window.go 扩大 API 面。
func WeekFromPeriod(period int64) (start, end, p int64) {
	span := 7 * timeutil.SecondsPerDay
	start = weekAnchor + period*span
	return start, start + span, period
}

// TestWeekBoundary 周界：周日 23:59:59 与周一 00:00:00 必须分属相邻两周。
func TestWeekBoundary(t *testing.T) {
	lastSec := bjTime(t, "2026-03-01 23:59:59")  // 周日
	firstSec := bjTime(t, "2026-03-02 00:00:00") // 周一

	_, e1, p1 := Week(lastSec)
	s2, e2, p2 := Week(firstSec)

	if p2 != p1+1 {
		t.Fatalf("周序号未跨周: %d -> %d", p1, p2)
	}
	if e1 != s2 {
		t.Fatalf("相邻周不衔接: 上周结束 %d, 本周开始 %d", e1, s2)
	}
	if e2-s2 != 7*timeutil.SecondsPerDay {
		t.Fatalf("本周跨度 = %d, 期望 7 天", e2-s2)
	}
}

// TestWindowUnchanged 确认 Window 行为没被改动：它仍是「周三起的一周」。
//
// 这条测试的价值是**固定现状**：如果哪天有人为了改周锚点去动 epoch，
// 所有按 duration_days 等分的活动实例会整体平移，这里会立刻红。
func TestWindowUnchanged(t *testing.T) {
	now := bjTime(t, "2026-03-03 05:30:00")
	start, end, _ := Window(7, now)
	if start != bjTime(t, "2026-02-25 00:00:00") {
		t.Fatalf("Window(7) 起点 = %s, 期望 2026-02-25（周三）",
			time.Unix(start, 0).In(timeutil.BusinessZone).Format("2006-01-02 15:04 Monday"))
	}
	if end-start != 7*timeutil.SecondsPerDay {
		t.Fatalf("跨度 = %d, 期望 7 天", end-start)
	}
	if wd := timeutil.Weekday(time.Unix(start, 0)); wd != 3 {
		t.Fatalf("Window(7) 起点是周%d, 本来就是周三；变了说明 epoch 被动过", wd)
	}
}

// TestWindowFromEqualsWindow 锚点传 epoch 时两条路径等价。
func TestWindowFromEqualsWindow(t *testing.T) {
	for _, now := range []int64{
		bjTime(t, "2026-03-03 05:30:00"),
		bjTime(t, "2020-01-01 00:00:00"),
		bjTime(t, "2020-01-01 00:00:01"),
	} {
		for _, d := range []int32{1, 7, 14, 30} {
			s1, e1, p1 := Window(d, now)
			s2, e2, p2 := WindowFrom(epoch, d, now)
			if s1 != s2 || e1 != e2 || p1 != p2 {
				t.Errorf("d=%d now=%d: Window(%d,%d,%d) != WindowFrom(%d,%d,%d)",
					d, now, s1, e1, p1, s2, e2, p2)
			}
		}
	}
}

// TestWindowFromBadArgs 非法参数兜底：配置错不能让活动永不开始或永不结束。
func TestWindowFromBadArgs(t *testing.T) {
	now := bjTime(t, "2026-03-03 05:30:00")

	// durationDays <= 0 按 1 天
	s, e, _ := WindowFrom(epoch, 0, now)
	if e-s != timeutil.SecondsPerDay {
		t.Errorf("durationDays=0 跨度 = %d, 期望 1 天", e-s)
	}
	s, e, _ = WindowFrom(epoch, -3, now)
	if e-s != timeutil.SecondsPerDay {
		t.Errorf("durationDays=-3 跨度 = %d, 期望 1 天", e-s)
	}

	// anchor <= 0 退回 epoch，而不是算出位于 1970 年的周期
	s, _, _ = WindowFrom(0, 7, now)
	want, _, _ := Window(7, now)
	if s != want {
		t.Errorf("anchor=0 未退回 epoch: 起点 %d, 期望 %d", s, want)
	}

	// now < anchor 归入第 0 期，不产生负周期号
	s, e, p := WindowFrom(now, 7, now-timeutil.SecondsPerDay)
	if p != 0 || s != now || e != now+7*timeutil.SecondsPerDay {
		t.Errorf("now<anchor: (start=%d,end=%d,period=%d), 期望 (%d,%d,0)", s, e, p, now, now+7*timeutil.SecondsPerDay)
	}
}

// TestActiveHalfOpen 左闭右开，避免边界时刻同属两个周期。
func TestActiveHalfOpen(t *testing.T) {
	s, e := int64(1000), int64(2000)
	if !Active(s, e, s) {
		t.Error("起点应属于本期")
	}
	if Active(s, e, e) {
		t.Error("终点不应属于本期（那是下一期的起点）")
	}
}
