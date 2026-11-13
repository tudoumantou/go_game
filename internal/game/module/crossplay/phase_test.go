package crossplay

import (
	"testing"
	"time"

	"game_study/pkg/timeutil"
)

// bj 北京时间字面量 → unix 秒（测试里用可读日期，不用裸数字）。
func bj(t *testing.T, s string) int64 {
	t.Helper()
	tm, err := time.ParseInLocation("2006-01-02 15:04:05", s, timeutil.BusinessZone)
	if err != nil {
		t.Fatalf("解析时间 %q: %v", s, err)
	}
	return tm.Unix()
}

// TestPhaseTimeline 四个阶段的边界必须与设计一致（合计 168h）。
//
// 用 2026-03-02（周一）这一周：
//
//	周一 00:00 报名期开始
//	周二 00:00 分组期
//	周二 06:00 活动期
//	周日 22:00 结束期
//	周一 00:00 下一期
func TestPhaseTimeline(t *testing.T) {
	cfg := DefaultConfig()

	cases := []struct {
		at    string
		want  Phase
		start string
		end   string
	}{
		{"2026-03-02 00:00:00", PhaseEnroll, "2026-03-02 00:00:00", "2026-03-03 00:00:00"},
		{"2026-03-02 12:00:00", PhaseEnroll, "2026-03-02 00:00:00", "2026-03-03 00:00:00"},
		{"2026-03-02 23:59:59", PhaseEnroll, "2026-03-02 00:00:00", "2026-03-03 00:00:00"},
		{"2026-03-03 00:00:00", PhaseGrouping, "2026-03-03 00:00:00", "2026-03-03 06:00:00"},
		{"2026-03-03 05:59:59", PhaseGrouping, "2026-03-03 00:00:00", "2026-03-03 06:00:00"},
		{"2026-03-03 06:00:00", PhaseActive, "2026-03-03 06:00:00", "2026-03-08 22:00:00"},
		{"2026-03-07 12:00:00", PhaseActive, "2026-03-03 06:00:00", "2026-03-08 22:00:00"},
		{"2026-03-08 21:59:59", PhaseActive, "2026-03-03 06:00:00", "2026-03-08 22:00:00"},
		{"2026-03-08 22:00:00", PhaseSettle, "2026-03-08 22:00:00", "2026-03-09 00:00:00"},
		{"2026-03-08 23:59:59", PhaseSettle, "2026-03-08 22:00:00", "2026-03-09 00:00:00"},
	}
	for _, c := range cases {
		now := bj(t, c.at)
		got, start, end := cfg.PhaseAt(now)
		if got != c.want {
			t.Errorf("%s 阶段 = %s, 期望 %s", c.at, got, c.want)
		}
		if start != bj(t, c.start) {
			t.Errorf("%s 阶段起点 = %s, 期望 %s", c.at,
				time.Unix(start, 0).In(timeutil.BusinessZone).Format("2006-01-02 15:04:05"), c.start)
		}
		if end != bj(t, c.end) {
			t.Errorf("%s 阶段终点 = %s, 期望 %s", c.at,
				time.Unix(end, 0).In(timeutil.BusinessZone).Format("2006-01-02 15:04:05"), c.end)
		}
	}

	// 周一 00:00 必须是下一期的报名期起点（左闭右开，无缝衔接）
	next := bj(t, "2026-03-09 00:00:00")
	if p, s, _ := cfg.PhaseAt(next); p != PhaseEnroll || s != next {
		t.Errorf("下一周起点 = (%s, %d), 期望 (enroll, %d)", p, s, next)
	}
}

// TestPhaseCoversWholeWeek 四个阶段无缝且不重叠地铺满一周。
//
// 这条防的是「改了一个偏移忘了另一个」：比如活动期结束改成 166h
// 而结束期还是 2h，就会漏掉或重叠一段。
func TestPhaseCoversWholeWeek(t *testing.T) {
	cfg := DefaultConfig()
	start := bj(t, "2026-03-02 00:00:00")
	week := int64(7) * timeutil.SecondsPerDay

	var total int64
	for off := int64(0); off < week; off += 600 { // 每 10 分钟采一次
		_, s, e := cfg.PhaseAt(start + off)
		if s > start+off || e <= start+off {
			t.Fatalf("时刻 +%dh 不落在自己阶段的区间内: [%d,%d)", off/3600, s, e)
		}
		total = e - s // 同一周内每个阶段的 end-start 恒定
	}
	// 直接验证四段之和等于一周（用边界算，不靠采样）
	b := cfg.boundsOf(start)
	sum := (b.enrollEnd - start) + (b.activeStart - b.enrollEnd) +
		(b.settleStart - b.activeStart) + (b.weekEnd - b.settleStart)
	if sum != week {
		t.Errorf("四阶段合计 %d 秒, 期望 %d（一周）", sum, week)
	}
	if b.weekEnd-start != week {
		t.Errorf("周窗口 %d 秒, 期望 %d", b.weekEnd-start, week)
	}
	_ = total
}

// TestPhaseConfigFallback 配置写错时退回默认值，不能算出零长度阶段。
func TestPhaseConfigFallback(t *testing.T) {
	// 全零 → 默认值（24/6/2）
	zero := Config{}
	b := zero.boundsOf(bj(t, "2026-03-02 00:00:00"))
	if b.enrollEnd-b.weekEnd+7*timeutil.SecondsPerDay != 24*3600 {
		t.Errorf("报名期长度 = %d, 期望 24h（零值应退回默认）", b.enrollEnd-b.weekEnd+7*timeutil.SecondsPerDay)
	}

	// 负数同样退回
	neg := Config{EnrollHours: -5, GroupHours: -1, SettleHours: -3}
	if p, _, _ := neg.PhaseAt(bj(t, "2026-03-02 12:00:00")); p != PhaseEnroll {
		t.Errorf("负数配置下周一中午阶段 = %s, 期望 enroll", p)
	}
}

// TestPhaseWeekAlignsMonday 跨周也对齐周一（不只这一周）。
func TestPhaseWeekAlignsMonday(t *testing.T) {
	cfg := DefaultConfig()
	base := bj(t, "2026-03-02 00:00:00") // 周一
	for k := 0; k < 52; k++ {
		at := base + int64(k)*7*timeutil.SecondsPerDay
		p, s, _ := cfg.PhaseAt(at)
		if p != PhaseEnroll {
			t.Fatalf("第 %d 周起点阶段 = %s, 期望 enroll", k, p)
		}
		if s != at {
			t.Fatalf("第 %d 周起点 = %d, 期望 %d", k, s, at)
		}
		if wd := timeutil.Weekday(time.Unix(s, 0)); wd != 1 {
			t.Fatalf("第 %d 周起点是周%d, 期望周一", k, wd)
		}
	}
}
