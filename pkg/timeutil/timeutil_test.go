package timeutil

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWeekday 验证北京时间下的周一至周日映射，与进程时区无关。
func TestWeekday(t *testing.T) {
	// 2026-10-19 是周一，2026-10-25 是周日。
	monday := time.Date(2026, 10, 19, 10, 0, 0, 0, BusinessZone)
	for i := 0; i < 7; i++ {
		got, want := Weekday(monday.Add(time.Duration(i)*24*time.Hour)), int32(i+1)
		require.EqualValues(t, want, got, fmt.Sprintf("day +%d: got %d want %d", i, got, want))
	}
	// 北京周一 00:30 在 UTC 仍是周日 16:30，必须按北京时间算周一。
	utc := time.Date(2026, 10, 18, 16, 30, 0, 0, time.UTC)
	require.EqualValues(t, 1, Weekday(utc), "utc boundary must map to monday")
}

// TestBusinessDayBoundary 日序号的切换点必须落在北京 0 点，不是 UTC 0 点。
//
// 这条用例就是原来三份实现分叉的地方：商店/登录礼包按 +8 偏移切，
// 活动按 UTC 切，两边差 8 小时。
func TestBusinessDayBoundary(t *testing.T) {
	// 北京 2026-10-20 23:59:59 与 2026-10-21 00:00:00 必须是相邻两天。
	before := time.Date(2026, 10, 20, 23, 59, 59, 0, BusinessZone)
	after := time.Date(2026, 10, 21, 0, 0, 0, 0, BusinessZone)
	require.EqualValues(t, BusinessDay(before)+1, BusinessDay(after),
		"跨天应发生在北京 0 点")

	// 同一个瞬间用 UTC 表达，结果必须一致：2026-10-20 16:00:00Z = 北京 10-21 00:00。
	utcSameInstant := time.Date(2026, 10, 20, 16, 0, 0, 0, time.UTC)
	require.EqualValues(t, BusinessDay(after), BusinessDay(utcSameInstant),
		"同一瞬间的不同时区表达必须给出同一天")

	// 反证：UTC 日界算法（sec/86400）在这个时刻还停在「昨天」。
	require.NotEqualValues(t, utcSameInstant.Unix()/SecondsPerDay, BusinessDay(utcSameInstant),
		"北京时间 0-8 点区间，UTC 日界与业务日界必然不同")
}

// TestBusinessDayMatchesLegacy 锁住与存量数据的等价性。
//
// loginreward 的 LastDay、shop 的 Day 都是按 (Unix+8h)/86400 落库的，
// 换算法会让所有在线玩家在同一刻「多跨一天」（连续登录断签、限购重置）。
func TestBusinessDayMatchesLegacy(t *testing.T) {
	moments := []time.Time{
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),          // Unix 0
		time.Date(2026, 1, 1, 0, 0, 0, 0, BusinessZone),      // 年初
		time.Date(2026, 10, 20, 15, 59, 59, 0, time.UTC),     // 北京 23:59:59
		time.Date(2026, 10, 20, 16, 0, 0, 0, time.UTC),       // 北京次日 00:00
		time.Date(2026, 12, 31, 23, 59, 59, 0, BusinessZone), // 年末
	}
	for _, m := range moments {
		want := (m.Unix() + 8*60*60) / 86400
		require.EqualValues(t, want, BusinessDay(m), "legacy formula mismatch at %s", m)
		require.EqualValues(t, want, BusinessDayUnix(m.Unix()), "unix variant mismatch at %s", m)
	}
}

// TestBusinessDayZeroIsNever 确认零值不可达：Day=0 表示「从未发生」，
// 这依赖 1970-01-01 早已过去这个事实。
func TestBusinessDayZeroIsNever(t *testing.T) {
	require.EqualValues(t, 0, BusinessDayUnix(0), "Unix 0 就是第 0 天")
	now := BusinessDay(time.Now())
	require.Greater(t, now, int64(0), "当前时间必须远离零值")
}
