package archive

import (
	"fmt"
	"testing"
	"time"

	"game_study/internal/center/stats"
	"game_study/pkg/timeutil"
	"github.com/stretchr/testify/require"
)

func TestNextSeason(t *testing.T) {
	cases := map[string]string{"s1": "s2", "s9": "s10", "s99": "s100"}
	for in, want := range cases {
		got := nextSeason(in)
		require.EqualValues(t, want, got, fmt.Sprintf("nextSeason(%q) = %q, want %q", in, got, want))
	}
	// 无法解析的赛季名必须变成一个新名字，不能原样返回（否则赛季永不切换）。
	got := nextSeason("season-a")
	require.NotEqualValues(t, "season-a", got, fmt.Sprintf("unparseable season must change: %q", got))
	require.NotEqualValues(t, "", got)
}

func TestSeasonNumber(t *testing.T) {
	got := seasonNumber("s3")
	require.EqualValues(t, 3, got, fmt.Sprintf("seasonNumber(s3) = %d", got))
	got = seasonNumber("bad")
	require.EqualValues(t, 0, got, fmt.Sprintf("seasonNumber(bad) = %d, want 0", got))
}

func TestWindowRangeAligned(t *testing.T) {
	now := time.Date(2026, 10, 19, 22, 47, 31, 0, time.Local)

	m, _ := windowRange(now, stats.Window1m)
	require.EqualValues(t, 47, m.Minute(), fmt.Sprintf("1m start = %v", m))
	require.EqualValues(t, 0, m.Second())
	h, _ := windowRange(now, stats.Window1h)
	require.EqualValues(t, 22, h.Hour(), fmt.Sprintf("1h start = %v", h))
	require.EqualValues(t, 0, h.Minute())
	d, de := windowRange(now, stats.Window1d)
	require.EqualValues(t, 0, d.Hour(), fmt.Sprintf("1d range = %v ~ %v", d, de))
	require.EqualValues(t, 0, d.Minute())
	require.EqualValues(t, 24*time.Hour, de.Sub(d))
}

// TestStableID 同一业务键必须得到同一个 mail_id（重跑归档不重复发奖）。
func TestStableID(t *testing.T) {
	a := stableID("rank_reward:s1:arena_score:1001")
	b := stableID("rank_reward:s1:arena_score:1001")
	c := stableID("rank_reward:s1:arena_score:1002")
	require.EqualValues(t, b, a, "stable id must be deterministic")
	require.NotEqualValues(t, c, a, "different players must get different mail ids")
	require.False(t, a <= 0, "mail id must be positive")
}

// TestWindowRangeDailyUsesBusinessZone 1d 桶必须落在业务时区零点。
//
// 此前用 now.Date() + now.Location()：容器默认 UTC 时切出的是 UTC 零点，
// 与玩法侧 timeutil.BusinessDay（北京日界）差 8 小时——运维在北京看「今日
// 统计」会发现日曲线到早上 8 点才换桶，而玩家的每日任务 0 点就重置了。
//
// 用「UTC 17:00」这个时刻是因为它正好骑在日界上：北京已是次日 01:00，
// UTC 还是当日 17:00。按本地时区算会得到昨天的桶，按业务时区算才是今天。
func TestWindowRangeDailyUsesBusinessZone(t *testing.T) {
	now := time.Date(2025, 12, 31, 17, 0, 0, 0, time.UTC) // 北京 2026-01-01 01:00

	start, end := windowRange(now, stats.Window1d)
	want := time.Date(2026, 1, 1, 0, 0, 0, 0, timeutil.BusinessZone)

	require.True(t, want.Equal(start), "1d 起点应是北京零点，实际 %v（UTC %v）", start, start.UTC())
	require.Equal(t, 24*time.Hour, end.Sub(start))
	require.EqualValues(t, 0, start.In(timeutil.BusinessZone).Hour())
}
