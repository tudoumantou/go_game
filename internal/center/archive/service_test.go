package archive

import (
	"fmt"
	"testing"
	"time"

	"game_study/internal/center/stats"
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
