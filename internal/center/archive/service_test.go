package archive

import (
	"testing"
	"time"

	"game_study/internal/center/stats"
)

func TestNextSeason(t *testing.T) {
	cases := map[string]string{"s1": "s2", "s9": "s10", "s99": "s100"}
	for in, want := range cases {
		if got := nextSeason(in); got != want {
			t.Fatalf("nextSeason(%q) = %q, want %q", in, got, want)
		}
	}
	// 无法解析的赛季名必须变成一个新名字，不能原样返回（否则赛季永不切换）。
	if got := nextSeason("season-a"); got == "season-a" || got == "" {
		t.Fatalf("unparseable season must change: %q", got)
	}
}

func TestSeasonNumber(t *testing.T) {
	if got := seasonNumber("s3"); got != 3 {
		t.Fatalf("seasonNumber(s3) = %d", got)
	}
	if got := seasonNumber("bad"); got != 0 {
		t.Fatalf("seasonNumber(bad) = %d, want 0", got)
	}
}

func TestWindowRangeAligned(t *testing.T) {
	now := time.Date(2026, 10, 19, 22, 47, 31, 0, time.Local)

	m, _ := windowRange(now, stats.Window1m)
	if m.Minute() != 47 || m.Second() != 0 {
		t.Fatalf("1m start = %v", m)
	}
	h, _ := windowRange(now, stats.Window1h)
	if h.Hour() != 22 || h.Minute() != 0 {
		t.Fatalf("1h start = %v", h)
	}
	d, de := windowRange(now, stats.Window1d)
	if d.Hour() != 0 || d.Minute() != 0 || de.Sub(d) != 24*time.Hour {
		t.Fatalf("1d range = %v ~ %v", d, de)
	}
}

// TestStableID 同一业务键必须得到同一个 mail_id（重跑归档不重复发奖）。
func TestStableID(t *testing.T) {
	a := stableID("rank_reward:s1:arena_score:1001")
	b := stableID("rank_reward:s1:arena_score:1001")
	c := stableID("rank_reward:s1:arena_score:1002")
	if a != b {
		t.Fatal("stable id must be deterministic")
	}
	if a == c {
		t.Fatal("different players must get different mail ids")
	}
	if a <= 0 {
		t.Fatal("mail id must be positive")
	}
}
