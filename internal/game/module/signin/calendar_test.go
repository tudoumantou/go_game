package signin

import (
	"testing"
	"time"
)

func TestBusinessDayMidnight(t *testing.T) {
	before := time.Date(2026, 10, 18, 15, 59, 59, 0, time.UTC)
	if businessDay(before.Add(time.Second)) != businessDay(before)+1 {
		t.Fatal("Beijing midnight must change business day")
	}
	if businessDay(before.Add(-time.Hour)) != businessDay(before) {
		t.Fatal("same day changed")
	}
}

func TestSevenDayCalendar(t *testing.T) {
	for _, tc := range []struct{ offset, start, day int64 }{{0, 0, 1}, {1, 0, 2}, {6, 0, 7}, {7, 7, 1}, {9, 7, 3}, {28, 28, 1}} {
		start, day := cycleFor(20000, 20000+tc.offset)
		if start != 20000+tc.start || day != int32(tc.day) {
			t.Fatalf("offset=%d start=%d day=%d", tc.offset, start, day)
		}
	}
}
