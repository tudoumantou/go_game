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
