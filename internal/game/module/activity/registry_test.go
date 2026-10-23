// internal/game/module/activity/registry_test.go —— 窗口注册表单测：版本乱序幂等、开放判定。
//
// 直接操作进程级 defaultRegistry，用例之间靠重置注册表隔离。
package activity

import (
	"fmt"
	"testing"

	"game_study/pkg/activity"
	"github.com/stretchr/testify/require"
)

// TestRegistryApplyStale 乱序/重复消息不能把窗口改回去（R7.2）。
func TestRegistryApplyStale(t *testing.T) {
	r := NewRegistry()
	newer := Window{ActivityID: 1, StartAt: 200, EndAt: 300, Version: 200}
	older := Window{ActivityID: 1, StartAt: 100, EndAt: 200, Version: 100}

	require.False(t, !r.Apply(newer), "first apply must succeed")
	require.False(t, r.Apply(newer), "same version must be ignored")
	require.False(t, r.Apply(older), "older version must be ignored")
	got, _ := r.Get(1)
	require.EqualValues(t, 200, got.Version, fmt.Sprintf("version = %d, want 200", got.Version))
}

// TestOpenNow 未下发视为开放；下发且过期则关闭（列表与领取共用）。
func TestOpenNow(t *testing.T) {
	defaultRegistry = NewRegistry() // 用例隔离
	require.False(t, !openNow(1, 1000), "no schedule must be treated as open")

	defaultRegistry.Apply(Window{ActivityID: 1, StartAt: 100, EndAt: 200, Version: 100})
	require.EqualValues(t, true, openNow(1, 150), "inside window must be open")
	require.EqualValues(t, false, openNow(1, 250), "expired window must be closed")
}

// TestWindowDeterministic 同一时刻，中心服与游戏服算出同一个窗口。
func TestWindowDeterministic(t *testing.T) {
	start, end, period := activity.Window(7, 1_700_000_000)
	require.EqualValues(t, 7*86400, end-start)
	require.False(t, period <= 0, fmt.Sprintf("period = %d", period))
	require.False(t, !activity.Active(start, end, 1_700_000_000), "now must be inside its own window")
	// 边界：end 属于下一期，不属于本期
	require.False(t, activity.Active(start, end, end), "end must be exclusive")
}
