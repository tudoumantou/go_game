// internal/game/module/activity/registry_test.go —— 窗口注册表单测：版本乱序幂等、开放判定。
//
// 直接操作进程级 defaultRegistry，用例之间靠重置注册表隔离。
package activity

import (
	"testing"

	"game_study/pkg/activity"
)

// TestRegistryApplyStale 乱序/重复消息不能把窗口改回去（R7.2）。
func TestRegistryApplyStale(t *testing.T) {
	r := NewRegistry()
	newer := Window{ActivityID: 1, StartAt: 200, EndAt: 300, Version: 200}
	older := Window{ActivityID: 1, StartAt: 100, EndAt: 200, Version: 100}

	if !r.Apply(newer) {
		t.Fatal("first apply must succeed")
	}
	if r.Apply(newer) {
		t.Fatal("same version must be ignored")
	}
	if r.Apply(older) {
		t.Fatal("older version must be ignored")
	}
	if got, _ := r.Get(1); got.Version != 200 {
		t.Fatalf("version = %d, want 200", got.Version)
	}
}

// TestOpenNow 未下发视为开放；下发且过期则关闭（列表与领取共用）。
func TestOpenNow(t *testing.T) {
	defaultRegistry = NewRegistry() // 用例隔离
	if !openNow(1, 1000) {
		t.Fatal("no schedule must be treated as open")
	}

	defaultRegistry.Apply(Window{ActivityID: 1, StartAt: 100, EndAt: 200, Version: 100})
	if openNow(1, 150) != true {
		t.Fatal("inside window must be open")
	}
	if openNow(1, 250) != false {
		t.Fatal("expired window must be closed")
	}
}

// TestWindowDeterministic 同一时刻，中心服与游戏服算出同一个窗口。
func TestWindowDeterministic(t *testing.T) {
	start, end, period := activity.Window(7, 1_700_000_000)
	if end-start != 7*86400 {
		t.Fatalf("span = %d", end-start)
	}
	if period <= 0 {
		t.Fatalf("period = %d", period)
	}
	if !activity.Active(start, end, 1_700_000_000) {
		t.Fatal("now must be inside its own window")
	}
	// 边界：end 属于下一期，不属于本期
	if activity.Active(start, end, end) {
		t.Fatal("end must be exclusive")
	}
}
