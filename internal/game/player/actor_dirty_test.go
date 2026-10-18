package player

import (
	"sync/atomic"
	"testing"

	"game_study/pkg/log"
)

// TestPreloadRebindsDirtyHook 预加载替换 State 后，dirty 回调必须仍然生效。
//
// 回归背景：preload() 曾直接 `a.state = newState`，而 dirtyHook 是挂在旧 State
// 实例上的 —— 加载完成后的所有修改（升级、背包变化、金币消耗）都不会再通知
// flusher。表现是「数据永远不落库」，且没有任何报错，只有玩家反馈进度丢失。
// 这条链路在集成测试里要等落库周期才能发现，所以用单测钉住。
func TestPreloadRebindsDirtyHook(t *testing.T) {
	var calls atomic.Int64

	a := NewActor(42, nil, log.New("test"), nil, func(int64) {
		calls.Add(1)
	}, nil)

	// 模拟 preload：换成全新的 State 实例
	a.setState(NewState(42))

	a.state.AddItem(1001, 3)
	if calls.Load() == 0 {
		t.Fatal("dirty hook lost after setState: AddItem did not notify flusher")
	}

	before := calls.Load()
	a.state.MarkDirty("bag")
	if calls.Load() == before {
		t.Fatal("dirty hook lost after setState: MarkDirty did not notify flusher")
	}

	before = calls.Load()
	if !a.state.RemoveItem(1001, 1) {
		t.Fatal("remove item failed")
	}
	if calls.Load() == before {
		t.Fatal("dirty hook lost after setState: RemoveItem did not notify flusher")
	}
}

// TestNewActorBindsDirtyHook 构造时就应挂上回调。
func TestNewActorBindsDirtyHook(t *testing.T) {
	var calls atomic.Int64
	a := NewActor(7, nil, log.New("test"), nil, func(int64) { calls.Add(1) }, nil)

	a.state.AddItem(1002, 1)
	if calls.Load() == 0 {
		t.Fatal("NewActor did not bind dirty hook")
	}
}

// TestSetStateWithoutHook onDirty 为空时不 panic（单测/无落库场景）。
func TestSetStateWithoutHook(t *testing.T) {
	a := NewActor(8, nil, log.New("test"), nil, nil, nil)
	a.setState(NewState(8))
	a.state.AddItem(1003, 1) // 不应 panic
}
