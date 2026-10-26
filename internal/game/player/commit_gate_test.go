// internal/game/player/commit_gate_test.go —— 提交通道行为（R2.5）
package player

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGateOutsideWindow 窗口外立即执行并返回错误（非缓存模式与纯下行消息走这条）。
func TestGateOutsideWindow(t *testing.T) {
	var g commitGate
	ran := false
	require.NoError(t, g.Send(func() error { ran = true; return nil }))
	require.True(t, ran, "窗口外应立即执行")

	boom := errors.New("send failed")
	require.ErrorIs(t, g.Send(func() error { return boom }), boom, "窗口外应返回真实错误")
}

// TestGateInsideWindow 窗口内只入队，不执行。
func TestGateInsideWindow(t *testing.T) {
	var g commitGate
	g.Begin()
	ran := 0
	require.NoError(t, g.Send(func() error { ran++; return nil }))
	require.NoError(t, g.Send(func() error { ran++; return nil }))
	require.Zero(t, ran, "窗口内不应执行")
	require.Equal(t, 2, g.Pending())
}

// TestGateCommitFlushesInOrder 提交按入队顺序执行，并退出窗口。
func TestGateCommitFlushesInOrder(t *testing.T) {
	var g commitGate
	g.Begin()
	var order []int
	_ = g.Send(func() error { order = append(order, 1); return nil })
	_ = g.Send(func() error { order = append(order, 2); return nil })
	_ = g.Send(func() error { order = append(order, 3); return nil })

	errs := g.Commit()
	require.Empty(t, errs)
	require.Equal(t, []int{1, 2, 3}, order, "必须按产生顺序下发")
	require.False(t, g.Active(), "提交后应退出窗口")
	require.Zero(t, g.Pending())
}

// TestGateCommitCollectsErrors 单个动作失败不阻断其余动作（数据已落定，只记日志）。
func TestGateCommitCollectsErrors(t *testing.T) {
	var g commitGate
	g.Begin()
	boom := errors.New("reply failed")
	_ = g.Send(func() error { return boom })
	ran := false
	_ = g.Send(func() error { ran = true; return nil })

	errs := g.Commit()
	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], boom)
	require.True(t, ran, "前一个失败不应阻断后续动作")
}

// TestGateDiscardDropsActions 丢弃后动作不执行——客户端收不到「假成功」。
func TestGateDiscardDropsActions(t *testing.T) {
	var g commitGate
	g.Begin()
	ran := false
	_ = g.Send(func() error { ran = true; return nil })
	g.Discard()
	require.False(t, ran, "丢弃的动作不应执行")
	require.False(t, g.Active())
	require.Zero(t, g.Pending())
}

// TestGateBeginResetsStale 重复 Begin 会清掉上一轮残留（防御性）。
func TestGateBeginResetsStale(t *testing.T) {
	var g commitGate
	g.Begin()
	_ = g.Send(func() error { return nil })
	g.Begin() // 未 Commit/Discard 就重入
	require.Zero(t, g.Pending(), "Begin 应清空上一轮暂存")
}
