// internal/center/leader/leader_test.go —— 选主管理器的单元测试
//
// 真实竞选（抢主/让位/租约过期）依赖 etcd，放在 test/integration 里用容器跑；
// 这里只覆盖不碰 etcd 的部分：默认值、状态去重、幂等、未启动时的错误语义。
//
// 这些看起来是「边角」，但每一条都对应过真实故障：
//   - 回调去重：etcd 抖动时状态高频翻转，上层任务会被反复重启；
//   - Stop 幂等：Run 与外部并发调用，重复 close channel 直接 panic；
//   - Resign 空安全：启动即关闭的实例会在 election 为 nil 时走到这里。
package leader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewDefaults Key/TTL 落默认值，未参与竞选时不是主。
func TestNewDefaults(t *testing.T) {
	e := New(nil, Config{}, nil)
	require.Equal(t, DefaultKey, e.key, "空 Key 必须落默认值")
	require.EqualValues(t, 10, e.ttl, "非正 TTL 必须落 10s")

	require.False(t, e.IsLeader(), "刚构造的实例不该是主")

	custom := New(nil, Config{Key: "/x/y", TTL: 3}, nil)
	require.Equal(t, "/x/y", custom.key, "显式配置不能被默认值覆盖")
	require.EqualValues(t, 3, custom.ttl)
}

// TestLeaderIDBeforeRun 竞选未开始时 LeaderID 返回错误（调用方按「未知」处理）。
func TestLeaderIDBeforeRun(t *testing.T) {
	e := New(nil, Config{SelfID: "a"}, nil)

	id, err := e.LeaderID(context.Background())
	require.Error(t, err, "没有 election 时必须报错，而不是返回空串冒充「无主」")
	require.Empty(t, id)
}

// TestSetLeaderOnlyNotifiesOnChange 状态未变化时不重复回调。
//
// 上层（归档/调度/里程碑）以回调为唯一的切换信号，重复回调会让任务
// 循环空转重启——所以去重是这里的核心语义，不是优化。
func TestSetLeaderOnlyNotifiesOnChange(t *testing.T) {
	var calls []bool
	e := New(nil, Config{SelfID: "a", OnChange: func(v bool) { calls = append(calls, v) }}, nil)

	e.setLeader(true)
	e.setLeader(true) // 重复置 true：不回调
	e.setLeader(false)
	e.setLeader(false) // 重复置 false：不回调

	require.Equal(t, []bool{true, false}, calls, "只有真正的状态翻转才回调")
	require.False(t, e.IsLeader())
}

// TestSetLeaderWithoutLoggerOrCallback 半成品实例（无 logger/无回调）不能 panic。
func TestSetLeaderWithoutLoggerOrCallback(t *testing.T) {
	e := New(nil, Config{SelfID: "a"}, nil)
	require.NotPanics(t, func() {
		e.setLeader(true)
		e.setLeader(false)
	})
}

// TestStopIsIdempotent 重复 Stop 不能 panic（Run 与优雅关闭会并发调用）。
func TestStopIsIdempotent(t *testing.T) {
	e := New(nil, Config{SelfID: "a"}, nil)
	require.NotPanics(t, func() {
		e.Stop()
		e.Stop()
		e.Stop()
	})
}

// TestResignBeforeCampaign 还没参与竞选时让位：安全返回、状态仍为非主。
func TestResignBeforeCampaign(t *testing.T) {
	e := New(nil, Config{SelfID: "a"}, nil)
	require.NoError(t, e.Resign(context.Background()))
	require.False(t, e.IsLeader())
}

// TestResignClearsLeaderState 已经是主时让位，状态必须立刻翻转。
func TestResignClearsLeaderState(t *testing.T) {
	var calls []bool
	e := New(nil, Config{SelfID: "a", OnChange: func(v bool) { calls = append(calls, v) }}, nil)
	e.setLeader(true)

	require.NoError(t, e.Resign(context.Background()))
	require.False(t, e.IsLeader(), "让位后不能还自称主")
	require.Equal(t, []bool{true, false}, calls)
}
