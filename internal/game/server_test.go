// server_test.go —— R3.1 顶号后旧会话的解绑不能影响权威会话
package game

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/pkg/config"
	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

// TestOnUnbindIgnoresStaleSession 旧会话被顶号后断开：不能删除玩家→会话映射，
// 也不能（后续步骤里）清在线归属，否则新会话会被打成离线。
func TestOnUnbindIgnoresStaleSession(t *testing.T) {
	s := &Server{
		cfg:          &config.Config{Server: config.ServerConfig{Name: "game-1"}},
		logger:       log.New("r31-test", log.WithLevel("error")),
		sessions:     map[uint64]int64{1: 7, 2: 7},
		accounts:     map[uint64]int64{},
		players2sess: map[int64]uint64{7: 2},
	}

	// 旧会话 1 迟到解绑
	err := s.OnUnbind(context.Background(), 1)
	require.NoError(t, err)
	got := s.players2sess[7]
	require.EqualValues(t, 2, got, fmt.Sprintf("stale unbind removed authoritative session: %d", got))
	_, ok := s.sessions[1]
	require.False(t, ok, "stale session still registered")
	// 新会话的登记不受影响
	_, ok = s.sessions[2]
	require.True(t, ok, "authoritative session lost")
}

// TestReconcileInterval 核对周期必须来自配置，非法值回落默认。
//
// v3.21 之前 idtab.reconcile_interval 配了不生效（两个维护循环各写死
// 10 * time.Minute）。这条用例锁住「改配置能改变周期」，并确认非法值
// 不会把核对关掉（d<=0 会让 ticker panic，解析失败必须回落而不是传 0）。
func TestReconcileInterval(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"未配置", "", 10 * time.Minute},
		{"合法值", "30m", 30 * time.Minute},
		{"解析失败", "半小时", 10 * time.Minute},
		{"零值", "0s", 10 * time.Minute},
		{"负数", "-5m", 10 * time.Minute},
	}
	for _, c := range cases {
		s := &Server{cfg: &config.Config{IDTab: config.IDTabConfig{ReconcileInterval: c.raw}}}
		require.Equal(t, c.want, s.reconcileInterval(), c.name)
	}
}

// TestRelayGroupFromConfig 锁定「relay 广播分组以配置为初值」这一行为。
//
// 这不是在测字段赋值。要防的退化是：有人把它改回「只由 GroupUpdate 决定」，
// 于是中心服不可用时新起的游戏服订不到 relay.group.*，组内广播静默失效，
// 且没有任何报错——这类退化在联调阶段极难发现（只表现为"有些人收不到广播"）。
func TestRelayGroupFromConfig(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Group: "group-07"}}
	require.Equal(t, "group-07", New(cfg).relayGroupID)

	// 未配置分组时留空：只订阅点对点，不订广播
	require.Empty(t, New(&config.Config{}).relayGroupID)
}
