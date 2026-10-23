// server_test.go —— R3.1 顶号后旧会话的解绑不能影响权威会话
package game

import (
	"context"
	"fmt"
	"testing"

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
