// internal/game/server_kick_test.go —— 按角色踢人（游戏服侧）
//
// 网关只转达 player_id（它不知道、也不该存「角色 → 会话」映射），
// 由这里查 players2sess 定位会话。查错表的后果是踢错人；
// 查不到就当成「不在本节点」正常返回——广播到每个节点时这是常态。
package game

import (
	"context"
	"testing"

	"game_study/pkg/log"

	"github.com/stretchr/testify/require"
)

// TestResolveSessionByPlayer 查表返回当前权威会话，未知角色返回 0。
func TestResolveSessionByPlayer(t *testing.T) {
	s := &Server{
		logger:       log.New("kick-test", log.WithLevel("error")),
		players2sess: map[int64]uint64{42: 7},
	}

	require.EqualValues(t, 7, s.resolveSessionByPlayer(42))
	require.EqualValues(t, 0, s.resolveSessionByPlayer(43), "未知角色必须返回 0（不在本节点）")
}

// TestOnKickUnknownPlayerIsNoop 角色不在本节点：正常返回，不动任何会话。
//
// 广播模式下这是最常见的结果：N 个节点里只有 1 个持有该玩家，
// 其余节点必须安静地忽略，否则会把「没找到」当故障刷日志。
func TestOnKickUnknownPlayerIsNoop(t *testing.T) {
	s := &Server{
		logger:       log.New("kick-test", log.WithLevel("error")),
		players2sess: map[int64]uint64{42: 7},
	}

	require.NoError(t, s.OnKick(context.Background(), 43, "banned"))
}

// TestOnKickKnownPlayerWithoutRPCStillReturnsNil 命中角色但下行通道不可用：
// 只记日志，不把错误抛给网关。
//
// kickSession 内部吞掉发送失败（旧会话可能已经断开，UNBIND 迟早会到），
// 因此这里断言 nil 而不是 error——如果改成上抛，网关会把「旧连接已断」
// 当成踢人失败，运营看到大量假告警。
func TestOnKickKnownPlayerWithoutRPCStillReturnsNil(t *testing.T) {
	s := &Server{
		logger:       log.New("kick-test", log.WithLevel("error")),
		players2sess: map[int64]uint64{42: 7},
	}

	require.NoError(t, s.OnKick(context.Background(), 42, "banned"))
}

// TestOnKickDefaultReason 没给原因时用默认值，客户端不能收到空文案。
func TestOnKickDefaultReason(t *testing.T) {
	s := &Server{
		logger:       log.New("kick-test", log.WithLevel("error")),
		players2sess: map[int64]uint64{},
	}

	require.NoError(t, s.OnKick(context.Background(), 42, ""))
}
