// internal/game/player/kick_test.go —— 跨节点顶号消息
//
// 链路两头都要能验证：发出去的 KickMsg 必须是「可远程投递」的纯数据，
// 收到后必须真的触发回调（而不是静默吞掉——那会让玩家在两个节点上
// 各有一份会话）。
package player

import (
	"context"
	"testing"

	"game_study/pkg/actor"
	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

// TestKickMsgInvokesHook 收到 KickMsg 时回调必须触发，且带上来源节点。
func TestKickMsgInvokesHook(t *testing.T) {
	ctx := context.Background()
	a := NewActor(42, nil, log.New("kick-test", log.WithLevel("error")), nil, nil, nil)

	var gotPlayer int64
	var gotFrom string
	a.SetKickHook(func(playerID int64, fromNode string) {
		gotPlayer, gotFrom = playerID, fromNode
	})

	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()

	pid, err := sys.Spawn(ActorID(42), a)
	require.NoError(t, err)

	require.NoError(t, sys.Send(pid, KickMsg{FromNode: "game-01"}))
	waitIdle(t, sys)

	require.EqualValues(t, 42, gotPlayer)
	require.Equal(t, "game-01", gotFrom, "来源节点要能传到日志/排查")
}

// TestKickMsgWithoutHook 未注入回调（单机/测试）：不 panic、不崩 Actor。
func TestKickMsgWithoutHook(t *testing.T) {
	ctx := context.Background()
	a := NewActor(43, nil, log.New("kick-test", log.WithLevel("error")), nil, nil, nil)

	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()

	pid, err := sys.Spawn(ActorID(43), a)
	require.NoError(t, err)
	require.NoError(t, sys.Send(pid, KickMsg{FromNode: "game-01"}))
	waitIdle(t, sys)

	// 再发一条业务消息：Actor 还活着（没被 panic 带走）
	require.NoError(t, sys.Send(pid, &SaveMsg{}))
	waitIdle(t, sys)
}

// TestKickMsgRemoteCodec 编解码往返：跨节点传输只认这个格式。
func TestKickMsgRemoteCodec(t *testing.T) {
	in := KickMsg{FromNode: "game-09"}
	require.Equal(t, "game_study.player.KickMsg", in.RemoteTypeURL())

	body, err := in.MarshalRemote()
	require.NoError(t, err)

	decoded, err := actor.DecodeRemote(in.RemoteTypeURL(), body)
	require.NoError(t, err, "KickMsg 必须在 init 里注册过")

	out, ok := decoded.(KickMsg)
	require.True(t, ok, "解码出来必须是值类型（Actor 的 switch 按值匹配）")
	require.Equal(t, in.FromNode, out.FromNode)
}

// TestActorIDFormat Actor ID 格式是跨节点路由的契约：变了就投不到目标 Actor。
func TestActorIDFormat(t *testing.T) {
	require.Equal(t, "player:7", ActorID(7))
	require.Equal(t, "player:0", ActorID(0))
}
