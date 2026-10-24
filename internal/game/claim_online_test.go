// internal/game/claim_online_test.go —— 跨节点顶号的归属抢占逻辑
//
// 这段逻辑的产品语义只有一句话：**顶号与连接落在哪个节点无关**。
// 早先的实现是同节点踢旧会话、跨节点直接拒绝，玩家换设备时如果网关
// 把连接路由到另一个游戏服，就会看到「已在其它节点登录」。
package game

import (
	"context"
	"testing"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/config"
	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

// fakePresence 记录抢占与覆盖调用。
type fakePresence struct {
	owner    string // 当前持有者（空 = 无人在线）
	tryErr   error
	setErr   error
	setCalls []string // 每次 SetOnline 的 nodeID
}

func (f *fakePresence) TrySetOnline(_ context.Context, _ int64, nodeID string, _ time.Duration) (bool, string, error) {
	if f.tryErr != nil {
		return false, "", f.tryErr
	}
	if f.owner != "" && f.owner != nodeID {
		return false, f.owner, nil
	}
	f.owner = nodeID
	return true, nodeID, nil
}

func (f *fakePresence) SetOnline(_ context.Context, _ int64, nodeID string, _ time.Duration) error {
	f.setCalls = append(f.setCalls, nodeID)
	if f.setErr != nil {
		return f.setErr
	}
	f.owner = nodeID
	return nil
}

// TestClaimOnlineTakesOverFromOtherNode 别的节点持有时：先通知它踢人，再强制接管。
func TestClaimOnlineTakesOverFromOtherNode(t *testing.T) {
	p := &fakePresence{owner: "game-01"}
	var notifiedNode string
	var notifiedPlayer int64

	took, err := claimOnline(context.Background(), p, "game-02", 7, time.Minute,
		func(nodeID string, playerID int64) {
			notifiedNode, notifiedPlayer = nodeID, playerID
		})
	require.NoError(t, err)
	require.True(t, took, "必须报告「已接管」，调用方据此记日志")
	require.Equal(t, "game-01", notifiedNode, "要通知的是当前持有者")
	require.EqualValues(t, 7, notifiedPlayer)
	require.Equal(t, "game-02", p.owner, "占用必须切到本节点")
	require.Equal(t, []string{"game-02"}, p.setCalls, "接管要强制覆盖一次")
}

// TestClaimOnlineSameNodeNoKick 同节点重连：不踢人、不重复覆盖。
//
// 这是重连路径，踢自己会把玩家刚建立的会话关掉。
func TestClaimOnlineSameNodeNoKick(t *testing.T) {
	p := &fakePresence{owner: "game-02"}
	called := false

	took, err := claimOnline(context.Background(), p, "game-02", 7, time.Minute,
		func(string, int64) { called = true })
	require.NoError(t, err)
	require.False(t, took)
	require.False(t, called, "同节点不是顶号，不能踢")
	require.Empty(t, p.setCalls)
}

// TestClaimOnlineFirstLogin 首次上线：直接成功，不踢人。
func TestClaimOnlineFirstLogin(t *testing.T) {
	p := &fakePresence{}
	called := false

	took, err := claimOnline(context.Background(), p, "game-02", 7, time.Minute,
		func(string, int64) { called = true })
	require.NoError(t, err)
	require.False(t, took)
	require.False(t, called)
	require.Equal(t, "game-02", p.owner)
}

// TestClaimOnlineRedisDownDoesNotBlock 抢占查询失败：放行绑定，不阻断登录。
//
// 宁可双开一小会儿（围栏令牌会挡住旧 Actor 的提交），也别让玩家登不进来。
func TestClaimOnlineRedisDownDoesNotBlock(t *testing.T) {
	p := &fakePresence{owner: "game-01", tryErr: context.DeadlineExceeded}

	took, err := claimOnline(context.Background(), p, "game-02", 7, time.Minute, nil)
	require.NoError(t, err, "Redis 抖动不能变成登录失败")
	require.False(t, took)
}

// TestClaimOnlineForceSetFails 覆盖失败：报错（此时新旧节点都可能自认权威，必须让调用方知道）。
func TestClaimOnlineForceSetFails(t *testing.T) {
	p := &fakePresence{owner: "game-01", setErr: context.DeadlineExceeded}

	took, err := claimOnline(context.Background(), p, "game-02", 7, time.Minute, nil)
	require.Error(t, err)
	require.True(t, took, "已经通知过旧节点，即使覆盖失败也算接管动作已发起")
}

// TestClaimOnlineNilNotify 无踢人实现（单机/测试）：只覆盖，不 panic。
func TestClaimOnlineNilNotify(t *testing.T) {
	p := &fakePresence{owner: "game-01"}

	took, err := claimOnline(context.Background(), p, "game-02", 7, time.Minute, nil)
	require.NoError(t, err)
	require.True(t, took)
	require.Equal(t, "game-02", p.owner)
}

// remoteSender 记录跨节点投递（actor.RemoteSender 的测试实现）。
type remoteSender struct {
	to  *actor.PID
	msg any
}

func (r *remoteSender) Deliver(to *actor.PID, msg any) error {
	r.to, r.msg = to, msg
	return nil
}

// TestKickRemotePlayerSendsKickMsg 通知旧节点时发的是 KickMsg 而不是别的消息。
//
// 这条守的是「粘合层」：claimOnline 决定要踢人，kickRemotePlayer 负责
// 把「踢谁、投给哪个节点的哪个 Actor」翻译成远程消息。翻译错了（比如
// PID 少了 Node）消息会静默投到本地，永远到不了旧节点。
func TestKickRemotePlayerSendsKickMsg(t *testing.T) {
	rs := &remoteSender{}
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8, Remote: rs})
	ctx := context.Background()
	sys.Start(ctx)
	defer sys.Stop()

	s := &Server{
		cfg:      &config.Config{Server: config.ServerConfig{Name: "game-02"}},
		actorSys: sys,
		logger:   log.New("kick-test", log.WithLevel("error")),
	}

	s.kickRemotePlayer("game-01", 7)

	require.NotNil(t, rs.to, "必须真的投递出去")
	require.Equal(t, "game-01", rs.to.Node, "目标节点错了消息会落回本地")
	require.Equal(t, "player:7", rs.to.ID, "Actor ID 必须与 Spawn 时一致")

	kick, ok := rs.msg.(player.KickMsg)
	require.True(t, ok, "投的必须是 KickMsg")
	require.Equal(t, "game-02", kick.FromNode, "来源节点用于旧节点日志")
}

// TestKickRemotePlayerWithoutActorSystem 无 Actor 系统（单机/测试）：静默跳过。
func TestKickRemotePlayerWithoutActorSystem(t *testing.T) {
	s := &Server{
		cfg:    &config.Config{Server: config.ServerConfig{Name: "game-02"}},
		logger: log.New("kick-test", log.WithLevel("error")),
	}
	require.NotPanics(t, func() { s.kickRemotePlayer("game-01", 7) })
}
