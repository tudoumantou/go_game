// internal/gateway/backend/pool_test.go —— 管理流
//
// 管理流是「不属于任何玩家会话」的下行通道：GM 踢人只知道角色号，
// 挑不出一条会话流来发。用 bufconn 跑真实 gRPC，验证首帧 BIND（会话号 0）
// 与后续控制帧确实到达游戏服——这两步任何一步错了，表现都是「踢了没反应」。
package backend

import (
	"context"
	"net"
	"testing"
	"time"

	"game_study/internal/game/rpc"
	"game_study/pkg/log"
	internalv2 "game_study/pkg/pb/internalv2"
	rpcpkg "game_study/pkg/rpc"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// kickRecorder 记录游戏服收到的踢人指令。
type kickRecorder struct {
	binds []uint64
	kicks []int64
	got   chan int64
}

func (k *kickRecorder) OnBind(_ context.Context, sessionID uint64, _, _ int64) error {
	k.binds = append(k.binds, sessionID)
	return nil
}

func (k *kickRecorder) OnUnbind(context.Context, uint64) error { return nil }

func (k *kickRecorder) OnClientPacket(context.Context, uint64, uint32, uint32, []byte) error {
	return nil
}

func (k *kickRecorder) OnKick(_ context.Context, playerID int64, _ string) error {
	k.kicks = append(k.kicks, playerID)
	select {
	case k.got <- playerID:
	default:
	}
	return nil
}

// TestAdminSessionIDMatchesRPC 两个包各自定义了管理会话号，必须一致。
//
// 不一致的表现：网关发的首帧被游戏服当成真实会话绑定（或反过来被拒），
// 踢人指令永远到不了。
func TestAdminSessionIDMatchesRPC(t *testing.T) {
	require.EqualValues(t, rpc.AdminSessionID, adminSessionID)
}

// TestAdminSendDeliversKick 管理流能建立并送达 KICK，且不产生玩家绑定。
func TestAdminSendDeliversKick(t *testing.T) {
	logger := log.New("admin-test", log.WithLevel("error"))

	rec := &kickRecorder{got: make(chan int64, 1)}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	internalv2.RegisterGameServiceServer(srv, rpc.NewGameServer(rec, logger))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// 用 bufconn 的 dialer 建连接；target 走 passthrough（默认 scheme）
	pool := NewPool(
		rpcpkg.NewClientPool(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		})),
		logger, nil,
	)
	t.Cleanup(func() { _ = pool.Close() })

	frame := &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_Ctrl{Ctrl: &internalv2.CtrlMsg{
			Type:     internalv2.CtrlMsg_KICK,
			PlayerId: 42,
			Payload:  []byte("banned"),
		}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, pool.AdminSend(ctx, "passthrough:///bufnet", frame))

	select {
	case pid := <-rec.got:
		require.EqualValues(t, 42, pid)
	case <-time.After(3 * time.Second):
		t.Fatal("KICK 帧没有送达游戏服")
	}

	require.Empty(t, rec.binds, "管理流不能触发 OnBind（它不绑定任何玩家）")
}

// TestAdminSendReusesStream 第二次发送复用同一条管理流（不重复建连）。
func TestAdminSendReusesStream(t *testing.T) {
	logger := log.New("admin-test", log.WithLevel("error"))

	rec := &kickRecorder{got: make(chan int64, 4)}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	internalv2.RegisterGameServiceServer(srv, rpc.NewGameServer(rec, logger))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	pool := NewPool(
		rpcpkg.NewClientPool(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		})),
		logger, nil,
	)
	t.Cleanup(func() { _ = pool.Close() })

	ctx := context.Background()
	mk := func(pid int64) *internalv2.StreamFrame {
		return &internalv2.StreamFrame{Data: &internalv2.StreamFrame_Ctrl{Ctrl: &internalv2.CtrlMsg{
			Type: internalv2.CtrlMsg_KICK, PlayerId: pid,
		}}}
	}

	require.NoError(t, pool.AdminSend(ctx, "passthrough:///bufnet", mk(1)))
	first := pool.admin["passthrough:///bufnet"]
	require.NotNil(t, first)

	require.NoError(t, pool.AdminSend(ctx, "passthrough:///bufnet", mk(2)))
	require.Same(t, first, pool.admin["passthrough:///bufnet"], "必须复用同一条流")
}
