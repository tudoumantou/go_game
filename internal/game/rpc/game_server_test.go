// internal/game/rpc/game_server_test.go —— 控制帧分发
//
// 网关发下来的 CtrlMsg 只有两种：解绑与踢人。分发写错的表现很隐蔽：
// KICK 被当成 UNBIND 处理 → 玩家会话被解绑但收不到 KickNotify，
// 客户端只看到「连接异常」；或者干脆什么都没做（之前的 TODO 状态）。
package rpc

import (
	"context"
	"testing"

	"game_study/pkg/log"
	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/stretchr/testify/require"
)

// fakeHandler 记录网关下行指令。
type fakeHandler struct {
	binds   []uint64
	unbinds []uint64
	kicks   []struct {
		SessionID uint64
		Reason    string
	}
}

func (f *fakeHandler) OnBind(_ context.Context, sessionID uint64, _, _ int64) error {
	f.binds = append(f.binds, sessionID)
	return nil
}

func (f *fakeHandler) OnUnbind(_ context.Context, sessionID uint64) error {
	f.unbinds = append(f.unbinds, sessionID)
	return nil
}

func (f *fakeHandler) OnClientPacket(context.Context, uint64, uint32, uint32, []byte) error {
	return nil
}

func (f *fakeHandler) OnKick(_ context.Context, sessionID uint64, reason string) error {
	f.kicks = append(f.kicks, struct {
		SessionID uint64
		Reason    string
	}{sessionID, reason})
	return nil
}

// TestHandleCtrlKick KICK 帧必须分发到 OnKick，带上会话号与原因。
func TestHandleCtrlKick(t *testing.T) {
	h := &fakeHandler{}
	s := NewGameServer(h, log.New("rpc-test", log.WithLevel("error")))

	s.handleCtrl(context.Background(), &internalv2.CtrlMsg{
		Type:      internalv2.CtrlMsg_KICK,
		SessionId: 77,
		Payload:   []byte("banned"),
	})

	require.Empty(t, h.unbinds, "KICK 不能被当成 UNBIND")
	require.Len(t, h.kicks, 1)
	require.EqualValues(t, 77, h.kicks[0].SessionID)
	require.Equal(t, "banned", h.kicks[0].Reason)
}

// TestHandleCtrlUnbind UNBIND 仍然走解绑，不能被 KICK 分支抢走。
func TestHandleCtrlUnbind(t *testing.T) {
	h := &fakeHandler{}
	s := NewGameServer(h, log.New("rpc-test", log.WithLevel("error")))

	s.handleCtrl(context.Background(), &internalv2.CtrlMsg{
		Type:      internalv2.CtrlMsg_UNBIND,
		SessionId: 88,
	})

	require.Empty(t, h.kicks)
	require.Equal(t, []uint64{88}, h.unbinds)
}

// TestHandleCtrlKickWithoutPayload 原因为空也不能 panic（Payload 可能没带）。
func TestHandleCtrlKickWithoutPayload(t *testing.T) {
	h := &fakeHandler{}
	s := NewGameServer(h, log.New("rpc-test", log.WithLevel("error")))

	require.NotPanics(t, func() {
		s.handleCtrl(context.Background(), &internalv2.CtrlMsg{
			Type:      internalv2.CtrlMsg_KICK,
			SessionId: 9,
		})
	})
	require.Len(t, h.kicks, 1)
	require.Empty(t, h.kicks[0].Reason)
}
