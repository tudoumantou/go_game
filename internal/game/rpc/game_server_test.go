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
		PlayerID int64
		Reason   string
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

func (f *fakeHandler) OnKick(_ context.Context, playerID int64, reason string) error {
	f.kicks = append(f.kicks, struct {
		PlayerID int64
		Reason   string
	}{playerID, reason})
	return nil
}

// TestHandleCtrlKick KICK 帧必须分发到 OnKick，带上角色号与原因。
//
// 传的是 PlayerId 而不是 SessionId：网关不知道玩家的会话号（也不需要知道），
// 会话由游戏服自己查。把 SessionId 当角色号传会踢错人。
func TestHandleCtrlKick(t *testing.T) {
	h := &fakeHandler{}
	s := NewGameServer(h, log.New("rpc-test", log.WithLevel("error")))

	s.handleCtrl(context.Background(), &internalv2.CtrlMsg{
		Type:      internalv2.CtrlMsg_KICK,
		SessionId: 77, // 故意给一个会话号，确认它不被当成目标
		PlayerId:  42,
		Payload:   []byte("banned"),
	})

	require.Empty(t, h.unbinds, "KICK 不能被当成 UNBIND")
	require.Len(t, h.kicks, 1)
	require.EqualValues(t, 42, h.kicks[0].PlayerID)
	require.Equal(t, "banned", h.kicks[0].Reason)
}

// TestHandleCtrlKickWithoutPlayerID 没带角色号的 KICK 直接忽略。
//
// 忽略而不是回退用 SessionId：那条回退路径会把「会话号」当角色号去查表，
// 查不到就静默失败，查到了就是踢错人——两种都比丢弃更糟。
func TestHandleCtrlKickWithoutPlayerID(t *testing.T) {
	h := &fakeHandler{}
	s := NewGameServer(h, log.New("rpc-test", log.WithLevel("error")))

	s.handleCtrl(context.Background(), &internalv2.CtrlMsg{
		Type:      internalv2.CtrlMsg_KICK,
		SessionId: 77,
	})

	require.Empty(t, h.kicks, "没有角色号时不能动任何会话")
	require.Empty(t, h.unbinds)
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
			Type:     internalv2.CtrlMsg_KICK,
			PlayerId: 9,
		})
	})
	require.Len(t, h.kicks, 1)
	require.Empty(t, h.kicks[0].Reason)
}
