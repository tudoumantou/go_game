// internal/gateway/backend/pool_test.go —— 广播下发
//
// GM 踢人只知道角色号，不知道会话语义上的「发到哪条流」，因此按节点广播：
// 每个 target 只发一帧（同一节点上有很多会话流，逐条发会让该节点收到 N 份）。
package backend

import (
	"errors"
	"testing"

	"game_study/pkg/log"
	internalv2 "game_study/pkg/pb/internalv2"

	"github.com/stretchr/testify/require"
)

// fakeStream 只实现 Send：广播路径不发 Recv。
type fakeStream struct {
	internalv2.GameService_ChannelClient
	sent []*internalv2.StreamFrame
	err  error
}

func (f *fakeStream) Send(fr *internalv2.StreamFrame) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, fr)
	return nil
}

// TestBroadcastOncePerTarget 同节点多条会话流只发一次；不同节点各发一次。
func TestBroadcastOncePerTarget(t *testing.T) {
	logger := log.New("pool-test", log.WithLevel("error"))
	p := NewPool(nil, logger, nil)

	a1 := &fakeStream{}
	a2 := &fakeStream{} // 同属 game-01 的另一个会话
	b1 := &fakeStream{}
	p.streams[1] = &streamConn{client: a1, target: "game-01", cancel: func() {}}
	p.streams[2] = &streamConn{client: a2, target: "game-01", cancel: func() {}}
	p.streams[3] = &streamConn{client: b1, target: "game-02", cancel: func() {}}

	sent, errs := p.Broadcast(kickFrameForTest(42))

	require.Empty(t, errs)
	require.Equal(t, 2, sent, "两个节点各一次")
	// 同 target 下选中哪条流是不确定的（map 遍历），只要求合计恰好一条
	require.Equal(t, 1, len(a1.sent)+len(a2.sent), "同一节点不能重复下发")
	require.Len(t, b1.sent, 1)

	var delivered *internalv2.StreamFrame
	if len(a1.sent) > 0 {
		delivered = a1.sent[0]
	} else {
		delivered = a2.sent[0]
	}
	require.EqualValues(t, 42, delivered.GetCtrl().GetPlayerId())
}

// TestBroadcastReportsPartialFailure 部分节点发失败：成功的照发，失败的要报出来。
//
// 静默忽略失败会让「踢了没反应」无从排查——运营需要一个明确的错误。
func TestBroadcastReportsPartialFailure(t *testing.T) {
	logger := log.New("pool-test", log.WithLevel("error"))
	p := NewPool(nil, logger, nil)

	ok := &fakeStream{}
	bad := &fakeStream{err: errors.New("stream broken")}
	p.streams[1] = &streamConn{client: ok, target: "game-01", cancel: func() {}}
	p.streams[2] = &streamConn{client: bad, target: "game-02", cancel: func() {}}

	sent, errs := p.Broadcast(kickFrameForTest(42))

	require.Equal(t, 1, sent)
	require.Len(t, errs, 1)
	require.Contains(t, errs[0].Error(), "game-02")
}

// TestBroadcastNoStreams 一个节点都没连：返回 0 与空错误，由上层决定怎么报。
func TestBroadcastNoStreams(t *testing.T) {
	p := NewPool(nil, log.New("pool-test", log.WithLevel("error")), nil)

	sent, errs := p.Broadcast(kickFrameForTest(42))

	require.Equal(t, 0, sent)
	require.Empty(t, errs)
}

// kickFrameForTest 构造一帧带角色号的 KICK（真实构造在 gateway 包，这里只需形状）。
func kickFrameForTest(playerID int64) *internalv2.StreamFrame {
	return &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_Ctrl{Ctrl: &internalv2.CtrlMsg{
			Type:     internalv2.CtrlMsg_KICK,
			PlayerId: playerID,
			Payload:  []byte("banned"),
		}},
	}
}
