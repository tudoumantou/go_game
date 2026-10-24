// internal/gateway/router/router_test.go —— 协议隔离的入站防线
//
// 网关是客户端唯一入口，「内部号段不能来自 socket」这条规则只能靠这里守。
// 之前只靠「注册表恰好没注册 9000+」兜底——那是碰巧而非契约：
// 谁把一个内部号注册进客户端表，客户端就能直接触发它。
//
// 两条独立闸门，测试要分别覆盖（顺序也是防线顺序）：
//  1. pre-auth：未鉴权连接只放行系统消息（心跳/握手）；
//  2. 号段隔离：已鉴权的连接也只能发 <9000 的客户端号。
package router

import (
	"bytes"
	"net"
	"testing"

	"game_study/internal/common/constant"
	"game_study/internal/gateway/conn"
	"game_study/internal/gateway/session"
	"game_study/pkg/codec"
	"game_study/pkg/crypto"
	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

// nopHandler 满足 conn.Handler：本测试只关心「连接是否被关」，不关心回调内容。
type nopHandler struct{}

func (nopHandler) OnPacket(*conn.Conn, codec.Packet) {}
func (nopHandler) OnClose(*conn.Conn, error)         {}

type rig struct {
	h            *Handler
	c            *conn.Conn
	client       net.Conn
	clientCipher *crypto.SessionCipher // nil = 未完成握手
}

// newRig 组装一条连接。
//
// authed=true 时同时装好会话密钥（线上「已鉴权」必然伴随握手完成，
// 只 SetAuth 不装 cipher 会让所有包卡在解密失败，测不到号段逻辑）。
func newRig(t *testing.T, authed bool) *rig {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})

	logger := log.New("router-test", log.WithLevel("error"))
	connMgr := conn.NewManager()
	sessMgr := session.NewManager(logger)
	h := NewHandler(sessMgr, connMgr, logger, nil, nil)

	c := conn.New(1, server, nopHandler{}, logger)
	connMgr.Add(c)
	sess := sessMgr.NewSession(c) // 登记 byConn，OnPacket 才找得到会话

	r := &rig{h: h, c: c, client: client}
	if authed {
		key := bytes.Repeat([]byte{7}, 32)
		serverCipher, err := crypto.NewSessionCipher(key)
		require.NoError(t, err)
		clientCipher, err := crypto.NewSessionCipher(key)
		require.NoError(t, err)

		sess.SetAuth("tok", 1)
		sess.SetCipher(serverCipher)
		r.clientCipher = clientCipher
	}
	return r
}

// send 按连接的加密状态发一个包：有 cipher 走密文（内层 msgID 与外层一致），
// 没有则走明文。
func (r *rig) send(t *testing.T, msgID uint16) {
	t.Helper()
	body := []byte{}
	if r.clientCipher != nil {
		ct, _, err := r.clientCipher.Encrypt(msgID, nil)
		require.NoError(t, err)
		body = ct
	}
	r.h.OnPacket(r.c, codec.Packet{MsgID: msgID, Body: body})
}

// TestRejectsInternalMsgID 已鉴权连接发 9000+ 内部号：必须断连。
//
// 威胁模型是「客户端伪造内部 gRPC 号」：注册表恰好没注册它们，
// 所以会走「透传业务包」分支——显式检查把这条路径封死。
func TestRejectsInternalMsgID(t *testing.T) {
	for _, msgID := range []uint16{9000, 9001, 9999} {
		r := newRig(t, true)
		r.send(t, msgID)
		require.True(t, r.c.IsClosed(), "内部号 %d 必须被拒绝并断连", msgID)
	}
}

// TestAllowsClientMsgID 已鉴权连接发客户端号段不能被协议隔离误杀。
//
// 未注入 forward / 未注册 handler 时只告警、不断连——断言「连接仍存活」。
//
// 这里刻意不含 1（心跳）：心跳是本地处理的消息，会真的回包；而测试里的连接
// 没跑 Serve 写循环，回包会阻塞到超时——那是 send 路径的事，与号段隔离无关。
func TestAllowsClientMsgID(t *testing.T) {
	for _, msgID := range []uint16{1002, 2001, 3001, 7001, 8999} {
		r := newRig(t, true)
		r.send(t, msgID)
		require.False(t, r.c.IsClosed(), "客户端号 %d 不该被拒", msgID)
	}
}

// TestUnauthedRejectsBusinessMsg 未鉴权连接只放行系统消息（心跳/握手）。
//
// 注意别拿 1001 当业务包：MsgIDHandshakeReq 就是 1001，属于 pre-auth 白名单。
//
// 这是既有的 pre-auth 闸门，在解密之前就用外层 MsgID 判定；
// 与号段隔离是两道独立防线，所以未鉴权时先被它挡掉。
func TestUnauthedRejectsBusinessMsg(t *testing.T) {
	for _, msgID := range []uint16{2001, 3001, 9001} {
		r := newRig(t, false)
		r.send(t, msgID)
		require.True(t, r.c.IsClosed(), "未鉴权连接的业务包 %d 必须断连", msgID)
	}
}

// TestAllowsPreAuthMsg 未鉴权时心跳/握手要放行，否则连登录都进不来。
func TestAllowsPreAuthMsg(t *testing.T) {
	for _, msgID := range []uint16{constant.MsgIDHeartbeatReq, constant.MsgIDHandshakeReq} {
		r := newRig(t, false)
		require.NotPanics(t, func() { r.send(t, msgID) }, "pre-auth 消息 %d 不能 panic", msgID)
		require.False(t, r.c.IsClosed(), "pre-auth 消息 %d 不该断连", msgID)
	}
}

// TestDropsPacketFromUnknownConn 查不到会话的迟到包只断连、不 panic。
func TestDropsPacketFromUnknownConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	logger := log.New("router-test", log.WithLevel("error"))
	h := NewHandler(session.NewManager(logger), conn.NewManager(), logger, nil, nil)

	c := conn.New(42, server, nopHandler{}, logger) // 没登记会话
	require.NotPanics(t, func() {
		h.OnPacket(c, codec.Packet{MsgID: 1001})
	})
	require.True(t, c.IsClosed(), "未知会话的包必须断连")
}
