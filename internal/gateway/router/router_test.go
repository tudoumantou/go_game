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
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"game_study/internal/common/constant"
	"game_study/internal/gateway/conn"
	"game_study/internal/gateway/session"
	"game_study/pkg/codec"
	"game_study/pkg/crypto"
	"game_study/pkg/log"
	gatev1 "game_study/pkg/pb/gate/v1"
	"game_study/pkg/ratelimit"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
func (r *rig) send(t *testing.T, msgID uint32) {
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
	for _, msgID := range []uint32{9000, 9001, 9999} {
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
	for _, msgID := range []uint32{1002, 2001, 3001, 7001, 8999} {
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
	for _, msgID := range []uint32{2001, 3001, 9001} {
		r := newRig(t, false)
		r.send(t, msgID)
		require.True(t, r.c.IsClosed(), "未鉴权连接的业务包 %d 必须断连", msgID)
	}
}

// TestAllowsPreAuthMsg 未鉴权时心跳/握手要放行，否则连登录都进不来。
func TestAllowsPreAuthMsg(t *testing.T) {
	for _, msgID := range []uint32{constant.MsgIDHeartbeatReq, constant.MsgIDHandshakeReq} {
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

// ============================================================================
// 登出
// ============================================================================

// readPacket 从客户端侧读一个完整帧（net.Pipe 的 Write 会阻塞到对端读，
// 所以调用 OnPacket 必须放 goroutine，否则 Send 与 Read 互等死锁）。
func (r *rig) readPacket(t *testing.T) codec.Packet {
	t.Helper()
	_ = r.client.SetReadDeadline(time.Now().Add(3 * time.Second))

	var lenBuf [2]byte
	_, err := io.ReadFull(r.client, lenBuf[:])
	require.NoError(t, err, "读包长度失败")
	n := binary.BigEndian.Uint16(lenBuf[:])

	// ReadLength 的 length 不含自身：后续 n 字节就是 DecodePacket 的入参
	// （msgID|seq|body），不能再把 2 字节长度拼回去，否则整体错位 2 字节。
	rest := make([]byte, n)
	_, err = io.ReadFull(r.client, rest)
	require.NoError(t, err, "读包体失败")

	pkt, err := codec.DecodePacket(rest)
	require.NoError(t, err)
	return pkt
}

// readLogoutResp 解密并解析登出响应。
func (r *rig) readLogoutResp(t *testing.T) *gatev1.LogoutResp {
	t.Helper()
	pkt := r.readPacket(t)
	msgID, _, plain, err := r.clientCipher.Decrypt(pkt.Body)
	require.NoError(t, err, "登出响应必须是密文（已鉴权连接）")
	require.EqualValues(t, constant.MsgIDLogoutResp, msgID)

	resp := &gatev1.LogoutResp{}
	require.NoError(t, proto.Unmarshal(plain, resp))
	return resp
}

// TestLogoutRevokesTokenAndCloses 登出：token 被作废、回 code=0、连接断开。
func TestLogoutRevokesTokenAndCloses(t *testing.T) {
	r := newRig(t, true)

	var revoked []string
	r.h.SetRevoker(func(token string) error {
		revoked = append(revoked, token)
		return nil
	})

	go r.h.OnPacket(r.c, encryptedPacket(t, r.clientCipher, constant.MsgIDLogoutReq))
	resp := r.readLogoutResp(t)

	require.EqualValues(t, 0, resp.Code)
	require.Equal(t, []string{"tok"}, revoked, "必须按会话里的 token 作废，且只作废一次")
	require.Eventually(t, r.c.IsClosed, time.Second, 10*time.Millisecond,
		"登出后必须断连（断连才会触发游戏服下线落库）")
}

// TestLogoutReportsRevokeFailure 作废失败回 1005，但仍断连。
//
// 留着连接更糟：用户以为已登出，却还能继续发业务包。
func TestLogoutReportsRevokeFailure(t *testing.T) {
	r := newRig(t, true)
	r.h.SetRevoker(func(string) error { return fmt.Errorf("redis down") })

	go r.h.OnPacket(r.c, encryptedPacket(t, r.clientCipher, constant.MsgIDLogoutReq))
	resp := r.readLogoutResp(t)

	require.EqualValues(t, 1005, resp.Code, "作废失败必须让客户端知道并重试")
	require.Eventually(t, r.c.IsClosed, time.Second, 10*time.Millisecond)
}

// TestLogoutWithoutRevoker 未注入 revoker（无 Redis 环境）：不 panic，仍断连。
func TestLogoutWithoutRevoker(t *testing.T) {
	r := newRig(t, true)

	go r.h.OnPacket(r.c, encryptedPacket(t, r.clientCipher, constant.MsgIDLogoutReq))
	resp := r.readLogoutResp(t)

	require.EqualValues(t, 0, resp.Code)
	require.Eventually(t, r.c.IsClosed, time.Second, 10*time.Millisecond)
}

// TestLogoutBeforeAuthRejected 未鉴权连接发登出：被 pre-auth 闸门断连。
//
// 没有 token 可作废，登出无意义；这条锁的是「未鉴权不能触发本地 handler」。
func TestLogoutBeforeAuthRejected(t *testing.T) {
	r := newRig(t, false)

	called := false
	r.h.SetRevoker(func(string) error { called = true; return nil })

	r.h.OnPacket(r.c, codec.Packet{MsgID: constant.MsgIDLogoutReq})
	require.True(t, r.c.IsClosed())
	require.False(t, called, "未鉴权时不能调用 revoker")
}

// encryptedPacket 用客户端 cipher 构造一个加密包（内外 msgID 一致）。
func encryptedPacket(t *testing.T, c *crypto.SessionCipher, msgID uint32) codec.Packet {
	t.Helper()
	ct, _, err := c.Encrypt(msgID, nil)
	require.NoError(t, err)
	return codec.Packet{MsgID: msgID, Body: ct}
}

// TestPacketRateLimitDropsExcess 超过突发额度的包被丢弃，连接保持存活。
//
// 断言的是「透传次数」而不是「连接状态」：限流的处理是丢弃，不是断连
// （移动网络抖动会短暂堆包，断连等于把正常玩家踢下线）。
func TestPacketRateLimitDropsExcess(t *testing.T) {
	r := newRig(t, true)

	var forwarded int
	r.h.SetForward(func(*session.Session, uint32, []byte) error {
		forwarded++
		return nil
	})

	// 几乎不补充令牌、突发 1：只有第一个包能过
	r.h.SetPacketLimiter(ratelimit.NewKeyed(0.001, 1, time.Minute))

	for i := 0; i < 5; i++ {
		r.send(t, 2001) // 已鉴权的业务号，走透传分支
	}

	require.Equal(t, 1, forwarded, "超过突发额度后必须丢弃，不能再透传")
	require.False(t, r.c.IsClosed(), "限流不该断连——抖动期的堆包不是攻击")
}

// TestPacketRateLimitDisabled 未注入限流器（单测/关闭配置）时不限制。
func TestPacketRateLimitDisabled(t *testing.T) {
	r := newRig(t, true)

	var forwarded int
	r.h.SetForward(func(*session.Session, uint32, []byte) error {
		forwarded++
		return nil
	})

	for i := 0; i < 20; i++ {
		r.send(t, 2001)
	}
	require.Equal(t, 20, forwarded)
}
