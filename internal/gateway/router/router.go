// internal/gateway/router/router.go —— 网关系统消息路由
package router

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/common/constant"
	"game_study/internal/gateway/conn"
	"game_study/internal/gateway/session"
	"game_study/pkg/codec"
	"game_study/pkg/crypto"
	"game_study/pkg/log"

	gatev1 "game_study/pkg/pb/gate/v1"
	"google.golang.org/protobuf/proto"
)

// Handler 路由处理器。
type Handler struct {
	sessMgr     *session.Manager
	connMgr     *conn.Manager
	registry    *codec.HandlerRegistry
	msgRegistry *codec.MessageRegistry
	logger      *log.Logger
	validator   TokenValidator
	onAuthed    func(sess *session.Session)
}

// TokenValidator 校验 token 并返回 player_id。
type TokenValidator func(token string) (int64, error)

// NewHandler 创建路由处理器。
func NewHandler(sessMgr *session.Manager, connMgr *conn.Manager, logger *log.Logger, validator TokenValidator, onAuthed func(sess *session.Session)) *Handler {
	r := &Handler{
		sessMgr:     sessMgr,
		connMgr:     connMgr,
		registry:    codec.NewHandlerRegistry(),
		msgRegistry: codec.NewMessageRegistry(),
		logger:      logger,
		validator:   validator,
		onAuthed:    onAuthed,
	}
	r.registerMessages()
	r.registerHandlers()
	return r
}

func (h *Handler) registerMessages() {
	h.msgRegistry.Register(constant.MsgIDHeartbeatReq, func() proto.Message { return &gatev1.HeartbeatReq{} })
	h.msgRegistry.Register(constant.MsgIDHeartbeatResp, func() proto.Message { return &gatev1.HeartbeatResp{} })
	h.msgRegistry.Register(constant.MsgIDKickNotify, func() proto.Message { return &gatev1.KickNotify{} })
	h.msgRegistry.Register(constant.MsgIDHandshakeReq, func() proto.Message { return &gatev1.HandshakeReq{} })
	h.msgRegistry.Register(constant.MsgIDHandshakeResp, func() proto.Message { return &gatev1.HandshakeResp{} })
}

func (h *Handler) registerHandlers() {
	h.registry.Register(constant.MsgIDHeartbeatReq, h.handleHeartbeat)
	h.registry.Register(constant.MsgIDHandshakeReq, h.handleHandshakeReq)
}

// OnPacket 实现 conn.Handler。
func (h *Handler) OnPacket(c *conn.Conn, p codec.Packet) {
	sess := h.sessMgr.GetByConn(c.ID())
	if sess == nil {
		h.logger.Warn("packet from unknown conn", "conn_id", c.ID(), "msg_id", p.MsgID)
		_ = c.Close(fmt.Errorf("unknown session"))
		return
	}
	sess.Touch()

	// 握手完成前，只允许系统消息（心跳/握手）
	if !sess.IsAuthed() && !isPreAuthMsg(p.MsgID) {
		h.logger.Warn("pre-auth illegal msg", "conn_id", c.ID(), "msg_id", p.MsgID)
		_ = c.Close(fmt.Errorf("pre-auth illegal msg"))
		return
	}

	// 加密握手阶段（明文系统消息号）
	if !sess.IsAuthed() && (p.MsgID == constant.MsgIDClientKeyExchange || p.MsgID == constant.MsgIDFinished) {
		if err := h.handleCryptoHandshake(c, sess, p.MsgID, p.Body); err != nil {
			h.logger.Warn("crypto handshake failed", "conn_id", c.ID(), "err", err)
			_ = c.Close(err)
		}
		return
	}

	var plaintext []byte
	var msgID uint16
	var err error

	if sess.IsAuthed() || sess.Cipher() != nil {
		// 已建立会话密钥：走 AES-GCM 解密
		msgID, _, plaintext, err = sess.Decrypt(p.Body)
		if err != nil {
			h.logger.Warn("decrypt failed", "conn_id", c.ID(), "err", err)
			_ = c.Close(fmt.Errorf("decrypt failed: %w", err))
			return
		}
	} else {
		// 明文阶段（理论上只剩非法包）
		msgID = p.MsgID
		plaintext = p.Body
	}

	if msgID != p.MsgID {
		h.logger.Warn("msgID mismatch", "outer", p.MsgID, "inner", msgID)
		_ = c.Close(fmt.Errorf("msgID mismatch"))
		return
	}

	msg, err := h.msgRegistry.Unmarshal(msgID, plaintext)
	if err != nil {
		h.logger.Warn("unmarshal failed", "msg_id", msgID, "err", err)
		_ = c.Close(fmt.Errorf("unmarshal failed"))
		return
	}

	ctx := context.WithValue(context.Background(), ctxKeyConnID{}, c.ID())
	if err := h.registry.Handle(ctx, msgID, msg); err != nil {
		h.logger.Warn("handle failed", "msg_id", msgID, "err", err)
	}
}

// handleCryptoHandshake 处理 ClientKeyExchange / Finished。
func (h *Handler) handleCryptoHandshake(c *conn.Conn, sess *session.Session, msgID uint16, body []byte) error {
	hs := sess.HandshakeServer()
	if hs == nil {
		return fmt.Errorf("handshake server not initialized")
	}

	switch msgID {
	case constant.MsgIDClientKeyExchange:
		if err := hs.OnClientKeyExchange(body); err != nil {
			return err
		}
		cipher, err := crypto.NewSessionCipher(hs.SessionKey())
		if err != nil {
			return err
		}
		sess.SetCipher(cipher)
		h.logger.Debug("client key exchange done", "conn_id", c.ID())
		return nil

	case constant.MsgIDFinished:
		// 客户端 Finished 用派生密钥加密（SessionCipher 含 msgID/seq 头）
		_, _, plain, err := sess.Decrypt(body)
		if err != nil {
			return fmt.Errorf("decrypt finished: %w", err)
		}
		if string(plain) != "finished" {
			return fmt.Errorf("invalid finished marker")
		}
		// 回复服务端 Finished
		if err := sess.Send(constant.MsgIDFinished, []byte("finished")); err != nil {
			return err
		}
		h.logger.Debug("handshake finished", "conn_id", c.ID())
		return nil
	}
	return fmt.Errorf("unexpected handshake msg %d", msgID)
}

// ctxKeyConnID context key 类型。
type ctxKeyConnID struct{}

// SendFinished 向客户端发送服务端 Finished（可选，双向认证）。
func (h *Handler) SendFinished(c *conn.Conn, sess *session.Session) error {
	body := []byte("finished")
	return sess.Send(constant.MsgIDFinished, body)
}


// OnClose 实现 conn.Handler。
func (h *Handler) OnClose(c *conn.Conn, err error) {
	sess := h.sessMgr.GetByConn(c.ID())
	if sess == nil {
		return
	}
	h.logger.Info("conn closed", "conn_id", c.ID(), "sid", sess.ID(), "err", err)
	h.sessMgr.Remove(sess)
}

func isPreAuthMsg(msgID uint16) bool {
	return msgID == constant.MsgIDHeartbeatReq || msgID == constant.MsgIDServerHello ||
		msgID == constant.MsgIDClientKeyExchange || msgID == constant.MsgIDFinished ||
		msgID == constant.MsgIDHandshakeReq || msgID == constant.MsgIDHandshakeResp
}

// SendKick 向指定连接发送踢下线。
func (h *Handler) SendKick(c *conn.Conn, reason string, banUntil int64) error {
	sess := h.sessMgr.GetByConn(c.ID())
	if sess == nil {
		return fmt.Errorf("session not found")
	}
	notify := &gatev1.KickNotify{Reason: reason, BanUntil: banUntil}
	body, err := proto.Marshal(notify)
	if err != nil {
		return err
	}
	return sess.Send(constant.MsgIDKickNotify, body)
}

// handleHeartbeat 心跳。
func (h *Handler) handleHeartbeat(ctx context.Context, msg proto.Message) error {
	connID := ctx.Value(ctxKeyConnID{}).(int64)
	c := h.connMgr.Get(connID)
	if c == nil {
		return fmt.Errorf("conn %d not found", connID)
	}
	sess := h.sessMgr.GetByConn(connID)
	if sess == nil {
		return fmt.Errorf("session not found")
	}
	resp := &gatev1.HeartbeatResp{ServerTime: timeNowMs()}
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return sess.Send(constant.MsgIDHeartbeatResp, body)
}

// handleHandshakeReq 握手请求。
func (h *Handler) handleHandshakeReq(ctx context.Context, msg proto.Message) error {
	req := msg.(*gatev1.HandshakeReq)
	connID := ctx.Value(ctxKeyConnID{}).(int64)
	sess := h.sessMgr.GetByConn(connID)
	if sess == nil {
		return fmt.Errorf("session not found")
	}

	// 校验 token
	var playerID int64
	code := int32(0)
	if h.validator != nil {
		pid, err := h.validator(req.Token)
		if err != nil {
			h.logger.Warn("token invalid", "conn_id", connID, "err", err)
			// 1001 参数错误 / 1002 token 过期，见 4.10
			code = 1002
		} else {
			playerID = pid
		}
	}
	if code == 0 {
		sess.SetAuth(req.Token, playerID)
		if h.onAuthed != nil {
			h.onAuthed(sess)
		}
	}

	resp := &gatev1.HandshakeResp{
		Code:              code,
		PlayerId:          playerID,
		ServerTime:        timeNowMs(),
		HeartbeatInterval: int32(constant.DefaultHeartbeatInterval.Seconds()),
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return sess.Send(constant.MsgIDHandshakeResp, body)
}

func timeNowMs() int64 {
	return timeNow().UnixMilli()
}

var timeNow = func() time.Time { return time.Now() }

// SendServerHello 向新连接发送 ServerHello（未加密）。
func (h *Handler) SendServerHello(c *conn.Conn, pubKeyPEM, salt []byte) error {
	// 外层包：MsgID=1001, Seq=0, Body=pubKeyPEM|salt
	body := make([]byte, len(pubKeyPEM)+len(salt))
	copy(body, pubKeyPEM)
	copy(body[len(pubKeyPEM):], salt)
	return c.Send(codec.Packet{MsgID: constant.MsgIDServerHello, Seq: 0, Body: body})
}
