// internal/gateway/session/session.go —— 网关会话状态机
//
// Package session 网关会话层：一个 Session 跟踪一条客户端连接从 TCP 建立、
// 加密握手、鉴权到绑定游戏服的完整状态机；Manager 提供 SID/数值 ID/连接/
// 玩家四维索引与超时回收。本包是「谁在线」的唯一事实源。
package session

import (
	"fmt"
	"sync"
	"time"

	"game_study/internal/gateway/conn"
	"game_study/pkg/codec"
	"game_study/pkg/crypto"
	"game_study/pkg/log"
)

// State 会话状态。
//
// 状态机单向推进：CONNECTED → HELLO_SENT →（密钥交换/Finished 后）
// AUTHED → BOUND → CLOSED。IsAuthed 用 >= 判断，因此「已绑定」也视为已鉴权。
type State int

const (
	// StateConnected TCP 已建立，尚未发 ServerHello
	StateConnected State = iota
	// StateHelloSent 已发 ServerHello（RSA 公钥+盐），等待 ClientKeyExchange
	StateHelloSent
	// StateAuthed 握手完成且 token 校验通过（handleHandshakeReq 内置）
	StateAuthed
	// StateBound 已绑定游戏服并建流（Manager.BindPlayer 置位）
	StateBound
	// StateClosed 已关闭（终态，Manager.GC 负责回收条目）
	StateClosed
)

// String 返回状态名的稳定字符串（日志与 /status 输出用，勿随意改文案）。
func (s State) String() string {
	switch s {
	case StateConnected:
		return "CONNECTED"
	case StateHelloSent:
		return "HELLO_SENT"
	case StateAuthed:
		return "AUTHED"
	case StateBound:
		return "BOUND"
	case StateClosed:
		return "CLOSED"
	default:
		return "UNKNOWN"
	}
}

// Session 网关侧会话，一个 Session 对应一个玩家在线状态。
//
// 字段分组：标识（id/numID/conn/时间戳）、握手（serverHS/cipher）、
// 认证（token/playerID）。所有可变字段受 mu 保护；conn 创建后不变。
// 不变量：cipher 在握手完成后非 nil，此前 Send/Decrypt 必须报错
// （防止明文期误把业务包当密文处理）。
type Session struct {
	// id/numID/conn 三种标识：id 用于日志，numID 用于 gRPC 与下行路由，
	// conn 回指底层 TCP 连接（New 后不变）
	id        string
	numID     uint64
	conn      *conn.Conn
	state     State
	createdAt time.Time
	lastAlive time.Time

	// 握手相关
	serverHS *crypto.HandshakeServer
	cipher   *crypto.SessionCipher

	// 认证相关
	token    string
	playerID int64

	// mu 保护以上所有可变字段（读多写少用 RWMutex）；logger 创建后不变
	mu     sync.RWMutex
	logger *log.Logger
}

// New 创建 Session。
func New(id string, c *conn.Conn, logger *log.Logger) *Session {
	now := time.Now()
	return &Session{
		id:        id,
		conn:      c,
		state:     StateConnected,
		createdAt: now,
		lastAlive: now,
		logger:    logger.With("session", id),
	}
}

// ID 返回会话 ID。
func (s *Session) ID() string { return s.id }

// NumID 返回数值会话 ID（gRPC 用）。
func (s *Session) NumID() uint64 { return s.numID }

// SetNumID 设置数值会话 ID。
func (s *Session) SetNumID(id uint64) { s.numID = id }

// Conn 返回底层连接。
func (s *Session) Conn() *conn.Conn { return s.conn }

// State 返回当前状态。
func (s *Session) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// SetState 设置状态。
func (s *Session) SetState(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
}

// IsAuthed 是否已认证。
func (s *Session) IsAuthed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state >= StateAuthed
}

// IsBound 是否已绑定游戏服。
func (s *Session) IsBound() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state == StateBound
}

// SetHandshakeServer 设置服务端握手状态机。
func (s *Session) SetHandshakeServer(hs *crypto.HandshakeServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.serverHS = hs
}

// HandshakeServer 获取握手状态机。
func (s *Session) HandshakeServer() *crypto.HandshakeServer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.serverHS
}

// SetCipher 设置会话加密器。
func (s *Session) SetCipher(c *crypto.SessionCipher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cipher = c
}

// Cipher 获取会话加密器。
func (s *Session) Cipher() *crypto.SessionCipher {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cipher
}

// SetAuth 设置认证信息。
func (s *Session) SetAuth(token string, playerID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = token
	s.playerID = playerID
	s.state = StateAuthed
}

// PlayerID 返回玩家 ID。
func (s *Session) PlayerID() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.playerID
}

// Token 返回 token。
func (s *Session) Token() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.token
}

// Touch 更新活跃时间。
func (s *Session) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAlive = time.Now()
}

// LastAlive 返回最后活跃时间。
func (s *Session) LastAlive() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastAlive
}

// Close 关闭会话。
func (s *Session) Close(err error) {
	s.SetState(StateClosed)
	if s.conn != nil {
		_ = s.conn.Close(err)
	}
}

// Send 发送逻辑包（自动加密）。
//
// cipher 未就绪（握手未完成）时直接报错：握手期的系统消息由 router 走
// 明文通道（conn.Send 原始包），业务下行只可能发生在握手完成后。
func (s *Session) Send(msgID uint16, plaintext []byte) error {
	s.mu.RLock()
	cipher := s.cipher
	s.mu.RUnlock()
	if cipher == nil {
		return fmt.Errorf("session cipher not ready")
	}
	body, seq, err := cipher.Encrypt(msgID, plaintext)
	if err != nil {
		return err
	}
	return s.conn.Send(codec.Packet{MsgID: msgID, Seq: seq, Body: body})
}

// Decrypt 解密收到的包体。
//
// 与 Send 对称：cipher 未就绪即拒绝。解密成功后需校验内层 msgID 与
// 外层一致（router.OnPacket 负责），防重放/错序包混入。
func (s *Session) Decrypt(body []byte) (msgID uint16, seq uint32, plaintext []byte, err error) {
	s.mu.RLock()
	cipher := s.cipher
	s.mu.RUnlock()
	if cipher == nil {
		return 0, 0, nil, fmt.Errorf("session cipher not ready")
	}
	return cipher.Decrypt(body)
}
