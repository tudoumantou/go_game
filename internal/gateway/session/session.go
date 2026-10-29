// internal/gateway/session/session.go —— 网关会话状态机
//
// Package session 网关会话层：一个 Session 跟踪一条客户端连接从 TCP 建立、
// 加密握手、鉴权到绑定游戏服的完整状态机；Manager 提供 SID/数值 ID/连接/
// 玩家四维索引与超时回收。本包是「谁在线」的唯一事实源。
// 协作关系：conn 层每接受一个连接由 Manager.NewSession 创建会话；
// router 驱动状态推进（握手/鉴权）并调用 Send/Decrypt；server 在
// bindToGame 成功后置 BOUND（表示已转发过业务包）。
// 设计取舍：会话纯内存、不持久化——网关无状态，断线重连 = 新会话
// 重走握手（token 仍在 Redis 所以玩家无感）；reconnect 窗口
// （DefaultSessionReconnectTTL）只用于回收半开连接，不恢复旧会话。
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
// 知识点：iota 从 0 递增定义有序枚举，配合 >= 比较表达「达到某阶段」，
// 比布尔标志位组合（hasKey && authed）更不易出现非法中间态。
type State int

const (
	// StateConnected TCP 已建立，尚未发 ServerHello
	StateConnected State = iota
	// StateHelloSent 已发 ServerHello（RSA 公钥+盐），等待 ClientKeyExchange
	StateHelloSent
	// StateAuthed 握手完成且 token 校验通过（handleHandshakeReq 内置）
	StateAuthed
	// StateBound 已绑定游戏服并建流（bindToGame 成功后置位）
	StateBound
	// StateClosed 已关闭（终态，Manager.GC 负责回收条目）
	StateClosed
)

// String 返回状态名的稳定字符串（日志与 /status 输出用，勿随意改文案）。
// 知识点：实现 fmt.Stringer 后，%v/%s 打印 State 会自动走本方法，
// 日志里看到的是 CONNECTED 而不是 0，排障时省一次查表。
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
// 知识点：RWMutex 读多写少——状态/密钥查询远多于推进，读锁之间不
// 互斥；但 SetCipher 与读 cipher 之间必须互斥，否则可能读到半个指针
// （指针大小的写在 amd64 上原子，但 Go 内存模型不保证，必须加锁）。
type Session struct {
	// id/numID/conn 三种标识：id 用于日志，numID 用于 gRPC 与下行路由，
	// conn 回指底层 TCP 连接（New 后不变）
	id        string
	numID     uint64
	conn      *conn.Conn
	state     State
	createdAt time.Time
	lastAlive time.Time

	// 握手相关：serverHS 是 RSA 握手状态机（ServerHello 时创建），
	// cipher 是密钥交换完成后的 AES-GCM 会话加密器（此前为 nil）
	serverHS *crypto.HandshakeServer
	cipher   *crypto.SessionCipher

	// 认证相关：token 即 login 服签发的字符串，accountID 在握手鉴权时置入。
	//
	// 这里只有账号 ID，没有角色 ID：会话 ↔ 角色的映射属于游戏服
	// （players2sess 是唯一权威）。网关若再存一份，顶号/换角时两边必然
	// 不一致——所以「按角色踢人」是把 player_id 透传给游戏服，由它查表。
	// 历史坑：早期本字段名叫 playerID 却存的是 account_id，读代码的人
	// 很容易把它当角色用。
	token     string
	accountID int64

	// mu 保护以上所有可变字段（读多写少用 RWMutex）；logger 创建后不变
	mu     sync.RWMutex
	logger *log.Logger
}

// New 创建 Session。
// createdAt/lastAlive 同初始化为 now：GC 的「半开连接超时」从建立时刻
// 起算，不是从最后一个包起算——只连上不发包的连接也必须被回收。
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
// 只读字段无需加锁：id 在 New 后不再变化（不可变对象线程安全）。
func (s *Session) ID() string { return s.id }

// NumID 返回数值会话 ID（gRPC 用）。
// 与 conn.ID() 同值，是 backend.Pool / BIND 帧 / 下行路由的统一寻址键。
func (s *Session) NumID() uint64 { return s.numID }

// SetNumID 设置数值会话 ID。
// 仅在 Manager.NewSession 装配期调用一次，运行期不变（与 ID() 同理免锁）。
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
// 状态机只前进不后退（Close 置 CLOSED 例外，见 Close 注释）。
func (s *Session) SetState(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
}

// IsAuthed 是否已认证。
// 用 >= 判断：BOUND 是 AUTHED 之后的状态，语义上「已绑定必然已鉴权」，
// 这样新增更高状态时无需修改所有判断点。
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
// server.onAccept 在发 ServerHello 前注入；之后只读。
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
// ClientKeyExchange 处理成功后调用一次；此后该会话所有收发都走 AES-GCM。
func (s *Session) SetCipher(c *crypto.SessionCipher) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cipher = c
}

// Cipher 获取会话加密器。
// 返回 nil 表示握手未完成——调用方（Send/Decrypt/router）必须判空，
// 这是「接口/指针 nil 陷阱」的典型防守点。
func (s *Session) Cipher() *crypto.SessionCipher {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cipher
}

// SetAuth 设置认证信息。
// token 校验通过时调用，同时把状态推到 AUTHED——「置认证信息」与
// 「状态推进」在同一把锁里完成，避免中间态被并发读到。
//
// accountID 来自 token 校验结果。角色 ID 不在这里——它属于游戏服
// （players2sess），网关不维护「角色 → 会话」映射。
func (s *Session) SetAuth(token string, accountID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = token
	s.accountID = accountID
	s.state = StateAuthed
}

// AccountID 返回账号 ID（鉴权前为 0）。
func (s *Session) AccountID() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.accountID
}

// Token 返回 token。
func (s *Session) Token() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.token
}

// Touch 更新活跃时间。
// 每收到一个包调用（router.OnPacket）；写锁路径频率高但临界区极短
// （一次赋值），不会成为瓶颈。
func (s *Session) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAlive = time.Now()
}

// LastAlive 返回最后活跃时间。
// GC 据此判断半开连接是否超过 reconnect 窗口。
func (s *Session) LastAlive() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastAlive
}

// Close 关闭会话。
// 先置 CLOSED 再关连接：反序的话 OnClose 回调里读到的状态还是活的，
// 清理逻辑（如 Manager.Remove）会基于错误状态决策。conn.Close 幂等，
// 重复调用安全。
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
// 知识点：先在读锁下快照 cipher 再出锁使用——加密是 CPU 密集操作
// （AES-GCM 每秒可做数十万次，但持锁做仍会阻塞其他读者），出锁
// 后 cipher 对象本身不可变，快照安全。
func (s *Session) Send(msgID uint32, plaintext []byte) error {
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
func (s *Session) Decrypt(body []byte) (msgID uint32, seq uint32, plaintext []byte, err error) {
	s.mu.RLock()
	cipher := s.cipher
	s.mu.RUnlock()
	if cipher == nil {
		return 0, 0, nil, fmt.Errorf("session cipher not ready")
	}
	return cipher.Decrypt(body)
}
