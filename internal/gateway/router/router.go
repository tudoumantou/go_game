// internal/gateway/router/router.go —— 网关系统消息路由
//
// 架构职责：网关的「协议前厅」——在客户端连接上处理一切非业务语义：
// 心跳握手、RSA/AES 加密握手、token 鉴权、合法性与重放校验，以及把
// 业务消息透传给游戏服（4.5 客户端上行透传，网关不解析业务协议）。
// 协作关系：实现 conn.Handler 接口被 conn.Serve 的读循环回调；通过
// session.Manager 查会话状态，通过注入的 TokenValidator（server.go 的
// Redis 查询）校验 token，通过 PacketForwarder（server.forwardPacket）
// 把业务包送到游戏服；onAuthed 回调（server.bindToGame）在鉴权成功后
// 触发会话→游戏服绑定。
// 设计取舍：网关只认识消息号不认识消息体——msgRegistry 登记的是系统
// 消息的 proto 类型，未登记的一律透传；这样新增业务协议不需要改网关，
// 网关版本与游戏协议解耦。安全上前置于透传：握手未完成只放行系统消息，
// 解密失败/msgID 不一致一律断连，把攻击面收敛在连接入口。
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
	"game_study/pkg/metrics"
	"game_study/pkg/ratelimit"
	"strconv"

	gatev1 "game_study/pkg/pb/gate/v1"
	"google.golang.org/protobuf/proto"
)

// Handler 路由处理器。
//
// 组装期通过 Set* 注入可选能力（forward/onClosed），New 时注入必需
// 依赖（sessMgr/connMgr/validator/onAuthed）——依赖注入让单测可以替换
// TokenValidator 与转发函数，不起 Redis/gRPC 也能跑通握手链路。
// 注意接口 nil 陷阱：TokenValidator/PacketForwarder 是函数类型，nil
// 调用会 panic，因此每个调用点前都必须判空（validator/forward 均如此）。
type Handler struct {
	sessMgr     *session.Manager
	connMgr     *conn.Manager
	registry    *codec.HandlerRegistry
	msgRegistry *codec.MessageRegistry
	logger      *log.Logger
	validator   TokenValidator
	revoker     TokenRevoker
	onAuthed    func(sess *session.Session)
	// packetLimiter 连接级包频率限制（nil = 不限流，单测环境）。
	// 按 connID 分桶：一个连接一个桶，互不挤占。
	packetLimiter *ratelimit.Keyed
	forward       PacketForwarder
	onClosed      func(sess *session.Session)
}

// PacketForwarder 业务消息透传（网关不解析，直接转发游戏服）。
// 返回错误时调用方（OnPacket）只记日志不断连——断连决策权在
// server.forwardPacket（后端流已死时才升级为断连）。
type PacketForwarder func(sess *session.Session, msgID uint32, body []byte) error

// SetForward 注入透传能力（组装期调用）。
func (h *Handler) SetForward(f PacketForwarder) { h.forward = f }

// SetOnClosed 注入会话关闭回调（清理转发路由）。
// 由 server.go 注入：删除 sessGame 表项并关闭 backend stream，
// 保证「会话消亡 → 下游路由同步清理」。
func (h *Handler) SetOnClosed(f func(sess *session.Session)) { h.onClosed = f }

// TokenValidator 校验 token 并返回 player_id。
// 同步阻塞调用，实现方（Redis 查询）应自带超时；返回错误即鉴权失败。
type TokenValidator func(token string) (int64, error)

// TokenRevoker 作废 token（登出时调用）。
//
// 与 Validator 分开而不是合成一个接口：两者的失败语义完全不同——
// 校验失败只是拒绝登录，作废失败意味着「用户以为登出了，token 其实还有效」，
// 必须让客户端知道并重试。合成接口会让调用方误以为两者可以同等对待。
type TokenRevoker func(token string) error

// SetPacketLimiter 注入连接级限流器（组装期调用；nil 表示不限流）。
func (h *Handler) SetPacketLimiter(l *ratelimit.Keyed) { h.packetLimiter = l }

// SetRevoker 注入 token 作废能力（组装期调用）。
// 未注入时登出仍会断连但不作废 token——只在无 Redis 的单测环境可接受。
func (h *Handler) SetRevoker(fn TokenRevoker) { h.revoker = fn }

// NewHandler 创建路由处理器。
// 注册表（消息工厂 + handler）在构造期一次性建好，运行期只读——
// 避免运行期加锁，也防止热更新半个路由表。
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

// registerMessages 登记系统消息的 proto 工厂函数。
// msgID → 构造函数 的映射供 Unmarshal 时按号创建具体类型；
// 只登记网关认识的消息，业务消息不在此表（见 OnPacket 透传分支）。
func (h *Handler) registerMessages() {
	h.msgRegistry.Register(constant.MsgIDHeartbeatReq, func() proto.Message { return &gatev1.HeartbeatReq{} })
	h.msgRegistry.Register(constant.MsgIDHeartbeatResp, func() proto.Message { return &gatev1.HeartbeatResp{} })
	h.msgRegistry.Register(constant.MsgIDKickNotify, func() proto.Message { return &gatev1.KickNotify{} })
	h.msgRegistry.Register(constant.MsgIDHandshakeReq, func() proto.Message { return &gatev1.HandshakeReq{} })
	h.msgRegistry.Register(constant.MsgIDHandshakeResp, func() proto.Message { return &gatev1.HandshakeResp{} })
	h.msgRegistry.Register(constant.MsgIDLogoutReq, func() proto.Message { return &gatev1.LogoutReq{} })
	h.msgRegistry.Register(constant.MsgIDLogoutResp, func() proto.Message { return &gatev1.LogoutResp{} })
}

// registerHandlers 登记网关本地处理的消息（心跳/握手）。
// 未登记的消息走透传——这是「网关无业务语义」的落地方式。
func (h *Handler) registerHandlers() {
	h.registry.Register(constant.MsgIDHeartbeatReq, h.handleHeartbeat)
	h.registry.Register(constant.MsgIDHandshakeReq, h.handleHandshakeReq)
	h.registry.Register(constant.MsgIDLogoutReq, h.handleLogout)
}

// OnPacket 实现 conn.Handler。
// 处理顺序即安全顺序：会话存在性 → 活跃时间刷新 → 鉴权前消息过滤 →
// 加密握手分支 → 解密/明文分支 → 内外 msgID 一致性 → 系统消息本地处理
// 或业务透传。任何校验失败都直接断连，不做「尝试恢复」——握手期的
// 连接成本极低，错杀比放过安全。
func (h *Handler) OnPacket(c *conn.Conn, p codec.Packet) {
	// connID 是会话的数值 ID（server.onAccept 的约定），据此找回会话；
	// 找不到说明连接已进入关闭流程（迟到包），直接断
	sess := h.sessMgr.GetByConn(c.ID())
	if sess == nil {
		h.logger.Warn("packet from unknown conn", "conn_id", c.ID(), "msg_id", p.MsgID)
		_ = c.Close(fmt.Errorf("unknown session"))
		return
	}
	// 连接级限流：先于解密/分发执行，避免刷包把 CPU 和 Actor 信箱打满。
	//
	// 超限的处理是「丢弃」而不是断连：移动网络抖动会短暂堆包，
	// 断连等于把正常玩家踢下线。丢弃已经消除了后续业务成本——
	// 客户端有超时重试；真要更严就把 burst 调小。
	if h.packetLimiter != nil && !h.packetLimiter.Allow(strconv.FormatInt(c.ID(), 10)) {
		metrics.MsgInc("in", "rate_limited")
		h.logger.Debug("packet rate limited", "conn_id", c.ID(), "msg_id", p.MsgID)
		return
	}

	sess.Touch()

	// 握手完成前，只允许系统消息（心跳/握手）
	// 未鉴权连接的业务包一律视为探测/攻击，断连处理
	if !sess.IsAuthed() && !isPreAuthMsg(p.MsgID) {
		h.logger.Warn("pre-auth illegal msg", "conn_id", c.ID(), "msg_id", p.MsgID)
		_ = c.Close(fmt.Errorf("pre-auth illegal msg"))
		return
	}

	// 加密握手阶段（明文系统消息号）
	// ClientKeyExchange/Finished 在 token 鉴权之前发生，此时还没有
	// 会话密钥，两个消息本身必须按明文规则处理
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
		// cipher 已就绪但业务上还没过 token 校验（握手完但未鉴权）也走这里，
		// 解密成功后的合法性由后续 msgID 一致性与 handler 层保证
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

	// 外层 MsgID 与密文内层 MsgID 必须一致：防止把 A 消息的密文塞进
	// B 消息的外壳（错包/重放混入），不一致即断
	if msgID != p.MsgID {
		h.logger.Warn("msgID mismatch", "outer", p.MsgID, "inner", msgID)
		_ = c.Close(fmt.Errorf("msgID mismatch"))
		return
	}

	// 内部号段（9000+）只能走 gRPC，绝不允许来自客户端 socket。
	// 注册表恰好没注册这些号（所以下面会走透传分支），但那是「碰巧」
	// 而非契约：靠注册表兜底时，将来谁把一个内部号注册进客户端表，
	// 客户端就能直接触发它。这里显式挡一次，把协议隔离写成代码。
	if !constant.IsClientMsgID(uint32(msgID)) {
		h.logger.Warn("reject internal msg id from client", "conn_id", c.ID(), "msg_id", msgID)
		_ = c.Close(fmt.Errorf("internal msg id %d not allowed", msgID))
		return
	}

	// 业务消息：网关不解析，透传给游戏服（4.5 客户端上行透传）
	// forward 未注入（单测/无 etcd 环境）时丢弃并告警，不崩服务
	if !h.msgRegistry.Has(msgID) {
		if h.forward == nil {
			h.logger.Warn("no forwarder for business msg", "msg_id", msgID)
			return
		}
		if err := h.forward(sess, uint32(msgID), plaintext); err != nil {
			h.logger.Warn("forward failed", "msg_id", msgID, "err", err)
		}
		return
	}

	msg, err := h.msgRegistry.Unmarshal(msgID, plaintext)
	if err != nil {
		h.logger.Warn("unmarshal failed", "msg_id", msgID, "err", err)
		_ = c.Close(fmt.Errorf("unmarshal failed"))
		return
	}

	// connID 借 context 传给 handler：handler 签名由 codec 注册表固定，
	// 不方便加参数；用 WithValue 传少量请求级数据（key 用私有类型防碰撞）
	ctx := context.WithValue(context.Background(), ctxKeyConnID{}, c.ID())
	if err := h.registry.Handle(ctx, msgID, msg); err != nil {
		h.logger.Warn("handle failed", "msg_id", msgID, "err", err)
	}
}

// handleCryptoHandshake 处理 ClientKeyExchange / Finished。
// 两个消息都发生在 token 鉴权之前，是全流程唯一的明文协议阶段
// （15 章握手）：ClientKeyExchange 里客户端用网关 RSA 公钥加密
// pre-master，网关私钥解密后双方派生同一把 AES-GCM 会话密钥。
func (h *Handler) handleCryptoHandshake(c *conn.Conn, sess *session.Session, msgID uint16, body []byte) error {
	// 握手状态机随会话走：ServerHello 时由 server.onAccept 注入
	hs := sess.HandshakeServer()
	if hs == nil {
		return fmt.Errorf("handshake server not initialized")
	}

	switch msgID {
	case constant.MsgIDClientKeyExchange:
		// 解出 pre-master → 派生会话密钥 → 立即装入会话，
		// 此后该连接所有包都走 AES-GCM
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
		// 解密成功本身即证明双方密钥一致；明文负载固定为 "finished"
		_, _, plain, err := sess.Decrypt(body)
		if err != nil {
			return fmt.Errorf("decrypt finished: %w", err)
		}
		if string(plain) != "finished" {
			return fmt.Errorf("invalid finished marker")
		}
		// 回复服务端 Finished（双向认证：客户端也能验证服务端持有同一密钥）
		if err := sess.Send(constant.MsgIDFinished, []byte("finished")); err != nil {
			return err
		}
		h.logger.Debug("handshake finished", "conn_id", c.ID())
		return nil
	}
	return fmt.Errorf("unexpected handshake msg %d", msgID)
}

// ctxKeyConnID context key 类型。
// 知识点：context key 用自定义私有类型（而非 string）——避免不同包
// 用同一个字符串 key 相互覆盖，WithValue/Value 靠类型+值双重匹配。
type ctxKeyConnID struct{}

// SendFinished 向客户端发送服务端 Finished（可选，双向认证）。
// 当前握手流程里服务端 Finished 已在 handleCryptoHandshake 内回复，
// 本函数保留给外部流程（如重协商）主动触发。
func (h *Handler) SendFinished(c *conn.Conn, sess *session.Session) error {
	body := []byte("finished")
	return sess.Send(constant.MsgIDFinished, body)
}

// OnClose 实现 conn.Handler。
// 清理顺序：先通知 server（清 sessGame + 关 backend stream），
// 再从 Manager 摘表——反序会让下行帧在路由表已删、stream 未关的
// 窗口里找不到落点。
func (h *Handler) OnClose(c *conn.Conn, err error) {
	sess := h.sessMgr.GetByConn(c.ID())
	if sess == nil {
		return
	}
	h.logger.Info("conn closed", "conn_id", c.ID(), "sid", sess.ID(), "err", err)
	if h.onClosed != nil {
		h.onClosed(sess)
	}
	h.sessMgr.Remove(sess)
}

// isPreAuthMsg 鉴权前放行的系统消息白名单。
// 覆盖握手全链路：ServerHello → ClientKeyExchange → Finished →
// HandshakeReq/Resp（token 鉴权）；心跳随时可用来保活。
func isPreAuthMsg(msgID uint16) bool {
	return msgID == constant.MsgIDHeartbeatReq || msgID == constant.MsgIDServerHello ||
		msgID == constant.MsgIDClientKeyExchange || msgID == constant.MsgIDFinished ||
		msgID == constant.MsgIDHandshakeReq || msgID == constant.MsgIDHandshakeResp
}

// SendKick 向指定连接发送踢下线。
// 先发通知再断连（deliverToClient 同款顺序）：客户端需要收到 Reason
// 才能向玩家展示原因；banUntil 供客户端本地做禁登录倒计时提示。
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
// 双保险：connMgr 与 sessMgr 各查一次——连接在、会话已摘的窗口期
// （OnClose 回调跨了两个 Manager）要能识别出来而不是 panic。
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
	// 回包带服务端时间：客户端可校准本地时钟、计算 RTT
	resp := &gatev1.HeartbeatResp{ServerTime: timeNowMs()}
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return sess.Send(constant.MsgIDHeartbeatResp, body)
}

// handleHandshakeReq 握手请求。
// 这是 token 鉴权入口：校验通过即置会话为 AUTHED，并触发 onAuthed
// （server.bindToGame 绑定游戏服）。失败只回错误码不断连——客户端
// 可能换 token 重试，连接成本已付，没有安全理由断开。
// handleLogout 处理主动登出：作废 token → 回包 → 断连。
//
// 顺序不能变：先作废再回包再断连。
//   - 先作废：回包代表「已登出」，若回包后作废失败，客户端会以为 token 已失效；
//   - 回包后才断：conn.Send 是同步 raw.Write，返回即已交给内核；
//     先断会导致客户端收不到 LogoutResp，只能靠读 EOF 猜结果。
//
// 不作废成功也要断连：token 还在是安全隐患，但把连接留着更糟
// （用户会继续用一条「已登出」的连接发业务包）。回 code=1005 让客户端重试。
func (h *Handler) handleLogout(ctx context.Context, _ proto.Message) error {
	connID := ctx.Value(ctxKeyConnID{}).(int64)
	sess := h.sessMgr.GetByConn(connID)
	if sess == nil {
		return fmt.Errorf("session not found")
	}

	code := int32(0)
	if token := sess.Token(); token != "" && h.revoker != nil {
		if err := h.revoker(token); err != nil {
			h.logger.Warn("revoke token failed", "conn_id", connID, "err", err)
			code = 1005 // 服务器繁忙：token 未能作废，客户端应重试登出
		}
	}

	body, err := proto.Marshal(&gatev1.LogoutResp{Code: code})
	if err != nil {
		return err
	}
	if err := sess.Send(constant.MsgIDLogoutResp, body); err != nil {
		h.logger.Warn("send logout resp failed", "conn_id", connID, "err", err)
	}
	// 断连触发 OnClose → 路由表清理 → 游戏服 OnUnbind → Actor 下线落库
	_ = sess.Conn().Close(fmt.Errorf("client logout"))
	return nil
}

func (h *Handler) handleHandshakeReq(ctx context.Context, msg proto.Message) error {
	req := msg.(*gatev1.HandshakeReq)
	connID := ctx.Value(ctxKeyConnID{}).(int64)
	sess := h.sessMgr.GetByConn(connID)
	if sess == nil {
		return fmt.Errorf("session not found")
	}

	// 校验 token（token 绑定 account_id）
	// validator 为 nil 只发生在无 Redis 的单测环境：放行并置 0，
	// 生产环境 server.go 必注入 Redis 查询实现
	var accountID int64
	code := int32(0)
	if h.validator != nil {
		aid, err := h.validator(req.Token)
		if err != nil {
			h.logger.Warn("token invalid", "conn_id", connID, "err", err)
			// 1001 参数错误 / 1002 token 过期，见 4.10
			code = 1002
		} else {
			accountID = aid
		}
	}
	if code == 0 {
		sess.SetAuth(req.Token, accountID)
		if h.onAuthed != nil {
			h.onAuthed(sess)
		}
	}

	resp := &gatev1.HandshakeResp{
		Code:              code,
		AccountId:         accountID,
		PlayerId:          0, // 角色在进游戏/创角后确定
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

// timeNow 包级可替换时钟：单测注入固定时间，避免心跳断言依赖真实时钟。
// 知识点：函数变量（var f = func）是最轻量的「 seam 」，比接口注入
// 代码量小一个数量级，适合只替换一个时间源的场景。
var timeNow = func() time.Time { return time.Now() }

// SendServerHello 向新连接发送 ServerHello（未加密）。
// 外层包：MsgID=1001, Seq=0, Body=salt|pubKeyPEM
//
// **salt 必须在前**：salt 是 32 字节随机二进制，可能以 0x0a/0x0d 开头。
// 若把 salt 放在 PEM 之后，接收方用「跳过 PEM 尾随换行」来切分时会把
// salt 的首字节当成换行吃掉（概率 2/256），导致会话密钥不一致、
// Finished 帧 GCM 校验失败。实测登录成功率 ~98.7%，且与并发无关。
// 知识点：二进制不安全嵌入文本协议边界的典型案例——分隔符依赖
// 「某字节不会出现」的假设，随机数据让概率性故障变成现实。
func (h *Handler) SendServerHello(c *conn.Conn, pubKeyPEM, salt []byte) error {
	body := make([]byte, len(salt)+len(pubKeyPEM))
	copy(body, salt)
	copy(body[len(salt):], pubKeyPEM)
	return c.Send(codec.Packet{MsgID: constant.MsgIDServerHello, Seq: 0, Body: body})
}
