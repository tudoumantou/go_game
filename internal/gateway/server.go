// Package gateway 网关服（设计文档第 7 章）
//
// 本文件（server.go）是网关进程的组装层入口：把 conn/session/router/backend
// 四个子包装配成完整服务，并编排「加载 RSA 密钥 → 连 Redis/etcd → 注册 →
// 监听 TCP/HTTP → 优雅退出」的全生命周期。协议细节在各子包，这里只写清
// 组件间的依赖与启动/停机顺序。
package gateway

import (
	"context"
	"crypto/rsa"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"game_study/internal/common/constant"
	"game_study/internal/gateway/backend"
	"game_study/internal/gateway/conn"
	"game_study/internal/gateway/router"
	"game_study/internal/gateway/session"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/crypto"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	netpkg "game_study/pkg/net"
	gameetcd "game_study/pkg/registry/etcd"
	rpcpkg "game_study/pkg/rpc"
	resolver "game_study/pkg/rpc/resolver"

	internalv2 "game_study/pkg/pb/internalv2"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// Server 网关服（组装层入口，职责见设计文档 3.1 目录树）
//
// 字段分四组：基础组件（cfg/logger/connMgr/sessMgr/router，New 时即就绪）、
// 加解密（gatewayKey/gatewayPub，Start 时从 PEM 加载）、服务发现与后端
// （etcd*/backend/redis/metricsSrv）、会话→游戏服路由表（sessGame）。
// 不变量：sessGame 的 key 与 backend.Pool 的 session_id 必须一致；
// 会话消亡时两侧都要清理（SetOnClosed 回调与 forwardPacket 失败路径）。
type Server struct {
	// 基础组件：无外部依赖，New 即可用
	cfg     *config.Config
	logger  *log.Logger
	connMgr *conn.Manager
	sessMgr *session.Manager
	router  *router.Handler
	tcpSrv  *netpkg.TCPServer
	httpSrv *http.Server

	// 网关 RSA 密钥对：ServerHello 下发公钥，私钥解客户端的 pre-master（15 章握手）
	gatewayKey *rsa.PrivateKey
	gatewayPub *rsa.PublicKey

	// etcd 客户端/注册器 + 游戏服连接池；etcdCancel 是保活注册的注销函数
	etcdClient *clientv3.Client
	etcdReg    *gameetcd.Registry
	etcdCancel context.CancelFunc
	backend    *backend.Pool
	redis      *cache.Client
	metricsSrv *metrics.Server

	// sessMu 保护 sessGame（读多写少，用 RWMutex）
	sessMu   sync.RWMutex
	sessGame map[uint64]string // session_id -> game target
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server {
	logger := log.New("gateway", log.WithLevel(cfg.Log.Level))
	connMgr := conn.NewManager()
	sessMgr := session.NewManager(logger)

	return &Server{
		cfg:      cfg,
		logger:   logger,
		connMgr:  connMgr,
		sessMgr:  sessMgr,
		sessGame: make(map[uint64]string),
	}
}

// Start 启动服务（阻塞至 ctx 取消）。
func (s *Server) Start(ctx context.Context) error {
	// 指标（20.3）：注册服务名 + 起 /metrics 端点
	if s.cfg.Metrics.Enable {
		metrics.Init(s.cfg.Server.Name)
		mSrv, err := metrics.NewServer(s.cfg.Metrics.Addr, nil)
		if err != nil {
			return fmt.Errorf("metrics server: %w", err)
		}
		mSrv.Start()
		s.metricsSrv = mSrv
		s.logger.Info("metrics server started", "addr", mSrv.Addr())
	}
	// 加载网关 RSA 密钥对（优先用配置路径，缺省回退 keys/）
	// 密钥对是握手根基：公钥随 ServerHello 明文下发，私钥只在网关内存，
	// 解出 pre-master 后派生 AES-GCM 会话密钥（15 章）
	privPath := s.cfg.Crypto.RSAPrivateKey
	if privPath == "" {
		privPath = "keys/gateway_rsa.pem"
	}
	if _, statErr := os.Stat(privPath); statErr != nil {
		// 配置路径不可用（本地未部署到 /etc）时回退项目内 keys/
		privPath = "keys/gateway_rsa.pem"
	}
	pubPath := strings.TrimSuffix(privPath, ".pem") + "_pub.pem"
	pair, err := crypto.LoadRSAKeyPair(pubPath, privPath)
	if err != nil {
		return fmt.Errorf("load gateway rsa keys: %w", err)
	}
	s.gatewayKey = pair.Private
	s.gatewayPub = pair.Public

	// Redis（token 校验）
	if len(s.cfg.Redis.Addrs) > 0 {
		redisClient, err := cache.New(cache.RedisConfig{
			Addrs:    s.cfg.Redis.Addrs,
			Password: s.cfg.Redis.Password,
			DB:       s.cfg.Redis.DB,
			PoolSize: s.cfg.Redis.PoolSize,
			MinIdle:  s.cfg.Redis.MinIdleConn,
		})
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		s.redis = redisClient
	}

	// 路由（token 校验器 + 鉴权成功回调）
	// token 校验器：握手期 token → account_id 只查 Redis，2s 超时防止
	// 慢 Redis 拖死握手流程；无 Redis 环境（单测）放行
	validator := func(token string) (int64, error) {
		if s.redis == nil {
			return 0, nil // 无 Redis 环境（单测）放行
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return s.redis.GetClient().Get(ctx, "token:"+token).Int64()
	}
	s.router = router.NewHandler(s.sessMgr, s.connMgr, s.logger, validator, s.bindToGame)
	s.router.SetForward(s.forwardPacket)
	s.router.SetOnClosed(func(sess *session.Session) {
		s.sessMu.Lock()
		delete(s.sessGame, sess.NumID())
		s.sessMu.Unlock()
		if s.backend != nil {
			s.backend.CloseSession(sess.NumID())
		}
	})

	// etcd
	//
	// 端点来自 rpc.etcd_endpoints（网关配置里没有顶层 etcd 段）。
	// 之前误读 s.cfg.Etcd.Endpoints，导致整个分支被跳过 —— backend 为 nil，
	// 会话永远绑不到游戏服，客户端进游戏只能等超时。
	etcdEndpoints := s.cfg.RPC.EtcdEndpoints
	if len(etcdEndpoints) == 0 {
		etcdEndpoints = s.cfg.Etcd.Endpoints // 兼容顶层写法
	}
	if len(etcdEndpoints) > 0 {
		cli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: s.cfg.GetDialTimeout()})
		if err != nil {
			return fmt.Errorf("etcd: %w", err)
		}
		s.etcdClient = cli
		s.etcdReg, _ = gameetcd.New(etcdEndpoints)

		// 注册自己到 etcd
		node := gameetcd.Node{
			ID:      s.cfg.Server.Name,
			Service: "gateway",
			Addr:    s.cfg.Server.TCPAddr,
			Version: "1.0.0",
		}
		cancel, err := s.etcdReg.RegisterWithKeepalive(ctx, node, s.cfg.Etcd.LeaseTTL)
		if err != nil {
			return fmt.Errorf("register gateway: %w", err)
		}
		s.etcdCancel = cancel

		// backend pool with etcd resolver
		resolverBuilder := grpc.WithResolvers(resolver.NewBuilder(cli, s.cfg.RPC.ServicePrefix))
		clientPool := rpcpkg.NewClientPool(resolverBuilder)
		s.backend = backend.NewPool(clientPool, s.logger, s.deliverToClient)
	}

	// HTTP 健康检查 / 网关状态
	// 只承载 /healthz、/status 探针与运维查询，不接业务流量（业务 HTTP 在登录服）
	s.httpSrv = &http.Server{
		Addr:    s.cfg.Server.HTTPAddr,
		Handler: s.httpHandler(),
	}
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", "err", err)
		}
	}()

	// TCP 接入
	s.tcpSrv = netpkg.NewTCPServer(s.cfg.Server.TCPAddr, s.onAccept)
	if err := s.tcpSrv.Start(ctx); err != nil {
		return err
	}
	return nil
}

// bindToGame 鉴权成功后把会话绑定到游戏服。
//
// 选路策略：按 player_id 对存活 game 节点取模——同一玩家始终落到同一节点，
// 玩家数据（Actor）无需跨节点迁移；节点列表来自 etcd（故障节点已被摘除）。
// 绑定即建流：首帧 BIND 随 stream 一起发出（4.5），失败必须断客户端连接，
// 否则玩家会对着一个「永远进不去」的会话反复发包。
func (s *Server) bindToGame(sess *session.Session) {
	if s.backend == nil || s.etcdReg == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodes, err := s.etcdReg.List(ctx, "game")
	if err != nil || len(nodes) == 0 {
		s.logger.Warn("no game server available", "err", err)
		return
	}
	// 按 player_id 取模选 game server
	idx := int(sess.PlayerID() % int64(len(nodes)))
	node := nodes[idx]

	// target 用 etcd resolver scheme 表达，gRPC 目标解析交给 backend.ClientPool
	target := "etcd:///" + node.Service
	frame := &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_Ctrl{
			Ctrl: &internalv2.CtrlMsg{
				Type:      internalv2.CtrlMsg_BIND,
				SessionId: sess.NumID(),
				AccountId: sess.PlayerID(), // 鉴权后此处为 account_id
			},
		},
	}
	// 每会话独立 stream，首帧即 BIND（4.5）
	if _, err := s.backend.GetOrConnect(ctx, target, sess.NumID(), frame); err != nil {
		s.logger.Warn("bind to game failed", "game", node.ID, "err", err)
		s.sessMgr.Remove(sess)
		sess.Close(err)
		return
	}
	s.sessMu.Lock()
	s.sessGame[sess.NumID()] = target
	s.sessMu.Unlock()
	s.logger.Info("bound to game", "session_id", sess.NumID(), "game", node.ID)
}

// deliverToClient 下行：游戏服回包 → 加密写回客户端。
//
// 会话可能已随断连消亡，此时丢弃并告警即可（游戏服侧迟到的帧无接收方）。
func (s *Server) deliverToClient(sessionID uint64, msgID uint32, body []byte) {
	sess := s.sessMgr.GetByNumID(sessionID)
	if sess == nil {
		s.logger.Warn("downlink session not found", "session_id", sessionID, "msg_id", msgID)
		return
	}
	if err := sess.Send(uint16(msgID), body); err != nil {
		s.logger.Warn("downlink send failed", "session_id", sessionID, "msg_id", msgID, "err", err)
	}
	// 踢下线：发出通知后主动断开，不依赖客户端配合（R3.1）。
	// 先 Send 再 Close，保证客户端能收到原因；断连会触发 OnUnbind，
	// 游戏服按会话身份校验忽略迟到的解绑请求。
	if msgID == uint32(constant.MsgIDKickNotify) {
		s.logger.Info("session kicked by game server", "session_id", sessionID)
		s.sessMgr.Remove(sess)
		sess.Close(nil)
	}
}

// forwardPacket 业务消息透传：按会话绑定的 game target 走同一 stream（4.5）。
//
// 网关不解析业务协议，只按 session_id 找到 stream 原样转发；
// 帧内必须带 session_id，游戏服据此找回 Actor。
func (s *Server) forwardPacket(sess *session.Session, msgID uint32, body []byte) error {
	s.sessMu.RLock()
	target := s.sessGame[sess.NumID()]
	s.sessMu.RUnlock()
	if target == "" {
		return fmt.Errorf("session %d not bound to any game", sess.NumID())
	}
	frame := &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_ClientPacket{
			ClientPacket: &internalv2.ClientPacket{
				SessionId: sess.NumID(),
				MsgId:     msgID,
				Body:      body,
			},
		},
	}
	if err := s.backend.Send(sess.NumID(), frame); err != nil {
		// 后端实例下线时，流已断开：把「这次请求失败」升级为「关闭客户端连接」。
		// 客户端重连后由 resolver 选到存活实例——否则客户端会对着一条死连接反复超时。
		s.logger.Info("backend send failed, closing client connection",
			"session_id", sess.NumID(), "target", target, "err", err)
		s.backend.CloseSession(sess.NumID())
		s.sessMu.Lock()
		delete(s.sessGame, sess.NumID())
		s.sessMu.Unlock()
		sess.Close(err)
		return err
	}
	return nil
}

// onAccept 新连接接入：限流 → 分配 ID → 建会话 → 发 ServerHello → 进入读写循环。
//
// ServerHello 是全流程唯一一段明文协议（握手期，15 章）；此后所有包走
// AES-GCM。注意 Serve 用 context.Background()：单连接的生命周期独立于
// 进程启动 ctx，停机时由 sessMgr.CloseAll 统一断连。
func (s *Server) onAccept(raw net.Conn) {
	// 连接数上限保护：达到即拒并计数，避免 fd 耗尽拖垮整机
	if s.connMgr.Count() >= s.cfg.Server.MaxConns {
		s.logger.Warn("max connections reached", "max", s.cfg.Server.MaxConns)
		metrics.ConnRejected("max_conns")
		_ = raw.Close()
		return
	}

	// connID 同时充当会话数值 ID（NewSession 内绑定），
	// 下行路由与 gRPC BIND 帧都用它标识会话
	connID := s.connMgr.NextID()
	c := conn.New(connID, raw, s.router, s.logger)
	s.connMgr.Add(c)
	metrics.ConnInc()

	sess := s.sessMgr.NewSession(c)

	// 发送 ServerHello（明文）
	// 握手状态机随会话走：RSA 公钥 + 32B 随机盐，见 crypto.HandshakeServer
	hs := crypto.NewHandshakeServer(s.gatewayPub, s.gatewayKey)
	sess.SetHandshakeServer(hs)
	pub, salt := hs.ServerHello()
	if err := s.router.SendServerHello(c, pub, salt); err != nil {
		s.logger.Warn("send server hello failed", "conn_id", connID, "err", err)
		metrics.ConnRejected("hello_failed")
		_ = c.Close(err)
		return
	}
	sess.SetState(session.StateHelloSent)

	// Serve 阻塞到连接关闭；TCPServer 每连接起一个 goroutine，
	// 返回后经 conn.Close → router.OnClose 完成会话清理
	c.Serve(context.Background())
	metrics.ConnDec()
}

// httpHandler 健康检查/状态查询路由（探针由 K8s 高频调用，实现保持零依赖）。
func (s *Server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "conns=%d sessions=%d", s.connMgr.Count(), s.sessMgr.Count())
	})
	return mux
}

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，pkg/shutdown 编排）。
func (s *Server) Shutdown(ctx context.Context) error {
	// DRAIN：摘注册
	if s.etcdCancel != nil {
		s.etcdCancel()
	}
	// DRAINING：关所有会话连接，让 conn handler 退出
	s.sessMgr.CloseAll()
	if s.tcpSrv != nil {
		if err := s.tcpSrv.Stop(); err != nil {
			s.logger.Warn("tcp stop", "err", err)
		}
	}
	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(ctx); err != nil {
			s.logger.Warn("http shutdown", "err", err)
		}
	}
	if s.etcdCancel != nil {
		s.etcdCancel()
	}
	if s.backend != nil {
		_ = s.backend.Close()
	}
	if s.etcdReg != nil {
		_ = s.etcdReg.Close()
	}
	if s.etcdClient != nil {
		_ = s.etcdClient.Close()
	}
	if s.redis != nil {
		_ = s.redis.Close()
	}
	if s.metricsSrv != nil {
		_ = s.metricsSrv.Stop(ctx)
	}
	s.logger.Info("gateway stopped")
	return nil
}
