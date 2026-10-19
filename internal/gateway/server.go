// Package gateway 网关服（设计文档第 7 章）
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
type Server struct {
	cfg     *config.Config
	logger  *log.Logger
	connMgr *conn.Manager
	sessMgr *session.Manager
	router  *router.Handler
	tcpSrv  *netpkg.TCPServer
	httpSrv *http.Server

	gatewayKey *rsa.PrivateKey
	gatewayPub *rsa.PublicKey

	etcdClient *clientv3.Client
	etcdReg    *gameetcd.Registry
	etcdCancel context.CancelFunc
	backend    *backend.Pool
	redis      *cache.Client
	metricsSrv *metrics.Server

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
	return s.backend.Send(sess.NumID(), frame)
}

func (s *Server) onAccept(raw net.Conn) {
	if s.connMgr.Count() >= s.cfg.Server.MaxConns {
		s.logger.Warn("max connections reached", "max", s.cfg.Server.MaxConns)
		metrics.ConnRejected("max_conns")
		_ = raw.Close()
		return
	}

	connID := s.connMgr.NextID()
	c := conn.New(connID, raw, s.router, s.logger)
	s.connMgr.Add(c)
	metrics.ConnInc()

	sess := s.sessMgr.NewSession(c)

	// 发送 ServerHello（明文）
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

	c.Serve(context.Background())
	metrics.ConnDec()
}

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
