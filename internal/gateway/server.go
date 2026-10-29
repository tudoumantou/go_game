// Package gateway 网关服（设计文档第 7 章）
//
// 本文件（server.go）是网关进程的组装层入口：把 conn/session/router/backend
// 四个子包装配成完整服务，并编排「加载 RSA 密钥 → 连 Redis/etcd → 注册 →
// 监听 TCP/HTTP → 优雅退出」的全生命周期。协议细节在各子包，这里只写清
// 组件间的依赖与启动/停机顺序。
// 架构职责：网关是分布式五进程中的「接入层」——对客户端维持海量长连接
// （TCP + protobuf + RSA/AES 握手），对内部通过 etcd 发现 game 节点并
// 按会话建立 gRPC 流；自身不落地任何业务数据，是天然的无状态层
// （唯一状态是内存会话表），因此可以水平扩容、宕机无恢复成本。
// 协作关系：login 服签发 token（Redis）→ 客户端持 token 来网关握手 →
// 网关校验 token 后按 player_id 取模选 game 节点绑定 → 此后业务包
// 在客户端与 game 之间经网关透传。
// 设计取舍：① 无状态：会话只在内存，重启即全部断连重连，换来实现简单
// 与弹性；② 顶号/断连由会话状态机 + 事件驱动，不做持久化重连恢复；
// ③ 健康检查与限流前置在 onAccept，保证异常连接不消耗后续资源。
package gateway

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
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
	"game_study/pkg/consistent"
	"game_study/pkg/crypto"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	netpkg "game_study/pkg/net"
	"game_study/pkg/ratelimit"
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
// 知识点：Server 本身就是「依赖注入容器」——New 只装配无外部依赖的
// 组件，Start 才注入有 IO 依赖的（Redis/etcd），这样单测可以只跑到
// New 层；字段分组注释即架构图。
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
	// 私钥只驻内存、永不下发；公钥随 ServerHello 明文广播
	gatewayKey *rsa.PrivateKey
	gatewayPub *rsa.PublicKey

	// etcd 客户端/注册器 + 游戏服连接池；etcdCancel 是保活注册的注销函数
	// etcdReg 同时是 game 节点列表的来源（bindToGame 的 List）
	etcdClient *clientv3.Client
	etcdReg    *gameetcd.Registry
	etcdCancel context.CancelFunc
	backend    *backend.Pool
	redis      *cache.Client
	metricsSrv *metrics.Server

	// sessMu 保护 sessGame（读多写少，用 RWMutex）
	// key = session 数值 ID，value = game 节点的 gRPC target（etcd:// scheme）
	sessMu   sync.RWMutex
	sessGame map[uint64]string // session_id -> game target
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
// 只创建纯内存组件——不碰网络/磁盘，失败不可能发生，返回的 Server
// 立即可用于单测的 Handler 装配。
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
// 启动顺序即依赖顺序：指标 → RSA 密钥 → Redis → router（依赖 redis/
// sessMgr）→ etcd + backend → HTTP → TCP。TCP 放最后：前置依赖全部
// 就绪后才接受客户端连接，失败早暴露而不是接进来再断。
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
		// 双路径探测：生产用绝对路径，开发用仓库内文件，互不影响
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
	// 未配置 Addrs 时 s.redis 留 nil：validator 走放行分支（单测环境），
	// 生产配置必填——「无 Redis = 不校验 token」只在测试可接受
	if len(s.cfg.Redis.Addrs) > 0 {
		redisClient, err := cache.New(cache.RedisConfig{
			Addrs:    s.cfg.Redis.Addrs,
			Password: s.cfg.Redis.Password,
			DB:       s.cfg.Redis.DB,
			PoolSize: s.cfg.Redis.PoolSize,
			MinIdle:  s.cfg.Redis.MinIdleConn,
			Breaker: cache.BreakerOptions{
				Threshold: s.cfg.Redis.CircuitBreaker.Threshold,
				Cooldown:  s.cfg.Redis.CircuitBreaker.CooldownDuration(),
				Probes:    s.cfg.Redis.CircuitBreaker.Probes,
			},
		})
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		s.redis = redisClient
	}

	// 路由（token 校验器 + 鉴权成功回调）
	// token 校验器：握手期 token → account_id 只查 Redis，2s 超时防止
	// 慢 Redis 拖死握手流程；无 Redis 环境（单测）放行
	//
	// key 由 cache.TokenKey 生成，与登录服 TokenRepo 共用一处定义
	// （pkg/cache/keygen.go），不再是「靠两处注释维持的契约」。
	validator := func(token string) (int64, error) {
		if s.redis == nil {
			return 0, nil // 无 Redis 环境（单测）放行
		}
		// 知识点：context.WithTimeout + defer cancel——超时是「查询级」
		// 的，cancel 必须 defer，否则 timer 要等超时才释放（context 泄漏）
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return s.redis.GetClient().Get(ctx, cache.TokenKey(token)).Int64()
	}
	s.router = router.NewHandler(s.sessMgr, s.connMgr, s.logger, validator, s.bindToGame)

	// 连接级限流：默认 20 包/秒（突发 40），配置里给负数可关闭。
	// 放在网关上而不是业务模块里：越早拦越省 CPU，且能挡住心跳之外的
	// 任何协议刷包（模块级限流只覆盖各自的消息）。
	if rate, burst := s.cfg.Limit.PacketLimit(); rate > 0 {
		s.router.SetPacketLimiter(ratelimit.NewKeyed(rate, burst, 5*time.Minute))
		s.logger.Info("packet rate limit enabled", "rate", rate, "burst", burst)
	} else {
		s.logger.Warn("packet rate limit disabled by config")
	}

	// 登出：作废 token（删 key）。失败时上层回 1005，客户端重试——
	// 「用户以为登出了、token 其实还有效」是安全问题，不能静默。
	s.router.SetRevoker(func(token string) error {
		if s.redis == nil {
			return nil // 无 Redis 环境（单测）：没有可作废的存储
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return s.redis.GetClient().Del(ctx, cache.TokenKey(token)).Err()
	})
	s.router.SetForward(s.forwardPacket)
	// 会话关闭回调：两侧路由表（sessGame / backend.Pool）同步清理，
	// 是「会话消亡无残留」不变量唯一的集中收口
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
	// 教训：配置读取路径错误不会编译失败，只会让分支静默跳过——
	// 对「分支跳过 = 功能全废」的配置项，日志必须显式告知。
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
		// 保活注册（Keepalive）：租约到期未续约 etcd 自动摘除本节点，
		// 客户端（login 的分派） therefore 只会看到活着的网关
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
		// resolver 让 gRPC 目标 "etcd:///game" 动态解析为实例列表，
		// 节点上下线由 resolver 推送更新，网关无需重启
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
		// ErrServerClosed 是 Shutdown 正常返回的错误，不算故障
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", "err", err)
		}
	}()

	// TCP 接入
	// TCPServer 内部 accept goroutine + 每连接一个 Serve goroutine
	s.tcpSrv = netpkg.NewTCPServer(s.cfg.Server.TCPAddr, s.onAccept)
	if err := s.tcpSrv.Start(ctx); err != nil {
		return err
	}
	return nil
}

// bindToGame 鉴权成功后把会话绑定到游戏服。
//
// 选路策略：account_id 走一致性哈希环——同一账号稳定落到同一节点，且节点
// 增删只迁移该节点负责的那一段（取模法会让几乎所有账号跳变，表现为大规模
// 顶号重连）；节点列表来自 etcd（故障节点已被摘除）。
// 绑定即建流：首帧 BIND 随 stream 一起发出（4.5），失败必须断客户端连接，
// 否则玩家会对着一个「永远进不去」的会话反复发包。
// 注意：本函数由 router.handleHandshakeReq 在读循环线程同步调用，
// 3s 超时意味着握手响应最坏晚 3s——超时值需平衡 etcd 抖动与体验。
func (s *Server) bindToGame(sess *session.Session) {
	// backend/etcd 未就绪（单测环境）时静默跳过：会话保持 AUTHED，
	// 业务透传会因「not bound」失败，由上层感知
	if s.backend == nil || s.etcdReg == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodes, err := s.etcdReg.List(ctx, "game")
	if err != nil || len(nodes) == 0 {
		// 无可用 game 节点：不断连（可能只是瞬态），玩家重发业务包
		// 时会再次走到这里由 forwardPacket 报 not bound
		s.logger.Warn("no game server available", "err", err)
		return
	}
	// 选路要跟「账号」绑，不能跟角色绑——同一账号换角色重连时应当落回同一
	// 节点（Actor 缓存、presence 都在那边）。
	//
	// 环只依赖「节点 ID 集合」：节点先排序再去重，所以多个网关实例、不同
	// etcd 返回顺序下算出的归属完全一致（不一致会导致同一玩家被两个节点
	// 同时持有，靠 claimOnline 顶号兜底，但那是补救不是设计）。
	ids := make([]string, 0, len(nodes))
	byID := make(map[string]gameetcd.Node, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
		byID[n.ID] = n
	}
	picked, ok := consistent.New(ids).Get(strconv.FormatInt(sess.AccountID(), 10))
	if !ok {
		s.logger.Warn("no game node in ring", "nodes", len(nodes))
		return
	}
	node := byID[picked]

	// target 用 etcd resolver scheme 表达，gRPC 目标解析交给 backend.ClientPool
	target := "etcd:///" + node.Service
	// BIND 帧：游戏服凭 session_id + account_id 建 Actor 关联，
	// 此后同会话所有帧都能找回角色上下文
	frame := &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_Ctrl{
			Ctrl: &internalv2.CtrlMsg{
				Type:      internalv2.CtrlMsg_BIND,
				SessionId: sess.NumID(),
				AccountId: sess.AccountID(),
			},
		},
	}
	// 每会话独立 stream，首帧即 BIND（4.5）
	// 建流失败 = 玩家进不了游戏：必须断连让客户端重试/换节点，
	// 留着一个绑不上的会话只会让客户端反复超时
	if _, err := s.backend.GetOrConnect(ctx, target, sess.NumID(), frame); err != nil {
		s.logger.Warn("bind to game failed", "game", node.ID, "err", err)
		s.sessMgr.Remove(sess)
		sess.Close(err)
		return
	}
	// 绑定成功后登记路由表：forwardPacket 凭此找到 game target
	s.sessMu.Lock()
	s.sessGame[sess.NumID()] = target
	s.sessMu.Unlock()
	s.logger.Info("bound to game", "session_id", sess.NumID(), "game", node.ID)
}

// deliverToClient 下行：游戏服回包 → 加密写回客户端。
//
// 会话可能已随断连消亡，此时丢弃并告警即可（游戏服侧迟到的帧无接收方）。
// 本函数是 backend.Pool 的 onFrame 回调，在 recvLoop goroutine 上执行，
// 因此 Send 的写并发方是「每会话一个 recvLoop」，天然按会话隔离。
func (s *Server) deliverToClient(sessionID uint64, msgID uint32, body []byte) {
	sess := s.sessMgr.GetByNumID(sessionID)
	if sess == nil {
		s.logger.Warn("downlink session not found", "session_id", sessionID, "msg_id", msgID)
		return
	}
	if err := sess.Send(msgID, body); err != nil {
		s.logger.Warn("downlink send failed", "session_id", sessionID, "msg_id", msgID, "err", err)
	}
	// 踢下线：发出通知后主动断开，不依赖客户端配合（R3.1）。
	// 先 Send 再 Close，保证客户端能收到原因；断连会触发 OnUnbind，
	// 游戏服按会话身份校验忽略迟到的解绑请求。
	if msgID == constant.MsgIDKickNotify {
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
	// 读锁快路径：转发是纯查表 + 发送，不阻塞其他会话的转发
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
		// 设计取舍：宁可错杀（后端瞬断时误断客户端）不留死连接——
		// 客户端重连成本低，对着死连接超时的体验更差。
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
// 知识点：本函数在 TCPServer 的 accept goroutine 中串行执行，之后
// c.Serve 阻塞住当前 goroutine——即 goroutine per conn，连接数上限
// （MaxConns）同时是 goroutine 数上限。
func (s *Server) onAccept(raw net.Conn) {
	// 连接数上限保护：达到即拒并计数，避免 fd 耗尽拖垮整机
	// 先限流再分配任何资源：被拒连接不产生 Conn/Session/goroutine
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
	// salt 参与密钥派生，防预计算/重放；每次连接新的 HandshakeServer
	hs := crypto.NewHandshakeServer(s.gatewayPub, s.gatewayKey)
	sess.SetHandshakeServer(hs)
	pub, salt := hs.ServerHello()
	if err := s.router.SendServerHello(c, pub, salt); err != nil {
		// hello 都发不出去（对端已消失/写超时）：连接无意义，计数后关闭
		s.logger.Warn("send server hello failed", "conn_id", connID, "err", err)
		metrics.ConnRejected("hello_failed")
		_ = c.Close(err)
		return
	}
	sess.SetState(session.StateHelloSent)

	// Serve 阻塞到连接关闭；TCPServer 每连接起一个 goroutine，
	// 返回后经 conn.Close → router.OnClose 完成会话清理
	// 知识点：阻塞模型下无需消息队列——每个连接的读、写、关闭全部
	// 串行化在自己的 goroutine 里，状态天然线程封闭（thread confinement）。
	c.Serve(context.Background())
	metrics.ConnDec()
}

// httpHandler 健康检查/状态查询路由（探针由 K8s 高频调用，实现保持零依赖）。
// 不查 Redis/etcd/后端——探针只回答「进程活着」，深活由 login 的
// readiness（Ping Redis/MySQL）承担；/status 输出连接与会话数供扩容观测。
func (s *Server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, "conns=%d sessions=%d", s.connMgr.Count(), s.sessMgr.Count())
	})
	mux.HandleFunc("/admin/kick", s.handleAdminKick)
	return mux
}

// KickPlayer 让游戏服踢掉某个角色（GM 踢人 / 封禁生效）。
//
// 参数是角色 ID，不是会话号：会话↔角色的映射是游戏服的事
// （players2sess 是唯一权威），网关不该再存一份。
//
// 下发方式：按 etcd 里的 game 节点列表，对每个节点各发一帧管理指令。
// 用的是「管理流」（不属于任何玩家会话），因此不依赖本网关是否有业务流量——
// 刚启动、空载的网关也能踢人。非持有该玩家的节点收到后查不到会话，静默忽略。
func (s *Server) KickPlayer(playerID int64, reason string) error {
	if s.backend == nil {
		return fmt.Errorf("gateway: backend pool not ready")
	}
	if s.etcdReg == nil {
		return fmt.Errorf("gateway: game registry not ready")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	nodes, err := s.etcdReg.List(ctx, "game")
	if err != nil {
		return fmt.Errorf("list game nodes: %w", err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("no game node registered")
	}

	frame := kickFrame(playerID, reason)
	sent := 0
	var errs []error
	for _, n := range nodes {
		if n.Addr == "" {
			continue
		}
		if err := s.backend.AdminSend(ctx, n.Addr, frame); err != nil {
			errs = append(errs, fmt.Errorf("node %s: %w", n.ID, err))
			continue
		}
		sent++
	}

	if sent == 0 {
		return fmt.Errorf("kick broadcast failed: %w", errors.Join(errs...))
	}
	if len(errs) > 0 {
		// 部分节点失败不算致命：踢人指令是幂等的，持有该玩家的节点若在
		// 失败集合里，运营重试一次即可；这里只记日志，不回滚已成功的部分。
		s.logger.Warn("kick partially delivered",
			"player_id", playerID, "sent", sent, "failed", len(errs), "err", errors.Join(errs...))
	}
	s.logger.Info("kick delivered", "player_id", playerID, "reason", reason, "nodes", sent)
	return nil
}

// kickFrame 构造踢人控制帧。
//
// 抽成纯函数是为了能测：帧内容错了（类型写成 UNBIND、player_id 漏填）
// 的表现是「游戏服收到后什么都不做」，线上只能从「踢了没反应」发现。
// SessionId 刻意留空——这条指令不针对某个会话，由游戏服按角色号自己定位。
func kickFrame(playerID int64, reason string) *internalv2.StreamFrame {
	return &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_Ctrl{Ctrl: &internalv2.CtrlMsg{
			Type:     internalv2.CtrlMsg_KICK,
			PlayerId: playerID,
			Payload:  []byte(reason),
		}},
	}
}

// handleAdminKick POST /admin/kick {"session_id":123,"reason":"banned"}
//
// 鉴权：配置 admin_token 时校验 Authorization: Bearer <token>；
// 未配置时只接受本机来源（127.0.0.1/::1）。这样默认部署不必新增配置
// 也能给运维用 curl 执行，但外部网络打不进来。
func (s *Server) handleAdminKick(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.adminAllowed(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	var req struct {
		PlayerID int64  `json:"player_id"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.PlayerID <= 0 {
		http.Error(w, "player_id required", http.StatusBadRequest)
		return
	}

	// 广播成功即返回 200：本实例没有该玩家的会话是正常情况
	// （他可能连在别的网关），踢人指令已经交给游戏服判断。
	if err := s.KickPlayer(req.PlayerID, req.Reason); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// adminAllowed 管理接口鉴权（token 优先，否则只允许本机）。
func (s *Server) adminAllowed(r *http.Request) bool {
	if s.cfg.Admin.Token != "" {
		return r.Header.Get("Authorization") == "Bearer "+s.cfg.Admin.Token
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return host == "127.0.0.1" || host == "::1"
}

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，pkg/shutdown 编排）。
// 顺序即安全顺序：先摘 etcd 注册（不再接新流量）→ 断所有客户端连接
// （在途请求随断连失败，客户端重连到新节点）→ 停 TCP/HTTP → 关后端流
// → 关 etcd/Redis。反序会让新请求继续打到正在关闭的组件上。
func (s *Server) Shutdown(ctx context.Context) error {
	// DRAIN：摘注册
	// 先摘除再断连：否则 etcd 仍宣称本节点存活，新玩家连进来即被断
	if s.etcdCancel != nil {
		s.etcdCancel()
	}
	// DRAINING：关所有会话连接，让 conn handler 退出
	// CloseAll 内部先摘表再逐个 Close，避免持锁回调造成死锁（见 session.Manager）
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
	// 后端流全部取消：recvLoop 随 Recv 报错退出，goroutine 收敛
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
