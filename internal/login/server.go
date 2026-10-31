// Package login 登录服（设计文档第 6 章）
//
// 架构职责：登录服是玩家进入游戏的第一站——签发/校验 token、分派网关、
// 承接充值下单与渠道支付回调，并把登录/充值事件投递 MQ 供游戏服消费。
// 它是五进程中唯一对外暴露 HTTP 的进程（客户端直连），对内通过 Kafka
// 与游戏服协作。
// 协作关系：auth/guest 签发的 token 写入 Redis，网关握手时校验同一
// key；/v1/server/assign 把客户端引导到网关；支付回调推进订单后经
// gs.recharge.order 通知游戏服发货，游戏服发货成功后经
// gs.recharge.delivered 回确认，本服消费确认推进订单到「已发货」。
// 设计取舍：① 充值链路 Tier-1 严格一致——订单状态机 + CAS + 每小时
// 对账补偿，宁可重试不可丢单；② 各中间件按配置分级启用（Redis 必需、
// MySQL/Kafka 可选），无 MySQL 时充值域整体关闭但登录/分派照常，
// 单测与最小部署都能跑；③ 就绪探针 Ping Redis/MySQL——依赖不可用时
// 主动摘流量，比让请求打进来再失败更优雅。
package login

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"game_study/internal/login/handler"
	"game_study/internal/login/mq"
	"game_study/internal/login/repo"
	"game_study/internal/login/service"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	pkgmq "game_study/pkg/mq"
	"game_study/pkg/periodic"
	"game_study/pkg/ratelimit"
	"game_study/pkg/shutdown"
)

// Server 登录服（组装层入口，职责见设计文档 3.1 目录树）
// 字段按依赖顺序分组：基础设施（redis/database）、消息（producer/
// delivered）、HTTP 门面（三个 handler）、服务端（httpSrv/metricsSrv）。
// 不变量：rechargeSvc 仅在 MySQL 配置就绪时创建——充值域的三件套
// （订单表/Kafka/角色查询）都依赖它，nil 即「充值域关闭」。
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	redis      *cache.Client
	database   *db.DB
	httpSrv    *http.Server
	metricsSrv *metrics.Server

	producer  *pkgmq.Producer
	delivered *mq.DeliveredConsumer
	roleIndex *mq.RoleCreatedConsumer

	// userRoleRepo 角色索引（选角列表数据源）：HTTP 读、消费者写，同一个实例。
	userRoleRepo *repo.UserRoleRepo

	authHandler       *handler.AuthHandler
	serverListHandler *handler.ServerListHandler
	rechargeHandler   *handler.RechargeHandler
	roleListHandler   *handler.RoleListHandler
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
// 只存配置与 logger——所有 IO 依赖推迟到 Start，与 gateway.New 同款
// 模式：构造零失败，测试可随意 New 后只跑需要的部分。
func New(cfg *config.Config) *Server {
	logger := log.New("login", log.WithLevel(cfg.Log.Level))
	return &Server{cfg: cfg, logger: logger}
}

// Start 启动服务（阻塞至 ctx 取消）。
// 启动顺序即依赖顺序：① Redis（token 根基，必需）→ ② MySQL（充值域，
// 可选）→ ③ Kafka（订单消息，依赖 MySQL 分支内）→ ④ service/handler
// 装配 → 路由注册 → ⑤ 后台任务（消费者 + 对账）→ HTTP 监听 → 阻塞
// 等待 ctx。任何必需依赖失败立即返回，绝不在半初始化状态接流量。
func (s *Server) Start(ctx context.Context) error {
	// 指标（20.3）：注册服务名 + 起 /metrics、/healthz、/readyz
	if s.cfg.Metrics.Enable {
		metrics.Init(s.cfg.Server.Name)
		mSrv, err := metrics.NewServer(s.cfg.Metrics.Addr, s.readiness)
		if err != nil {
			return fmt.Errorf("metrics server: %w", err)
		}
		mSrv.Start()
		s.metricsSrv = mSrv
		s.logger.Info("metrics server started", "addr", mSrv.Addr())
	}

	// ① Redis
	// 唯一必需中间件：token 存取全在这里，连不上 = 登录服无意义，
	// 直接启动失败（fail-fast）
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
		return fmt.Errorf("connect redis: %w", err)
	}
	s.redis = redisClient

	// ② MySQL（充值订单，Tier-1）
	// DSN 为空即「充值域关闭」：登录/分派照常，/v1/order/create 与
	// /v1/pay/callback 不注册路由（见下文 mux）——最小部署/单测路径
	var rechargeSvc *service.RechargeService
	if s.cfg.MySQL.DSN != "" {
		database, err := db.New(db.Config{
			DSN:     s.cfg.MySQL.DSN,
			MaxOpen: s.cfg.MySQL.MaxOpenConns,
			MaxIdle: s.cfg.MySQL.MaxIdleConns,
		})
		if err != nil {
			return fmt.Errorf("mysql: %w", err)
		}
		s.database = database
		// 角色索引仓储：选角列表的读入口，也是 gs.role.created 的写入口。
		// 放在 MySQL 分支里而不是 Kafka 分支里——没有 Kafka 时索引照样可读
		// （历史数据/手工补写），只是不再自动更新。
		s.userRoleRepo = repo.NewUserRoleRepo(database, redisClient.GetClient())

		// ③ Kafka（生产者 + 发货确认消费者）
		// 生产者也未配时 orderProducer 留 nil——NewRechargeService
		// 内部降级为 NopOrderProducer：订单照常落库，靠每小时对账
		// 补偿重投，支付链路不在 MQ 缺失时断掉
		var orderProducer service.OrderProducer
		if len(s.cfg.Kafka.Brokers) > 0 {
			s.producer = pkgmq.NewProducer(pkgmq.Config{
				Brokers:      s.cfg.Kafka.Brokers,
				RequiredAcks: s.cfg.Kafka.ProducerRequiredAcks,
			})
			orderProducer = mq.NewRechargeProducer(s.producer)
			s.logger.Info("kafka producer ready", "brokers", s.cfg.Kafka.Brokers)
		}

		rechargeSvc = service.NewRechargeService(repo.NewOrderRepo(database), orderProducer, s.logger)

		// 发货确认消费者（登录服单实例组：订单表只在本服库）
		// 消费组单实例：订单状态推进不能并发，否则 CAS 相互覆盖；
		// 高可用靠「每小时对账」而非「消费组多实例」
		if s.producer != nil {
			consumer := pkgmq.NewConsumer(pkgmq.Config{
				Brokers: s.cfg.Kafka.Brokers,
			}, pkgmq.TopicRechargeDelivered, kafkaGroup(s.cfg), s.producer)
			s.delivered = mq.NewDeliveredConsumer(consumer, rechargeSvc, s.logger)

			// 角色索引消费者（gs.role.created）：游戏服创角 → user_role 索引表。
			// 与发货确认共用 Kafka 连接但独立 topic/消费组（两者无顺序关系，
			// 合在一起只会让「创角洪峰」拖慢「发货确认」）。
			idxConsumer := pkgmq.NewConsumer(pkgmq.Config{
				Brokers: s.cfg.Kafka.Brokers,
			}, pkgmq.TopicRoleCreated, kafkaGroup(s.cfg), s.producer)
			s.roleIndex = mq.NewRoleCreatedConsumer(idxConsumer, s.userRoleRepo, s.logger)
		}
	}

	// ④ 服务与 handler
	tokenRepo := repo.NewTokenRepo(redisClient.GetClient())
	// accountRepo 依赖 MySQL：无库时降级为 nil，AuthService 内部
	// 走固定账号分支（单测环境）
	var accountRepo *repo.AccountRepo
	if s.database != nil {
		accountRepo = repo.NewAccountRepo(s.database)
	}
	authSvc := service.NewAuthService(tokenRepo, accountRepo, parseTTL(s.cfg.Token.TTL))
	dispatchSvc := service.NewDispatchService(s.cfg)

	// loginAudit 以值类型实现 LoginLogger：producer 为 nil 时静默，
	// 审计链路可选（R3.2）
	s.authHandler = handler.NewAuthHandler(authSvc, loginAudit{producer: s.producer, logger: s.logger})
	// 登录频控：默认 20 次/分钟/IP；配置负数则关闭（排查问题时的临时开关）
	if perMin := s.cfg.Limit.LoginLimit(); perMin > 0 {
		s.authHandler.SetLoginLimiter(ratelimit.NewKeyed(perMin/60, 10, 10*time.Minute))
		s.logger.Info("login rate limit enabled", "per_min", perMin)
	} else {
		s.authHandler.SetLoginLimiter(nil)
		s.logger.Warn("login rate limit disabled by config")
	}
	s.serverListHandler = handler.NewServerListHandler(dispatchSvc)
	if rechargeSvc != nil {
		// 支付渠道：未配置密钥直接启动失败，不允许「无验签回调」上线（R3.3）。
		// 安全底线：宁可起不来，不能放行未验签的支付回调——那等于
		// 谁都可以伪造「已支付」通知给玩家发货
		payWindow, _ := config.ParseDuration(s.cfg.Pay.SignWindow)
		payChannel, err := service.NewPayChannel(s.cfg.Pay.Channel, s.cfg.Pay.SandboxSecret, payWindow)
		if err != nil {
			return fmt.Errorf("pay channel: %w", err)
		}
		s.rechargeHandler = handler.NewRechargeHandler(rechargeSvc, authSvc, repo.NewRoleRepo(s.database), payChannel, s.logger)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/guest", s.authHandler.GuestLogin)
	mux.HandleFunc("/v1/server/assign", s.serverListHandler.Assign)
	mux.HandleFunc("/v1/server/gateway/list", s.serverListHandler.GatewayList)
	// 选角列表：角色索引的出口。userRoleRepo 为 nil（无 MySQL）时不注册，
	// 客户端拿到 404 比拿到空的角色列表更接近真相。
	if s.userRoleRepo != nil {
		s.roleListHandler = handler.NewRoleListHandler(authSvc, s.userRoleRepo, s.logger)
		mux.HandleFunc("/v1/role/list", s.roleListHandler.List)
	}
	// 充值路由按域注册：rechargeHandler 为 nil（无 MySQL）时这两个
	// 端点 404——比注册一个必然 500 的 handler 更诚实
	if s.rechargeHandler != nil {
		mux.HandleFunc("/v1/order/create", s.rechargeHandler.CreateOrder)
		mux.HandleFunc("/v1/pay/callback", s.rechargeHandler.PayCallback)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// ⑤ 后台任务：发货确认消费 + 每小时对账
	// 两者都随 ctx 退出：消费者靠 ctx 取消，对账循环靠 select
	// ctx.Done——goroutine 泄漏防护点明确
	if s.delivered != nil {
		go func() {
			if err := s.delivered.Run(ctx); err != nil {
				s.logger.Error("delivered consumer stopped", "err", err)
			}
		}()
	}
	if s.roleIndex != nil {
		go func() {
			if err := s.roleIndex.Run(ctx); err != nil {
				s.logger.Error("role index consumer stopped", "err", err)
			}
		}()
	}
	if rechargeSvc != nil {
		go s.reconcileLoop(ctx, rechargeSvc)
	}

	s.httpSrv = &http.Server{
		Addr:              s.cfg.Server.HTTPAddr,
		Handler:           withMetrics(mux, s.logger),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		// ErrServerClosed 是 Shutdown 的正常返回，不当故障记录
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", "err", err)
		}
	}()

	s.logger.Info("LoginServer started", "addr", s.cfg.Server.HTTPAddr)
	// 阻塞至停机信号：与 gateway.Start 同款契约（进程生命周期由
	// pkg/bootstrap 的关闭编排控制）
	<-ctx.Done()
	// 返回 nil 而不是 ctx.Err()：契约是「只有启动失败才返回 error」。
	// 返回 ctx.Err() 会让调用方当成启动失败而直接退出，Shutdown 永不执行
	// （支付回调链路不会按序关闭）。
	return nil
}

// reconcileLoop 每小时对账：已支付未发货订单重投（13.3 模式③）。
// 为什么需要对账：PayCallback 里 Kafka 发送失败只记日志（不阻塞回调），
// 那些「订单已支付但消息没到游戏服」的单靠本循环补偿——支付链路
// 的最终一致性兜底。失败只记 Warn 继续：下次对账还是同一批单，
// 退出循环等于让丢单永久留在地上。
func (s *Server) reconcileLoop(ctx context.Context, svc *service.RechargeService) {
	periodic.Run(ctx, periodic.Options{
		Name:     "reconcile",
		Interval: time.Hour,
		Logger:   s.logger,
		Fn: func(ctx context.Context) error {
			n, err := svc.ReplayPaidUndelivered(ctx, 100)
			if err != nil {
				return err
			}
			if n > 0 {
				s.logger.Info("reconcile replayed orders", "count", n)
			}
			return nil
		},
	})
}

// closeConsumers 关闭全部 Kafka 消费者，逐个收集错误。
//
// 不用 errors.Join 直接返回的原因：停机编排需要「一个消费者关不掉不影响
// 另一个提交 offset」，所以这里先各自尝试、再统一交给调用方合并。
func (s *Server) closeConsumers() []error {
	var errs []error
	if s.delivered != nil {
		if err := s.delivered.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close delivered consumer: %w", err))
		}
	}
	if s.roleIndex != nil {
		if err := s.roleIndex.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close role index consumer: %w", err))
		}
	}
	return errs
}

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，由 pkg/shutdown 编排）。
// 顺序即安全顺序：先停 HTTP（不再接新请求）→ 停消费（提交 offset）
// → Flush + 关生产者（在途消息落盘）→ 关 MySQL → 关 Redis → 停指标。
// 反序会让新支付回调打进正在关闭的链路，丢单风险最高。
//
// 原先六步的失败全部只打 Warn、函数恒返回 nil：「回调丢单」与「指标端口
// 没关掉」在退出码上没有任何区别。现在统一进返回值，由 main 转非零退出。
func (s *Server) Shutdown(ctx context.Context) error {
	mgr := shutdown.NewManager(s.logger)

	// DRAINING：关 HTTP 入口（不再接新回调），等在途请求返回。
	mgr.Register("stop-http", shutdown.PhaseDraining, func(cctx context.Context) error {
		if s.httpSrv == nil {
			return nil
		}
		return s.httpSrv.Shutdown(cctx)
	})

	// DRAINING：停消费（内部提交 offset 后退出）。必须在关 Kafka 之前。
	mgr.Register("stop-consumer", shutdown.PhaseDraining, func(context.Context) error {
		// 两个消费者各关一次并汇总错误：任何一个漏关都会让
		// 「已处理未提交」的消息在重启后重放（幂等能兜住，但没必要）。
		return errors.Join(s.closeConsumers()...)
	})

	// FLUSH：把缓冲区里未确认的消息尽量发出；Close 之后 producer 不再收 Send。
	mgr.Register("flush-producer", shutdown.PhaseFlush, func(cctx context.Context) error {
		if s.producer == nil {
			return nil
		}
		err := s.producer.Flush(cctx)
		closeErr := s.producer.Close()
		return errors.Join(err, closeErr)
	})

	// STOP：DB → Redis → 指标。
	mgr.Register("close-db", shutdown.PhaseStop, func(context.Context) error {
		if s.database == nil {
			return nil
		}
		return s.database.Close()
	})
	mgr.Register("close-redis", shutdown.PhaseStop, func(context.Context) error {
		if s.redis == nil {
			return nil
		}
		return s.redis.Close()
	})
	mgr.Register("stop-metrics", shutdown.PhaseStop, func(cctx context.Context) error {
		if s.metricsSrv != nil {
			return s.metricsSrv.Stop(cctx)
		}
		return nil
	})

	if err := mgr.Run(ctx); err != nil {
		return fmt.Errorf("login shutdown: %w", err)
	}
	s.logger.Info("login stopped")
	return nil
}

// readiness 就绪探针：Redis/MySQL 可用才接流量（19.6 探针表）。
// 与 /healthz（进程活着）分层：K8s 的 readiness 探针打这里，
// 依赖抖动时本服从 Service Endpoints 摘除，恢复后自动摘回——
// 把「依赖不可用」转化为「不接流量」而不是「请求失败」。
func (s *Server) readiness(ctx context.Context) error {
	if s.redis != nil {
		if err := s.redis.GetClient().Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
	}
	if s.database != nil {
		if err := s.database.PingContext(ctx); err != nil {
			return fmt.Errorf("mysql: %w", err)
		}
	}
	return nil
}

// kafkaGroup 消费组名：配置优先，缺省 login-server。
// 单实例语义依赖这个组名固定——改组名 = 换消费组 = 从头消费，
// 已处理的消息会重放一遍（有 CAS 幂等兜底，但没必要）。
func kafkaGroup(cfg *config.Config) string {
	if cfg.Kafka.ConsumerGroup != "" {
		return cfg.Kafka.ConsumerGroup
	}
	return "login-server"
}

// parseTTL 解析 token TTL 配置；空值/解析失败回落 24h。
// 防御式默认值：配置写错不该让登录服务起不来——token 24h 过期
// 是安全可接受的底线，写错配置的代价是「所有人 24h 后重新登录」。
func parseTTL(s string) time.Duration {
	if s == "" {
		return 24 * time.Hour
	}
	d, err := config.ParseDuration(s)
	if err != nil {
		return 24 * time.Hour
	}
	return d
}

// withMetrics HTTP 埋点中间件（20.3：登录服 QPS / 延迟 / 错误率）。
//
// 只统计业务接口（/v1/ 前缀），跳过 /healthz 这类探针 —— 探针由 K8s 高频调用，
// 计入会稀释真实业务指标。
// 知识点：装饰器模式——包装 http.Handler 而不改 handler 本身；
// statusRecorder 内嵌 http.ResponseWriter 覆写 WriteHeader 捕状态码，
// 这是 net/http 中间件记录响应码的标准手法（ ResponseWriter 是接口，
// 嵌入后未覆写的方法自动转发给真实 writer）。
func withMetrics(next http.Handler, logger *log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		if metrics.Enabled() {
			metrics.MsgObserve("in", time.Since(start), 0)
			if rec.status >= 500 {
				metrics.MsgInc("in", "error")
			} else {
				metrics.MsgInc("in", "ok")
			}
		}
		if rec.status >= 500 {
			logger.Warn("http handler error", "path", r.URL.Path, "status", rec.status)
		}
	})
}

// statusRecorder 记录响应码（默认 200，handler 未显式写时也正确）。
// 默认值 200 是关键：http.Error 会显式 WriteHeader，而正常返回的
// handler 可能一次都没调 WriteHeader——此时真实响应也是 200，
// 默认值让记录与事实一致。
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader 记录状态码。
// 覆写后必须转发给内嵌 ResponseWriter：只记不发会让客户端永远
// 收不到响应头（挂起到超时）。
func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
