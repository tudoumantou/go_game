// Package login 登录服（设计文档第 6 章）
package login

import (
	"context"
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
)

// Server 登录服（组装层入口，职责见设计文档 3.1 目录树）
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	redis      *cache.Client
	database   *db.DB
	httpSrv    *http.Server
	metricsSrv *metrics.Server

	producer  *pkgmq.Producer
	delivered *mq.DeliveredConsumer

	authHandler       *handler.AuthHandler
	serverListHandler *handler.ServerListHandler
	rechargeHandler   *handler.RechargeHandler
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server {
	logger := log.New("login", log.WithLevel(cfg.Log.Level))
	return &Server{cfg: cfg, logger: logger}
}

// Start 启动服务（阻塞至 ctx 取消）。
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
	redisClient, err := cache.New(cache.RedisConfig{
		Addrs:    s.cfg.Redis.Addrs,
		Password: s.cfg.Redis.Password,
		DB:       s.cfg.Redis.DB,
		PoolSize: s.cfg.Redis.PoolSize,
		MinIdle:  s.cfg.Redis.MinIdleConn,
	})
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	s.redis = redisClient

	// ② MySQL（充值订单，Tier-1）
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

		// ③ Kafka（生产者 + 发货确认消费者）
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
		if s.producer != nil {
			consumer := pkgmq.NewConsumer(pkgmq.Config{
				Brokers: s.cfg.Kafka.Brokers,
			}, pkgmq.TopicRechargeDelivered, kafkaGroup(s.cfg), s.producer)
			s.delivered = mq.NewDeliveredConsumer(consumer, rechargeSvc, s.logger)
		}
	}

	// ④ 服务与 handler
	tokenRepo := repo.NewTokenRepo(redisClient.GetClient())
	var accountRepo *repo.AccountRepo
	if s.database != nil {
		accountRepo = repo.NewAccountRepo(s.database)
	}
	authSvc := service.NewAuthService(tokenRepo, accountRepo, parseTTL(s.cfg.Token.TTL))
	dispatchSvc := service.NewDispatchService(s.cfg)

	s.authHandler = handler.NewAuthHandler(authSvc, loginAudit{producer: s.producer, logger: s.logger})
	s.serverListHandler = handler.NewServerListHandler(dispatchSvc)
	if rechargeSvc != nil {
		// 支付渠道：未配置密钥直接启动失败，不允许「无验签回调」上线（R3.3）。
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
	if s.rechargeHandler != nil {
		mux.HandleFunc("/v1/order/create", s.rechargeHandler.CreateOrder)
		mux.HandleFunc("/v1/pay/callback", s.rechargeHandler.PayCallback)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// ⑤ 后台任务：发货确认消费 + 每小时对账
	if s.delivered != nil {
		go func() {
			if err := s.delivered.Run(ctx); err != nil {
				s.logger.Error("delivered consumer stopped", "err", err)
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
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("http server error", "err", err)
		}
	}()

	s.logger.Info("LoginServer started", "addr", s.cfg.Server.HTTPAddr)
	<-ctx.Done()
	return ctx.Err()
}

// reconcileLoop 每小时对账：已支付未发货订单重投（13.3 模式③）。
func (s *Server) reconcileLoop(ctx context.Context, svc *service.RechargeService) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := svc.ReplayPaidUndelivered(ctx, 100)
			if err != nil {
				s.logger.Warn("reconcile failed", "err", err)
				continue
			}
			if n > 0 {
				s.logger.Info("reconcile replayed orders", "count", n)
			}
		}
	}
}

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，pkg/shutdown 编排）。
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(ctx); err != nil {
			s.logger.Warn("http shutdown", "err", err)
		}
	}
	// 关闭顺序：停消费（提交 offset）→ Flush 生产者 → 关连接
	if s.delivered != nil {
		if err := s.delivered.Close(); err != nil {
			s.logger.Warn("consumer close", "err", err)
		}
	}
	if s.producer != nil {
		_ = s.producer.Flush(ctx)
		if err := s.producer.Close(); err != nil {
			s.logger.Warn("producer close", "err", err)
		}
	}
	if s.database != nil {
		_ = s.database.Close()
	}
	if s.redis != nil {
		if err := s.redis.Close(); err != nil {
			s.logger.Warn("redis close", "err", err)
		}
	}
	if s.metricsSrv != nil {
		_ = s.metricsSrv.Stop(ctx)
	}
	s.logger.Info("login stopped")
	return nil
}

// readiness 就绪探针：Redis/MySQL 可用才接流量（19.6 探针表）。
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

func kafkaGroup(cfg *config.Config) string {
	if cfg.Kafka.ConsumerGroup != "" {
		return cfg.Kafka.ConsumerGroup
	}
	return "login-server"
}

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
type statusRecorder struct {
	http.ResponseWriter
	status int
}

// WriteHeader 记录状态码。
func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
