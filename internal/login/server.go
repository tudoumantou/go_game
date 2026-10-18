// Package login 登录服（设计文档第 6 章）
package login

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"game_study/internal/login/handler"
	"game_study/internal/login/repo"
	"game_study/internal/login/service"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/log"
)

// Server 登录服（组装层入口，职责见设计文档 3.1 目录树）
type Server struct {
	cfg      *config.Config
	logger   *log.Logger
	redis    *cache.Client
	httpSrv  *http.Server

	authHandler      *handler.AuthHandler
	serverListHandler *handler.ServerListHandler
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server {
	logger := log.New("login", log.WithLevel(cfg.Log.Level))
	return &Server{cfg: cfg, logger: logger}
}

// Start 启动服务（阻塞至 ctx 取消）。
func (s *Server) Start(ctx context.Context) error {
	// Redis
	redisCfg := cache.RedisConfig{
		Addrs:    s.cfg.Redis.Addrs,
		Password: s.cfg.Redis.Password,
		DB:       s.cfg.Redis.DB,
		PoolSize: s.cfg.Redis.PoolSize,
		MinIdle:  s.cfg.Redis.MinIdleConn,
	}
	redisClient, err := cache.New(redisCfg)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	s.redis = redisClient

	// 服务与 handler
	tokenRepo := repo.NewTokenRepo(redisClient.GetClient())
	authSvc := service.NewAuthService(tokenRepo, parseTTL(s.cfg.Token.TTL))
	dispatchSvc := service.NewDispatchService(s.cfg)

	s.authHandler = handler.NewAuthHandler(authSvc)
	s.serverListHandler = handler.NewServerListHandler(dispatchSvc)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/auth/guest", s.authHandler.GuestLogin)
	mux.HandleFunc("/v1/server/assign", s.serverListHandler.Assign)
	mux.HandleFunc("/v1/server/gateway/list", s.serverListHandler.GatewayList)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	s.httpSrv = &http.Server{
		Addr:    s.cfg.Server.HTTPAddr,
		Handler: mux,
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

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，pkg/shutdown 编排）。
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv != nil {
		if err := s.httpSrv.Shutdown(ctx); err != nil {
			s.logger.Warn("http shutdown", "err", err)
		}
	}
	if s.redis != nil {
		if err := s.redis.Close(); err != nil {
			s.logger.Warn("redis close", "err", err)
		}
	}
	s.logger.Info("login stopped")
	return nil
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
