// cmd/login/main.go —— 登录服启动入口
// 组装层（设计文档 3.3）：配置 → 组件 → 服务 → 注册中心 → 优雅退出；不含业务。
package main

import (
	"context"
	"flag"
	"game_study/pkg/log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"game_study/internal/login"
	"game_study/pkg/config"
)

var (
	configPath = flag.String("config", "configs/login/login-dev.yaml", "配置文件路径")
	version    = "dev" // 构建时 -ldflags "-X main.version=..." 注入
)

func main() {
	flag.Parse()
	// ① 先用默认 logger（配置加载前不可读 cfg.Log.Level）
	logger := log.New("login")
	logger.Info("LoginServer server starting", "version", version, "config", *configPath)

	// ② 配置（19.1：YAML + ${VAR} 环境变量注入）
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后，按配置级别重建 logger
	logger = log.New("login", log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	// ② 信号上下文（SIGINT/SIGTERM → 优雅关闭，18.1）
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ③ 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心；内部组装细节见 internal/login）
	//   启动顺序有依赖：etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）
	srv := login.New(cfg)
	logger.Info("LoginServer bootstrap done, starting", "listen", cfg.Server.HTTPAddr)
	if err := srv.Start(ctx); err != nil {
		logger.Error("server start failed", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown signal received")

	// ④ 优雅关闭总超时（18.1；游戏服需覆盖 Actor Flush + flusher + Kafka Producer 全流程）
	total := 60 * time.Second
	if d, err := config.ParseDuration(cfg.Shutdown.TotalTimeout); err == nil && d > 0 {
		total = d
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), total)
	defer cancel()

	// 18.1：DRAIN→DRAINING→FLUSH→STOP，由 Server.Shutdown 内部编排
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "err", err)
		os.Exit(1)
	}

	logger.Info("LoginServer server stopped")
}
