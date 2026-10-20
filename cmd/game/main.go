// cmd/game/main.go —— 游戏服启动入口
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

	"game_study/internal/game"
	"game_study/pkg/config"
)

var (
	// configPath 默认指向 dev 配置；生产环境用 -config 显式覆盖（配置分层见 19.1）
	configPath = flag.String("config", "configs/game/game-dev.yaml", "配置文件路径")
	version    = "dev" // 构建时 -ldflags "-X main.version=..." 注入
)

func main() {
	flag.Parse()
	// ① 先用默认 logger（配置加载前不可读 cfg.Log.Level）
	logger := log.New("game")
	logger.Info("GameServer server starting", "version", version, "config", *configPath)

	// ② 配置（19.1：YAML + ${VAR} 环境变量注入）
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后，按配置级别重建 logger
	logger = log.New("game", log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	// ② 信号上下文（SIGINT/SIGTERM → 优雅关闭，18.1）
	// ctx 即进程生命周期：游戏服的 Start 内部监听它退出阻塞，随后进入关闭编排
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ③ 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心；内部组装细节见 internal/game）
	//   启动顺序有依赖：etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）
	//   游戏服启动后向中心服/etcd 注册，网关才能通过 resolver 发现并建流
	// New 只做纯内存组装不返回 error；监听与外部依赖连接都在 Start 内完成
	srv := game.New(cfg)
	logger.Info("GameServer bootstrap done, starting", "listen", cfg.Server.GRPCAddr)
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

	logger.Info("GameServer server stopped")
}
