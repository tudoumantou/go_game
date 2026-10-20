// cmd/game/main.go —— 游戏服（GameServer）进程启动入口
//
// 架构职责：承载玩家业务——Actor 系统跑地图/公会等有状态实体，gRPC 同时服务
// 网关（客户端会话）与中心服（Report 双向流），并经 Kafka 与其它进程交换消息。
// 协作关系：启动顺序 etcd → 中间件 → 中心服 → 游戏服 → 网关 → 登录服（19.5）；
// 游戏服起来后向中心服/etcd 注册，网关才能通过 resolver 发现并建流；
// 玩家状态经写穿透交 flusher 进程落库（Tier-2）。
// 设计取舍：main 是纯组装层（3.3）不含业务；New 只做无副作用的内存装配，
// 连中间件与监听都收进 Start；signal.NotifyContext 把 SIGINT/SIGTERM 转成
// ctx 取消即进程生命周期；优雅退出两阶段——Start 收取消返回后由 main 以
// 总超时驱动 Shutdown，内部覆盖 Actor Flush → flusher 衔接 → Kafka Producer
// 刷出的全流程，任何一步超时都会强停，保证发布窗口可控。
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
	// configPath 默认指向 dev 配置；生产环境用 -config 显式覆盖（配置分层见 19.1）。
	// flag.String 返回 *string：Parse 前即可安全解引用拿默认值，Parse 后原变量被就地改写
	configPath = flag.String("config", "configs/game/game-dev.yaml", "配置文件路径")
	version    = "dev" // 构建时 -ldflags "-X main.version=..." 注入（-X 只能改写字符串变量）
)

func main() {
	flag.Parse()
	// ① 先用默认 logger（配置加载前不可读 cfg.Log.Level）
	logger := log.New("game")
	logger.Info("GameServer server starting", "version", version, "config", *configPath)

	// ② 配置（19.1：YAML + ${VAR} 环境变量注入）
	// 配置失败直接 os.Exit：游戏服依赖最多（Redis/MySQL/Kafka/etcd），配错即起不来
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后，按配置级别重建 logger
	logger = log.New("game", log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	// ② 信号上下文（SIGINT/SIGTERM → 优雅关闭，18.1）
	// ctx 即进程生命周期：游戏服的 Start 内部监听它退出阻塞，随后进入关闭编排。
	// signal.NotifyContext（Go 1.16+）收信号即 cancel；defer stop() 恢复默认信号行为
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ③ 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心；内部组装细节见 internal/game）
	//   启动顺序有依赖：etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）
	//   游戏服启动后向中心服/etcd 注册，网关才能通过 resolver 发现并建流
	// New 只做纯内存组装不返回 error；监听与外部依赖连接都在 Start 内完成——
	// 装配顺序即依赖顺序：先 Redis/MySQL/Kafka 句柄，再 Actor 系统，最后才是
	// 对外监听，保证第一个客户端连上时内部已就绪
	srv := game.New(cfg)
	logger.Info("GameServer bootstrap done, starting", "listen", cfg.Server.GRPCAddr)
	if err := srv.Start(ctx); err != nil {
		logger.Error("server start failed", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown signal received")

	// ④ 优雅关闭总超时（18.1；游戏服需覆盖 Actor Flush + flusher + Kafka Producer 全流程）
	// 游戏服是五进程里关闭链路最长的：在途会话要排空、Actor 状态要 Flush 进
	// Redis 任务队列、Kafka 生产端缓冲要刷出，因此兜底 60s 并允许配置覆盖
	total := 60 * time.Second
	if d, err := config.ParseDuration(cfg.Shutdown.TotalTimeout); err == nil && d > 0 {
		total = d
	}
	// 用 Background 而非继承已取消的 ctx：关闭流程需要一份独立且仍然有效的超时预算
	shutdownCtx, cancel := context.WithTimeout(context.Background(), total)
	defer cancel()

	// 18.1：DRAIN→DRAINING→FLUSH→STOP，由 Server.Shutdown 内部编排
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "err", err)
		os.Exit(1)
	}

	logger.Info("GameServer server stopped")
}
