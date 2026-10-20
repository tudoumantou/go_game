// cmd/gateway/main.go —— 网关服（GatewayServer）进程启动入口
//
// 架构职责：客户端唯一入口——TCP 长连接接入、会话管理、协议编解码，
// 并把客户端请求以 gRPC 流转发给游戏服（设计文档第 8 章）。
// 协作关系：启动顺序 etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）；
// 网关排在游戏服之后：它要靠中心服/etcd 的 resolver 解析出游戏服地址才能建流，
// 客户端连上来之前目标必须已知。
// 设计取舍：main 是纯组装层（3.3）不含业务；New 只做无副作用的内存装配，
// 连 Redis/etcd、监听端口都收进 Start；signal.NotifyContext 把 SIGINT/SIGTERM
// 转成 ctx 取消即进程生命周期；优雅退出两阶段——Start 收取消返回后由 main 以
// 总超时驱动 Shutdown，内部覆盖会话排空 + 到游戏服的 gRPC 流关闭 + 摘注册，
// 让客户端尽早重连到其它网关（滚动发布时玩家无感）。
package main

import (
	"context"
	"flag"
	"game_study/pkg/log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"game_study/internal/gateway"
	"game_study/pkg/config"
)

var (
	// configPath 默认指向 dev 配置；生产环境用 -config 显式覆盖（配置分层见 19.1）。
	// flag.String 返回 *string：Parse 前即可安全解引用拿默认值，Parse 后原变量被就地改写
	configPath = flag.String("config", "configs/gateway/gateway-dev.yaml", "配置文件路径")
	version    = "dev" // 构建时 -ldflags "-X main.version=..." 注入（-X 只能改写字符串变量）
)

func main() {
	flag.Parse()
	// ① 先用默认 logger（配置加载前不可读 cfg.Log.Level）
	logger := log.New("gateway")
	logger.Info("GatewayServer server starting", "version", version, "config", *configPath)

	// ② 配置（19.1：YAML + ${VAR} 环境变量注入）
	// 配置失败直接 os.Exit：没有监听地址与 resolver 配置，网关无法提供服务
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后，按配置级别重建 logger
	// （配置加载前只能用默认级别，否则 dev 环境看不到 debug 日志）
	logger = log.New("gateway", log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	// ② 信号上下文（SIGINT/SIGTERM → 优雅关闭，18.1）
	// ctx 即进程生命周期：网关的 Start 内部阻塞在 TCP 监听上等 ctx.Done()，
	// 信号到达后返回，main 随即进入下面的关闭编排。
	// signal.NotifyContext（Go 1.16+）收信号即 cancel；defer stop() 恢复默认信号行为
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ③ 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心；内部组装细节见 internal/gateway）
	//   启动顺序有依赖：etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）
	// New 只做纯内存组装不返回 error；连 Redis/etcd、监听端口都在 Start 内完成——
	// 监听放在最后一步：先 resolver 后 accept，避免游戏服地址还没解析就开始接客
	srv := gateway.New(cfg)
	logger.Info("GatewayServer bootstrap done, starting", "listen", cfg.Server.TCPAddr)
	if err := srv.Start(ctx); err != nil {
		logger.Error("server start failed", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown signal received")

	// ④ 优雅关闭总超时（18.1；游戏服需覆盖 Actor Flush + flusher + Kafka Producer 全流程）
	// 配置未显式指定时兜底 60s：需覆盖会话排空 + 到游戏服的 gRPC 流关闭 + 摘注册
	// （会话排空是重点：给在途请求留出完成时间，超时的连接由客户端重连兜底）
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

	logger.Info("GatewayServer server stopped")
}
