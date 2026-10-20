// cmd/cross/main.go —— 跨服节点（CrossServer）进程启动入口
//
// 架构职责：承载小组跨服战场——房间 Actor 的生命周期管理、战斗消息透传、
// 结算结果回推（设计文档第 10 章）。
// 协作关系：跨服节点不注册 etcd、不连网关（10.1），只被组内游戏服主动连接；
// 依赖中心服先完成启动与注册，游戏服经中心服 GroupUpdate 拿到跨服地址后才来建流。
// 设计取舍：main 是纯组装层（3.3）不含业务；New 只做内存装配，Actor 系统与
// gRPC 监听等副作用收进 Start；signal.NotifyContext 把 SIGINT/SIGTERM 转成
// ctx 取消即进程生命周期；优雅退出两阶段——Start 收取消返回后由 main 以
// 总超时驱动 Shutdown，内部依次完成「拒新房 → 存量房间兜底结算 → 退流」。
package main

import (
	"context"
	"flag"
	"game_study/pkg/log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"game_study/internal/cross"
	"game_study/pkg/config"
)

var (
	// configPath 默认指向 dev 配置；生产环境用 -config 显式覆盖（配置分层见 19.1）。
	// flag.String 返回 *string：Parse 前即可安全解引用拿到默认值，Parse 后原变量被就地改写
	configPath = flag.String("config", "configs/cross/cross-dev.yaml", "配置文件路径")
	version    = "dev" // 构建时 -ldflags "-X main.version=..." 注入（-X 只能改写字符串变量）
)

func main() {
	flag.Parse()
	// ① 先用默认 logger（配置加载前不可读 cfg.Log.Level）
	logger := log.New("cross")
	logger.Info("CrossServer server starting", "version", version, "config", *configPath)

	// ② 配置（19.1：YAML + ${VAR} 环境变量注入）
	// 配置失败直接 os.Exit：跨服节点无外部强依赖，配错地址时静默连本地默认库比显式失败更糟
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后，按配置级别重建 logger
	logger = log.New("cross", log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	// ② 信号上下文（SIGINT/SIGTERM → 优雅关闭，18.1）
	// ctx 即进程生命周期：跨服节点的 Start 内部监听它退出阻塞，随后进入关闭编排。
	// signal.NotifyContext（Go 1.16+）收信号即 cancel；defer stop() 恢复默认信号行为
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ③ 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心；内部组装细节见 internal/cross）
	//   启动顺序有依赖：etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）
	//   跨服节点按小组横向扩容，无单点；依赖中心服已完成 etcd 注册
	// New 只做纯内存组装不返回 error；监听与外部依赖连接都在 Start 内完成——
	// 跨服节点依赖极少（Actor 系统 + gRPC），这也是它能按小组随意扩容的原因
	srv := cross.New(cfg)
	logger.Info("CrossServer bootstrap done, starting", "listen", cfg.Server.GRPCAddr)
	if err := srv.Start(ctx); err != nil {
		logger.Error("server start failed", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown signal received")

	// ④ 优雅关闭总超时（18.1；游戏服需覆盖 Actor Flush + flusher + Kafka Producer 全流程）
	// 跨服侧重点是房间兜底结算：存量房间要按超时平局打完再退流，
	// 因此总超时必须大于内部 Drain 时长，配置未指定时兜底 60s
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

	logger.Info("CrossServer server stopped")
}
