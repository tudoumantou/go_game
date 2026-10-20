// cmd/center/main.go —— 中心服（CenterServer）进程启动入口（五进程中的协调者）
//
// 架构职责：中心服提供节点注册/发现、跨服匹配、消息中转、排行榜/统计，
// 以及仅 leader 执行的赛季归档、活动调度与全服里程碑（设计文档第 9 章）。
// 协作关系：启动顺序上中心服必须最先就绪（etcd → 中间件 → center → game/cross
// → gateway → login，19.5）——游戏服/跨服靠它完成注册与互相发现。
// 设计取舍：main 是纯组装层（3.3）不含业务；New 只做无副作用的内存装配，
// 连中间件与监听都收进 Start，失败可重试；signal.NotifyContext 把
// SIGINT/SIGTERM 转成 ctx 取消，ctx 即进程生命周期；优雅退出分两阶段——
// Start 收取消返回后，main 再用总超时（缺省 60s）驱动 Shutdown，
// 避免某条 Report 流不结束把关闭流程无限卡死。
package main

import (
	"context"
	"flag"
	"game_study/pkg/log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"game_study/internal/center"
	"game_study/pkg/config"
)

var (
	// configPath 默认指向 dev 配置；生产环境用 -config 显式覆盖（配置分层见 19.1）。
	// flag.String 返回 *string 而非 string：指针让 flag 包在 Parse 之前也能给出
	// 可用的默认值，Parse 之后原变量被就地改写，因此后面统一解引用取值。
	configPath = flag.String("config", "configs/center/center-dev.yaml", "配置文件路径")
	version    = "dev" // 构建时 -ldflags "-X main.version=..." 注入（-X 只能改写字符串变量）
)

func main() {
	flag.Parse()
	// ① 先用默认 logger：cfg.Log.Level 要等配置加载后才能读，此前的日志只能按默认级别输出
	logger := log.New("center")
	logger.Info("CenterServer server starting", "version", version, "config", *configPath)

	// ② 配置（19.1：YAML + ${VAR} 环境变量注入）
	// 配置失败直接 os.Exit：没有配置后续步骤都无从谈起，且此刻尚无资源需要清理
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后，按配置级别重建 logger
	logger = log.New("center", log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	// ② 信号上下文（SIGINT/SIGTERM → 优雅关闭，18.1）
	// ctx 即进程生命周期：中心服的 Start 内部监听它退出阻塞，随后进入关闭编排。
	// signal.NotifyContext（Go 1.16+）在收到信号时自动 cancel ctx；defer stop()
	// 恢复默认信号行为，防止信号监听器泄漏到 main 退出之后。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ③ 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心；内部组装细节见 internal/center）
	//   启动顺序有依赖：etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）
	//   中心服最先起：其它节点靠它做 etcd 注册与节点发现
	// New 只做纯内存组装不返回 error；监听与外部依赖连接都在 Start 内完成——
	// 这是依赖注入的边界：main 只认识 *config.Config，子系统之间的相互引用
	// 全部收敛在 internal/center 内部，main 不需要知道 registry/matcher 的存在
	srv := center.New(cfg)
	logger.Info("CenterServer bootstrap done, starting", "listen", cfg.Server.GRPCAddr)
	// Start 阻塞至 ctx 取消，正常返回即代表关闭信号已到达
	if err := srv.Start(ctx); err != nil {
		logger.Error("server start failed", "err", err)
		os.Exit(1) // 注意：os.Exit 不执行 defer，此时资源由 Start 内部自行清理
	}
	logger.Info("shutdown signal received")

	// ④ 优雅关闭总超时（18.1；游戏服需覆盖 Actor Flush + flusher + Kafka Producer 全流程）
	// 中心服侧重点是：Kick 全部节点让它们重连其它实例 + gRPC 强停 + Kafka/MySQL 连接释放，
	// 配置未显式指定时兜底 60s
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

	logger.Info("CenterServer server stopped")
}
