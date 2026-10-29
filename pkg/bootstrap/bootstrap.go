// Package bootstrap 进程启动样板：配置 → 日志 → 信号 → 组装 → 启动 → 关闭。
//
// # 为什么要有这个包
//
// 六个进程（gateway/login/game/center/cross/map）的 main.go 曾是同一个模板
// 抄了六遍，共 617 行，其中五份是 92±2 行的近似复制：默认 logger → 加载
// 配置 → 按配置级别重建 logger → signal.NotifyContext → 组装 → Start 阻塞
// → 关掉信号 → 用总超时驱动 Shutdown。差异只有三个字段：服务名、默认配置
// 路径、构造函数。
//
// 这不是「重复得难看」的审美问题，而是**会漏**：第 6 份（map）在抄的时候
// 漏掉了 metrics 接入（没有 metrics.Init / metrics.NewServer），于是它在
// Prometheus 里不存在，且没有任何地方会报错。复制粘贴的失效方式就是安静地
// 少一行。
//
// # 启动顺序为什么是这个顺序
//
//   - 先用默认 logger：cfg.Log.Level 要等配置加载后才能读，此前的日志只能
//     按默认级别输出。配置加载失败要能看到「哪一步失败」，所以第一步就得有
//     能用的日志器。
//   - 配置失败直接退出：没有配置，后续步骤都无从谈起，且此刻尚无资源需要
//     清理（没有 defer 会因为 os.Exit 被跳过而泄漏）。
//   - 信号 ctx 即进程生命周期：Start 内部监听它退出阻塞，随后进入关闭编排。
//   - 关闭用**新的** Background ctx：继承已取消的 ctx 会让所有关闭步骤立刻
//     超时——关闭流程需要一份独立且仍然有效的超时预算。
//
// # 关闭预算
//
// 配置里的 shutdown.total_timeout 优先；没配就用 Options.ShutdownBudget；
// 都没有则 60s。这个预算必须覆盖「Actor 排空 + flusher 落库 + Kafka 刷出」
// 的完整链路，具体由各服务的 Shutdown（pkg/shutdown.Manager）内部按阶段分解。
package bootstrap

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"game_study/pkg/config"
	"game_study/pkg/log"
)

// defaultShutdownBudget 未配置且未指定时的关闭总预算。
//
// 60s 的依据：滚动发布窗口通常按分钟计，60s 足够让 Actor 排空 + flusher
// 落库 + Kafka 刷出跑完；再长会让 K8s 的 terminationGracePeriod 先到，
// 进程被 SIGKILL，等于关闭流程白做。
const defaultShutdownBudget = 60 * time.Second

// Server 各服务必须满足的生命周期契约。
//
// # Start 的返回值
//
// Start 阻塞到关闭信号到达，**正常关闭必须返回 nil**；只有「启动失败」
// 才返回 error。这条契约不是风格问题：调用方按它决定「立刻退出」还是
// 「继续执行关服编排」——返回 ctx.Err() 会让一次 SIGTERM 变成「启动失败」，
// 于是 Shutdown（排空、落库、关组件）整个被跳过。
//
// 六个服务原先都写成了 `<-ctx.Done(); return ctx.Err()`，配上一份
// `if err != nil { os.Exit(1) }` 的 main，结果是：**优雅关闭从未被触发过**。
// 关服相关的能力（Actor 排空、flusher 落库、房间兜底结算）只在单测里跑过。
//
// # Shutdown 的调用时机
//
// Start 返回之后调用。正常关闭与「关闭期间出错」两种情况都会调，
// 后者用 errors.Join 把两边的错误一起上报。
type Server interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

// Options 启动参数。差异只有这三个字段，其余全在本包。
type Options struct {
	// Name 服务名：日志的固定字段、metrics 的 service 标签。
	Name string
	// DefaultConfig -config 的默认值（各环境的实际路径由部署参数覆盖）。
	DefaultConfig string
	// Version 由 -ldflags "-X main.version=..." 注入，必须由各 main 自己
	// 持有这个变量（-X 只能改写本包的字符串变量）。
	Version string
	// ShutdownBudget 兜底关闭预算；0 取 60s。配置里的 shutdown.total_timeout
	// 优先级更高。
	ShutdownBudget time.Duration
	// Build 组装服务器。logger 已是按配置级别重建过的那一个，直接拿去用。
	Build func(cfg *config.Config, logger *log.Logger) (Server, error)
}

// Run 执行完整的进程生命周期，直到收到 SIGINT/SIGTERM 并完成关闭。
//
// 任何一步失败都以非零码退出：启动失败、关闭失败（含关闭预算耗尽）都算。
// 关闭失败必须体现在退出码上——「保存失败」被一行 Warn 掩盖是事故模式。
func Run(opts Options) {
	// 用独立 FlagSet 而不是全局 flag：全局 flag 在测试里重复注册会 panic，
	// 而这个包理应是可测的（Run 的失败路径靠 os.Exit，但参数解析不该也是）。
	fs := flag.NewFlagSet(opts.Name, flag.ExitOnError)
	configPath := fs.String("config", opts.DefaultConfig, "配置文件路径")
	_ = fs.Parse(os.Args[1:])

	logger := log.New(opts.Name)
	logger.Info(opts.Name+" server starting", "version", opts.Version, "config", *configPath)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后按配置级别重建：之前的日志只用于「配置为什么没加载成功」。
	logger = log.New(opts.Name, log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv, err := opts.Build(cfg, logger)
	if err != nil {
		logger.Error("build server failed", "err", err)
		os.Exit(1)
	}
	logger.Info(opts.Name+" bootstrap done, starting", "listen", listenAddr(cfg))

	// Start 阻塞至 ctx 取消（正常，返回 nil）或启动失败（返回错误）。
	startErr := srv.Start(ctx)
	if startErr != nil && ctx.Err() != nil && errors.Is(startErr, context.Canceled) {
		// 防御：契约要求正常关闭返回 nil，但哪怕某个服务写成 ctx.Err()，
		// 也不该让 SIGTERM 变成「启动失败」而跳过后面的关服编排。
		startErr = nil
	}
	if startErr != nil && ctx.Err() == nil {
		// 启动失败：没有「已就绪的组件」可关闭，立即退出。
		// 走到这里说明错误发生在信号之前，与关服流程无关。
		logger.Error("server start failed", "err", startErr)
		os.Exit(1)
	}
	if startErr != nil {
		// 关闭期间 Start 返回错误（例如排空时的写入失败）：仍要走完 Shutdown，
		// 让剩下的阶段有机会收尾，最后由退出码体现。
		logger.Warn("server exited with error during shutdown", "err", startErr)
	}
	logger.Info("shutdown signal received")

	budget := opts.ShutdownBudget
	if budget <= 0 {
		budget = defaultShutdownBudget
	}
	if d, err := config.ParseDuration(cfg.Shutdown.TotalTimeout); err == nil && d > 0 {
		budget = d
	}
	// Background 而非继承已取消的 ctx：关闭需要独立的有效预算。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	shutdownErr := srv.Shutdown(shutdownCtx)
	if err := errors.Join(startErr, shutdownErr); err != nil {
		logger.Error("shutdown failed", "err", err)
		os.Exit(1)
	}
	logger.Info(opts.Name + " server stopped")
}

// listenAddr 取「这个进程的业务监听地址」用于启动日志。
//
// 三类服务各用一个字段：网关听 TCP（客户端长连接）、其余听 gRPC
// （login 听 HTTP）。顺序把 TCP 与 gRPC 排在 HTTP 之前，是因为 HTTP 在
// 网关/地图这类进程里只是运维端口（/healthz、/status），它开着不代表
// 业务可连——日志里报运维端口会让人以为服务已对外。
// 全空时返回 "-"：留空会让人误以为没有监听。
func listenAddr(cfg *config.Config) string {
	for _, a := range []string{cfg.Server.TCPAddr, cfg.Server.GRPCAddr, cfg.Server.HTTPAddr} {
		if a != "" {
			return a
		}
	}
	return "-"
}
