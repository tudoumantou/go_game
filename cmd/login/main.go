// cmd/login/main.go —— 登录服（LoginServer）进程启动入口
//
// 架构职责：账号鉴权、登录态签发与校验、选服推送（设计文档第 6 章），
// 对外暴露 HTTP 接口，对内经 Kafka 与游戏服交换发货/封禁等消息。
// 协作关系：启动顺序 etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5），
// 登录服排最后：它依赖游戏服就绪后才能把玩家选中的服真正开起来；
// 与中心服/etcd 协作完成注册，网关经 resolver 发现登录服 HTTP 地址。
// 设计取舍：main 是纯组装层（3.3）不含业务；New 只做无副作用的内存装配，
// 连 Redis/MySQL/Kafka、注册路由都收进 Start；signal.NotifyContext 把
// SIGINT/SIGTERM 转成 ctx 取消即进程生命周期；优雅退出两阶段——Start 收取消
// 返回后由 main 以总超时驱动 Shutdown，登录服侧重点是 Kafka：消费者先提交
// offset、再 Flush 生产者，顺序颠倒会丢发货消息。
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

	// automaxprocs：按 cgroup CPU 配额设置 GOMAXPROCS（K8s 限核时
	// 避免按宿主机核数开 P 导致 GC/调度抖动）；无限制时自动 no-op。
	_ "go.uber.org/automaxprocs"
)

var (
	// configPath 默认指向 dev 配置；生产环境用 -config 显式覆盖（配置分层见 19.1）。
	// flag.String 返回 *string：Parse 前即可安全解引用拿默认值，Parse 后原变量被就地改写
	configPath = flag.String("config", "configs/login/login-dev.yaml", "配置文件路径")
	version    = "dev" // 构建时 -ldflags "-X main.version=..." 注入（-X 只能改写字符串变量）
)

func main() {
	flag.Parse()
	// ① 先用默认 logger（配置加载前不可读 cfg.Log.Level）
	logger := log.New("login")
	logger.Info("LoginServer server starting", "version", version, "config", *configPath)

	// ② 配置（19.1：YAML + ${VAR} 环境变量注入）
	// 配置失败直接 os.Exit：鉴权依赖 Redis/MySQL，配错时静默降级等于放行所有登录
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	// 配置加载成功后，按配置级别重建 logger
	logger = log.New("login", log.WithLevel(cfg.Log.Level))
	logger.Info("config loaded", "name", cfg.Server.Name, "env", cfg.Server.Env)

	// ② 信号上下文（SIGINT/SIGTERM → 优雅关闭，18.1）
	// 登录服的 Start 内部以 <-ctx.Done() 阻塞；信号到达后返回，main 继续关闭编排。
	// signal.NotifyContext（Go 1.16+）收信号即 cancel；defer stop() 恢复默认信号行为
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ③ 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心；内部组装细节见 internal/login）
	//   启动顺序有依赖：etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）
	// New 只做纯内存组装不返回 error；连 Redis/MySQL/Kafka、注册路由都在 Start 内完成——
	// 路由注册放在监听之前，保证第一个请求到达时 handler 已就位
	srv := login.New(cfg)
	logger.Info("LoginServer bootstrap done, starting", "listen", cfg.Server.HTTPAddr)
	if err := srv.Start(ctx); err != nil {
		logger.Error("server start failed", "err", err)
		os.Exit(1)
	}
	logger.Info("shutdown signal received")

	// ④ 优雅关闭总超时（18.1；游戏服需覆盖 Actor Flush + flusher + Kafka Producer 全流程）
	// 登录服侧重点是 Kafka：消费者先提交 offset、再 Flush 生产者，防止丢发货消息。
	// 在途登录请求很短，兜底 60s 足够覆盖 Kafka 两端的收尾
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

	logger.Info("LoginServer server stopped")
}
