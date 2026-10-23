// cmd/map/main.go —— SLG 大地图服务（MapServer）进程入口
//
// 架构职责：承载世界地图的权威状态——城池、部队、资源点、采集与战斗队列。
// 它是独立于 game 服的进程：地图状态量大（单世界 2.5 万格、25 个场景循环），
// 与玩家 Actor 放在同一进程会互相拖慢，且地图需要独立的停服/迁移节奏。
//
// 协作关系（设计文档第 3 章）：
//
//	game 服（玩家 Actor） --内部 RPC--> map 服（地图事务）
//	map 服 --交接单--> game 服（资产入包）
//
// 本进程启动顺序：配置 → 世界管理器（25 个场景循环）→ 加载世界 → 阻塞等信号。
// 没有 Redis/MySQL 硬依赖之前（M1），可以用最小配置直接跑起来做联调。
//
// 设计取舍：main 只做组装与生命周期，不含业务。signal.NotifyContext 把
// SIGINT/SIGTERM 转成 ctx 取消；关闭走两阶段（先拒新写、再等在途事务），
// 任何一步超时都强停，保证发布窗口可控。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"game_study/internal/worldmap"
	maprpc "game_study/internal/worldmap/rpc"
	"game_study/pkg/config"
	"game_study/pkg/log"
	rpcpkg "game_study/pkg/rpc"

	// automaxprocs：按 cgroup CPU 配额设置 GOMAXPROCS（K8s 限核时
	// 避免按宿主机核数开 P 导致 GC/调度抖动）；无限制时自动 no-op。
	_ "go.uber.org/automaxprocs"
)

var (
	// configPath 默认指向 dev 配置；生产用 -config 覆盖。
	configPath = flag.String("config", "configs/map/map-dev.yaml", "配置文件路径")
	// version 由构建时 -ldflags "-X main.version=..." 注入。
	version = "dev"
)

func main() {
	flag.Parse()

	logger := log.New("map")
	logger.Info("MapServer starting", "version", version, "config", *configPath)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config failed", "err", err)
		os.Exit(1)
	}
	logger = log.New("map", log.WithLevel(cfg.Log.Level))

	if !cfg.Map.Enabled {
		// 显式失败而不是静默空转：map 进程起来却没启用地图，
		// 说明配置指向错了文件（比如拿了 game 的配置），静默运行会让人误以为部署成功。
		logger.Error("map.enabled=false，该进程无任何世界可服务，请检查配置")
		os.Exit(1)
	}

	verifier, err := tokenVerifier(cfg)
	if err != nil {
		logger.Error("map 鉴权配置无效", "err", err)
		os.Exit(1)
	}

	// MySQL 连接配置从顶层段取（与其它服务共用一套连接参数）；
	// 为空则走内存模式，适合本地联调。
	srv, err := worldmap.New(cfg.Map, loggerAdapter{logger},
		worldmap.WithMySQL(cfg.MySQL),
		worldmap.WithTokenVerifier(verifier),
	)
	if err != nil {
		logger.Error("map server init failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// gRPC 服务：注册必须在 Start 之前（gRPC 不允许运行中改服务表）。
	rs := rpcpkg.NewServer(cfg.Server.GRPCAddr, logger)
	srv.API().Register(rs.GRPCServer())
	go func() {
		if err := rs.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("grpc server stopped with error", "err", err)
		}
	}()

	runErr := srv.Run(ctx)
	rs.Stop()
	logger.Info("shutdown signal received")

	// 关闭预算：地图是"等在途事务"型服务，超时太短会把已提交的命令
	// 截断成结果未知。默认 30s，允许配置覆盖。
	total := 30 * time.Second
	if d, err := config.ParseDuration(cfg.Shutdown.TotalTimeout); err == nil && d > 0 {
		total = d
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), total)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)

	if err := errors.Join(runErr, shutdownErr); err != nil {
		logger.Error("map server exit with error", "err", err)
		os.Exit(1)
	}
	logger.Info("MapServer stopped")
}

// tokenVerifier 构造内部 RPC 鉴权器。
//
// dev/test 环境用静态 token（配置 map 段暂时没有 token 字段，先用环境变量
// MAP_INTERNAL_TOKEN）；生产未配置 token 时直接拒绝启动——
// 地图服务暴露的是"改别人家城池"的能力，不能默认裸奔。
func tokenVerifier(cfg *config.Config) (maprpc.TokenVerifier, error) {
	token := os.Getenv("MAP_INTERNAL_TOKEN")
	if token == "" {
		// 生产环境没有 token 等于允许任意内部调用者伪造 player_id，
		// 这是"改别人家城池"级别的风险，必须拒绝启动。
		if cfg.Server.Env == "prod" {
			return nil, fmt.Errorf("MAP_INTERNAL_TOKEN 未设置：生产环境不允许裸奔")
		}
		// dev/test：不校验，便于本地联调（日志明确提示）。
		return maprpc.NopVerifier{}, nil
	}
	if v := maprpc.NewStaticTokenVerifier(token); v != nil {
		return v, nil
	}
	return nil, fmt.Errorf("MAP_INTERNAL_TOKEN 为空")
}

// loggerAdapter 把项目日志器适配成 runtime.Logger。
//
// 为什么用适配器而不是让 runtime 直接依赖 pkg/log：
// runtime 是纯领域运行时，测试里只需要"能记日志"，不应该被迫
// 拉起真实日志器（它可能写文件、连 Kafka）。三个方法的小接口
// 让测试可以传一个把日志收进 slice 的假实现。
type loggerAdapter struct{ l *log.Logger }

func (a loggerAdapter) Info(msg string, args ...any)  { a.l.Info(msg, args...) }
func (a loggerAdapter) Warn(msg string, args ...any)  { a.l.Warn(msg, args...) }
func (a loggerAdapter) Error(msg string, args ...any) { a.l.Error(msg, args...) }
