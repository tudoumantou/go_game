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
// 设计取舍：启动样板统一在 pkg/bootstrap，但本进程有三处与其余五个不同，
// 全部收在下面的 mapServer 适配器里（而不是让 bootstrap 长出一堆开关）：
//  1. New 需要 MapConfig（不是整个 cfg）与 runtime.Logger；
//  2. gRPC 服务的注册必须在 Start 之前（gRPC 不允许运行中改服务表）；
//  3. 生命周期方法叫 Run 而不是 Start。
//
// 另外，本进程此前**没有接 metrics**——它是第六份 main.go，抄模板时漏了。
// 现在由适配器在 Start 里补上（/metrics、/healthz、/readyz 一并可用）。
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"game_study/internal/worldmap"
	maprpc "game_study/internal/worldmap/rpc"
	"game_study/pkg/bootstrap"
	"game_study/pkg/config"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	rpcpkg "game_study/pkg/rpc"

	// automaxprocs：按 cgroup CPU 配额设置 GOMAXPROCS（K8s 限核时
	// 避免按宿主机核数开 P 导致 GC/调度抖动）；无限制时自动 no-op。
	_ "go.uber.org/automaxprocs"
)

// version 构建时 -ldflags "-X main.version=..." 注入（-X 只能改写字符串变量）。
var version = "dev"

func main() {
	bootstrap.Run(bootstrap.Options{
		Name:          "map",
		DefaultConfig: "configs/map/map-dev.yaml",
		Version:       version,
		// 地图是「等在途事务」型服务：超时太短会把已提交的命令截断成结果未知。
		// 30s 是原实现的经验值（比游戏服短，因为它没有 Actor 排空与 Kafka 刷出）。
		ShutdownBudget: 30 * time.Second,
		Build:          buildMapServer,
	})
}

// buildMapServer 组装地图服务。失败即拒绝启动（bootstrap 会打日志并非零退出）。
func buildMapServer(cfg *config.Config, logger *log.Logger) (bootstrap.Server, error) {
	if !cfg.Map.Enabled {
		// 显式失败而不是静默空转：map 进程起来却没启用地图，说明配置指向错了
		// 文件（比如拿了 game 的配置），静默运行会让人误以为部署成功。
		return nil, errors.New("map.enabled=false，该进程无任何世界可服务，请检查配置")
	}

	verifier, err := tokenVerifier(cfg)
	if err != nil {
		return nil, fmt.Errorf("map 鉴权配置无效: %w", err)
	}

	// MySQL 连接配置从顶层段取（与其它服务共用一套连接参数）；
	// 为空则走内存模式，适合本地联调。
	base, err := worldmap.New(cfg.Map, loggerAdapter{logger},
		worldmap.WithMySQL(cfg.MySQL),
		worldmap.WithTokenVerifier(verifier),
	)
	if err != nil {
		return nil, fmt.Errorf("map server init failed: %w", err)
	}

	// 注册必须在 Start 之前：gRPC 不允许运行中改服务表。
	rs := rpcpkg.NewServer(cfg.Server.GRPCAddr, logger)
	base.API().Register(rs.GRPCServer())

	return &mapServer{base: base, rs: rs, logger: logger, cfg: cfg}, nil
}

// mapServer 把 worldmap.Server 适配成 bootstrap.Server。
//
// 适配点：生命周期方法名（Run/Shutdown）、gRPC 的起停（外部持有）、
// metrics（本进程原先没有）。
type mapServer struct {
	base       *worldmap.Server
	rs         *rpcpkg.Server
	logger     *log.Logger
	cfg        *config.Config
	metricsSrv *metrics.Server
}

// Start 起指标与 gRPC，然后阻塞在世界管理器上。
//
// 顺序：指标最先（它失败要尽早暴露，且它不影响业务正确性——放在业务之前
// 起，业务起来时就已可观测）；gRPC 次之（客户端连上时服务已注册）；
// 最后才是 Run（加载世界、跑场景循环）。
func (m *mapServer) Start(ctx context.Context) error {
	if m.cfg.Metrics.Enable {
		metrics.Init(m.cfg.Server.Name)
		mSrv, err := metrics.NewServer(m.cfg.Metrics.Addr, nil)
		if err != nil {
			return fmt.Errorf("metrics server: %w", err)
		}
		mSrv.Start()
		m.metricsSrv = mSrv
		m.logger.Info("metrics server started", "addr", mSrv.Addr())
	}

	go func() {
		if err := m.rs.Start(ctx); err != nil && ctx.Err() == nil {
			m.logger.Error("grpc server stopped with error", "err", err)
		}
	}()

	return m.base.Run(ctx)
}

// Shutdown 先停 gRPC（不再接新命令），再交给世界管理器收尾（拒新写 →
// 等在途事务 → 关场景 → 释放租约），最后停指标。
//
// gRPC 必须在最前：世界管理器的排空依赖「不再有新命令进来」，
// 反序会让排空永远收敛不到零。
func (m *mapServer) Shutdown(ctx context.Context) error {
	m.rs.Stop()
	err := m.base.Shutdown(ctx)
	if m.metricsSrv != nil {
		err = errors.Join(err, m.metricsSrv.Stop(ctx))
	}
	return err
}

// tokenVerifier 构造内部 RPC 鉴权器。
//
// dev/test 环境用静态 token（配置 map 段暂时没有 token 字段，先用环境变量
// MAP_INTERNAL_TOKEN）；生产未配置 token 时直接拒绝启动——
// 地图服务暴露的是「改别人家城池」的能力，不能默认裸奔。
func tokenVerifier(cfg *config.Config) (maprpc.TokenVerifier, error) {
	token := os.Getenv("MAP_INTERNAL_TOKEN")
	if token == "" {
		// 生产环境没有 token 等于允许任意内部调用者伪造 player_id，
		// 这是「改别人家城池」级别的风险，必须拒绝启动。
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
// runtime 是纯领域运行时，测试里只需要「能记日志」，不应该被迫
// 拉起真实日志器（它可能写文件、连 Kafka）。三个方法的小接口
// 让测试可以传一个把日志收进 slice 的假实现。
type loggerAdapter struct{ l *log.Logger }

func (a loggerAdapter) Info(msg string, args ...any)  { a.l.Info(msg, args...) }
func (a loggerAdapter) Warn(msg string, args ...any)  { a.l.Warn(msg, args...) }
func (a loggerAdapter) Error(msg string, args ...any) { a.l.Error(msg, args...) }
