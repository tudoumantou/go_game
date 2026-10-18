// Package cross 小组跨服节点（设计文档第 10 章）
//
// 架构位置：
//   - 不注册 etcd、不连网关（10.1）—— 只被组内游戏服主动连接；
//   - 复用 pkg/actor 运行时（房间 Actor）、gRPC Server、pkg/metrics；
//   - 房间数据纯内存，结算结果经 CrossService 流回推参战游戏服（不落 MySQL）。
package cross

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/cross/battle"
	"game_study/internal/cross/rpc"
	"game_study/pkg/actor"
	"game_study/pkg/config"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	rpcpkg "game_study/pkg/rpc"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Server 小组跨服节点（组装层入口，职责见设计文档 3.1 目录树）
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	metricsSrv *metrics.Server
	actorSys   *actor.System
	rooms      *battle.Manager
	crossRPC   *rpc.Server
	rpcServer  *rpcpkg.Server
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server {
	return &Server{
		cfg:    cfg,
		logger: log.New("cross", log.WithLevel(cfg.Log.Level)),
	}
}

// Start 启动服务（阻塞至 ctx 取消）。
func (s *Server) Start(ctx context.Context) error {
	// ① 指标
	if s.cfg.Metrics.Enable {
		metrics.Init(s.cfg.Server.Name)
		mSrv, err := metrics.NewServer(s.cfg.Metrics.Addr, s.readiness)
		if err != nil {
			return fmt.Errorf("metrics server: %w", err)
		}
		mSrv.Start()
		s.metricsSrv = mSrv
		s.logger.Info("metrics server started", "addr", mSrv.Addr())
	}

	// ② Actor 运行时（承载房间 Actor）
	s.actorSys = actor.NewSystem(actor.Config{
		ShardCount:  s.cfg.Actor.ShardCount,
		MailboxSize: s.cfg.Actor.MailboxSize,
		Logger:      s.logger,
	})
	s.actorSys.Start(ctx)

	// ③ CrossService 服务端（先建 rpc.Server，再注入给房间管理器作 Sink）
	s.crossRPC = rpc.NewServer(s.cfg.Server.Name, nil, s.logger)
	s.rooms = battle.NewManager(s.actorSys, s.crossRPC, s.logger)
	s.crossRPC.AttachRooms(s.rooms)

	// ④ gRPC server
	s.rpcServer = rpcpkg.NewServer(s.cfg.Server.GRPCAddr, s.logger)
	internalv2.RegisterCrossServiceServer(s.rpcServer.GRPCServer(), s.crossRPC)

	go func() {
		if err := s.rpcServer.Start(ctx); err != nil && err != context.Canceled {
			s.logger.Error("grpc server error", "err", err)
		}
	}()

	// ⑤ 房间指标轮询
	go s.reportRoomStats(ctx)

	s.logger.Info("CrossServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"node", s.cfg.Server.Name)
	<-ctx.Done()
	return ctx.Err()
}

// reportRoomStats 定期上报房间与节点指标。
func (s *Server) reportRoomStats(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !metrics.Enabled() {
				continue
			}
			st := s.rooms.Stats()
			metrics.CrossRooms("waiting", st["waiting"])
			metrics.CrossRooms("running", st["running"])
			metrics.CrossRooms("total", st["total"])
			metrics.ChannelStreams(s.crossRPC.PeerCount())
		}
	}
}

// readiness 就绪探针：跨服节点没有外部依赖，Actor 起来即就绪。
func (s *Server) readiness(_ context.Context) error {
	if s.actorSys == nil {
		return fmt.Errorf("actor system not started")
	}
	return nil
}

// Shutdown 优雅关闭（10.5）：不接受新房间 → 存量房间兜底结算（最多 5 分钟）→ 退流。
func (s *Server) Shutdown(ctx context.Context) error {
	// ① 拒新房 + 存量兜底结算
	if s.rooms != nil {
		drainTimeout := 5 * time.Minute
		if d, err := config.ParseDuration(s.cfg.Shutdown.DrainTimeout); err == nil && d > 0 {
			drainTimeout = d
		}
		if deadline, ok := ctx.Deadline(); ok {
			// 总超时更短时以总超时为准
			if remain := time.Until(deadline); remain < drainTimeout {
				drainTimeout = remain
			}
		}
		s.rooms.Drain(drainTimeout)
	}

	// ② 主动断所有游戏服流 —— 否则 GracefulStop 会一直等这些永不结束的双向流
	if s.crossRPC != nil {
		s.crossRPC.CloseAllPeers()
	}
	// ③ 停 gRPC（带超时：2s 内让正在写的帧发完，超时强制断）
	if s.rpcServer != nil {
		s.rpcServer.StopWithTimeout(2 * time.Second)
	}
	// ④ 停 Actor（房间全部结算完）
	if s.actorSys != nil {
		s.actorSys.Stop()
	}
	// ⑤ 停指标
	if s.metricsSrv != nil {
		_ = s.metricsSrv.Stop(ctx)
	}
	s.logger.Info("cross stopped")
	return nil
}
