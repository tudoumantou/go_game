// Package cross 小组跨服节点（设计文档第 10 章）
//
// 架构位置：
//   - 不注册 etcd、不连网关（10.1）—— 只被组内游戏服主动连接；
//   - 复用 pkg/actor 运行时（房间 Actor）、gRPC Server、pkg/metrics；
//   - 房间数据纯内存，结算结果经 CrossService 流回推参战游戏服（不落 MySQL）。
//
// 本文件是组装层（3.3）：指标 → Actor 系统 → CrossService → 房间管理器 →
// gRPC 监听 → 指标轮询。存在一个构造环（rpc.Server 需要 Manager 作 rooms、
// Manager 需要 rpc.Server 作 Sink），用两段式初始化打破：先 NewServer(rooms=nil)，
// 再 NewManager，最后 AttachRooms——详见 rpc.AttachRooms 注释。
package cross

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/cross/battle"
	"game_study/internal/cross/rpc"
	"game_study/internal/cross/seat"
	"game_study/pkg/actor"
	"game_study/pkg/config"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	rpcpkg "game_study/pkg/rpc"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Server 小组跨服节点（组装层入口，职责见设计文档 3.1 目录树）
//
// 五个组件的依赖链：actorSys（房间运行时）← rooms（管理器）← crossRPC
// （协议入口）← rpcServer（监听）。metricsSrv 独立。New 只填 cfg/logger，
// 其余全部在 Start 内创建——跨服节点没有任何可提前装配的有状态组件
// （不连 Redis/MySQL），这也是它能按小组随意扩容的原因。
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	metricsSrv *metrics.Server
	actorSys   *actor.System
	rooms      *battle.Manager
	seatRooms  *seat.Manager
	crossRPC   *rpc.Server
	rpcServer  *rpcpkg.Server
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
// 此时只做 logger 装配：跨服节点无外部依赖，全部创建推迟到 Start，
// 保证 New 永不失败、可安全用于测试。
func New(cfg *config.Config) *Server {
	return &Server{
		cfg:    cfg,
		logger: log.New("cross", log.WithLevel(cfg.Log.Level)),
	}
}

// Start 启动服务（阻塞至 ctx 取消）。
// 顺序即依赖：Actor 系统要先于房间（房间要 Spawn 进去），rpc.Server 要先于
// Manager（Manager 的 Sink 是它），监听放最后（第一个游戏服连上时
// 内部必须全就绪）。任一步失败直接返回，由进程管理器拉起重试。
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
	// rooms 传 nil：两段式初始化的上半段，见 AttachRooms 注释——
	// 直接传 Manager 会构成构造环（互相需要对方）
	s.crossRPC = rpc.NewServer(s.cfg.Server.Name, nil, s.logger)
	s.rooms = battle.NewManager(s.actorSys, s.crossRPC, s.logger)
	s.crossRPC.AttachRooms(s.rooms)

	// 抢位置房间：常驻设施，启动即拉起；所有游戏服统一把座位流量发到
	// 配置指定的那个 cross 节点（cross.seat_addr），这里才承载共享座位。
	s.seatRooms = seat.NewManager(s.actorSys, s.crossRPC, s.logger)
	s.crossRPC.AttachSeats(s.seatRooms)

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
// 10s 一拍：房间数是慢变量，秒级没有意义；metrics 未启用时整轮 skip——
// 探针式检查放在循环内而不是起不起线程，省一个 goroutine。
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
// 只查 actorSys：房间可以慢热（等玩家），但 Actor 系统没起说明
// 进程半死，该摘流量。
func (s *Server) readiness(_ context.Context) error {
	if s.actorSys == nil {
		return fmt.Errorf("actor system not started")
	}
	return nil
}

// Shutdown 优雅关闭（10.5）：不接受新房间 → 存量房间兜底结算（最多 5 分钟）→ 退流。
//
// 顺序不可换的三点：Drain 要在停流之前（房间结算结果还要经流推给游戏服）；
// 退流要在 GracefulStop 之前（gRPC 会永远等双向流自己结束）；停 Actor
// 要在最后（房间全部 settled 后它的邮箱才空）。
//
// drainTimeout 双重封顶：配置的 Drain 时长与 ctx 剩余超时取小——
// 总超时先到时强停，保证整个关闭流程不超 main 给的预算。
func (s *Server) Shutdown(ctx context.Context) error {
	// ① 拒新房 + 存量兜底结算
	// closing 标志先置位：Drain 里第一件事，保证关闭期间到达的
	// CreateReq 被拒绝而不是又开一间永远等不到结算的新房
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
	// 断流前 Drain 已把结算推完：这里断的是「空转的流」，玩家侧
	// 早已收到结果并离房
	if s.crossRPC != nil {
		s.crossRPC.CloseAllPeers()
	}
	// ③ 停 gRPC（带超时：2s 内让正在写的帧发完，超时强制断）
	if s.rpcServer != nil {
		s.rpcServer.StopWithTimeout(2 * time.Second)
	}
	// ④ 停 Actor（房间全部结算完）
	// Stop 会等 mailbox 排空：settle 推送是异步的，直接杀进程会让
	// 最后几帧结果丢在队列里——玩家看到「战斗结束但没结算」
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
