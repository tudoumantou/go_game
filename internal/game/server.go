// Package game 游戏服（设计文档第 8 章）
package game

import (
	"context"
	"fmt"
	"sync"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/attr"
	"game_study/internal/game/module/bag"
	"game_study/internal/game/module/chat"
	"game_study/internal/game/module/item"
	"game_study/internal/game/module/mail"
	gamemq "game_study/internal/game/mq"
	"game_study/internal/game/player"
	"game_study/internal/game/rpc"
	"game_study/pkg/actor"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	gameetcd "game_study/pkg/registry/etcd"
	rpcpkg "game_study/pkg/rpc"

	internalv2 "game_study/pkg/pb/internalv2"
)

// onlineTTL 在线状态 TTL（客户端 15s 心跳，3 倍冗余）。
const onlineTTL = 45 * time.Second

// Server 游戏服（组装层入口，职责见设计文档 3.1 目录树）
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	rpcServer  *rpcpkg.Server
	etcdReg    *gameetcd.Registry
	etcdCancel context.CancelFunc
	gameRPC    *rpc.GameServer

	actorSys *actor.System
	modules  *module.Registry
	players  *player.Manager
	database *db.DB
	repo     *player.Repository
	flusher  *flush.Flusher

	redis     *cache.Client
	presence  *cache.Presence
	producer  *pkgmq.Producer
	consumers *gamemq.Group

	mu       sync.RWMutex
	sessions map[uint64]int64 // session_id -> player_id
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server {
	return &Server{
		cfg:      cfg,
		logger:   log.New("game", log.WithLevel(cfg.Log.Level)),
		sessions: make(map[uint64]int64),
	}
}

// Start 启动服务（阻塞至 ctx 取消）。
func (s *Server) Start(ctx context.Context) error {
	// ① MySQL（业务库）
	if s.cfg.MySQL.DSN != "" {
		database, err := db.New(db.Config{
			DSN:     s.cfg.MySQL.DSN,
			MaxOpen: s.cfg.MySQL.MaxOpenConns,
			MaxIdle: s.cfg.MySQL.MaxIdleConns,
		})
		if err != nil {
			return fmt.Errorf("mysql: %w", err)
		}
		s.database = database
		s.repo = player.NewRepository(database)
	}

	// ② Redis（在线状态 / 幂等 / 离线暂存）
	var deduper pkgmq.Deduper = pkgmq.NoopDeduper{}
	if len(s.cfg.Redis.Addrs) > 0 {
		redisClient, err := cache.New(cache.RedisConfig{
			Addrs:    s.cfg.Redis.Addrs,
			Password: s.cfg.Redis.Password,
			DB:       s.cfg.Redis.DB,
			PoolSize: s.cfg.Redis.PoolSize,
			MinIdle:  s.cfg.Redis.MinIdleConn,
		})
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		s.redis = redisClient
		s.presence = cache.NewPresence(redisClient.GetClient())
		deduper = cache.NewRedisDeduper(redisClient.GetClient())
	}

	// ③ Actor 系统 + 模块
	s.actorSys = actor.NewSystem(actor.Config{
		ShardCount:  s.cfg.Actor.ShardCount,
		MailboxSize: s.cfg.Actor.MailboxSize,
	})
	s.actorSys.Start(ctx)

	s.modules = module.NewRegistry()
	s.modules.Register(attr.New(s.logger))
	s.modules.Register(bag.New(s.logger))
	s.modules.Register(item.New(s.logger))

	var mailMod *mail.Module
	var chatMod *chat.Module
	if s.database != nil {
		mailMod = mail.New(mail.NewRepo(s.database), s.logger)
		chatMod = chat.New(chat.NewRepo(s.database), s.logger)
		s.modules.Register(mailMod)
		s.modules.Register(chatMod)
	}

	// ④ Write-Behind flusher
	flushInterval, _ := config.ParseDuration(s.cfg.Flush.Interval)
	stagger, _ := config.ParseDuration(s.cfg.Flush.Stagger)
	s.flusher = flush.New(flush.Config{
		ShardCount: s.cfg.Flush.ShardCount,
		Interval:   flushInterval,
		Stagger:    stagger,
		BatchRows:  s.cfg.Flush.BatchRows,
	}, s.persistPlayer, s.logger)
	s.flusher.Start(ctx)

	s.players = player.NewManager(s.actorSys, s.modules, s.logger, s.repo, s.flusher.MarkDirty)

	// ⑤ Kafka 消费者（充值/邮件/跨服私聊）
	if len(s.cfg.Kafka.Brokers) > 0 {
		s.producer = pkgmq.NewProducer(pkgmq.Config{
			Brokers:      s.cfg.Kafka.Brokers,
			RequiredAcks: s.cfg.Kafka.ProducerRequiredAcks,
		})
		group, err := gamemq.NewGroup(s.cfg, gamemq.Deps{
			Deduper:  deduper,
			Presence: s.presence,
			Players:  s.players,
			Sys:      s.actorSys,
			Producer: s.producer,
			Mail:     mailMod,
			Chat:     chatMod,
			Logger:   s.logger,
			NodeID:   s.cfg.Server.Name,
		})
		if err != nil {
			return fmt.Errorf("kafka consumers: %w", err)
		}
		s.consumers = group
		group.Run(ctx)
		s.logger.Info("kafka consumers started", "count", group.Len())
	}

	// ⑥ etcd 注册
	etcdReg, err := gameetcd.New(s.cfg.Etcd.Endpoints)
	if err != nil {
		return fmt.Errorf("etcd: %w", err)
	}
	s.etcdReg = etcdReg

	node := gameetcd.Node{
		ID:      s.cfg.Server.Name,
		Service: "game",
		Addr:    s.cfg.Server.GRPCAddr,
		Version: "1.0.0",
	}
	cancel, err := etcdReg.RegisterWithKeepalive(ctx, node, s.cfg.Etcd.LeaseTTL)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	s.etcdCancel = cancel

	// ⑦ gRPC server
	s.gameRPC = rpc.NewGameServer(s, s.logger)
	s.rpcServer = rpcpkg.NewServer(s.cfg.Server.GRPCAddr, s.logger)
	internalv2.RegisterGameServiceServer(s.rpcServer.GRPCServer(), s.gameRPC)

	go func() {
		if err := s.rpcServer.Start(ctx); err != nil && err != context.Canceled {
			s.logger.Error("grpc server error", "err", err)
		}
	}()

	s.logger.Info("GameServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"players", s.players.Count())
	<-ctx.Done()
	return ctx.Err()
}

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，pkg/shutdown 编排）。
func (s *Server) Shutdown(ctx context.Context) error {
	// DRAIN：摘注册
	if s.etcdCancel != nil {
		s.etcdCancel()
	}
	// FLUSH：全量落库（Actor 停止前）
	if s.flusher != nil {
		if err := s.flusher.FlushAll(ctx); err != nil {
			s.logger.Warn("flush all", "err", err)
		}
		s.flusher.Stop()
	}
	// STOP：停消费者 → 停 gRPC → 停 Actor → Flush 生产者
	if s.consumers != nil {
		if err := s.consumers.Close(); err != nil {
			s.logger.Warn("consumer close", "err", err)
		}
	}
	if s.rpcServer != nil {
		s.rpcServer.Stop()
	}
	if s.actorSys != nil {
		s.actorSys.Stop()
	}
	if s.producer != nil {
		_ = s.producer.Flush(ctx)
		if err := s.producer.Close(); err != nil {
			s.logger.Warn("producer close", "err", err)
		}
	}
	if s.etcdReg != nil {
		_ = s.etcdReg.Close()
	}
	if s.database != nil {
		_ = s.database.Close()
	}
	if s.redis != nil {
		_ = s.redis.Close()
	}
	s.logger.Info("game stopped")
	return nil
}

// persistPlayer 由 flusher 回调，把单个玩家落库。
func (s *Server) persistPlayer(ctx context.Context, playerID int64) error {
	pid, ok := s.players.Get(playerID)
	if !ok {
		return nil
	}
	_ = s.actorSys.Send(pid, &player.SaveMsg{})
	return nil
}

// OnBind 实现 rpc.StreamHandler。
func (s *Server) OnBind(ctx context.Context, sessionID uint64, playerID int64) error {
	if playerID == 0 {
		return fmt.Errorf("player_id is required for bind")
	}
	pid, err := s.players.GetOrSpawn(playerID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.sessions[sessionID] = playerID
	s.mu.Unlock()

	if err := s.actorSys.SendWithSender(pid, &player.BindMsg{SessionID: sessionID, PlayerID: playerID}, nil); err != nil {
		return err
	}
	// 写在线状态（Kafka 消费者据此做本地性判定）
	if s.presence != nil {
		if err := s.presence.SetOnline(ctx, playerID, s.cfg.Server.Name, onlineTTL); err != nil {
			s.logger.Warn("set online failed", "player_id", playerID, "err", err)
		}
	}
	s.logger.Info("session bound", "session_id", sessionID, "player_id", playerID)
	return nil
}

// OnUnbind 实现 rpc.StreamHandler。
func (s *Server) OnUnbind(ctx context.Context, sessionID uint64) error {
	s.mu.Lock()
	playerID, ok := s.sessions[sessionID]
	delete(s.sessions, sessionID)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	if pid, ok := s.players.Get(playerID); ok {
		_ = s.actorSys.Send(pid, &player.UnbindMsg{SessionID: sessionID})
	}
	// 摘在线状态（玩家可能已迁移到其它服，仅在归属本服时删除）
	if s.presence != nil {
		if node, online := s.presence.IsOnline(ctx, playerID); online && node == s.cfg.Server.Name {
			_ = s.presence.SetOffline(ctx, playerID)
		}
	}
	return nil
}

// OnClientPacket 实现 rpc.StreamHandler。
func (s *Server) OnClientPacket(ctx context.Context, sessionID uint64, msgID uint32, seq uint32, body []byte) error {
	s.mu.RLock()
	playerID, ok := s.sessions[sessionID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("session %d not bound", sessionID)
	}
	pid, ok := s.players.Get(playerID)
	if !ok {
		return fmt.Errorf("player %d not online", playerID)
	}
	return s.actorSys.Send(pid, &player.PacketMsg{MsgID: msgID, Seq: seq, Body: body})
}
