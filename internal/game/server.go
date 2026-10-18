// Package game 游戏服（设计文档第 8 章）
package game

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/activity"
	"game_study/internal/game/module/attr"
	"game_study/internal/game/module/bag"
	"game_study/internal/game/module/chat"
	"game_study/internal/game/module/copy"
	"game_study/internal/game/module/equip"
	"game_study/internal/game/module/guild"
	"game_study/internal/game/module/item"
	"game_study/internal/game/module/mail"
	"game_study/internal/game/module/profile"
	"game_study/internal/game/module/quest"
	"game_study/internal/game/module/shop"
	"game_study/internal/game/module/skill"
	"game_study/internal/game/module/team"
	gamemq "game_study/internal/game/mq"
	"game_study/internal/game/player"
	"game_study/internal/game/rpc"
	"game_study/internal/game/service"
	"game_study/pkg/actor"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	gameetcd "game_study/pkg/registry/etcd"
	rpcpkg "game_study/pkg/rpc"

	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"

	"google.golang.org/protobuf/proto"
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
	roleSvc   *service.RoleService

	mu       sync.RWMutex
	sessions map[uint64]int64 // session_id -> player_id（0 = 未选角）
	accounts map[uint64]int64 // session_id -> account_id
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server {
	return &Server{
		cfg:      cfg,
		logger:   log.New("game", log.WithLevel(cfg.Log.Level)),
		sessions: make(map[uint64]int64),
		accounts: make(map[uint64]int64),
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
		s.roleSvc = service.NewRoleService(database, s.logger)
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
	s.modules.Register(profile.New(s.logger))
	s.modules.Register(attr.New(s.logger))
	s.modules.Register(bag.New(s.logger))
	s.modules.Register(item.New(s.logger))

	var mailMod *mail.Module
	var chatMod *chat.Module
	if s.database != nil {
		mailMod = mail.New(mail.NewRepo(s.database), s.logger)
		chatMod = chat.New(chat.NewRepo(s.database), chatMessenger{srv: s}, s.logger)
		s.modules.Register(mailMod)
		s.modules.Register(chatMod)
		s.modules.Register(quest.New(quest.NewRepo(s.database), s.logger))
		s.modules.Register(equip.New(equip.NewRepo(s.database), s.logger))
		s.modules.Register(skill.New(skill.NewRepo(s.database), s.logger))
		s.modules.Register(guild.New(guild.NewRepo(s.database), s.logger))
		s.modules.Register(team.New(team.NewRepo(s.database), s.logger))
		s.modules.Register(copy.New(copy.NewRepo(s.database), s.logger))
		s.modules.Register(shop.New(shop.NewRepo(s.database), s.logger))
		s.modules.Register(activity.New(activity.NewRepo(s.database), s.logger))
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

	s.players = player.NewManager(s.actorSys, s.modules, s.logger, s.repo, s.flusher.MarkDirty, s.sendToClient)

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

// chatMessenger 实现 chat.Messenger（在线判定/本服推送/跨服生产/离线暂存）。
type chatMessenger struct {
	srv *Server
}

// SelfNode 实现 chat.Messenger。
func (m chatMessenger) SelfNode() string { return m.srv.cfg.Server.Name }

// OnlineNode 实现 chat.Messenger。
func (m chatMessenger) OnlineNode(ctx context.Context, playerID int64) (string, bool) {
	if m.srv.presence == nil {
		// 无 Redis：退化为本地在线判断
		_, ok := m.srv.players.Get(playerID)
		return m.srv.cfg.Server.Name, ok
	}
	return m.srv.presence.IsOnline(ctx, playerID)
}

// Push 实现 chat.Messenger。
func (m chatMessenger) Push(ctx context.Context, playerID int64, msgID uint32, body []byte) bool {
	pid, ok := m.srv.players.Get(playerID)
	if !ok {
		return false
	}
	return m.srv.actorSys.Send(pid, &player.PushMsg{MsgID: msgID, Body: body}) == nil
}

// ProduceCross 实现 chat.Messenger。
func (m chatMessenger) ProduceCross(ctx context.Context, msg *pkgmq.ChatCrossMsg) error {
	if m.srv.producer == nil {
		return fmt.Errorf("kafka producer not available")
	}
	env, err := pkgmq.NewEnvelope(fmt.Sprintf("%d", msg.MsgID), pkgmq.TopicChatCross, fmt.Sprintf("%d", msg.ToID), msg)
	if err != nil {
		return err
	}
	return m.srv.producer.ProduceEnvelope(ctx, env)
}

// PendOffline 实现 chat.Messenger。
func (m chatMessenger) PendOffline(ctx context.Context, playerID int64, msg *pkgmq.ChatCrossMsg) error {
	if m.srv.presence == nil {
		return nil
	}
	env, err := pkgmq.NewEnvelope(fmt.Sprintf("%d", msg.MsgID), pkgmq.TopicChatCross, fmt.Sprintf("%d", playerID), msg)
	if err != nil {
		return err
	}
	raw, err := env.Marshal()
	if err != nil {
		return err
	}
	return m.srv.presence.PendPush(ctx, "chat", playerID, raw, 200, 0)
}

// sendToClient 下行发送：把模块响应经 GameService.Channel 写回网关（18.2 同流保证有序）。
func (s *Server) sendToClient(sessionID uint64, msgID uint32, body []byte) error {
	if s.gameRPC == nil {
		return fmt.Errorf("game rpc not ready")
	}
	frame := &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_ClientPacket{
			ClientPacket: &internalv2.ClientPacket{
				SessionId: sessionID,
				MsgId:     msgID,
				Body:      body,
			},
		},
	}
	return s.gameRPC.SendToSession(sessionID, frame)
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

// 会话级协议号（未选角时由组装层直接处理，不进 Actor）。
const (
	msgIDEnterGame  uint32 = 1010
	msgIDCreateRole uint32 = 1020
)

// OnBind 实现 rpc.StreamHandler：按 account_id 查角色，有角色才拉起 Actor。
func (s *Server) OnBind(ctx context.Context, sessionID uint64, accountID, playerID int64) error {
	s.mu.Lock()
	s.accounts[sessionID] = accountID
	s.sessions[sessionID] = 0
	s.mu.Unlock()

	if playerID > 0 {
		return s.bindPlayer(ctx, sessionID, playerID)
	}
	if accountID <= 0 || s.roleSvc == nil {
		return nil
	}
	role, err := s.roleSvc.FindByAccount(ctx, accountID)
	if err != nil {
		// 无角色属于正常情况：等客户端发 CreateRoleReq / EnterGameReq
		s.logger.Info("session bind without role", "session_id", sessionID, "account_id", accountID)
		return nil
	}
	return s.bindPlayer(ctx, sessionID, role.Id)
}

// bindPlayer 拉起玩家 Actor 并绑定会话。
func (s *Server) bindPlayer(ctx context.Context, sessionID uint64, playerID int64) error {
	pid, err := s.players.GetOrSpawn(playerID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.sessions[sessionID] = playerID
	accountID := s.accounts[sessionID]
	s.mu.Unlock()

	if err := s.actorSys.SendWithSender(pid, &player.BindMsg{SessionID: sessionID, PlayerID: playerID}, nil); err != nil {
		return err
	}
	if s.presence != nil {
		if err := s.presence.SetOnline(ctx, playerID, s.cfg.Server.Name, onlineTTL); err != nil {
			s.logger.Warn("set online failed", "player_id", playerID, "err", err)
		}
	}
	s.logger.Info("session bound", "session_id", sessionID, "player_id", playerID, "account_id", accountID)
	return nil
}

// OnUnbind 实现 rpc.StreamHandler。
func (s *Server) OnUnbind(ctx context.Context, sessionID uint64) error {
	s.mu.Lock()
	playerID := s.sessions[sessionID]
	delete(s.sessions, sessionID)
	delete(s.accounts, sessionID)
	s.mu.Unlock()
	if playerID == 0 {
		return nil
	}
	if pid, ok := s.players.Get(playerID); ok {
		_ = s.actorSys.Send(pid, &player.UnbindMsg{SessionID: sessionID})
	}
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
	playerID := s.sessions[sessionID]
	accountID := s.accounts[sessionID]
	s.mu.RUnlock()

	// 未选角：仅放行进游戏/创角两条会话级协议
	if playerID == 0 {
		switch msgID {
		case msgIDEnterGame:
			return s.handleEnterGame(ctx, sessionID, accountID, body)
		case msgIDCreateRole:
			return s.handleCreateRole(ctx, sessionID, accountID, body)
		default:
			return fmt.Errorf("session %d has no role, drop msg %d", sessionID, msgID)
		}
	}

	pid, ok := s.players.Get(playerID)
	if !ok {
		return fmt.Errorf("player %d not online", playerID)
	}
	return s.actorSys.Send(pid, &player.PacketMsg{MsgID: msgID, Seq: seq, Body: body})
}

// handleEnterGame 进游戏：返回已有角色，无角色回 2003 引导创角。
func (s *Server) handleEnterGame(ctx context.Context, sessionID uint64, accountID int64, body []byte) error {
	var req gamev1.EnterGameReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode EnterGameReq: %w", err)
	}
	resp := &gamev1.EnterGameResp{Code: 0, ServerTime: time.Now().Format(time.RFC3339)}

	if s.roleSvc == nil {
		resp.Code = 2003
		return s.reply(sessionID, 1011, resp)
	}
	role, err := s.roleSvc.FindByAccount(ctx, accountID)
	if err != nil {
		resp.Code = 2003 // 无角色
		return s.reply(sessionID, 1011, resp)
	}
	resp.RoleId = role.Id
	if err := s.bindPlayer(ctx, sessionID, role.Id); err != nil {
		return err
	}
	return s.reply(sessionID, 1011, resp)
}

// handleCreateRole 创角：校验 → 落库 → 拉起 Actor → 回包。
func (s *Server) handleCreateRole(ctx context.Context, sessionID uint64, accountID int64, body []byte) error {
	var req gamev1.CreateRoleReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode CreateRoleReq: %w", err)
	}

	resp := &gamev1.CreateRoleResp{}
	if s.roleSvc == nil {
		resp.Code = 5000
		return s.reply(sessionID, 1021, resp)
	}
	roleID, err := s.roleSvc.Create(ctx, accountID, req.Name, req.Career)
	if err != nil {
		resp.Code = mapCreateRoleErr(err)
		s.logger.Info("create role failed", "account_id", accountID, "err", err)
		return s.reply(sessionID, 1021, resp)
	}
	resp.Code = 0
	resp.RoleId = roleID
	if err := s.bindPlayer(ctx, sessionID, roleID); err != nil {
		return err
	}
	return s.reply(sessionID, 1021, resp)
}

// reply 序列化并下行。
func (s *Server) reply(sessionID uint64, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.sendToClient(sessionID, msgID, body)
}

// mapCreateRoleErr 错误 → 业务码（4.10）。
func mapCreateRoleErr(err error) int32 {
	switch {
	case errors.Is(err, service.ErrNameUsed):
		return 2004
	case errors.Is(err, service.ErrNameIllegal):
		return 2005
	case errors.Is(err, service.ErrCareerBad):
		return 2001
	default:
		return 5000
	}
}
