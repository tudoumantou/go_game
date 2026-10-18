// Package game 游戏服（设计文档第 8 章）
package game

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	gamecenter "game_study/internal/game/center"
	gamecross "game_study/internal/game/cross"
	"game_study/internal/game/module"
	"game_study/internal/game/module/activity"
	"game_study/internal/game/module/attr"
	"game_study/internal/game/module/bag"
	"game_study/internal/game/module/battle"
	"game_study/internal/game/module/chat"
	"game_study/internal/game/module/copy"
	"game_study/internal/game/module/equip"
	"game_study/internal/game/module/guild"
	"game_study/internal/game/module/item"
	"game_study/internal/game/module/mail"
	"game_study/internal/game/module/profile"
	"game_study/internal/game/module/quest"
	"game_study/internal/game/module/shop"
	"game_study/internal/game/module/signin"
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
	"game_study/pkg/metrics"
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

	redis      *cache.Client
	presence   *cache.Presence
	producer   *pkgmq.Producer
	consumers  *gamemq.Group
	roleSvc    *service.RoleService
	metricsSrv *metrics.Server
	idTable    *cache.IDTable // ID 表：存在性判断从 MySQL 挪到 Redis（12.5）

	crossPool *gamecross.Pool    // 到跨服节点的连接池
	centerCli *gamecenter.Client // 到中心服的 Report 流
	battleMod *battle.Module     // 战斗模块（匹配/透传/结算）

	mu       sync.RWMutex
	sessions map[uint64]int64 // session_id -> player_id（0 = 未选角）
	accounts map[uint64]int64 // session_id -> account_id
	// players 反向索引：player_id -> session_id（跨服结算推送用）
	players2sess map[int64]uint64
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server {
	return &Server{
		cfg:          cfg,
		logger:       log.New("game", log.WithLevel(cfg.Log.Level)),
		sessions:     make(map[uint64]int64),
		accounts:     make(map[uint64]int64),
		players2sess: make(map[int64]uint64),
	}
}

// Start 启动服务（阻塞至 ctx 取消）。
func (s *Server) Start(ctx context.Context) error {
	// 指标（20.3）：注册服务名 + 起 /metrics、/healthz、/readyz
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

	// ②' ID 表（12.5）：Redis + MySQL 都可用才建。
	// 构建在后台进行：ready 未置位期间所有存在性判断回落 DB，服务可正常启动。
	if s.database != nil && s.redis != nil {
		s.idTable = cache.NewIDTable(s.redis.GetClient(), s.database.DB)
		if s.roleSvc != nil {
			s.roleSvc.SetIDTable(s.idTable)
		}
		go s.buildIDTables(ctx)
		go s.maintainIDTables(ctx)
	}

	// ③ Actor 系统 + 模块
	s.actorSys = actor.NewSystem(actor.Config{
		ShardCount:  s.cfg.Actor.ShardCount,
		MailboxSize: s.cfg.Actor.MailboxSize,
		Logger:      s.logger, // panic 恢复时记录堆栈（不再静默吞掉）
	})
	s.actorSys.Start(ctx)

	// 跨服连接池（P13）：在模块注册前建好，battle 模块依赖它
	if len(s.cfg.Cross.Nodes) > 0 {
		s.crossPool = gamecross.NewPool(s.cfg.Server.Name, crossHandler{srv: s}, s.logger)
	}
	// 中心服客户端（P8）：匹配主路径，未配置则 battle 降级为本服匹配
	if s.cfg.Center.Addr != "" {
		s.centerCli = gamecenter.NewClient(s.cfg.Server.Name, s.cfg.Center.Addr, centerHandler{srv: s}, s.logger)
	}
	s.battleMod = battle.New(s.crossPool, centerMatchAdapter{cli: s.centerCli}, gamePusher{srv: s}, s.logger)
	if d, err := config.ParseDuration(s.cfg.Cross.BattleTimeout); err == nil && d > 0 {
		s.battleMod.SetBattleTimeout(d)
	}
	// 排行上报（P14）：战斗结算后写全服竞技场榜
	if s.centerCli != nil {
		s.battleMod.SetRanker(rankReporter{cli: s.centerCli})
	}

	s.modules = module.NewRegistry()
	s.modules.Register(profile.New(s.logger))
	s.modules.Register(attr.New(s.logger))
	s.modules.Register(bag.New(s.logger))
	s.modules.Register(item.New(s.logger))
	s.modules.Register(s.battleMod)

	var mailMod *mail.Module
	var chatMod *chat.Module
	if s.database != nil {
		mailMod = mail.New(mail.NewRepo(s.database), s.logger)
		chatMod = chat.New(chat.NewRepo(s.database), chatMessenger{srv: s}, s.logger)
		s.modules.Register(mailMod)
		s.modules.Register(chatMod)
		s.modules.Register(quest.New(quest.NewRepo(s.database), s.logger))
		equipRepo := equip.NewRepo(s.database)
		if s.idTable != nil {
			equipRepo.SetIDTable(s.idTable)
		}
		s.modules.Register(equip.New(equipRepo, s.logger))
		s.modules.Register(skill.New(skill.NewRepo(s.database), s.logger))
		s.modules.Register(guild.New(guild.NewRepo(s.database), s.logger))
		s.modules.Register(team.New(team.NewRepo(s.database), s.logger))
		s.modules.Register(copy.New(copy.NewRepo(s.database), s.logger))
		s.modules.Register(shop.New(shop.NewRepo(s.database), s.logger))
		s.modules.Register(activity.New(activity.NewRepo(s.database), s.logger))
		s.modules.Register(signin.New(signin.NewRepo(s.database), s.logger))
	}

	// ④ Write-Behind flusher
	flushInterval, _ := config.ParseDuration(s.cfg.Flush.Interval)
	stagger, _ := config.ParseDuration(s.cfg.Flush.Stagger)
	// 墓碑删除（12.4.5）：业务登记 MarkDelete，本周期内合批 DELETE
	var deleteFn flush.DeleteFunc
	if s.database != nil {
		deleteFn = flush.NewSQLDeleter(s.database.DB).DeleteBatch
	}
	s.flusher = flush.New(flush.Config{
		ShardCount: s.cfg.Flush.ShardCount,
		Interval:   flushInterval,
		Stagger:    stagger,
		BatchRows:  s.cfg.Flush.BatchRows,
		DeleteFn:   deleteFn,
	}, s.persistPlayer, s.logger)
	s.flusher.Start(ctx)

	s.players = player.NewManager(s.actorSys, s.modules, s.logger, s.repo, s.flusher.MarkDirty, s.sendToClient)
	// 登录预加载（12.6）：5 表并行拉取 + 5s 超时保护
	if s.database != nil {
		preloadTimeout := player.DefaultPreloadTimeout
		s.players.SetPreloader(player.NewPreloader(s.database, s.logger, preloadTimeout))
	}

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

	// 建立到中心服的 Report 流（9.2：断流指数退避重连）
	if s.centerCli != nil {
		go s.centerCli.Start(ctx)
		s.logger.Info("center client started", "center", s.cfg.Center.Addr)
	}

	// 建立到跨服节点的 CrossService 流（10.2：断流指数退避重连）
	if s.crossPool != nil {
		s.crossPool.Start(ctx, s.cfg.Cross.Nodes)
		s.logger.Info("cross pool started", "nodes", s.cfg.Cross.Nodes)
	}

	s.logger.Info("GameServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"players", s.players.Count())
	<-ctx.Done()
	return ctx.Err()
}

// readiness 就绪探针：DB/Redis 可用才允许接流量（19.6 探针表）。
func (s *Server) readiness(ctx context.Context) error {
	if s.database != nil {
		if err := s.database.PingContext(ctx); err != nil {
			return fmt.Errorf("mysql: %w", err)
		}
	}
	if s.redis != nil {
		if err := s.redis.GetClient().Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
	}
	return nil
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
	// STOP：停跨服流 → 停消费者 → 停 gRPC → 停 Actor → Flush 生产者
	if s.centerCli != nil {
		s.centerCli.Stop()
	}
	if s.crossPool != nil {
		s.crossPool.Stop()
	}
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
	if s.metricsSrv != nil {
		_ = s.metricsSrv.Stop(ctx)
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
//
// 优先走 Actor（保证与玩家自身消息的处理顺序一致）；Actor 尚未创建时
// （玩家只登录、还没发过业务包）直接走会话下行 —— 推送是纯下行，不需要业务状态。
func (m chatMessenger) Push(ctx context.Context, playerID int64, msgID uint32, body []byte) bool {
	if pid, ok := m.srv.players.Get(playerID); ok {
		if err := m.srv.actorSys.Send(pid, &player.PushMsg{MsgID: msgID, Body: body}); err == nil {
			return true
		}
	}
	return m.srv.sendToPlayer(playerID, msgID, body)
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
	s.mu.Lock()
	s.sessions[sessionID] = playerID
	s.players2sess[playerID] = sessionID
	accountID := s.accounts[sessionID]
	s.mu.Unlock()

	s.markOnline(ctx, playerID)
	if err := s.bindActor(ctx, sessionID, playerID); err != nil {
		return err
	}
	s.logger.Info("session bound", "session_id", sessionID, "player_id", playerID, "account_id", accountID)
	return nil
}

// ensureActor 确保玩家 Actor 已创建并与当前会话绑定。
//
// 只在「Actor 不存在」或「会话变了（重连）」时才发 BindMsg —— 不能每个业务包都发：
// onBind 会跑一遍全部模块的 OnLogin（活动进度要写库），重复 Bind 等于每条消息
// 多两次落库，直接把延迟打到超时。
//
// 反过来，重连时必须补 Bind：Actor 离线后不销毁（Tier-2 保留 90s），
// 里面的 sessionID 还是旧的/0，不刷新的话回包发不出去，客户端只看到超时。
//
// presence 不在这里设置 —— 在线状态属于「会话」，不属于「业务状态」。
func (s *Server) ensureActor(ctx context.Context, sessionID uint64, playerID int64) error {
	if _, ok := s.players.Get(playerID); ok {
		s.mu.RLock()
		bound := s.players2sess[playerID]
		s.mu.RUnlock()
		if bound == sessionID {
			return nil // 已绑定到本会话
		}
	}
	return s.bindActor(ctx, sessionID, playerID)
}

// bindActor 无条件把会话绑定到 Actor（Actor 不存在则创建）。
func (s *Server) bindActor(ctx context.Context, sessionID uint64, playerID int64) error {
	pid, ok := s.players.Get(playerID)
	if !ok {
		spawned, err := s.players.GetOrSpawn(playerID)
		if err != nil {
			return err
		}
		pid = spawned
	}
	if err := s.actorSys.SendWithSender(pid,
		&player.BindMsg{SessionID: sessionID, PlayerID: playerID}, nil); err != nil {
		return err
	}
	s.mu.Lock()
	s.players2sess[playerID] = sessionID
	s.mu.Unlock()
	return nil
}

// markOnline 标记玩家在线（会话绑定/创角时调用，与 Actor 是否加载无关）。
func (s *Server) markOnline(ctx context.Context, playerID int64) {
	if s.presence == nil {
		return
	}
	if err := s.presence.SetOnline(ctx, playerID, s.cfg.Server.Name, onlineTTL); err != nil {
		s.logger.Warn("set online failed", "player_id", playerID, "err", err)
	}
}

// OnUnbind 实现 rpc.StreamHandler。
func (s *Server) OnUnbind(ctx context.Context, sessionID uint64) error {
	s.mu.Lock()
	playerID := s.sessions[sessionID]
	delete(s.sessions, sessionID)
	delete(s.accounts, sessionID)
	if playerID != 0 && s.players2sess[playerID] == sessionID {
		delete(s.players2sess, playerID)
	}
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

	// 会话级协议始终在此处理。
	//
	// 注意：不能只在 playerID==0 时才放行 —— 已有角色的账号在 OnBind 阶段就被
	// 自动绑定（省一次往返），此时客户端仍会发 EnterGame(1010)。若走 Actor 路由，
	// 会因「无模块处理 1010」而静默丢包，客户端只能等超时（重连场景必现）。
	switch msgID {
	case msgIDEnterGame:
		return s.handleEnterGame(ctx, sessionID, accountID, body)
	case msgIDCreateRole:
		return s.handleCreateRole(ctx, sessionID, accountID, body)
	}

	if playerID == 0 {
		return fmt.Errorf("session %d has no role, drop msg %d", sessionID, msgID)
	}

	// 懒加载：创角/进游戏只记 sessions，Actor 在首个业务包时创建。
	//
	// 好处有两个：① 创角后立刻加载会读到「刚建的空角色」，若随后有数据写入
	// （补偿道具、GM 改库）内存与库就不一致；② 只登录不操作的账号不占 Actor 资源。
	if err := s.ensureActor(ctx, sessionID, playerID); err != nil {
		return fmt.Errorf("bind player %d: %w", playerID, err)
	}
	pid, ok := s.players.Get(playerID)
	if !ok {
		return fmt.Errorf("player %d actor missing after bind", playerID)
	}
	// 同一 Actor 信箱 FIFO：Bind 先于业务包被处理，预加载完成后再处理业务
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
	if err := s.reply(sessionID, 1011, resp); err != nil {
		return err
	}
	// 离线暂存补投（12.6.2）：必须放在 1011 之后 ——
	// 客户端收到进游戏回执才开始处理推送，过早补投会被当成「未知消息」丢掉。
	go s.drainOffline(role.Id, sessionID)
	return nil
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
	// 只记录会话 → 角色映射，不立即创建 Actor（见 OnClientPacket 的懒加载说明）。
	// 否则随后的补偿道具/改库写入会被「创角时加载的空快照」覆盖。
	s.mu.Lock()
	s.sessions[sessionID] = roleID
	s.players2sess[roleID] = sessionID
	s.mu.Unlock()
	// 创角即有会话 → 在线；Actor 留到首个业务包再创建
	s.markOnline(ctx, roleID)
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

// gamePusher 实现 battle.Pusher：把推送转成对玩家会话的下行。
type gamePusher struct {
	srv *Server
}

// Push 实现 battle.Pusher。
func (p gamePusher) Push(playerID int64, msgID uint32, body []byte) bool {
	p.srv.mu.RLock()
	sessionID, ok := p.srv.players2sess[playerID]
	p.srv.mu.RUnlock()
	if !ok || sessionID == 0 {
		return false
	}
	if err := p.srv.sendToClient(sessionID, msgID, body); err != nil {
		p.srv.logger.Debug("battle push failed", "player_id", playerID, "msg_id", msgID, "err", err)
		return false
	}
	return true
}

// crossHandler 实现 cross.Handler：把跨服下行帧分发给 pool（建房响应）与 battle 模块。
type crossHandler struct {
	srv *Server
}

// OnCreateResp 实现 cross.Handler。
func (h crossHandler) OnCreateResp(resp *internalv2.RoomCreateResp) {
	if h.srv.crossPool != nil {
		h.srv.crossPool.OnCreateResp(resp)
	}
}

// OnRoomReady 实现 cross.Handler。
func (h crossHandler) OnRoomReady(roomID uint64) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnRoomReady(roomID)
	}
}

// OnBattleSync 实现 cross.Handler。
func (h crossHandler) OnBattleSync(msg *internalv2.BattleMsg) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnBattleSync(msg)
	}
}

// OnBattleResult 实现 cross.Handler。
func (h crossHandler) OnBattleResult(res *internalv2.BattleResult) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnBattleResult(res)
	}
	if h.srv.crossPool != nil {
		h.srv.crossPool.OnRoomClosed(res.GetRoomId())
	}
}

// CrossReady 已就绪的跨服节点数（测试/运维探针用）。
func (s *Server) CrossReady() int {
	if s.crossPool == nil {
		return 0
	}
	return s.crossPool.ReadyCount()
}

// BattleModule 暴露战斗模块（测试用）。
func (s *Server) BattleModule() *battle.Module { return s.battleMod }

// centerMatchAdapter 把中心服客户端适配为 battle.CenterMatcher。
type centerMatchAdapter struct {
	cli *gamecenter.Client
}

// Available 实现 battle.CenterMatcher。
func (a centerMatchAdapter) Available() bool {
	return a.cli != nil && a.cli.Ready()
}

// Match 实现 battle.CenterMatcher。
func (a centerMatchAdapter) Match(playerID int64, mode string, elo int32, snapshot []byte) error {
	if a.cli == nil {
		return fmt.Errorf("center client not configured")
	}
	return a.cli.SendMatch(playerID, mode, elo, snapshot)
}

// centerHandler 实现 center.Handler：把中心服下行指令转给对应模块。
type centerHandler struct {
	srv *Server
}

// OnMatchResult 实现 center.Handler。
func (h centerHandler) OnMatchResult(res *internalv2.MatchResult) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnMatchResult(res)
	}
}

// OnRelayPush 实现 center.Handler（P9 中转业务接入前只记日志）。
func (h centerHandler) OnRelayPush(push *internalv2.RelayPush) {
	h.srv.logger.Debug("relay push received", "from", push.GetFromNode(),
		"relay_id", push.GetRelayId(), "bytes", len(push.GetPayload()))
}

// OnGroupUpdate 实现 center.Handler：组内跨服地址变化 → 重建/新增跨服流。
func (h centerHandler) OnGroupUpdate(g *internalv2.GroupUpdate) {
	if h.srv.crossPool == nil || len(g.GetCrossAddrs()) == 0 {
		return
	}
	h.srv.crossPool.Start(context.Background(), g.GetCrossAddrs())
	h.srv.logger.Info("group update applied", "group", g.GetGroupId(),
		"games", len(g.GetGameNodes()), "cross", len(g.GetCrossAddrs()))
}

// OnKick 实现 center.Handler：中心服滚动重启，本节点会自行重连。
func (h centerHandler) OnKick(k *internalv2.KickNode) {
	h.srv.logger.Warn("center asked to reconnect", "reason", k.GetReason())
}

// OnConfigPush 实现 center.Handler（配置热更新 P7+ 接入）。
func (h centerHandler) OnConfigPush(c *internalv2.ConfigPush) {
	h.srv.logger.Info("config push received", "key", c.GetConfigKey(), "checksum", c.GetChecksum())
}

// CenterReady 中心服 Report 流是否就绪（测试/探针用）。
func (s *Server) CenterReady() bool {
	return s.centerCli != nil && s.centerCli.Ready()
}

// rankReporter 把中心服客户端适配为 battle.RankReporter。
type rankReporter struct {
	cli *gamecenter.Client
}

// ReportArenaScore 实现 battle.RankReporter。
func (r rankReporter) ReportArenaScore(playerID int64, score int64) error {
	if r.cli == nil {
		return fmt.Errorf("center client not configured")
	}
	return r.cli.SendRank("arena_score", playerID, score, "")
}

// buildIDTables 后台构建 ID 表（12.5.1）。
//
// 必须先建热表（player/player_equip/mail）再建其余表：热表决定登录与穿戴是否
// 能走 Redis 快路径，冷表（如 friend）缺失只影响对应功能的性能。
func (s *Server) buildIDTables(ctx context.Context) {
	hot := make(map[string]bool, len(cache.TableHotTables))
	for _, t := range cache.TableHotTables {
		hot[t] = true
	}

	// 热表在前
	ordered := make([]cache.TableMeta, 0, len(cache.DefaultIDTables))
	for _, m := range cache.DefaultIDTables {
		if hot[m.Name] {
			ordered = append(ordered, m)
		}
	}
	for _, m := range cache.DefaultIDTables {
		if !hot[m.Name] {
			ordered = append(ordered, m)
		}
	}

	var built, failed int
	for _, meta := range ordered {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		n, err := s.idTable.BuildOne(ctx, meta)
		elapsed := time.Since(start)
		if err != nil {
			failed++
			s.logger.Warn("idtab build failed", "table", meta.Name, "err", err)
			continue
		}
		built++
		if metrics.Enabled() {
			metrics.IDTabBuildObserve(meta.Name, elapsed)
			metrics.IDTabState(meta.Name, n, true)
		}
		s.logger.Info("idtab built", "table", meta.Name, "count", n,
			"elapsed", elapsed.Round(time.Millisecond))
	}
	s.logger.Info("idtab build done", "built", built, "failed", failed)
}

// maintainIDTables ID 表维护（12.5.3）。
//
//   - 每 10 分钟：对热表做数量核对，不一致则重建（重建期间回落 DB，不阻塞查询）
//   - 每小时：清理孤儿实体 hash（e:{table}:{id} 存在但 idtab 无此 id）
func (s *Server) maintainIDTables(ctx context.Context) {
	reconcile := time.NewTicker(10 * time.Minute)
	defer reconcile.Stop()
	clean := time.NewTicker(time.Hour)
	defer clean.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-reconcile.C:
			for _, meta := range cache.DefaultIDTables {
				if ctx.Err() != nil {
					return
				}
				rebuilt, err := s.idTable.Reconcile(ctx, meta)
				if err != nil {
					s.logger.Warn("idtab reconcile failed", "table", meta.Name, "err", err)
					continue
				}
				if rebuilt {
					if metrics.Enabled() {
						metrics.IDTabReconcileMismatch(meta.Name)
					}
					s.logger.Warn("idtab rebuilt after mismatch", "table", meta.Name)
				}
				if n, err := s.idTable.Count(ctx, meta.Name); err == nil {
					if metrics.Enabled() {
						metrics.IDTabState(meta.Name, n, s.idTable.Ready(ctx, meta.Name))
					}
				}
			}
		case <-clean.C:
			for _, meta := range cache.DefaultIDTables {
				if ctx.Err() != nil {
					return
				}
				if !s.idTable.Ready(ctx, meta.Name) {
					continue
				}
				n, err := s.idTable.CleanOrphans(ctx, meta.Name, 5000)
				if err != nil {
					s.logger.Warn("idtab orphan clean failed", "table", meta.Name, "err", err)
				} else if n > 0 {
					s.logger.Info("idtab orphan cleaned", "table", meta.Name, "removed", n)
				}
			}
		}
	}
}

// sendToPlayer 按 playerID 直接下发（不经过 Actor）。
//
// 用于「接收方还没加载业务状态」的推送场景（如私聊到达），
// Actor 只是业务状态载体，纯下行不该依赖它。
func (s *Server) sendToPlayer(playerID int64, msgID uint32, body []byte) bool {
	s.mu.RLock()
	sessionID, ok := s.players2sess[playerID]
	s.mu.RUnlock()
	if !ok || sessionID == 0 {
		return false
	}
	return s.sendToClient(sessionID, msgID, body) == nil
}
