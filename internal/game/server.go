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
	"game_study/internal/game/module/friend"
	"game_study/internal/game/module/guild"
	"game_study/internal/game/module/item"
	"game_study/internal/game/module/loginreward"
	"game_study/internal/game/module/mail"
	"game_study/internal/game/module/partner"
	"game_study/internal/game/module/profile"
	"game_study/internal/game/module/quest"
	rankmodule "game_study/internal/game/module/rank"
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
	"game_study/pkg/codec"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	pkgmq "game_study/pkg/mq"
	gameetcd "game_study/pkg/registry/etcd"
	rpcpkg "game_study/pkg/rpc"
	"game_study/pkg/shutdown"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	gatev1 "game_study/pkg/pb/gate/v1"
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
	cacheStore *writebehind.Store  // Redis 权威快照 + 脏队列
	wbWorker   *writebehind.Worker // 进程内落库者（缓存权威模式）
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

	// runCtx 是组件生命周期，与外部信号 ctx 解耦。
	//
	// 收到 SIGTERM 时外部 ctx 立刻取消；如果组件直接挂在它上面，Actor 会同时停掉，
	// Shutdown 再想「排空在途 → 确认保存」就没有对象可等了。所以内部统一用
	// runCtx，由 Shutdown 在正确时机 cancel。
	runCtx    context.Context
	runCancel context.CancelFunc
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
	// 组件生命周期与信号 ctx 解耦（见 runCtx 字段注释）。
	s.runCtx, s.runCancel = context.WithCancel(context.Background())
	// 只有「启动失败提前返回」才收掉内部 ctx；正常走到末尾时由 Shutdown 负责取消。
	started := false
	defer func() {
		if !started && s.runCancel != nil {
			s.runCancel()
		}
	}()
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
		if s.repo != nil {
			s.cacheStore = writebehind.NewStore(redisClient.GetClient())
			s.repo.SetCache(s.cacheStore)
		}
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
		go s.buildIDTables(s.runCtx)
		go s.maintainIDTables(s.runCtx)
	}

	// ③ Actor 系统 + 模块
	s.actorSys = actor.NewSystem(actor.Config{
		ShardCount:  s.cfg.Actor.ShardCount,
		MailboxSize: s.cfg.Actor.MailboxSize,
		Logger:      s.logger, // panic 恢复时记录堆栈（不再静默吞掉）
	})
	s.actorSys.Start(s.runCtx)

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

	// Kafka 生产者：活动贡献上报（R7.3）等业务共用；消费组在 ⑤ 创建。
	if len(s.cfg.Kafka.Brokers) > 0 {
		s.producer = pkgmq.NewProducer(pkgmq.Config{
			Brokers:      s.cfg.Kafka.Brokers,
			RequiredAcks: s.cfg.Kafka.ProducerRequiredAcks,
		})
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
		s.modules.Register(quest.New(s.logger))
		s.modules.Register(equip.New(s.logger))
		s.modules.Register(skill.New(s.logger))
		s.modules.Register(guild.New(guild.NewRepo(s.database), s.logger))
		s.modules.Register(team.New(team.NewRepo(s.database), s.logger))
		s.modules.Register(copy.New(s.logger))
		s.modules.Register(shop.New(s.logger))
		s.modules.Register(activity.New(s.logger, s.producer))
		s.modules.Register(signin.New(s.logger))
		s.modules.Register(loginreward.New(s.logger))
		// 榜单查询代理到中心服；未配置中心服时模块回「暂不可用」而不是空榜。
		s.modules.Register(rankmodule.New(rankQuerier{cli: s.centerCli}, s.logger))
		// 好友：关系直写 MySQL 事务（双人状态不适合放单玩家快照），缓存只用于在线展示。
		s.modules.Register(friend.New(s.database, s.presence, s.logger))
		// 伙伴：状态进快照，激活/升星与金币消耗同一份 CAS 提交。
		s.modules.Register(partner.New(s.logger))
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
	s.flusher.Start(s.runCtx)

	// 缓存权威状态下，脏队列的唯一消费者是 writebehind worker。
	// 进程内默认起一个，避免「忘了部署 cmd/flusher 就永远不落库」；
	// 部署了独立 flusher 时可关闭（版本栅栏保证两者并行也安全）。
	if s.repo.CacheEnabled() {
		if s.cfg.Flush.DisableInlineWritebehind {
			s.logger.Info("inline writebehind disabled; rely on standalone flusher")
		} else if s.database != nil {
			interval, err := config.ParseDuration(s.cfg.Flush.Interval)
			if err != nil {
				return fmt.Errorf("flush interval: %w", err)
			}
			worker := writebehind.NewWorker(s.cacheStore, s.database.DB, s.cfg.Flush.BatchRows)
			s.wbWorker = worker
			go func() {
				if err := worker.Run(s.runCtx, interval, func(err error) {
					s.logger.Warn("writebehind flush failed; Redis task retained", "err", err)
				}); err != nil {
					s.logger.Warn("writebehind worker stopped", "err", err)
				}
			}()
			s.logger.Info("inline writebehind started", "interval", interval.String(), "partitions", writebehind.Partitions)
		}
	}

	var onDirty func(int64) = s.flusher.MarkDirty
	if s.repo.CacheEnabled() {
		onDirty = nil
	} // Redis 内的持久队列由 writebehind worker 消费。
	s.players = player.NewManager(s.actorSys, s.modules, s.logger, s.repo, onDirty, s.sendToClient)
	// 登录预加载（12.6）：5 表并行拉取 + 5s 超时保护
	if s.database != nil {
		preloadTimeout := player.DefaultPreloadTimeout
		s.players.SetPreloader(player.NewPreloader(s.database, s.logger, preloadTimeout))
	}

	// ⑤ Kafka 消费者（充值/邮件/跨服私聊）
	if len(s.cfg.Kafka.Brokers) > 0 {
		group, err := gamemq.NewGroup(s.cfg, gamemq.Deps{
			Deduper:  deduper,
			DB:       s.database,
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
		group.Run(s.runCtx)
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
	cancel, err := etcdReg.RegisterWithKeepalive(s.runCtx, node, s.cfg.Etcd.LeaseTTL)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	s.etcdCancel = cancel

	// ⑦ gRPC server
	s.gameRPC = rpc.NewGameServer(s, s.logger)
	s.rpcServer = rpcpkg.NewServer(s.cfg.Server.GRPCAddr, s.logger)
	internalv2.RegisterGameServiceServer(s.rpcServer.GRPCServer(), s.gameRPC)

	go func() {
		if err := s.rpcServer.Start(s.runCtx); err != nil && err != context.Canceled {
			s.logger.Error("grpc server error", "err", err)
		}
	}()

	// 建立到中心服的 Report 流（9.2：断流指数退避重连）
	if s.centerCli != nil {
		go s.centerCli.Start(s.runCtx)
		s.logger.Info("center client started", "center", s.cfg.Center.Addr)
	}

	// 建立到跨服节点的 CrossService 流（10.2：断流指数退避重连）
	if s.crossPool != nil {
		s.crossPool.Start(s.runCtx, s.cfg.Cross.Nodes)
		s.logger.Info("cross pool started", "nodes", s.cfg.Cross.Nodes)
	}

	s.logger.Info("GameServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"players", s.players.Count())
	started = true
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

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP）。
//
// 顺序是这套流程的全部意义：
//  1. 摘注册：网关不再把新会话路由过来；
//  2. 停 gRPC、停消费者：外部与内部的新业务入口都先关掉；
//  3. 排空 Actor：等已经在信箱里的消息处理完，把状态写进 Redis；
//  4. FlushAll：把 Redis 脏任务确认落库，失败必须上报；
//  5. 停 Actor 与其余组件。
//
// 反过来（先 FlushAll 再停入口）会留一个窗口：FlushAll 扫完后仍有请求进来标脏，
// 进程退出时这批数据谁都没落库 —— R1.3 修的就是这个。
func (s *Server) Shutdown(ctx context.Context) error {
	mgr := shutdown.NewManager(s.logger)

	// DRAIN：摘注册，不再有新流量路由进来。
	mgr.Register("unregister", shutdown.PhaseDrain, func(cctx context.Context) error {
		if s.etcdCancel == nil {
			return nil
		}
		s.etcdCancel()
		// 摘注册后立刻通知在线会话：让客户端主动重连，而不是等请求超时。
		if n := s.kickAllSessions("server maintenance, please reconnect"); n > 0 {
			s.logger.Info("shutdown: notified sessions to reconnect", "count", n)
		}

		// 网关靠 etcd watch 感知节点下线，需要一点时间重建连接池，
		// 这段等待保证「新连接」不再路由到本实例。
		//
		// 它**不能**消除滚动发布期间的请求失败：玩家会话（Actor）绑定在本实例，
		// 实例下线必然断开这些连接。实测 7100 请求 45 失败（0.63%），
		// 加 3s 等待后仍是 45——失败来自「已绑定会话被断开」，不是路由滞后。
		wait := 3 * time.Second
		if d, err := config.ParseDuration(s.cfg.Shutdown.RegisterDrain); err == nil && d > 0 {
			wait = d
		}
		select {
		case <-time.After(wait):
		case <-cctx.Done():
		}
		return nil
	})

	// DRAINING：先关入口，再等在途。
	mgr.Register("stop-rpc", shutdown.PhaseDraining, func(context.Context) error {
		if s.rpcServer != nil {
			s.rpcServer.Stop()
		}
		return nil
	})
	mgr.Register("stop-consumers", shutdown.PhaseDraining, func(context.Context) error {
		if s.consumers == nil {
			return nil
		}
		return s.consumers.Close()
	})
	mgr.Register("drain-actors", shutdown.PhaseDraining, func(ctx context.Context) error {
		if s.actorSys == nil {
			return nil
		}
		return s.actorSys.Drain(ctx)
	})

	// FLUSH：确认落库。失败要出现在返回值里，由 main 非零退出。
	mgr.Register("flush-all", shutdown.PhaseFlush, func(ctx context.Context) error {
		var errs []error
		// 缓存权威模式：脏任务在 Redis 队列里，必须先把进程内 worker 的积压排空。
		// 只调 flusher.FlushAll 是空转（缓存玩家的 onDirty 已被置空），数据会随进程消失。
		if s.wbWorker != nil {
			if err := s.wbWorker.FlushAll(ctx); err != nil {
				errs = append(errs, fmt.Errorf("writebehind: %w", err))
			}
		}
		// 非缓存玩家的直连落库路径。
		if s.flusher != nil {
			if err := s.flusher.FlushAll(ctx); err != nil {
				errs = append(errs, fmt.Errorf("flusher: %w", err))
			}
			s.flusher.Stop()
		}
		return errors.Join(errs...)
	})

	mgr.Register("stop-cross", shutdown.PhaseStop, func(context.Context) error {
		if s.crossPool != nil {
			s.crossPool.Stop()
		}
		return nil
	})
	mgr.Register("stop-center", shutdown.PhaseStop, func(context.Context) error {
		if s.centerCli != nil {
			s.centerCli.Stop()
		}
		return nil
	})
	mgr.Register("stop-actors", shutdown.PhaseStop, func(context.Context) error {
		// 先取消内部生命周期（定时器与后台 worker 退出），再停分片。
		if s.runCancel != nil {
			s.runCancel()
		}
		if s.actorSys != nil {
			s.actorSys.Stop()
		}
		return nil
	})
	mgr.Register("close-producer", shutdown.PhaseStop, func(ctx context.Context) error {
		if s.producer == nil {
			return nil
		}
		_ = s.producer.Flush(ctx)
		return s.producer.Close()
	})
	mgr.Register("close-etcd", shutdown.PhaseStop, func(context.Context) error {
		if s.etcdReg != nil {
			return s.etcdReg.Close()
		}
		return nil
	})
	mgr.Register("close-database", shutdown.PhaseStop, func(context.Context) error {
		if s.database != nil {
			return s.database.Close()
		}
		return nil
	})
	mgr.Register("close-redis", shutdown.PhaseStop, func(context.Context) error {
		if s.redis != nil {
			return s.redis.Close()
		}
		return nil
	})
	mgr.Register("stop-metrics", shutdown.PhaseStop, func(ctx context.Context) error {
		if s.metricsSrv != nil {
			return s.metricsSrv.Stop(ctx)
		}
		return nil
	})

	if err := mgr.Run(ctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
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

// saveAckTimeout 等待 Actor 回执真实保存结果的上限。
//
// 超时后 flusher 会重新标脏、下轮重试；迟到回执由 Done 的容量 1 + 非阻塞写丢弃，
// 不会拖住 Actor。
const saveAckTimeout = 5 * time.Second

// persistPlayer 由 flusher 回调，把单个玩家落库。
//
// 返回 nil 的语义是「SQL 已成功」而不是「消息已投递」：以前只 Send 就返回 nil，
// flusher 会把玩家从脏队列移除，保存失败时数据静默丢失。
func (s *Server) persistPlayer(ctx context.Context, playerID int64) error {
	if s.repo.CacheEnabled() {
		return nil
	}
	pid, ok := s.players.Get(playerID)
	if !ok {
		return nil
	}
	done := make(chan error, 1)
	if err := s.actorSys.Send(pid, &player.SaveMsg{Done: done}); err != nil {
		return fmt.Errorf("player %d save: %w", playerID, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, saveAckTimeout)
	defer cancel()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("player %d save: %w", playerID, err)
		}
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("player %d save ack: %w", playerID, waitCtx.Err())
	}
}

// 会话级协议号（未选角时由组装层直接处理，不进 Actor）。
const (
	msgIDEnterGame  = codec.MsgEnterGameReq
	msgIDCreateRole = codec.MsgCreateRoleReq
	// msgIDKickNotify 与网关 common.MsgIDKickNotify 保持一致：10。
	msgIDKickNotify uint32 = 10
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
//
// 顶号规则（R3.1）：同一角色只允许一个权威会话。新会话绑定成功后主动踢掉旧会话，
// 旧连接收到 KickNotify 后断开，其迟到的 UNBIND 会被 Actor 的会话身份校验忽略。
func (s *Server) bindPlayer(ctx context.Context, sessionID uint64, playerID int64) error {
	// 跨服权威归属：先原子占用在线状态，别的节点正持有时拒绝绑定。
	// 同节点旧会话（重连）视为成功，由下面的顶号逻辑处理。
	if s.presence != nil {
		ok, owner, err := s.presence.TrySetOnline(ctx, playerID, s.cfg.Server.Name, onlineTTL)
		switch {
		case err != nil:
			s.logger.Warn("presence claim failed; bind continues", "player_id", playerID, "err", err)
		case !ok && owner != s.cfg.Server.Name:
			return fmt.Errorf("player %d is online on node %s", playerID, owner)
		}
	}

	s.mu.Lock()
	old := s.players2sess[playerID]
	s.sessions[sessionID] = playerID
	s.players2sess[playerID] = sessionID
	accountID := s.accounts[sessionID]
	s.mu.Unlock()

	if old != 0 && old != sessionID {
		s.kickSession(old, "login_elsewhere")
	}
	if err := s.bindActor(ctx, sessionID, playerID); err != nil {
		return err
	}
	s.logger.Info("session bound", "session_id", sessionID, "player_id", playerID, "account_id", accountID, "replaced", old)
	return nil
}

// kickAllSessions 关闭前通知所有在线会话重连。
//
// 有状态实例下线时，绑定在本实例的会话无法迁移（Actor 状态在内存），
// 服务端能做的是**尽早明确告知**：客户端收到 KickNotify 后立即重连到
// 其它实例，而不是让后续请求一个个超时。网关转发 KickNotify 后会断开连接。
//
// 返回通知数量（推送失败也计入，只是日志不同）。
func (s *Server) kickAllSessions(reason string) int {
	s.mu.RLock()
	ids := make([]uint64, 0, len(s.sessions))
	for sid := range s.sessions {
		ids = append(ids, sid)
	}
	s.mu.RUnlock()
	for _, sid := range ids {
		s.kickSession(sid, reason)
	}
	return len(ids)
}

// kickSession 通知旧会话下线；客户端收到 KickNotify 后断开，触发 OnUnbind。
//
// 推不出去不算失败：旧会话可能已经断开，此时 OnUnbind 迟早会到，
// Actor 的会话身份校验保证它不会影响新会话。
func (s *Server) kickSession(sessionID uint64, reason string) {
	body, err := proto.Marshal(&gatev1.KickNotify{Reason: reason})
	if err != nil {
		return
	}
	if err := s.sendToClient(sessionID, msgIDKickNotify, body); err != nil {
		s.logger.Info("kick stale session failed; waiting for stale unbind",
			"session_id", sessionID, "reason", reason, "err", err)
		return
	}
	s.logger.Info("stale session kicked", "session_id", sessionID, "reason", reason)
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
	stale := false
	if playerID != 0 {
		if s.players2sess[playerID] == sessionID {
			delete(s.players2sess, playerID)
		} else {
			stale = true // 该会话已被新会话顶号
		}
	}
	s.mu.Unlock()
	if playerID == 0 {
		return nil
	}
	if stale {
		// 旧连接被顶号后才断开：不能解绑 Actor，也不能清在线归属——
		// 否则会把仍在线的权威会话打成离线，其它节点还能趁机抢占。
		s.logger.Info("stale session unbind ignored", "player_id", playerID, "session_id", sessionID)
		return nil
	}
	if pid, ok := s.players.Get(playerID); ok {
		if err := s.actorSys.Send(pid, &player.UnbindMsg{SessionID: sessionID}); err != nil {
			// 解绑消息被拒（信箱满/Actor 已终止）：脏数据仍在 flusher 队列里会周期性落库，
			// 但这里要明确记录，不能静默吞掉。
			s.logger.Warn("unbind message rejected",
				"player_id", playerID, "session_id", sessionID, "err", err)
		}
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
func (r rankReporter) ReportArenaScore(playerID int64, score int64, bizID string) error {
	if r.cli == nil {
		return fmt.Errorf("center client not configured")
	}
	return r.cli.SendRank("arena_score", playerID, score, "", bizID)
}

// rankQuerier 把中心服客户端适配为 rank 模块的查询接口。
type rankQuerier struct{ cli *gamecenter.Client }

// Query 实现 rank.Querier；中心服回非 0 code 视为查询失败（不要当成空榜）。
func (q rankQuerier) Query(ctx context.Context, rankType string, playerID int64, topN int32) (*rankmodule.Result, error) {
	if q.cli == nil {
		return nil, fmt.Errorf("center client not configured")
	}
	res, err := q.cli.QueryRank(ctx, rankType, playerID, topN)
	if err != nil {
		return nil, err
	}
	if res.GetCode() != 0 {
		return nil, fmt.Errorf("center rank code %d", res.GetCode())
	}
	out := &rankmodule.Result{
		Season:    res.GetSeason(),
		OnBoard:   res.GetOnBoard(),
		MyRank:    res.GetMyRank(),
		MyScore:   res.GetMyScore(),
		UpdatedAt: res.GetUpdatedAt(),
	}
	for _, e := range res.GetTop() {
		out.Entries = append(out.Entries, rankmodule.Entry{PlayerID: e.GetPlayerId(), Score: e.GetScore(), Rank: e.GetRank()})
	}
	for _, e := range res.GetAround() {
		out.Around = append(out.Around, rankmodule.Entry{PlayerID: e.GetPlayerId(), Score: e.GetScore(), Rank: e.GetRank()})
	}
	return out, nil
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
