// internal/game/server_start.go —— 启动装配：Start 的每一步一个方法
//
// 为什么拆出来：Start 原本 320 行平铺，读者要一路滚到底才知道「服务是怎么
// 长出来的」。拆成「一步一个方法」后，Start 本身是启动顺序的目录。
//
// 三处**不能调换**的依赖：
//   - 中心服客户端在 Actor 系统之前：actor.Config.Remote 引用它（P0 跨节点）
//   - 场景管理器在模块注册之前：scene 模块要拿它转发协议
//   - 跨服池在 battle 模块之前：battle 依赖它透传战斗
package game

import (
	"context"
	"fmt"
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
	scenemod "game_study/internal/game/module/scene"
	seatmod "game_study/internal/game/module/seat"
	"game_study/internal/game/module/shop"
	"game_study/internal/game/module/signin"
	"game_study/internal/game/module/skill"
	"game_study/internal/game/module/team"
	gamemq "game_study/internal/game/mq"
	"game_study/internal/game/player"
	gameremote "game_study/internal/game/remote"
	"game_study/internal/game/rpc"
	"game_study/internal/game/scene"
	"game_study/internal/game/seat"
	"game_study/internal/game/service"
	"game_study/pkg/actor"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/metrics"
	pkgmq "game_study/pkg/mq"
	"game_study/pkg/ratelimit"
	gameetcd "game_study/pkg/registry/etcd"
	rpcpkg "game_study/pkg/rpc"
	"game_study/pkg/writebehind"

	internalv2 "game_study/pkg/pb/internalv2"
)

// ---- 启动装配：每一步一个方法，Start 本身就是启动顺序的目录 ----

// startMetrics 起指标服务（/metrics、/healthz、/readyz）。
func (s *Server) startMetrics() error {
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
	return nil
}

// initMySQL 连业务库；未配 DSN 时整体跳过（单测/演示模式）。
func (s *Server) initMySQL() error {
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
		// 创角服务只依赖持久化契约：SQL 与驱动在 player.RoleStore 内部，
		// service 层不再持有 *db.DB（本轮整改的边界）
		s.roleSvc = service.NewRoleService(player.NewRoleStore(database), s.logger)
	}
	return nil
}

// initRedis 连 Redis 并装配快照存储、在线状态、去重器。
//
// 返回去重器：未配 Redis 时是 Noop（消费侧「至少一次」，由业务幂等兜底）。
func (s *Server) initRedis() (pkgmq.Deduper, error) {
	// ② Redis（在线状态 / 幂等 / 离线暂存）
	var deduper pkgmq.Deduper = pkgmq.NoopDeduper{}
	// 默认不去重：未配 Redis 时消费侧退化为「至少一次」投递，由业务幂等兜底；
	// 配上 Redis 才换成 SETNX 去重（R1.4「死信成功才提交」规则的前提）。
	if len(s.cfg.Redis.Addrs) > 0 {
		redisClient, err := cache.New(cache.RedisConfig{
			Addrs:    s.cfg.Redis.Addrs,
			Password: s.cfg.Redis.Password,
			DB:       s.cfg.Redis.DB,
			PoolSize: s.cfg.Redis.PoolSize,
			MinIdle:  s.cfg.Redis.MinIdleConn,
			Breaker: cache.BreakerOptions{
				Threshold: s.cfg.Redis.CircuitBreaker.Threshold,
				Cooldown:  s.cfg.Redis.CircuitBreaker.CooldownDuration(),
				Probes:    s.cfg.Redis.CircuitBreaker.Probes,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		s.redis = redisClient
		if s.repo != nil {
			s.cacheStore = writebehind.NewStore(redisClient.GetClient())
			s.repo.SetCache(s.cacheStore)
		}
		s.presence = cache.NewPresence(redisClient.GetClient())
		deduper = cache.NewRedisDeduper(redisClient.GetClient())
	}
	return deduper, nil
}

// initIDTable 建 ID 表（12.5）：Redis + MySQL 都可用才建。
func (s *Server) initIDTable() {
	// ②' ID 表（12.5）：Redis + MySQL 都可用才建。
	// 构建在后台进行：ready 未置位期间所有存在性判断回落 DB，服务可正常启动。
	if s.database != nil && s.redis != nil {
		s.idTable = cache.NewIDTable(s.redis.GetClient(), s.database.DB)
		if s.roleSvc != nil {
			s.roleSvc.SetIDTable(s.idTable)
		}
		// 两个后台 goroutine 的退出都只依赖 runCtx：Start 返回前 started
		// 未置位时 defer 会 cancel 掉它们，不会泄漏；内部循环均以
		// ctx.Done() 为终止条件（见 buildIDTables/maintainIDTables）。
		go s.buildIDTables(s.runCtx)
		go s.maintainIDTables(s.runCtx)
	}
}

// initOwnerIndex 建归属索引（缓存架构设计 §3）：按 idtab.mappings 声明。
//
// 只给「有归属列」的映射建索引；preload_all 的映射走整表实体预热
// （idTable.Preheat），它也可能同时带 by——那索引也建，两件事都做。
// 构建/维护同样在后台 goroutine：ready 未置位期间 Members 回落 MySQL，
// 服务可用性不受影响（与 initIDTable 同一约定）。
func (s *Server) initOwnerIndex() {
	if s.database == nil || s.redis == nil {
		return
	}
	for _, m := range s.cfg.IDTab.Mappings {
		if m.By == "" {
			continue // 纯整表预热：没有归属列可索引
		}
		if s.ownerIdx == nil {
			s.ownerIdx = make(map[string]*cache.OwnerIndex, len(s.cfg.IDTab.Mappings))
		}
		s.ownerIdx[m.Table] = cache.NewOwnerIndex(
			s.redis.GetClient(), s.database.DB, m.Table, m.By, m.Key)
	}
	if len(s.ownerIdx) == 0 && !anyPreloadAll(s.cfg.IDTab.Mappings) {
		return
	}
	go s.buildOwnerIndexes(s.runCtx)
	go s.maintainOwnerIndexes(s.runCtx)
}

// anyPreloadAll 是否有整表预热的映射（决定要不要起构建 goroutine）。
func anyPreloadAll(mappings []config.CacheMapping) bool {
	for _, m := range mappings {
		if m.PreloadAll {
			return true
		}
	}
	return false
}

// initCenterClient 建中心服客户端（P8 匹配 + P0 跨节点投递）。
//
// 必须在 Actor 系统之前：actor.Config.Remote 引用它。
func (s *Server) initCenterClient() {
	// ② 中心服客户端（P8 + P0）：匹配主路径 + 跨节点 Actor 投递中转。
	// 必须建在 Actor 系统之前——actor.Config.Remote 引用它。
	// 未配置则 battle 降级为本服匹配、远程投递走「本地找不到即失败」。
	if s.cfg.Center.Addr != "" {
		s.centerCli = gamecenter.NewClient(s.cfg.Server.Name, s.cfg.Center.Addr, centerHandler{srv: s}, s.logger)
	}
}

// initActorSystem 建 Actor 系统。
//
// NodeID/Remote（P0 跨节点）：NodeID 是本节点标识；Remote 非 nil 时远程 PID
// 经中心服中转投递。未配中心服时不设 Remote——远程 PID 按本地找，找不到报错，
// 与单机部署行为一致（宁可显式失败，不静默丢消息）。
func (s *Server) initActorSystem() error {
	// ③ Actor 系统 + 模块
	//
	// NodeID/Remote（P0 跨节点）：NodeID 是本节点标识（Send 据此判断
	// 目标是否远程）；Remote 非 nil 时远程 PID 经中心服中转投递。
	// 未配中心服时不设 Remote——远程 PID 按本地找，找不到报错，
	// 与单机部署行为一致（宁可显式失败，不静默丢消息）。
	actorCfg := actor.Config{
		ShardCount:  s.cfg.Actor.ShardCount,
		MailboxSize: s.cfg.Actor.MailboxSize,
		Logger:      s.logger, // panic 恢复时记录堆栈（不再静默吞掉）
		NodeID:      s.cfg.Server.Name,
	}
	if s.centerCli != nil {
		actorCfg.Remote = gameremote.NewSender(s.centerCli, s.cfg.Server.Name, s.logger)
	}
	s.actorSys = actor.NewSystem(actorCfg)
	s.actorSys.Start(s.runCtx)
	return nil
}

// initScenes 拉起场景 Actor（数量由配置决定）。
//
// Boot 会校验配置（宽高/格宽/人数上限必须为正），非法配置直接启动失败。
func (s *Server) initScenes() error {
	// 场景 Actor：数量由配置决定，启动期一次性拉起。
	// Boot 会校验配置（宽高/格宽/人数上限必须为正），非法配置直接启动失败。
	s.sceneMgr = scene.NewManager(s.actorSys, s.logger)
	if err := s.sceneMgr.Boot(); err != nil {
		return fmt.Errorf("boot scenes: %w", err)
	}
	return nil
}

// initBattleAndProducer 建跨服连接池、battle 模块与 Kafka 生产者。
//
// 顺序：跨服池必须在 battle 之前（battle 依赖它透传战斗）。
func (s *Server) initBattleAndProducer() {
	// 跨服连接池（P13）：在模块注册前建好，battle 模块依赖它
	if len(s.cfg.Cross.Nodes) > 0 {
		s.crossPool = gamecross.NewPool(s.cfg.Server.Name, crossHandler{srv: s}, s.logger)
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
}

// initModules 注册全部模块。
//
// Module 对象进程级共享，一套 Registry 服务所有玩家：玩家数据只允许放
// Context.State 或按 player_id 索引的容器，严禁在共享 Module 上加
// currentPlayer 一类「以为只有一个玩家」的字段。
func (s *Server) initModules() {
	// Module 对象进程级共享，一套 Registry 服务所有玩家
	// （docs/游戏服启动流程解析.md 第 6 节）：玩家数据只允许放 Context.State
	// 或按 player_id 索引的容器，严禁在共享 Module 上加 currentPlayer 一类
	// 「以为只有一个玩家」的字段。
	s.modules = module.NewRegistry()
	s.modules.Register(profile.New(s.logger))
	s.modules.Register(attr.New(s.logger))
	s.modules.Register(bag.New(s.logger))
	s.modules.Register(item.New(s.logger))
	s.modules.Register(s.battleMod)
	// 抢位置：房间 Actor 与玩家同节点，不依赖 MySQL 也能玩（在线发奖）；
	// 离线奖励表在 initSeat 里按 database 是否可用决定是否注入。
	s.modules.Register(seatmod.M)

	// 以下模块依赖 MySQL：mail/chat/guild/team 直写库，quest/equip/skill/copy/
	// shop/activity/signin/loginreward/rank/friend/partner 经 Persister 落库。
	// 无库环境（单测/演示）整体跳过，玩家仍可用 profile/attr/bag/item/battle。
	if s.database != nil {
		s.mailMod = mail.New(mail.NewRepo(s.database), s.logger)
		// 邮件是第一个真正消费归属索引的模块（缓存架构设计 §4.2）：
		// 列表走「按需展开」——未配 mappings 或 Redis 不可用时 index 为 nil，
		// 模块内部自动回落 ListByPlayer，行为与接入前完全一致。
		s.mailMod.SetOwnerIndex(s.ownerIdx["mail"])
		s.chatMod = chat.New(chat.NewRepo(s.database), chatMessenger{srv: s}, s.logger)
		// 聊天频控：默认 1 条/秒（突发 3），配置负数关闭。
		// 与网关包级限流是两道：包限流管总量，这里管「单条成本」。
		if iv := s.cfg.Limit.ChatInterval(); iv > 0 {
			s.chatMod.SetChatLimiter(ratelimit.NewKeyed(1/iv.Seconds(), 3, 10*time.Minute))
		} else {
			s.chatMod.SetChatLimiter(nil)
		}
		s.modules.Register(s.mailMod)
		s.modules.Register(s.chatMod)
		s.modules.Register(quest.M)
		s.modules.Register(equip.M)
		s.modules.Register(skill.M)
		s.modules.Register(guild.New(guild.NewRepo(s.database), s.logger))
		s.modules.Register(team.New(team.NewRepo(s.database), s.logger))
		s.modules.Register(copy.M)
		s.modules.Register(shop.M)
		// 全服活动贡献上报走包级注入（模块本身已是单例，不再有 New）。
		if s.producer != nil {
			activity.SetProducer(s.producer)
		}
		s.modules.Register(activity.M)
		s.modules.Register(signin.M)
		s.modules.Register(loginreward.New(s.logger))
		// 榜单查询代理到中心服；未配置中心服时模块回「暂不可用」而不是空榜。
		s.modules.Register(rankmodule.New(rankQuerier{cli: s.centerCli}, s.logger))
		// 好友：关系直写 MySQL 事务（双人状态不适合放单玩家快照），缓存只用于在线展示。
		s.modules.Register(friend.New(s.database, s.presence, s.logger))
		// 伙伴：状态进快照，激活/升星与金币消耗同一份 CAS 提交。
		s.modules.Register(partner.M)
		// 场景：协议转发到场景 Actor，位置与视野由场景 Actor 独占。
		s.modules.Register(scenemod.New(s.sceneMgr, s.logger))
	}
}

// startFlusher 起 Write-Behind flusher（非缓存模式的落库通道）。
func (s *Server) startFlusher() error {
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
	return nil
}

// initPlayerManager 建玩家管理器并注入反向依赖。
func (s *Server) initPlayerManager() {
	// 落库调度：内存权威模式下，所有玩家状态变动都走 flusher——
	// 它的数据源是 Actor 内存（persistPlayer → SaveMsg → savePlayer），
	// Redis 只是尽力同步的副本，不参与落库。
	//
	// 历史上这里是二选一（缓存模式置空、交给 writebehind 消费 Redis 队列）。
	// 改成内存权威后两条路不再互斥：writebehind 仍可保留为「跨进程副本
	// 通道」，但落库的唯一来源是内存。
	s.players = player.NewManager(s.actorSys, s.modules, s.logger, s.repo, s.flusher.MarkDirty, s.sendToClient)
	// 场景心跳与重连同步：反向依赖由组装层注入，player 包不认识 scene。
	s.players.SetSceneLink(s.sceneMgr)
	// 跨节点顶号：别的节点接管本玩家的在线归属时，会发 KickMsg 过来，
	// Actor 收到后回调这里关闭本节点的网关会话（与同节点顶号同一条下线路径）。
	s.players.SetKickHook(s.kickLocalPlayer)
	// 跨节点（P0）：本节点标识 + 在线归属表。Resolve 据此在本地未命中时
	// 返回远程 PID（Send 自动走中心服中转）；presence 未配置（无 Redis）
	// 时退化为纯本地查询。
	s.players.SetNodeID(s.cfg.Server.Name)
	if s.presence != nil {
		s.players.SetPresence(s.presence)
	}
	// 登录预加载（12.6）：三表并行拉取 + 5s 超时保护
	if s.database != nil {
		preloadTimeout := player.DefaultPreloadTimeout
		s.players.SetPreloader(player.NewPreloader(s.database, s.logger, preloadTimeout))
	}
}

// initSeat 拉起单服抢位置房间，并把路由器/离线奖励表注入 seat 模块。
//
// 必须在 initPlayerManager 之后：在线发奖要用玩家管理器的本地索引；
// 但要在开始接收连接之前（Boot 里紧跟 initPlayerManager），保证首个玩家
// 登录时模块依赖已就绪。
func (s *Server) initSeat() {
	var store *seat.Store
	if s.database != nil {
		store = seat.NewStore(s.database)
	}
	rewarder := seatRewarder{players: s.players, sys: s.actorSys, store: store, logger: s.logger}
	s.seatMgr = seat.NewManager(s.actorSys, rewarder, store, s.logger)
	seatmod.SetRouter(s.seatMgr)
	if store != nil {
		seatmod.SetPending(store) // Store 同时实现待补发奖励表
	}
	s.logger.Info("seat rooms initialized", "rooms", s.seatMgr.Count())
}


// startKafkaConsumers 启动充值/邮件/跨服私聊的消费组。
func (s *Server) startKafkaConsumers(deduper pkgmq.Deduper) error {
	// ⑤ Kafka 消费者（充值/邮件/跨服私聊）
	if len(s.cfg.Kafka.Brokers) > 0 {
		group, err := gamemq.NewGroup(s.cfg, gamemq.Deps{
			Deduper:  deduper,
			DB:       s.database,
			Presence: s.presence,
			Players:  s.players,
			Sys:      s.actorSys,
			Producer: s.producer,
			Mail:     s.mailMod,
			Chat:     s.chatMod,
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
	return nil
}

// registerEtcd 向注册中心登记本节点（服务发现用）。
func (s *Server) registerEtcd() error {
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
	return nil
}

// startRPCAndClients 起 gRPC server，并连中心服与跨服节点。
func (s *Server) startRPCAndClients() {
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
}
