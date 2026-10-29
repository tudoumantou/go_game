// internal/game/server.go —— 游戏服进程装配与生命周期管理
//
// 本文件是 game 包的组装层：New 只构造轻量对象，Start 按固定顺序拉起全部外部
// 依赖（指标 → MySQL → Redis → Actor 系统 → 模块 → Flusher → Kafka → etcd →
// gRPC），收到退出信号后由 Shutdown 按 DRAIN→DRAINING→FLUSH→STOP 反向收场。
//
// 关键设计取舍：
//   - 组件生命周期统一挂内部 runCtx，与外部信号 ctx 解耦：SIGTERM 时外部 ctx
//     立即取消，但 Actor 必须活到「排空在途消息」之后才停，否则保存没有对象可等；
//   - 启动顺序即依赖顺序，后一步可能依赖前一步已就绪的能力（模块注册需要
//     Actor 系统、Kafka 消费组需要 PlayerManager），调整前先对照
//     docs/游戏服启动流程解析.md 第 3 节的顺序表；
//   - 缓存权威模式下进程内默认起 inline writebehind worker：避免忘记部署独立
//     flusher 时脏数据永远不落库；两者并行也安全（版本栅栏保证）；
//   - 会话编排（顶号/下行/懒加载）在本文件，玩家状态在 player 包，离线补投
//     在 offline.go——组装层只做「翻译」，不持有任何玩家业务数据。

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
	"game_study/internal/game/module/battle"
	"game_study/internal/game/module/chat"
	"game_study/internal/game/module/mail"
	gamemq "game_study/internal/game/mq"
	"game_study/internal/game/player"
	"game_study/internal/game/rpc"
	"game_study/internal/game/scene"
	"game_study/internal/game/seat"
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
	"game_study/pkg/shutdown"
	"game_study/pkg/writebehind"
)

// onlineTTL 在线状态 TTL（客户端 15s 心跳，3 倍冗余）。
const onlineTTL = 45 * time.Second

// Server 游戏服（组装层入口，职责见设计文档 3.1 目录树）
//
// 字段按「启动顺序」大致分组：基础设施（db/redis/actorSys）→ 业务容器
// （modules/players/sceneMgr）→ 持久化（repo/flusher）→ 外联
// （crossPool/centerCli/producer/consumers）→ 会话表（见下方 mu 注释）。
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	rpcServer  *rpcpkg.Server
	etcdReg    *gameetcd.Registry
	etcdCancel context.CancelFunc
	gameRPC    *rpc.GameServer

	actorSys *actor.System
	sceneMgr *scene.Manager
	modules  *module.Registry
	players  *player.Manager
	database *db.DB
	repo     *player.Repository
	flusher  *flush.Flusher

	redis      *cache.Client
	cacheStore *writebehind.Store // Redis 快照副本（只读写副本，不承载落库）
	presence   *cache.Presence
	producer   *pkgmq.Producer
	consumers  *gamemq.Group
	roleSvc    *service.RoleService
	metricsSrv *metrics.Server
	idTable    *cache.IDTable // ID 表：存在性判断从 MySQL 挪到 Redis（12.5）

	// ownerIdx 归属索引（by_owner:{table} ZSET）：一对多表按角色展开，
	// 登录时一次取出某玩家的全部实体 id（缓存架构设计 §3）。按表名索引，
	// 由两处声明合并驱动：配置文件 idtab.mappings（部署开关）与模块的
	// IndexSpecs（代码声明）。preload_all 的表只预热实体不建索引，
	// 不出现在这里。
	ownerIdx map[string]*cache.OwnerIndex

	// indexMappings 生效的索引声明（yaml + 模块声明合并后）。
	//
	// 构建与对账都遍历这一份，而不是 s.cfg.IDTab.Mappings：模块声明了但
	// yaml 漏配的索引也要被建、被对账，否则「代码要用、配置没写」的索引
	// 会是一个永远为空的 ZSET。
	indexMappings []config.CacheMapping

	// mailMod/chatMod 是模块实例，Kafka 消费者要直接调它们（发邮件/落聊天历史）。
	// 提升为字段而不是 initModules 的局部变量：装配被拆成多个方法后，
	// 跨方法的依赖只能挂在 Server 上。
	mailMod *mail.Module
	chatMod *chat.Module

	crossPool *gamecross.Pool    // 到跨服节点的连接池
	centerCli *gamecenter.Client // 到中心服的 Report 流
	battleMod *battle.Module     // 战斗模块（匹配/透传/结算）
	seatMgr   *seat.Manager      // 抢位置房间管理器（单服座位 + 离线奖励）

	// mu 保护下面三张会话映射表的全部读写；players2sess 是
	// player→session 的唯一权威映射，顶号判定与下行路由都读它，
	// 必须与 sessions/accounts 在同一把锁内原子更新。
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
	// ① 基础设施：指标 → MySQL → Redis → ID 表 → 中心服客户端。
	// 顺序不能换：中心服要在 Actor 系统之前建（actor.Config.Remote 引用它）。
	if err := s.startMetrics(); err != nil {
		return err
	}
	if err := s.initMySQL(); err != nil {
		return err
	}
	deduper, err := s.initRedis()
	if err != nil {
		return err
	}
	s.initIDTable()
	s.initOwnerIndex()
	s.initCenterClient()

	// ② Actor 系统 → 场景 → 跨服/battle → Kafka 生产者 → 模块注册
	if err := s.initActorSystem(); err != nil {
		return err
	}
	if err := s.initScenes(); err != nil {
		return err
	}
	s.initBattleAndProducer()
	s.initModules()
	// 归属索引的「收集声明 → 补齐 → 注入」必须在模块注册之后：声明挂在
	// 模块实例上（module.IndexDeclarer），没注册就看不到。构建 goroutine
	// 也由这里启动（initOwnerIndex 只建 yaml 里写了的那部分）。
	s.attachOwnerIndexes()

	// ③ 落库链路：flusher → writebehind worker → player manager
	if err := s.startFlusher(); err != nil {
		return err
	}
	s.initPlayerManager()
	s.initSeat()

	// ④ 外部系统：Kafka 消费者 → etcd 注册 → gRPC + 长连接
	if err := s.startKafkaConsumers(deduper); err != nil {
		return err
	}
	if err := s.registerEtcd(); err != nil {
		return err
	}
	s.startRPCAndClients()

	s.logger.Info("GameServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"players", s.players.Count())
	// 至此全部组件就绪：启动失败提前返回的窗口关闭。之后即使 ctx 取消也只表示
	// 「收到退出信号」，资源回收统一走 Shutdown 的有序收尾。
	started = true
	// 阻塞运行期：外部 ctx 只负责通知，不停任何组件（见 runCtx 字段注释）。
	<-ctx.Done()
	// 返回 nil 而不是 ctx.Err()：本函数的契约是「只有启动失败才返回 error」。
	// 返回 ctx.Err() 会让调用方把它当成启动失败而直接退出，Shutdown 永不执行
	// ——这条契约曾经在六个进程里各写错一次（v3.19 修复）。
	return nil
}

// readiness 就绪探针：DB/Redis 可用才允许接流量（19.6 探针表）。
func (s *Server) readiness(ctx context.Context) error {
	// nil 检查的含义是「未配置则不检查」：演示/单测环境可以只起部分依赖，
	// 探针只对实际启用的组件负责，不会因为没配 MySQL 就永远 not ready。
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
	mgr.Register("flush-seat", shutdown.PhaseFlush, func(context.Context) error {
		// 座位是合并写：Actor 已 Drain，这里把最后一轮脏标记写完再停 worker。
		if s.seatMgr == nil {
			return nil
		}
		if err := s.seatMgr.Stop(); err != nil {
			return fmt.Errorf("seat: %w", err)
		}
		return nil
	})
	mgr.Register("flush-all", shutdown.PhaseFlush, func(ctx context.Context) error {
		var errs []error
		// 落库只有一条路：flusher 从 Actor 内存取数写库。
		// （Redis 队列那条通道已删除，见 v3.6 的取舍说明。）
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

	// Run 按 Phase 顺序执行、同阶段内按注册顺序；单步失败不中断后续步骤
	// （跳过清理只会让进程更难收场），但所有错误会聚合进返回值。
	if err := mgr.Run(ctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	s.logger.Info("game stopped")
	return nil
}

// chatMessenger 实现 chat.Messenger（在线判定/本服推送/跨服生产/离线暂存）。
//
// 四个方法对应私聊的完整生命周期：找节点 → 推在线 → 跨服转发 → 离线暂存。
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

// defaultReconcileInterval 是 idtab.reconcile_interval 缺省/非法时的兜底。
//
// 与配置默认值同一个数，但必须在这里再兜一次：核对停摆不会报错，
// 只会让 ID 表与库里悄悄漂移，等到「玩家 ID 被判不存在」才暴露。
const defaultReconcileInterval = 10 * time.Minute

// reconcileInterval 读 idtab.reconcile_interval，非法值回落默认。
//
// ID 表与归属索引共用这一个值：两者都是「ZCARD/COUNT 与库里行数对一次」，
// 拆成两个配置项只会让运维配了一个以为两个都改了。
func (s *Server) reconcileInterval() time.Duration {
	if d, err := config.ParseDuration(s.cfg.IDTab.ReconcileInterval); err == nil && d > 0 {
		return d
	}
	return defaultReconcileInterval
}

// maintainIDTables ID 表维护（12.5.3）。
//
//   - 每 reconcile_interval（默认 10 分钟）：对热表做数量核对，不一致则重建
//     （重建期间回落 DB，不阻塞查询）
//   - 每小时：清理孤儿实体 hash（e:{table}:{id} 存在但 idtab 无此 id）
func (s *Server) maintainIDTables(ctx context.Context) {
	reconcile := time.NewTicker(s.reconcileInterval())
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

// buildOwnerIndexes 后台构建归属索引与整表预热（缓存架构设计 §3）。
//
// 每个映射二选一（或都做）：
//   - preload_all：idTable.Preheat 把整表实体行搬进 Redis hash；
//   - 带 by：OwnerIndex.BuildOne 建 ZSET（取材 → 临时 key + RENAME → ready）。
//
// 逐映射构建、记录第一错误但不中断——一个映射配错（比如 by 列不存在）
// 不该让其余映射也建不起来。ready 未置位期间登录展开回落 MySQL。
func (s *Server) buildOwnerIndexes(ctx context.Context) {
	for _, m := range s.indexMappings {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		if m.PreloadAll {
			n, err := s.idTable.Preheat(ctx, cache.TableMeta{Name: m.Table, PK: m.Key})
			if err != nil {
				s.logger.Warn("owner preheat failed", "table", m.Table, "err", err)
				continue
			}
			s.logger.Info("owner preheat done", "table", m.Table, "count", n,
				"elapsed", time.Since(start).Round(time.Millisecond))
			continue
		}
		oi, ok := s.ownerIdx[m.Table]
		if !ok {
			continue
		}
		n, err := oi.BuildOne(ctx)
		elapsed := time.Since(start)
		if err != nil {
			s.logger.Warn("owner index build failed", "table", m.Table, "err", err)
			continue
		}
		if metrics.Enabled() {
			metrics.OwnerIndexBuildObserve(m.Table, elapsed)
			metrics.OwnerIndexState(m.Table, n, true)
		}
		s.logger.Info("owner index built", "table", m.Table, "count", n,
			"elapsed", elapsed.Round(time.Millisecond))
	}
	s.logger.Info("owner index build done", "mappings", len(s.indexMappings))
}

// maintainOwnerIndexes 归属索引维护（缓存架构设计 §5）：每 10 分钟数量核对。
//
// 与 maintainIDTables 的 Reconcile 同构：ZCARD 与库里行数不一致 → 重建
// （重建期间 Members 回落 DB，不阻塞登录）。只核对索引，实体 hash 的
// 孤儿清理由 IDTable.CleanOrphans 负责（同一个 e:{table}:{id} 空间）。
func (s *Server) maintainOwnerIndexes(ctx context.Context) {
	ticker := time.NewTicker(s.reconcileInterval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, m := range s.indexMappings {
				if ctx.Err() != nil {
					return
				}
				oi, ok := s.ownerIdx[m.Table]
				if !ok {
					continue
				}
				rebuilt, err := oi.Reconcile(ctx)
				if err != nil {
					s.logger.Warn("owner index reconcile failed", "table", m.Table, "err", err)
					continue
				}
				if rebuilt {
					if metrics.Enabled() {
						metrics.OwnerIndexReconcileMismatch(m.Table)
					}
					s.logger.Warn("owner index rebuilt after mismatch", "table", m.Table)
				}
				if n, err := oi.Index().Count(ctx); err == nil {
					if metrics.Enabled() {
						metrics.OwnerIndexState(m.Table, n, oi.Ready(ctx))
					}
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
