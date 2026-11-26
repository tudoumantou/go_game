// Package center 中心服（设计文档第 9 章）
//
// 三项职责（9.1）：
//   - 节点管理：游戏服/跨服节点注册、心跳保活、超时摘除；
//   - 跨服匹配：按 elo 分桶收集 MatchRequest，凑人后签发 room_id + token 并下发 MatchResult；
//   - 分组统计 / 排行榜：内存聚合 + Redis 可选，GM 查询走 gRPC。
//
// 中心服是**全服业务中心**：报名裁决、分组、积分聚合、匹配、排行统计；
// 不持有玩家业务数据，也不参与业务服之间的消息转发（后者走 NATS relay.*）。
//
// 本文件是组装层：按「指标 → Redis → 排行榜/统计 → gRPC →
// MySQL 相关组件 → 选主 → 后台节拍」的顺序启动，全部中间件按配置惰性
// 创建——缺哪样就降级运行哪样，而不是启动失败。关闭顺序与启动相反：
// 先 Kick 节点让它们重连别处，再逐层释放。
package center

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"game_study/internal/center/archive"
	"game_study/internal/center/crossplay"
	"game_study/internal/center/leader"
	"game_study/internal/center/match"
	"game_study/internal/center/milestone"
	"game_study/internal/center/rank"
	"game_study/internal/center/registry"
	centerRPC "game_study/internal/center/rpc"
	"game_study/internal/center/scheduler"
	"game_study/internal/center/stats"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	pkgmq "game_study/pkg/mq"
	"game_study/pkg/natsx"
	rpcpkg "game_study/pkg/rpc"
	"game_study/pkg/shutdown"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Server 中心服。
//
// 组装层：把注册表/匹配/排行榜/统计/归档/调度/里程碑/选主八个子系统
// 装配到一台进程里，并持有它们共享的 Redis/MySQL/Kafka 句柄。各子系统
// 自身不感知集群——多实例协作的全部约定（选主、分实例统计）
// 都收敛在本文件的启动编排里。
//
// 字段分四组（顺序即依赖顺序）：
//   - 常驻：reg/matcher（New 即建，纯内存，永不 nil）；
//   - 可选中间件产物：rankSvc/statsSvc/redisCli（需 Redis）、election（需 etcd）、
//     database/producer（需 MySQL/Kafka）——为 nil 即对应能力关闭；
//   - 协议入口：centerRPC（Report + GM 查询）、rpcServer（gRPC 监听）；
//   - 集群身份：instanceID（stats 快照按实例分字段）、groupID（默认分组）。
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	metricsSrv *metrics.Server

	// reg 节点注册表；matcher 匹配池。
	// rankSvc/statsSvc/redisCli 仅当配置了 Redis 时非 nil；election 仅当配置了
	// etcd 时非 nil（无 etcd 时 IsLeader 恒 false，归档/调度/里程碑自动跳过）。
	reg       *registry.Registry
	matcher   *match.Matcher
	rankSvc   *rank.Service
	statsSvc  *stats.Service
	election  *leader.Election
	centerRPC *centerRPC.Server
	rpcServer *rpcpkg.Server
	redisCli  redis.UniversalClient
	// database 中心服 MySQL：赛季状态、归档任务、rank_history、feature_stats
	database *db.DB
	// producer 归档奖励邮件（gs.mail）；未配置 Kafka 时为空，归档仍继续，只是不发奖
	producer     *pkgmq.Producer
	archiveSvc   *archive.Service
	schedSvc     *scheduler.Service
	milestoneSvc *milestone.Service
	// crossSvc 跨服玩法（报名裁决 / 分组 / 积分同步）。nil = 未配 MySQL，
	// 报名时由 rpc 层返回 Code=2，其它功能照常。
	crossSvc *crossplay.Service
	// natsBus 跨服玩法唯一通道（NATS Core）；nil = 未配 NATS，玩法不启用。
	natsBus *natsx.Bus

	// instanceID 本实例唯一标识（stats 快照按实例分字段写，多实例求和而非覆盖）。
	// 用 hostname 派生（见 instanceID）：StatefulSet 下重启后保持稳定。
	instanceID string

	// groupID 本实例负责的分组（demo 单分组；多分组按 node 上报分配）
	// 未带 group_id 的节点统一落到这个默认分组，保证跨服匹配有组可依
	groupID string
}

// New 组装服务器。
//
// 只装配无外部依赖的组件（注册表/匹配器）；依赖 Redis/etcd/Kafka/
// MySQL 的组件保持 nil，由 Start 按配置惰性创建——未配置中间件时中心服以
// 降级模式运行（无排行榜/统计），而不是启动失败。
//
// 这是依赖注入的装配点：New 不碰任何 I/O，可安全用于测试；
// 所有可能失败的连接都在 Start，失败由进程管理器重启兜底。
func New(cfg *config.Config) *Server {
	logger := log.New("center", log.WithLevel(cfg.Log.Level))

	hb, _ := config.ParseDuration(cfg.Report.HeartbeatTimeout)
	reg := registry.New(hb, logger)

	// 跨服地址：P8 阶段用静态配置（生产由 GroupUpdate/配置中心下发）
	// 拷贝一份：append([]string(nil), ...) 防止后续 SetCrossAddrs 的
	// 内部拷贝与 cfg 共享底层数组
	crossAddrs := append([]string(nil), cfg.Cross.Nodes...)
	matcher := match.New(instanceIndex(cfg.Server.Name), crossAddrs, 10*time.Second)

	return &Server{
		cfg:        cfg,
		logger:     logger,
		reg:        reg,
		matcher:    matcher,
		groupID:    "group-01",
		instanceID: instanceID(cfg.Server.Name),
	}
}

// Start 启动服务（阻塞至 ctx 取消）。
//
// 启动顺序即依赖顺序：指标 → Redis → 排行榜/统计 → gRPC →
// MySQL 相关组件（归档/调度/里程碑，仅 leader 执行）→ 选主 → 后台节拍循环。
// 任一步失败直接返回错误由进程管理器重启；ctx 取消时返回 ctx.Err()。
//
// 顺序不可换的三个理由：Redis 要先于排行榜/统计（它们持有 client）；
// gRPC 要先于节点接入（节点一来就要能注册）；选主要晚于归档/调度
// （它们的 Run 需要 IsLeader 已可查询，提前当选会空转一拍）。
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

	// ② Redis（排行榜 ZSet + 统计快照）
	// 未配置时 rankSvc/statsSvc 保持 nil：GM 查询返回空、上报静默丢弃，
	// 中心服其余功能（注册/中转/匹配）不受影响
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
			return fmt.Errorf("redis: %w", err)
		}
		s.redisCli = redisClient.GetClient()
	}

	// ③ 排行榜与统计（Redis 可用时才启用；否则 GM 查询返回空）
	if s.redisCli != nil {
		s.rankSvc = rank.New(s.redisCli, "s1", s.logger)
		s.statsSvc = stats.New(s.redisCli, s.logger)
		// 快照按实例分字段写：多中心服实例求和而不是互相覆盖（R8.4）
		// instanceID 来自 hostname：重启后不变，Redis 里旧实例的 field
		// 才会被同名 field 覆盖刷新，否则会无限堆积
		s.statsSvc.SetInstanceID(s.instanceID)
	}

	// ④ gRPC（CenterService.Report + GM 查询）
	// 先 NewServer 再注入查询实现，最后才注册到 gRPC server——
	// 注入发生在任何节点连上来之前，无并发写问题
	s.centerRPC = centerRPC.NewServer(s.reg, s, s.logger)
	if s.rankSvc != nil {
		s.centerRPC.SetRankQuerier(s.rankSvc)
		s.centerRPC.SetRankSearcher(s.rankSvc)
	}
	if s.statsSvc != nil {
		s.centerRPC.SetStatsQuerier(s.statsSvc)
	}
	s.rpcServer = rpcpkg.NewServer(s.cfg.Server.GRPCAddr, s.logger)
	internalv2.RegisterCenterServiceServer(s.rpcServer.GRPCServer(), s.centerRPC)

	go func() {
		if err := s.rpcServer.Start(ctx); err != nil && err != context.Canceled {
			s.logger.Error("grpc server error", "err", err)
		}
	}()

	// ⑥ 赛季归档与统计落库（R8.3/R8.4）：只有 leader 执行，依赖 MySQL
	// MySQL 未配置时整块关闭：归档/调度/里程碑全部需要库，没有库
	// 连 leader 任务都没有意义（它们的断点全在库里）
	if s.cfg.MySQL.DSN != "" {
		database, err := db.New(db.Config{
			DSN:     s.cfg.MySQL.DSN,
			MaxOpen: s.cfg.MySQL.MaxOpenConns,
			MaxIdle: s.cfg.MySQL.MaxIdleConns,
		})
		if err != nil {
			return fmt.Errorf("center mysql: %w", err)
		}
		s.database = database

		if len(s.cfg.Kafka.Brokers) > 0 {
			// 归档发奖与里程碑发奖共用一个 producer：批量小、频率低，
			// 复用连接比分两套省资源；Flush 时两侧一起刷出
			s.producer = pkgmq.NewProducer(pkgmq.Config{Brokers: s.cfg.Kafka.Brokers})
		}
		s.archiveSvc = archive.New(database, s.rankSvc, s.statsSvc, s.producer, archive.Config{
			SeasonDays:   s.cfg.Rank.SeasonDays,
			RankTypes:    s.cfg.Rank.RankTypes,
			TopN:         int64(s.cfg.Rank.TopN),
			RewardTop:    s.cfg.Rank.RewardTop,
			RewardItemID: s.cfg.Rank.RewardItemID,
			RewardCount:  s.cfg.Rank.RewardCount,
			GroupID:      s.groupID,
			StatsEnabled: s.statsSvc != nil,
		}, s.logger)
		go s.archiveSvc.Run(ctx, s.IsLeader)
		// 活动实例调度：算当前周期 → 落库 → 下发 gs.activity.schedule（R7.1/R7.2）
		s.schedSvc = scheduler.New(database, s.producer, s.logger)
		go s.schedSvc.Run(ctx, s.IsLeader)

		// 跨服玩法：报名裁决 / 分组 / 积分同步下发。
		// 必须在 centerRPC 已经开始接客之前完成注入——否则启动瞬间的报名
		// 请求会命中 nil handler 返回「未启用」。这里注册在 gRPC server
		// 启动之后，但节点要先完成 Register 才会发业务报文，实际有足够提前量。
		s.crossSvc = crossplay.New(crossplay.NewRepo(database, s.logger), s.logger, crossplay.Config{
			// 分组与周期写任务只由 leader 跑：多实例时否则各算一份分组、各发一版快照。
			// 方法值在选主之前取也没问题——IsLeader 内部读 election.IsLeader()，
			// 选主未建立时返回 false，tickLoop 会安全跳过。
			IsLeader: s.IsLeader,
		})
		s.centerRPC.SetCrossHandler(s)

		// NATS 异步通道（NATS Core）：跨服玩法唯一传输（积分上报订阅 + 分组
		// 下发发布）。未配 URL 时 natsBus 为 nil、玩法不启用——报名仍可经
		// gRPC 到达，但会被游戏侧的 NATS 就绪检查拒绝。
		if s.cfg.Nats.URL != "" {
			rw, _ := config.ParseDuration(s.cfg.Nats.ReconnectWait)
			to, _ := config.ParseDuration(s.cfg.Nats.Timeout)
			bus, err := natsx.Connect(natsx.Config{
				URL:           s.cfg.Nats.URL,
				ReconnectWait: rw,
				MaxReconnect:  s.cfg.Nats.MaxReconnect,
				Timeout:       to,
			})
			if err != nil {
				return fmt.Errorf("nats: %w", err)
			}
			s.natsBus = bus
			s.startCrossplayNATS(bus)
		}

		// 重启恢复：先读分组表再开定时器，避免定时器拿着空分组去校准。
		// 恢复失败只告警不回滚——活动期内还能靠各服上报逐步重建。
		if err := s.crossSvc.Restore(ctx); err != nil {
			s.logger.Warn("cross play restore failed, start with empty groups", "err", err)
		}
		s.crossSvc.Start()

		// 全服里程碑（R7.3/R7.4）：消费贡献 + leader 判定与发奖
		// 需要 Redis + Kafka 双全：进度在 Redis、任务在 MySQL、奖励走 Kafka
		if s.redisCli != nil && len(s.cfg.Kafka.Brokers) > 0 {
			consumer := pkgmq.NewConsumer(pkgmq.Config{Brokers: s.cfg.Kafka.Brokers},
				pkgmq.TopicActivityProgress, "center-activity-progress", s.producer)
			s.milestoneSvc = milestone.New(database, s.redisCli, s.producer, consumer, s.logger)
			go func() {
				if err := s.milestoneSvc.RunConsumer(ctx); err != nil && ctx.Err() == nil {
					s.logger.Warn("activity progress consumer stopped", "err", err)
				}
			}()
			go s.milestoneSvc.Run(ctx, s.IsLeader)
		}
	} else {
		s.logger.Warn("mysql not configured, season archive and stats persistence disabled")
	}

	// ⑦ 选主（9.6）：leader 负责赛季归档、全服活动调度等有主任务
	// 放在所有 Run(ctx, IsLeader) 之后：IsLeader 回调随时可能触发，
	// 而各任务循环要已经跑起来才能接住状态翻转
	s.startElection(ctx)

	// ⑦ 后台任务：心跳巡检 + 匹配 sweep + 统计快照 + 指标
	go s.tickLoop(ctx)

	s.logger.Info("CenterServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"group", s.groupID, "cross_nodes", s.cfg.Cross.Nodes)
	// 阻塞点：Start 的返回即「关闭信号已到达」，main 随后编排 Shutdown
	//（两阶段优雅退出的第一阶段在这里结束，见 pkg/bootstrap）
	<-ctx.Done()
	// 返回 nil 而不是 ctx.Err()：契约是「只有启动失败才返回 error」。
	// 返回 ctx.Err() 会让调用方当成启动失败而直接退出，Shutdown 永不执行。
	return nil
}

// tickLoop 统一节拍循环（1s）：节点心跳巡检、匹配放宽、指标上报。
//
// 统计快照（5 分钟）与指标（10 秒）在同一循环里用时间差节流，而不是各起
// 定时器：中心服后台任务种类多但都很轻，单循环足够且关闭时只需等一个
// goroutine 退出。
//
// now 取自 ticker.C 而非 time.Now()：同一拍里巡检/超时判定用同一时钟，
// 避免「心跳刚更新但巡检用更晚的 now」造成的边界抖动。
func (s *Server) tickLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var (
		lastMetric   time.Time
		lastSnapshot time.Time
	)
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// 心跳巡检：摘除失联节点，并广播组内
			for _, dead := range s.reg.Tick(now) {
				s.broadcastGroupUpdate(dead.GroupID)
			}

			// 匹配放宽
			for _, res := range s.matcher.Sweep() {
				s.deliverMatchResult(res)
			}

			// 统计快照（每 5 分钟，9.5：内存聚合 → Redis Hash）
			// 5s 超时：快照失败只丢这一轮（下轮补），不该拖住节拍循环
			if s.statsSvc != nil && now.Sub(lastSnapshot) >= 5*time.Minute {
				lastSnapshot = now
				snapCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				if err := s.statsSvc.Snapshot(snapCtx); err != nil {
					s.logger.Warn("stats snapshot failed", "err", err)
				}
				cancel()
			}

			// 指标（每 10s）
			if now.Sub(lastMetric) >= 10*time.Second {
				lastMetric = now
				s.reportMetrics()
			}
		}
	}
}

// reportMetrics 上报中心服指标。
func (s *Server) reportMetrics() {
	if !metrics.Enabled() {
		return
	}
	total, game, cross := s.reg.Count()
	metrics.CenterNodes("total", total)
	metrics.CenterNodes("game", game)
	metrics.CenterNodes("cross", cross)

	waiting, _ := s.matcher.Stats()
	metrics.CenterMatchQueue(waiting)
	if s.statsSvc != nil {
		metrics.CenterStatsSeries(s.statsSvc.SeriesCount())
	}
	if s.election != nil {
		metrics.CenterIsLeader(s.election.IsLeader())
	}
}

// readiness 就绪探针：中心服无强依赖，注册表就绪即可。
// 刻意不检查 Redis/etcd/MySQL：那些是可选降级项，探针该回答
// 「进程能不能服务」而不是「功能全不全」。返回 error 让 metrics
// server 的 /ready 返回 503，k8s 据此摘流。
func (s *Server) readiness(_ context.Context) error {
	if s.reg == nil {
		return fmt.Errorf("registry not initialized")
	}
	return nil
}

// OnRegister 实现 centerRPC.Handler：节点注册后下发分组信息。
//
// 未携带 group_id 的节点分配到本实例默认分组并回写注册表；随后向组内广播
// 最新成员列表，保证新节点立刻拿到同组其它节点与跨服地址。返回值是回给
// 该节点的初始指令（GroupUpdate）。
//
// 回写用 Register(node, nil)：nil sender 保留原通道（rpc 层已注册过
// 同一个 session），只更新 GroupID 字段。
func (s *Server) OnRegister(node *registry.Node) []*internalv2.CenterCommand {
	// 未指定分组的节点 → 分配到本实例默认分组
	if node.GroupID == "" {
		node.GroupID = s.groupID
		s.reg.Register(node, nil) // 回写 group_id
	}
	s.broadcastGroupUpdate(node.GroupID)
	return []*internalv2.CenterCommand{s.groupUpdateCmd(node.GroupID)}
}

// broadcastGroupUpdate 通知组内所有节点最新的成员列表。
// 全组广播而不是只回给新节点：组成员变化（上线/下线/心跳超时）要
// 让所有人知道——匹配池的跨服地址、点对点中转的成员表都依赖它。
func (s *Server) broadcastGroupUpdate(groupID string) {
	if groupID == "" {
		return
	}
	cmd := s.groupUpdateCmd(groupID)
	for _, n := range s.reg.ListByGroup(groupID, "") {
		if sender, ok := s.reg.SenderFor(n.ID); ok {
			_ = sender.SendCommand(cmd)
		}
	}
}

// groupUpdateCmd 构造 GroupUpdate 指令。
func (s *Server) groupUpdateCmd(groupID string) *internalv2.CenterCommand {
	games := s.reg.ListByGroup(groupID, registry.TypeGame)
	crosses := s.reg.ListByGroup(groupID, registry.TypeCross)

	gameNodes := make([]string, 0, len(games))
	for _, n := range games {
		gameNodes = append(gameNodes, n.ID)
	}
	crossAddrs := make([]string, 0, len(crosses))
	for _, n := range crosses {
		crossAddrs = append(crossAddrs, n.Addr)
	}
	// 中心服配置的静态跨服地址兜底（跨服节点未上报时也能匹配）
	// 兜底顺序：动态注册的跨服节点优先，静态配置垫底——
	// 生产里跨服节点先注册，配置只是首启动时的引导
	if len(crossAddrs) == 0 {
		crossAddrs = append(crossAddrs, s.cfg.Cross.Nodes...)
	}
	// 匹配器同步可用跨服地址
	// 副作用放在构造函数里：GroupUpdate 每次构造都顺带刷新匹配器，
	// 不需要额外的同步调用点——代价是该函数不再是纯函数
	s.matcher.SetCrossAddrs(crossAddrs)

	return &internalv2.CenterCommand{
		Body: &internalv2.CenterCommand_Group{Group: &internalv2.GroupUpdate{
			GroupId:    groupID,
			GameNodes:  gameNodes,
			CrossAddrs: crossAddrs,
			Action:     "update",
		}},
	}
}

// OnMatchRequest 实现 centerRPC.Handler：入队匹配。
//
// nodeID 是发起匹配的游戏服（结果回传目标）。入队可能立即成局（返回多条
// Result，每条对应一名玩家），逐条下发；未成局时入队等待，由 tickLoop 的
// Sweep 在超时后放宽 elo 桶再配。
//
// 同步路径零延迟：Enqueue 内部就尝试同桶配对，大部分请求在入队时
// 已经成局，不需要等下一拍 Sweep。
func (s *Server) OnMatchRequest(nodeID string, req *internalv2.MatchRequest) {
	if req == nil || req.PlayerId == 0 {
		return
	}
	t := &match.Ticket{
		NodeID:   nodeID,
		PlayerID: req.PlayerId,
		Mode:     req.Mode,
		Elo:      req.Elo,
		Snapshot: req.Snapshot,
	}
	results := s.matcher.Enqueue(t)
	for _, res := range results {
		s.deliverMatchResult(res)
	}
}

// deliverMatchResult 把匹配结果下发给发起方游戏服。
// 逐玩家下发而不是整局一条：每个玩家的 token 不同，且各自经
// SenderFor 定位自己的流——节点掉线时只影响那条流上的玩家。
func (s *Server) deliverMatchResult(res *match.Result) {
	if res == nil || len(res.Players) == 0 {
		return
	}
	for _, p := range res.Players {
		sender, ok := s.reg.SenderFor(p.NodeID)
		if !ok {
			s.logger.Warn("match result target offline", "node", p.NodeID, "player", p.PlayerID)
			continue
		}
		cmd := &internalv2.CenterCommand{
			Body: &internalv2.CenterCommand_MatchResult{MatchResult: &internalv2.MatchResult{
				RoomId:       res.RoomID,
				BattleToken:  p.Token,
				Mode:         res.Mode,
				CrossAddr:    res.CrossAddr,
				PeerId:       p.PeerID,
				PeerSnapshot: p.PeerSnap,
				PlayerId:     p.PlayerID, // 接收方据此确认这条结果属于谁
			}},
		}
		if err := sender.SendCommand(cmd); err != nil {
			s.logger.Warn("deliver match result failed", "node", p.NodeID, "err", err)
			continue
		}
	}
	s.logger.Info("match result delivered", "room_id", res.RoomID,
		"mode", res.Mode, "cross", res.CrossAddr, "players", len(res.Players))
}

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，由 pkg/shutdown 编排）。
//
// 顺序即安全顺序：先通知节点改连（否则它们要等 gRPC 强停才发现本实例没了）
// → 停 gRPC → 刷 producer → 关 DB → 停指标。gRPC 带超时强停，避免 Report
// 流永不结束卡死整个关闭流程（与 P13 网关下线同类问题）。
//
// 原先每一步的失败只打一行 Warn、函数恒返回 nil：producer 刷不出去（发货
// 消息丢）与 metrics 关不掉（无关紧要）在返回值上没有区别。现在统一进
// 返回值，由 main 转成非零退出码。
func (s *Server) Shutdown(ctx context.Context) error {
	mgr := shutdown.NewManager(s.logger)

	// DRAIN：通知所有节点「准备关闭」，让它们尽早重连到其它中心服实例。
	mgr.Register("kick-nodes", shutdown.PhaseDrain, func(context.Context) error {
		s.kickAllNodes()
		return nil
	})

	// DRAINING：停 gRPC（带超时，避免 Report 流永不结束导致卡死 —— 与 P13 同类问题）。
	mgr.Register("stop-grpc", shutdown.PhaseDraining, func(context.Context) error {
		if s.rpcServer != nil {
			s.rpcServer.StopWithTimeout(3 * time.Second)
		}
		return nil
	})

	// FLUSH：归档/统计收尾 + producer 刷出。Flush 在 Close 之前：没刷出的
	// 发货消息会随 producer 一起蒸发。
	mgr.Register("close-milestone", shutdown.PhaseFlush, func(context.Context) error {
		if s.milestoneSvc == nil {
			return nil
		}
		return s.milestoneSvc.Close()
	})
	mgr.Register("flush-producer", shutdown.PhaseFlush, func(cctx context.Context) error {
		if s.producer == nil {
			return nil
		}
		err := s.producer.Flush(cctx)
		closeErr := s.producer.Close()
		return errors.Join(err, closeErr)
	})

	// FLUSH：跨服玩法把内存里的积分刷一次盘。
	//
	// 必须在 close-db 之前：积分以内存为权威，宕机丢可以接受，
	// 但正常关服丢就是流程错误。Stop 内部自己控制 context，
	// 不受本阶段 ctx 超时影响。
	mgr.Register("stop-crossplay", shutdown.PhaseFlush, func(context.Context) error {
		if s.crossSvc == nil {
			return nil
		}
		s.crossSvc.Stop()
		// 先关 NATS（停止收发），再让后续阶段关库：避免关库后还收到上报。
		if s.natsBus != nil {
			s.natsBus.Close()
		}
		return nil
	})

	// STOP：关 DB、停指标。
	mgr.Register("close-db", shutdown.PhaseStop, func(context.Context) error {
		if s.database == nil {
			return nil
		}
		return s.database.Close()
	})
	mgr.Register("stop-metrics", shutdown.PhaseStop, func(cctx context.Context) error {
		if s.metricsSrv != nil {
			return s.metricsSrv.Stop(cctx)
		}
		return nil
	})

	if err := mgr.Run(ctx); err != nil {
		return fmt.Errorf("center shutdown: %w", err)
	}
	s.logger.Info("center stopped")
	return nil
}

// kickAllNodes 向所有节点下发 KickNode（滚动重启时节点据此重连其它实例）。
// game/cross 全踢：匹配结果、中转、房间回推都可能落在任一类节点上，
// 只踢一类会让另一类在 gRPC 强停时瞬断。
func (s *Server) kickAllNodes() {
	nodes := s.reg.ListByType(registry.TypeGame)
	crosses := s.reg.ListByType(registry.TypeCross)
	all := append(nodes, crosses...)
	for _, n := range all {
		if sender, ok := s.reg.SenderFor(n.ID); ok {
			_ = sender.SendCommand(&internalv2.CenterCommand{
				Body: &internalv2.CenterCommand_Kick{Kick: &internalv2.KickNode{
					NodeId: n.ID,
					Reason: "rolling_restart",
				}},
			})
		}
	}
	if len(all) > 0 {
		s.logger.Info("notified nodes to reconnect", "count", len(all))
	}
}

// instanceIndex 从节点名推导实例序号（如 center-02 → 2），用于拼装全局唯一 room_id。
//
// 逆序遍历名字尾部的连续数字：StatefulSet 的 Pod 名固定以序号结尾；
// 名字不含数字时返回 0（所有实例共享 0 号段，靠时间戳段区分）。
func instanceIndex(name string) uint32 {
	var idx uint32
	for i := len(name) - 1; i >= 0; i-- {
		c := name[i]
		if c < '0' || c > '9' {
			break
		}
		idx = idx*1 + uint32(c-'0')*pow10(uint32(len(name)-1-i))
	}
	return idx
}

// pow10 计算 10 的 n 次方（instanceIndex 逐位还原实例序号用，n 为名字尾部数字位数）。
// 逐位 ×10 累加，比先转字符串再 ParseInt 少一次分配——
// 实例序号只在启动时算一次，但这是纯函数，怎么简单怎么来。
func pow10(n uint32) uint32 {
	out := uint32(1)
	for i := uint32(0); i < n; i++ {
		out *= 10
	}
	return out
}

// startElection 参与 etcd 选主（9.6）。etcd 未配置时跳过（单实例部署无需选主）。
//
// 租约 TTL 缺省 10s：leader 崩溃后最长约一个 TTL 完成切换，期间有主任务
// （归档/调度/里程碑）空转等新主；TTL 越短切换越快，但 etcd 续约压力越大。
//
// 客户端创建失败只告警不返回错误：选主是增强能力，etcd 挂了不该让
// 中心服整个起不来——退化成「无主」模式，无主任务不跑而已。
func (s *Server) startElection(ctx context.Context) {
	if len(s.cfg.Etcd.Endpoints) == 0 {
		s.logger.Info("etcd not configured, skip leader election")
		return
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   s.cfg.Etcd.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		s.logger.Warn("etcd client for election failed", "err", err)
		return
	}

	ttl := int64(s.cfg.Etcd.LeaseTTL)
	if ttl <= 0 {
		ttl = 10
	}
	s.election = leader.New(cli, leader.Config{
		Key:    s.cfg.Etcd.LeaderKey,
		SelfID: s.cfg.Server.Name,
		TTL:    ttl,
		OnChange: func(isLeader bool) {
			if isLeader {
				s.logger.Info("this instance is now the center leader")
			} else {
				s.logger.Info("this instance is no longer the leader")
			}
		},
	}, s.logger)

	go func() {
		if err := s.election.Run(ctx); err != nil && ctx.Err() == nil {
			s.logger.Warn("leader election stopped", "err", err)
		}
	}()
}

// IsLeader 当前实例是否为主（GM/诊断用）。
//
// 未配置 etcd 时恒为 false：此时归档/调度/里程碑等以 IsLeader 为开关的
// 任务都不会执行——单实例未配 etcd 的已知降级路径（要跑这些能力必须配 etcd）。
//
// 供 Run(ctx, s.IsLeader) 以方法值传入：tick 循环每 30s 调一次，
// atomic 读无锁，不构成热点。
func (s *Server) IsLeader() bool {
	return s.election != nil && s.election.IsLeader()
}

// instanceID 生成本实例唯一标识。
//
// 用 hostname 而非纯随机：StatefulSet 的 Pod 名稳定（center-0/center-1），
// 重启后消费组不变，可续上上次提交的 offset；纯随机会在每次重启时
// 创建新消费组，重启期间的中转消息直接丢失。
//
// hostname 获取失败才退回 serviceName+时间戳：极端环境（容器没配
// hostname）下至少保证唯一，只是失去重启稳定性。
func instanceID(serviceName string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return fmt.Sprintf("%s-%d", serviceName, time.Now().UnixNano())
	}
	return serviceName + "@" + host
}
