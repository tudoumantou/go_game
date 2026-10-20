// Package center 中心服（设计文档第 9 章）
//
// 四项职责（9.1）：
//   - 节点管理：游戏服/跨服节点注册、心跳保活、超时摘除；
//   - 消息中转：RelayRequest → RelayPush（点对点 / 组播），集群跨实例经 Kafka（P9）；
//   - 跨服匹配：按 elo 分桶收集 MatchRequest，凑人后签发 room_id + token 并下发 MatchResult；
//   - 分组统计 / 排行榜：P8 阶段落地基础版（内存聚合 + Redis 可选），GM 查询走 gRPC。
//
// 中心服**不持有玩家业务数据**，重启后由节点重连补报即可重建（9.1）。
//
// 本文件是组装层：按「指标 → Redis → 排行榜/统计 → gRPC → 中转桥 →
// MySQL 相关组件 → 选主 → 后台节拍」的顺序启动，全部中间件按配置惰性
// 创建——缺哪样就降级运行哪样，而不是启动失败。关闭顺序与启动相反：
// 先 Kick 节点让它们重连别处，再逐层释放。
package center

import (
	"context"
	"fmt"
	"os"
	"time"

	"game_study/internal/center/archive"
	"game_study/internal/center/leader"
	"game_study/internal/center/match"
	"game_study/internal/center/milestone"
	"game_study/internal/center/rank"
	"game_study/internal/center/registry"
	"game_study/internal/center/relay"
	centerRPC "game_study/internal/center/rpc"
	"game_study/internal/center/scheduler"
	"game_study/internal/center/stats"
	"game_study/pkg/cache"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	pkgmq "game_study/pkg/mq"
	rpcpkg "game_study/pkg/rpc"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/redis/go-redis/v9"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Server 中心服。
//
// 组装层：把注册表/匹配/中转/排行榜/统计/归档/调度/里程碑/选主九个子系统
// 装配到一台进程里，并持有它们共享的 Redis/MySQL/Kafka 句柄。各子系统
// 自身不感知集群——多实例协作的全部约定（选主、跨实例中转、分实例统计）
// 都收敛在本文件的启动编排里。
//
// 字段分四组（顺序即依赖顺序）：
//   - 常驻：reg/matcher/router（New 即建，纯内存，永不 nil）；
//   - 可选中间件产物：rankSvc/statsSvc/redisCli（需 Redis）、election（需 etcd）、
//     database/producer（需 MySQL/Kafka）——为 nil 即对应能力关闭；
//   - 协议入口：centerRPC（Report + GM 查询）、rpcServer（gRPC 监听）；
//   - 集群身份：instanceID（消费组后缀 + 自发自收去重）、groupID（默认分组）。
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	metricsSrv *metrics.Server

	// reg 节点注册表；matcher 匹配池；router 同实例中转路由。
	// rankSvc/statsSvc/redisCli 仅当配置了 Redis 时非 nil；election 仅当配置了
	// etcd 时非 nil（无 etcd 时 IsLeader 恒 false，归档/调度/里程碑自动跳过）。
	reg       *registry.Registry
	matcher   *match.Matcher
	router    *relay.Router
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

	// relayBridge 跨实例中转桥（9.3）：中心服集群部署时经 Kafka 把中转转发到承载目标节点的实例
	// nil = 未配 Kafka，中转退化为单实例模式（OnRelayRequest 里判断）
	relayBridge *relay.Bridge
	// instanceID 本实例唯一标识（消费组后缀 + 自发自收去重）
	// 用 hostname 派生（见 instanceID）：StatefulSet 下重启后保持稳定，
	// 消费组才能续上 offset
	instanceID string

	// groupID 本实例负责的分组（demo 单分组；多分组按 node 上报分配）
	// 未带 group_id 的节点统一落到这个默认分组，保证跨服匹配有组可依
	groupID string
}

// New 组装服务器。
//
// 只装配无外部依赖的组件（注册表/匹配器/中转路由）；依赖 Redis/etcd/Kafka/
// MySQL 的组件保持 nil，由 Start 按配置惰性创建——未配置中间件时中心服以
// 降级模式运行（无排行榜/统计，单实例中转），而不是启动失败。
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

	router := relay.NewRouter(cfg.Report.RelayRetry, 200*time.Millisecond)

	return &Server{
		cfg:        cfg,
		logger:     logger,
		reg:        reg,
		matcher:    matcher,
		router:     router,
		groupID:    "group-01",
		instanceID: instanceID(cfg.Server.Name),
	}
}

// Start 启动服务（阻塞至 ctx 取消）。
//
// 启动顺序即依赖顺序：指标 → Redis → 排行榜/统计 → gRPC → 中转桥 → MySQL
// 相关组件（归档/调度/里程碑，仅 leader 执行）→ 选主 → 后台节拍循环。
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

	// ⑤ 跨实例中转桥（9.3）：只有配置了 Kafka 才启用；否则退化为单实例模式
	// deliverLocal 以方法值传入：闭包捕获 s，Bridge 不需要认识 Server 类型
	s.relayBridge = relay.NewBridge(relay.BridgeConfig{
		Brokers:    s.cfg.RelayKafka.Brokers,
		Topic:      s.cfg.RelayKafka.Topic,
		InstanceID: s.instanceID,
	}, s.deliverLocal, s.logger)
	if s.relayBridge != nil {
		go func() {
			if err := s.relayBridge.Run(ctx); err != nil && ctx.Err() == nil {
				s.logger.Warn("relay bridge stopped", "err", err)
			}
		}()
		s.logger.Info("cross-instance relay enabled",
			"topic", s.cfg.RelayKafka.Topic, "instance", s.instanceID)
	} else {
		s.logger.Info("cross-instance relay disabled (no kafka brokers), single-instance mode")
	}

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

	// ⑦ 后台任务：心跳巡检 + 匹配 sweep + 中转重试 + 统计快照 + 指标
	go s.tickLoop(ctx)

	s.logger.Info("CenterServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"group", s.groupID, "cross_nodes", s.cfg.Cross.Nodes)
	// 阻塞点：Start 的返回即「关闭信号已到达」，main 随后编排 Shutdown
	//（两阶段优雅退出的第一阶段在这里结束，见 cmd/center/main.go）
	<-ctx.Done()
	return ctx.Err()
}

// tickLoop 统一节拍循环（1s）：节点心跳巡检、匹配放宽、中转重试、指标上报。
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

			// 中转重试
			for _, p := range s.router.NextRetry(now) {
				targets := s.resolveTargets(p.ToNode, p.Broadcast, p.GroupID)
				s.router.Relay(p, targets)
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
	metrics.CenterRelayPending(s.router.PendingCount())
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

// OnRelayRequest 实现 centerRPC.Handler：消息中转（9.3）。
//
// 两条投递路径，按目标节点归属选择：
//
//	本地有目标 → router.Relay 直接经 Report 流下发（同实例，一跳）
//	本地无目标 → 写入 Kafka center.relay，由承载该节点的实例消费后下发（跨实例）
//
// 广播同时走两条：本实例投自己承载的节点，其余实例各投各的 —— 每个节点只收一次。
//
// 一致性要点：点对点消息要么本地直达、要么经 Kafka 转给目标所在实例，两条
// 路径互斥（本地无目标才转发），同一 relay_id 不会被重复投递；广播则依赖
// 「每个实例只投本地节点」的约定天然去重。
//
// targets 为空且广播：本实例组内暂无节点，但仍要 Forward——
// 组里可能还有别的实例承载着节点，那是唯一的投递路径。
func (s *Server) OnRelayRequest(nodeID string, req *internalv2.RelayRequest) {
	if req == nil {
		return
	}
	p := relay.Payload{
		RelayID:   req.RelayId,
		FromNode:  nodeID,
		ToNode:    req.ToNode,
		Broadcast: req.Broadcast,
		GroupID:   req.GroupId,
		Body:      req.Payload,
	}

	targets := s.resolveTargets(p.ToNode, p.Broadcast, p.GroupID)
	if len(targets) > 0 {
		if n := s.router.Relay(p, targets); n < len(targets) {
			s.logger.Warn("relay partial delivery", "relay_id", p.RelayID,
				"delivered", n, "targets", len(targets))
		}
	}

	// 还需要跨实例转发的情形：
	//   - 广播：组内还有别的实例承载的节点；
	//   - 点对点且本实例没有该节点：目标一定在别的实例上。
	// 反过来——点对点且本地已全部送达——绝不再转发：Kafka 那条路上
	// 没有实例认领这个 node_id，纯属浪费；更重要的是避免「本地+远端」
	// 双投递破坏 relay_id 的唯一性
	needCross := p.Broadcast || len(targets) == 0
	if !needCross {
		return
	}

	if s.relayBridge == nil {
		if len(targets) == 0 {
			// 单实例部署时这就是「目标节点不存在」
			s.logger.Debug("relay target not connected", "from", nodeID,
				"to", p.ToNode, "broadcast", p.Broadcast, "group", p.GroupID)
		}
		return
	}

	if err := s.relayBridge.Forward(context.Background(), p); err != nil {
		s.logger.Warn("relay cross-instance forward failed",
			"relay_id", p.RelayID, "to", p.ToNode, "broadcast", p.Broadcast, "err", err)
	}
}

// deliverLocal 供中转桥调用：只投递本实例承载的节点，返回投递成功数。
//
// 返回 0 表示目标都不归本实例管（由其他实例处理），不是错误。
//
// 与 OnRelayRequest 内的本地投递不同：这里不登记重试——Kafka 已 ack，
// 跨实例路径刻意不重试（重复投递比丢失更糟，见 relay/bridge.go 可靠性约定）。
//
// 返回投递数而非错误：Bridge 用返回值区分「这不归我管」（0）与
// 「我投了几个」（>0），错误信息已由各 SendPush 内部记录。
func (s *Server) deliverLocal(p relay.Payload) int {
	targets := s.resolveTargets(p.ToNode, p.Broadcast, p.GroupID)
	if len(targets) == 0 {
		return 0
	}
	// 跨实例路径不再登记待重试：消息已被 Kafka ack，重复投递比丢失更糟（9.3 尽力而为）
	delivered := 0
	for _, t := range targets {
		if err := t.SendPush(&relay.PushPayload{
			RelayID:  p.RelayID,
			FromNode: p.FromNode,
			Payload:  p.Body,
		}); err == nil {
			delivered++
		}
	}
	return delivered
}

// resolveTargets 解析中转目标。
// 广播时 groupID 空兜底本实例默认组：调用方（relay.Payload）可能不带组，
// 而本实例只服务一个默认分组（demo 单组语义）。
func (s *Server) resolveTargets(toNode string, broadcast bool, groupID string) []relay.Pusher {
	if broadcast {
		gid := groupID
		if gid == "" {
			gid = s.groupID
		}
		nodes := s.reg.ListByGroup(gid, "")
		out := make([]relay.Pusher, 0, len(nodes))
		for _, n := range nodes {
			if sender, ok := s.reg.SenderFor(n.ID); ok {
				out = append(out, &senderAdapter{sender})
			}
		}
		return out
	}
	if toNode == "" {
		return nil
	}
	if sender, ok := s.reg.SenderFor(toNode); ok {
		return []relay.Pusher{&senderAdapter{sender}}
	}
	return nil
}

// senderAdapter 把 registry.Sender 适配为 relay.Pusher。
//
// 适配器模式：两个包各自的接口（Sender 收 any 指令、Pusher 收
// *PushPayload）形状不同，包依赖又是单向的（relay 不能依赖 registry），
// 在装配层做一层薄转换，两边都保持干净。
type senderAdapter struct {
	s registry.Sender
}

func (a *senderAdapter) NodeID() string { return a.s.NodeID() }

func (a *senderAdapter) SendPush(p *relay.PushPayload) error {
	return a.s.SendCommand(&internalv2.CenterCommand{
		Body: &internalv2.CenterCommand_RelayPush{RelayPush: &internalv2.RelayPush{
			RelayId:  p.RelayID,
			FromNode: p.FromNode,
			Payload:  p.Payload,
		}},
	})
}

// Shutdown 优雅关闭（9.7）。
//
// 顺序：先 Kick 全部节点让它们尽早重连其它实例（滚动发布时玩家无感），
// 再依次停 gRPC、中转桥、落库组件、指标。gRPC 带超时强停，避免 Report
// 流永不结束卡死整个关闭流程（与 P13 网关下线同类问题）。
//
// 顺序原理：先踢后停——反过来的话节点还连着自己，gRPC 一停全部瞬断，
// 重连风暴且玩家掉线；先 Kick 给节点 3s 缓冲期去建新连接。
func (s *Server) Shutdown(ctx context.Context) error {
	// ① 通知所有节点「准备关闭」，让它们尽早重连到其它中心服实例
	s.kickAllNodes()

	// ② 停 gRPC（带超时，避免 Report 流永不结束导致卡死 —— 与 P13 同类问题）
	if s.rpcServer != nil {
		s.rpcServer.StopWithTimeout(3 * time.Second)
	}

	// ③ 停中转桥（消费者 Run 随 ctx 退出，这里显式关闭释放 Kafka 连接）
	// 关之前打统计：forwarded/received/delivered/skipped 是判断集群
	// 中转健康度的唯一现场证据，进程一退就没了
	if s.relayBridge != nil {
		s.relayBridge.Close()
		if fwd, recv, deliv, skip := s.relayBridge.Stats(); fwd+recv > 0 {
			s.logger.Info("relay bridge stats",
				"forwarded", fwd, "received", recv, "delivered", deliv, "skipped", skip)
		}
	}

	// ④ 归档与统计落库：Run 随 ctx 退出；这里确保 producer 缓冲已刷出
	// Flush 在 Close 之前：没刷出的发货消息会随 producer 一起蒸发；
	// Flush 用传入的 shutdownCtx（带剩余超时），不会无限等
	if s.milestoneSvc != nil {
		_ = s.milestoneSvc.Close()
	}
	if s.producer != nil {
		_ = s.producer.Flush(ctx)
		s.producer.Close()
	}
	if s.database != nil {
		_ = s.database.Close()
	}

	// ⑤ 停指标
	if s.metricsSrv != nil {
		_ = s.metricsSrv.Stop(ctx)
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
