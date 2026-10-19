// Package center 中心服（设计文档第 9 章）
//
// 四项职责（9.1）：
//   - 节点管理：游戏服/跨服节点注册、心跳保活、超时摘除；
//   - 消息中转：RelayRequest → RelayPush（点对点 / 组播），集群跨实例经 Kafka（P9）；
//   - 跨服匹配：按 elo 分桶收集 MatchRequest，凑人后签发 room_id + token 并下发 MatchResult；
//   - 分组统计 / 排行榜：P8 阶段落地基础版（内存聚合 + Redis 可选），GM 查询走 gRPC。
//
// 中心服**不持有玩家业务数据**，重启后由节点重连补报即可重建（9.1）。
package center

import (
	"context"
	"fmt"
	"os"
	"time"

	"game_study/internal/center/archive"
	"game_study/internal/center/leader"
	"game_study/internal/center/match"
	"game_study/internal/center/rank"
	"game_study/internal/center/registry"
	"game_study/internal/center/relay"
	centerRPC "game_study/internal/center/rpc"
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
type Server struct {
	cfg        *config.Config
	logger     *log.Logger
	metricsSrv *metrics.Server

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
	producer   *pkgmq.Producer
	archiveSvc *archive.Service

	// relayBridge 跨实例中转桥（9.3）：中心服集群部署时经 Kafka 把中转转发到承载目标节点的实例
	relayBridge *relay.Bridge
	// instanceID 本实例唯一标识（消费组后缀 + 自发自收去重）
	instanceID string

	// groupID 本实例负责的分组（demo 单分组；多分组按 node 上报分配）
	groupID string
}

// New 组装服务器。
func New(cfg *config.Config) *Server {
	logger := log.New("center", log.WithLevel(cfg.Log.Level))

	hb, _ := config.ParseDuration(cfg.Report.HeartbeatTimeout)
	reg := registry.New(hb, logger)

	// 跨服地址：P8 阶段用静态配置（生产由 GroupUpdate/配置中心下发）
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
		s.statsSvc.SetInstanceID(s.instanceID)
	}

	// ④ gRPC（CenterService.Report + GM 查询）
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
	} else {
		s.logger.Warn("mysql not configured, season archive and stats persistence disabled")
	}

	// ⑦ 选主（9.6）：leader 负责赛季归档、全服活动调度等有主任务
	s.startElection(ctx)

	// ⑦ 后台任务：心跳巡检 + 匹配 sweep + 中转重试 + 统计快照 + 指标
	go s.tickLoop(ctx)

	s.logger.Info("CenterServer started", "grpc_addr", s.cfg.Server.GRPCAddr,
		"group", s.groupID, "cross_nodes", s.cfg.Cross.Nodes)
	<-ctx.Done()
	return ctx.Err()
}

// tickLoop 统一节拍循环（1s）：节点心跳巡检、匹配放宽、中转重试、指标上报。
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
func (s *Server) readiness(_ context.Context) error {
	if s.reg == nil {
		return fmt.Errorf("registry not initialized")
	}
	return nil
}

// OnRegister 实现 centerRPC.Handler：节点注册后下发分组信息。
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
	if len(crossAddrs) == 0 {
		crossAddrs = append(crossAddrs, s.cfg.Cross.Nodes...)
	}
	// 匹配器同步可用跨服地址
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
func (s *Server) Shutdown(ctx context.Context) error {
	// ① 通知所有节点「准备关闭」，让它们尽早重连到其它中心服实例
	s.kickAllNodes()

	// ② 停 gRPC（带超时，避免 Report 流永不结束导致卡死 —— 与 P13 同类问题）
	if s.rpcServer != nil {
		s.rpcServer.StopWithTimeout(3 * time.Second)
	}

	// ③ 停中转桥（消费者 Run 随 ctx 退出，这里显式关闭释放 Kafka 连接）
	if s.relayBridge != nil {
		s.relayBridge.Close()
		if fwd, recv, deliv, skip := s.relayBridge.Stats(); fwd+recv > 0 {
			s.logger.Info("relay bridge stats",
				"forwarded", fwd, "received", recv, "delivered", deliv, "skipped", skip)
		}
	}

	// ④ 归档与统计落库：Run 随 ctx 退出；这里确保 producer 缓冲已刷出
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

func pow10(n uint32) uint32 {
	out := uint32(1)
	for i := uint32(0); i < n; i++ {
		out *= 10
	}
	return out
}

// startElection 参与 etcd 选主（9.6）。etcd 未配置时跳过（单实例部署无需选主）。
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
func (s *Server) IsLeader() bool {
	return s.election != nil && s.election.IsLeader()
}

// instanceID 生成本实例唯一标识。
//
// 用 hostname 而非纯随机：StatefulSet 的 Pod 名稳定（center-0/center-1），
// 重启后消费组不变，可续上上次提交的 offset；纯随机会在每次重启时
// 创建新消费组，重启期间的中转消息直接丢失。
func instanceID(serviceName string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return fmt.Sprintf("%s-%d", serviceName, time.Now().UnixNano())
	}
	return serviceName + "@" + host
}
