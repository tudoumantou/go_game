package crossplay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"game_study/pkg/log"
	internalv2 "game_study/pkg/pb/internalv2"
)

// 默认节拍。
const (
	// DefaultWindow 下发窗口：同一组在一个窗口内只下发一次。
	//
	// 为什么是「窗口」而不是「收到就发」：一组 100 人来自约 30 个服，
	// 30 个服各自上报，如果每来一条就下发一次，同一组会被触发 30 次，
	// 其中 29 次的内容几乎一样。
	//
	// 为什么不是 debounce（等静默）：组内 30 个服轮流上报，永远等不到静默，
	// 其他服的分数就永不更新。所以是 throttle —— 窗口内累积、到点必发，
	// 期间新来的上报只累加不改窗口。
	DefaultWindow = 60 * time.Second

	// DefaultFlushDB 积分落库节拍。
	//
	// 积分以内存为准（宕机丢就丢了，已确认），落库只服务两件事：
	// center 重启后重建、GM 排查。5 分钟滞后对这两件事都没有影响。
	DefaultFlushDB = 5 * time.Minute

	// DefaultCalibrate 全量校准节拍。
	//
	// 增量下发丢一包可能长期不一致（如果该组之后没人再变），
	// 每 10 分钟对全部组重发一次快照。1000 组 × 6KB 是 6MB，
	// 频率不能再高。
	DefaultCalibrate = 10 * time.Minute

	// tickInterval 定时器扫描间隔。1s 足够：窗口精度是秒级，
	// 没必要更细（更细只是多消耗 CPU）。
	tickInterval = time.Second
)

// Phase 活动阶段。
//
// 阶段由活动管理器按周周期算出（两侧用同一套算法，不通过消息同步），
// 中心服据此决定「收不收报名」「能不能分组」。
type Phase int

// 阶段常量。顺序即时间顺序，便于用 < / > 判断「是否已过某阶段」。
const (
	PhaseClosed   Phase = iota // 无活动（周期之外）
	PhaseEnroll                // 报名期：周一 00:00 → 周二 00:00
	PhaseGrouping              // 分组期：周二 00:00 → 06:00
	PhaseActive                // 活动期：周二 06:00 → 周日 22:00
	PhaseSettling              // 结束期：周日 22:00 → 下周一 00:00
)

func (p Phase) String() string {
	switch p {
	case PhaseEnroll:
		return "enroll"
	case PhaseGrouping:
		return "grouping"
	case PhaseActive:
		return "active"
	case PhaseSettling:
		return "settling"
	default:
		return "closed"
	}
}

// PhaseFunc 返回某时刻的活动阶段。由活动管理器注入。
//
// 用函数而不是接口：只有一个方法，接口反而多一层间接。
// 默认实现返回 PhaseActive（活动永远开着），让 Service 在没有活动
// 管理器的环境（测试、单机调试）里也能跑通全链路。
type PhaseFunc func(now time.Time) Phase

// Dispatcher 下发通道（由 center 的 server 注入）。
//
// 与 registry.Sender 的分工：本接口按「区服」下发（业务标识），
// 由实现方反查节点 ID（部署态标识）。这样 crossplay 不需要知道
// node_id 与 server_id 的映射规则。
type Dispatcher interface {
	// NodeIDForServer 按区服编号反查承载它的节点 ID。
	NodeIDForServer(serverID int32) (string, bool)
	// SendTo 向节点发送一条指令。
	SendTo(nodeID string, cmd *internalv2.CenterCommand) error
}

// Config Service 的可调参数。零值取 Default* 常量。
type Config struct {
	Window    time.Duration
	FlushDB   time.Duration
	Calibrate time.Duration
	// Phase 阶段来源。nil 时恒为 PhaseActive。
	Phase PhaseFunc
	// SendGroup 分组快照/增量的下发函数（serverID 是业务标识）。
	//
	// 非 nil 时优先走它（NATS 异步通道），nil 时退回 Dispatcher 的 gRPC
	// SendTo。用函数钩子而不是再抽一个接口：下发只有一个动作，接口反而
	// 多一层；且不破坏既有单测里用 fakeDispatcher 验证下发内容的路径。
	SendGroup func(serverID int32, push *internalv2.CrossGroupDeltaPush) error
	// IsLeader 周期任务（分组、校准、落库）是否由本实例执行。
	//
	// 中心服多实例时，这些任务会写同一份权威状态，必须只由 leader 做，
	// 否则每个实例各算一份分组、各下一份发。nil 时恒为 true（单实例/
	// 测试）。报名/收上报这类无主动作不门控，任何实例都能处理。
	IsLeader func() bool
}

func (c *Config) normalize() {
	if c.Window <= 0 {
		c.Window = DefaultWindow
	}
	if c.FlushDB <= 0 {
		c.FlushDB = DefaultFlushDB
	}
	if c.Calibrate <= 0 {
		c.Calibrate = DefaultCalibrate
	}
	if c.Phase == nil {
		c.Phase = func(time.Time) Phase { return PhaseActive }
	}
	if c.IsLeader == nil {
		c.IsLeader = func() bool { return true }
	}
}

// Persister Service 对存储的全部依赖（接口隔离）。
//
// 抽成接口只为让「报名期校验」「分组流程」「下发窗口」这些纯编排逻辑能在
// 内存里测，不必为一条断言拉起 MySQL。*Repo 隐式实现它，装配层不变。
type Persister interface {
	UpsertEnroll(ctx context.Context, m Member, info []byte, now int64) error
	DeleteEnroll(ctx context.Context, playerID int64) error
	LoadEnrolls(ctx context.Context) ([]Member, error)
	SaveGroups(ctx context.Context, groups []Group) error
	UpsertScores(ctx context.Context, members []Member) error
	// ClearCurrentPeriod 清报名表与玩家表（分组完成、切期）。
	ClearCurrentPeriod(ctx context.Context) (int64, error)
	// LoadGroups 从分组表恢复内存（center 重启）。
	LoadGroups(ctx context.Context) ([]Group, error)
}

// Service 跨服玩法的中心服侧编排。
//
// 三件事：报名裁决、分组、积分同步下发。业务规则都在这里，
// 存储细节在 Repo、内存状态在 Store、协议在 rpc 包。
type Service struct {
	cfg    Config
	store  *Store
	repo   Persister
	disp   Dispatcher
	logger *log.Logger

	// mu 保护下面三张表。竞争点只有两个：收上报（多 goroutine）
	// 与下发窗口（单 goroutine），都是短临界区。
	mu      sync.Mutex
	pending map[int32]int64              // groupID → 本窗口开启时刻（unix 秒）
	dirty   map[int32]map[int64]struct{} // groupID → 本窗口内变化过的玩家

	// scoreDirty 待落库的积分（playerID → 最新值）。map 天然去重：
	// 同一玩家在 5 分钟内变 20 次，只落库一次最终值。
	scoreDirty map[int64]Member

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// groupedPhase 本期分组是否已执行。只由 tickLoop（单 goroutine）读写，
	// 不需要锁。离开分组期时清零，保证下一周期会再分一次。
	groupedPhase bool

	// now 便于测试注入时间。
	now func() time.Time
}

// New 创建 Service。repo 允许未就绪（无 MySQL 部署），此时写操作返回 ErrNoDB。
func New(repo Persister, disp Dispatcher, logger *log.Logger, cfg Config) *Service {
	cfg.normalize()
	if logger == nil {
		logger = log.New("crossplay")
	}
	return &Service{
		cfg:        cfg,
		store:      NewStore(),
		repo:       repo,
		disp:       disp,
		logger:     logger,
		pending:    make(map[int32]int64),
		dirty:      make(map[int32]map[int64]struct{}),
		scoreDirty: make(map[int64]Member),
		stopCh:     make(chan struct{}),
		now:        time.Now,
	}
}

// SetSendGroup 注入分组下发通道（NATS 装配期调用）。
//
// 用 setter 而不是构造参数：gRPC 路径的既有装配与单测不必改签名，NATS
// 装配时按需覆盖。nil 还原为 gRPC Dispatcher 路径。
func (s *Service) SetSendGroup(fn func(serverID int32, push *internalv2.CrossGroupDeltaPush) error) {
	s.cfg.SendGroup = fn
}

// Store 暴露内存分组（查询、GM、测试）。
func (s *Service) Store() *Store { return s.store }

// Phase 当前阶段。
func (s *Service) Phase() Phase { return s.cfg.Phase(s.now()) }

// Start 启动定时器：下发窗口扫描 + 积分落库 + 全量校准。
func (s *Service) Start() {
	s.wg.Add(1)
	go s.tickLoop()
}

// Restore 从分组表恢复内存（center 重启后调用）。
//
// 只读分组表：它是本期唯一的长期数据，玩家表只是按玩家查的索引。
// 恢复失败不回退成「空分组」——空分组会让所有上报都被当成未知玩家丢弃，
// 表面上看不出问题，实际整个活动静默失效。所以返回错误让调用方决定。
func (s *Service) Restore(ctx context.Context) error {
	groups, err := s.repo.LoadGroups(ctx)
	if err != nil {
		return fmt.Errorf("restore groups: %w", err)
	}
	if len(groups) == 0 {
		return nil // 本期还没分组，或已归档
	}
	s.store.Load(groups)
	s.logger.Info("cross groups restored", "groups", len(groups), "players", s.store.Len())
	return nil
}

// Stop 停止定时器，并把未落库的积分刷一次盘。
//
// 刷盘是「优雅关闭」的一部分：宕机丢分可以接受，但正常关服不该丢。
func (s *Service) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
	s.flushScores(context.Background())
}

// Enroll 报名（幂等）。
//
// 返回业务码而不是 error：玩家点报名失败要看到「不在报名期」而不是
// 「网络错误」，两者在客户端的处理完全不同。
//
// 幂等语义：已报名再点一次返回 0（成功）而不是「已报名」。这样
// 「center 写成功但回包丢了」时玩家重试能自愈，不需要额外的查询接口。
func (s *Service) Enroll(ctx context.Context, m Member, info []byte) (int32, string) {
	ph := s.Phase()
	if ph != PhaseEnroll {
		return 1, "不在报名期（当前：" + ph.String() + "）"
	}
	if m.PlayerID <= 0 || m.ServerID <= 0 {
		return 2, "参数错误"
	}
	if err := s.repo.UpsertEnroll(ctx, m, info, s.now().Unix()); err != nil {
		s.logger.Error("enroll failed", "player", m.PlayerID, "server", m.ServerID, "err", err)
		return 2, "内部错误"
	}
	return 0, ""
}

// Cancel 取消报名（幂等删除）。
//
// 只在报名期允许：分组之后玩家已占一个组的名额，取消会让组内人数
// 与已下发的快照对不上。
func (s *Service) Cancel(ctx context.Context, playerID int64) (int32, string) {
	ph := s.Phase()
	if ph != PhaseEnroll {
		return 1, "不在报名期（当前：" + ph.String() + "）"
	}
	if playerID <= 0 {
		return 2, "参数错误"
	}
	if err := s.repo.DeleteEnroll(ctx, playerID); err != nil {
		s.logger.Error("cancel enroll failed", "player", playerID, "err", err)
		return 2, "内部错误"
	}
	return 0, ""
}

// OnScoreBatch 处理一批积分上报（游戏服 → 中心服，按组打包）。
//
// 只收变化项：上报本身就是「本窗口内变过的玩家」，所以这里不需要
// 再和内存比对，但 ApplyScore 仍会返回 changed —— 重复上报（重发）
// 不算变化，不该触发下发。
func (s *Service) OnScoreBatch(entries []*internalv2.CrossScoreEntry) {
	if len(entries) == 0 {
		return
	}
	now := s.now().Unix()
	// 非 leader 只更新内存 Store（供 leader 切换后接管），不碰窗口/落库集合：
	// 那些集合由 leader 的 tickLoop 消费，非 leader 写进去永远不会被清，
	// 既是内存泄漏，也会在切换后把陈旧窗口带给新 leader。
	isLeader := s.cfg.IsLeader()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range entries {
		if e == nil || e.GetPlayerId() <= 0 {
			continue
		}
		m, ok, changed := s.store.ApplyScore(e.GetPlayerId(), e.GetScore(), e.GetUpdatedAt())
		if !ok {
			// 不在分组里：未报名、已被分到上一期、或 center 重启后分组表还没加载。
			// 丢弃而不是报错 —— 活动期报名表已清，这里认不出来就是脏数据。
			continue
		}
		if !isLeader {
			continue // 只更新 Store 副本
		}
		s.scoreDirty[m.PlayerID] = m
		if !changed {
			continue
		}
		gid := m.GroupID
		if _, started := s.pending[gid]; !started {
			// 开窗。窗口内后续的变更加进 dirty，但**不刷新**开启时刻 ——
			// 否则 30 个服轮流上报会让这个窗口永远等不到结束。
			s.pending[gid] = now
		}
		if s.dirty[gid] == nil {
			s.dirty[gid] = make(map[int64]struct{}, 8)
		}
		s.dirty[gid][m.PlayerID] = struct{}{}
	}
}

// RunGrouping 执行一次分组并把结果落库、下发快照。
//
// 只在分组期由 leader 调用一次。幂等性靠「整体覆盖」：重复调用会重新算一遍
// 并覆盖同组号的行，不会产生重复组（分组是确定性的，同输入同输出）。
func (s *Service) RunGrouping(ctx context.Context) (int, error) {
	members, err := s.repo.LoadEnrolls(ctx)
	if err != nil {
		return 0, fmt.Errorf("load enrolls: %w", err)
	}
	if len(members) == 0 {
		s.logger.Warn("grouping skipped: no enrolls")
		return 0, nil
	}

	groups := Distribute(members)
	if err := s.repo.SaveGroups(ctx, groups); err != nil {
		return 0, fmt.Errorf("save groups: %w", err)
	}
	s.store.Load(groups)
	// 分组表已完整写入之后才清报名表（顺序不能反：先清后写，写失败就永久丢数据）。
	if _, err := s.repo.ClearCurrentPeriod(ctx); err != nil {
		// 清表失败不影响本次活动：报名表残留只会在下一期报名期被再次清理，
		// 分组已经从 enroll 里读完了。
		s.logger.Warn("clear enroll failed, will retry next period", "err", err)
	}

	s.logger.Info("grouping done", "players", len(members), "groups", len(groups))
	s.pushSnapshots(groups)
	return len(groups), nil
}

// pushSnapshots 分组完成后按服下发全量快照。
//
// 每服一条报文（含它涉及的所有组），而不是一组一条：1000 组 × 30 服
// = 30000 条会把接收端打满。
func (s *Service) pushSnapshots(groups []Group) {
	if s.disp == nil {
		return
	}

	perServer := make(map[int32]*internalv2.CrossGroupDeltaPush, 32)
	for _, g := range groups {
		if len(g.Members) == 0 {
			continue
		}
		servers := make(map[int32]struct{}, 8)
		for _, m := range g.Members {
			servers[m.ServerID] = struct{}{}
		}
		for sid := range servers {
			// 快照**包含收件方自己的玩家**，与增量相反。
			//
			// 不是为了「多带几个字段」：分组结果只写在中心服，快照是游戏服
			// 知道自己分组的唯一通道。滤掉自己的人，那个玩家在本服就永远是
			// 「已报名、未分组」，界面一直转圈到活动结束。
			//
			// 覆盖风险由接收端分工解决：快照里的本服玩家只采纳归属
			// （group_id / power），分数以本地为准。归属听中心服的，
			// 分数听本服的——两件事。
			d := &internalv2.CrossGroupDelta{
				GroupId:  g.ID,
				Snapshot: true,
				Players:  make([]*internalv2.CrossScoreEntry, 0, len(g.Members)),
			}
			for _, m := range g.Members {
				d.Players = append(d.Players, &internalv2.CrossScoreEntry{
					GroupId:   g.ID,
					PlayerId:  m.PlayerID,
					Score:     m.Score,
					Power:     m.Power,
					UpdatedAt: m.UpdatedAt,
					ServerId:  m.ServerID,
				})
			}
			push, ok := perServer[sid]
			if !ok {
				push = &internalv2.CrossGroupDeltaPush{}
				perServer[sid] = push
			}
			push.Groups = append(push.Groups, d)
		}
	}

	var failed int
	for sid, push := range perServer {
		if err := s.sendToServer(sid, push); err != nil {
			failed++
		}
	}
	if failed > 0 {
		// 下发失败不影响分组结果：游戏服查不到时会回源 center 拉单组快照，
		// 且活动期内还有 10 分钟校准。
		s.logger.Warn("snapshot push partially failed", "failed_servers", failed, "total", len(perServer))
	}
}

// tickLoop 定时器：下发窗口到点、积分落库、全量校准。
//
// 不在每组上开 goroutine + timer：1000 组就是 1000 个定时器，
// 内存和调度开销都不值。一个 ticker 扫一遍 map 更简单也更快。
func (s *Service) tickLoop() {
	defer s.wg.Done()

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()

	lastFlush := s.now()
	lastCal := s.now()

	for {
		select {
		case <-s.stopCh:
			return
		case <-tick.C:
			now := s.now()
			phase := s.Phase()

			// 分组：只在分组期、只由 leader、每期一次。此前没有任何调用点，
			// 分组永远不会发生——这里补上真正的触发。
			if phase == PhaseGrouping && !s.groupedPhase {
				if s.cfg.IsLeader() {
					if _, err := s.RunGrouping(context.Background()); err != nil {
						s.logger.Warn("grouping failed", "err", err)
					} else {
						s.groupedPhase = true
					}
				}
			} else if phase != PhaseGrouping && phase != PhaseEnroll {
				// 离开分组期后允许下一期重新分。不放在 PhaseEnroll 清零：
				// 报名期紧邻分组期，清零后若时钟回拨到分组期会重复分组。
				s.groupedPhase = false
			}

			// 只有 leader 跑周期写任务：下发窗口、积分落库、全量校准都会
			// 改权威状态或对外发消息，多实例同时跑会重复下发、重复落库。
			if !s.cfg.IsLeader() {
				continue
			}
			s.flushWindows(now)
			if now.Sub(lastFlush) >= s.cfg.FlushDB {
				s.flushScores(context.Background())
				lastFlush = now
			}
			if now.Sub(lastCal) >= s.cfg.Calibrate {
				s.calibrate()
				lastCal = now
			}
		}
	}
}

// flushWindows 关闭到期的下发窗口。
func (s *Service) flushWindows(now time.Time) {
	windowSec := int64(s.cfg.Window / time.Second)

	s.mu.Lock()
	due := make([]int32, 0, 8)
	for gid, startAt := range s.pending {
		if now.Unix()-startAt >= windowSec {
			due = append(due, gid)
		}
	}
	// 先把到期的组摘下来再发：发送在锁外做，避免 gRPC 写入持锁。
	batch := make([]map[int64]struct{}, 0, len(due))
	for _, gid := range due {
		batch = append(batch, s.dirty[gid])
		delete(s.pending, gid)
		delete(s.dirty, gid)
	}
	s.mu.Unlock()

	for i, gid := range due {
		s.flushGroup(gid, batch[i])
	}
}

// flushGroup 把一个窗口内某组的变化按服聚合并下发。
func (s *Service) flushGroup(groupID int32, players map[int64]struct{}) {
	if len(players) == 0 || s.disp == nil {
		return
	}
	servers := s.store.ServersOf(groupID)
	if len(servers) < 2 {
		// 组内只有一个服：变化不需要跨服同步，省掉一次无意义的发送。
		return
	}

	b := NewPushBuilder()
	for pid := range players {
		m, ok := s.store.Member(pid)
		if !ok {
			continue
		}
		for _, sid := range servers {
			if sid == m.ServerID {
				continue // 自己的玩家不发：接收方本地是权威
			}
			b.AddTo(sid, m)
		}
	}

	for sid, push := range b.Messages() {
		if err := s.sendToServer(sid, push); err != nil {
			// 下发失败不重建窗口：其他服已经收到，重发会让它们收到重复增量。
			// 丢的那一份靠 10 分钟全量校准补回。
			s.logger.Warn("delta push failed", "server", sid, "group", groupID, "err", err)
		}
	}
}

// calibrate 对全部组重发快照（补增量丢包）。
func (s *Service) calibrate() {
	if s.disp == nil {
		return
	}
	all := s.store.All()
	if len(all) == 0 {
		return
	}
	s.logger.Info("calibrating groups", "groups", len(all))
	s.pushSnapshots(all)
}

// flushScores 把待落库的积分写进 center_cross_player。
func (s *Service) flushScores(ctx context.Context) {
	s.mu.Lock()
	if len(s.scoreDirty) == 0 {
		s.mu.Unlock()
		return
	}
	pending := s.scoreDirty
	s.scoreDirty = make(map[int64]Member, len(pending))
	s.mu.Unlock()

	members := make([]Member, 0, len(pending))
	for _, m := range pending {
		members = append(members, m)
	}
	if err := s.repo.UpsertScores(ctx, members); err != nil {
		// 落库失败不阻塞：内存仍是权威，下一轮再把它们加回去重试。
		s.logger.Warn("score flush failed, will retry", "rows", len(members), "err", err)
		s.mu.Lock()
		for _, m := range pending {
			if _, newer := s.scoreDirty[m.PlayerID]; !newer {
				s.scoreDirty[m.PlayerID] = m
			}
		}
		s.mu.Unlock()
	}
}

// sendToServer 按区服下发一条报文。
//
// 优先走 NATS（cfg.SendGroup）：按 server_id 直接 publish，不依赖该服此刻
// 有没有 gRPC 流在线——这是相对 gRPC 下发最大的收益，离线服上线后订阅即收。
// 未配 NATS 时退回 gRPC Dispatcher（按 server_id 反查 node_id）。
func (s *Service) sendToServer(serverID int32, push *internalv2.CrossGroupDeltaPush) error {
	if s.cfg.SendGroup != nil {
		return s.cfg.SendGroup(serverID, push)
	}
	if s.disp == nil {
		return fmt.Errorf("no downlink for server %d", serverID)
	}
	nodeID, ok := s.disp.NodeIDForServer(serverID)
	if !ok {
		// 该服当前无在线节点：不缓存、不重发。活动期玩家开界面时会回源
		// 拉单组快照，10 分钟校准也会再发一次。
		return fmt.Errorf("no node for server %d", serverID)
	}
	return s.disp.SendTo(nodeID, &internalv2.CenterCommand{
		Body: &internalv2.CenterCommand_CrossGroupDelta{CrossGroupDelta: push},
	})
}
