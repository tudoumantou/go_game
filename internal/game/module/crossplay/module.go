// module.go —— 跨服玩法模块（游戏服侧）。
//
// 职责：报名、界面查询、积分镜像、向中心服上报、处理中心服下发。
// 不含玩法规则（攻击消耗什么道具、加几分），那些在调用方：模块只提供
// Attack/AddScore 这类入口，规则由具体玩法决定。
//
// Ext 槽位：无（Define[NoState]）。积分是玩法数据不是玩家存档——离线玩家
// 也要能被打、被改分，所以它不能挂在 Actor 的 State 上，否则打一个离线玩家
// 就要把背包/任务/装备全部加载一遍。
//
// 事件订阅：无。
//
// 配置表关系：无（阶段偏移是代码常量 + 可调 Config，不走配表）。
package crossplay

import (
	"context"
	"sync/atomic"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/log"
	"game_study/pkg/model"
	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
	"game_study/pkg/periodic"
)

// 默认节拍。
const (
	// DefaultReportInterval 积分上报节拍。
	//
	// 60s 是「延迟」与「报文量」的折中：对手看到你的分最多滞后 120s
	// （60s 上报 + 中心服 60s 下发窗口）。降到 10s 能把滞后压到 70s，
	// 代价是报文数 ×6——按组打包后每条只有几百字节，量级上完全撑得住，
	// 但没必要：+1/−1 的对抗玩法对 2 分钟延迟不敏感。
	DefaultReportInterval = 60 * time.Second

	// DefaultPersistInterval 组镜像落库节拍。
	//
	// cross_group 在游戏服是**恢复副本**不是权威，写它只为了让「活动期
	// 重启」能立刻显示排行。5 分钟滞后没有任何业务影响。
	DefaultPersistInterval = 5 * time.Minute
)

// Center 中心服交互能力（*gamecenter.Client 实现；测试注入内存实现）。
//
// 只声明本模块用到的四个方法：模块不依赖整个中心服客户端，
// 这样单测不必拉起 gRPC。
type Center interface {
	// CrossEnroll 报名/取消（同步等裁决）。
	CrossEnroll(ctx context.Context, req *internalv2.CrossEnrollReq) (*internalv2.CrossEnrollResp, error)
	// SendCrossScore 上报本服玩家的积分变化（按组打包）。
	SendCrossScore(entries []*internalv2.CrossScoreEntry) error
	// SendCrossDelta 请中心服转发一条改分通知给别的服。
	SendCrossDelta(d *internalv2.CrossScoreDelta) error
	// Ready 与中心服的通路是否可用。
	Ready() bool
}

// Option 装配选项。
type Option func(*Module)

// WithServerID 设置本区区服编号（写进 cross_enroll/cross_player 的 server_id）。
func WithServerID(id int32) Option {
	return func(m *Module) { m.serverID = id }
}

// WithNodeID 设置本节点 ID（生成 relay_id 用）。
func WithNodeID(id string) Option {
	return func(m *Module) { m.nodeID = id }
}

// WithConfig 覆盖阶段偏移配置。
func WithConfig(cfg Config) Option {
	return func(m *Module) { m.cfg = cfg }
}

// WithClock 注入时钟（测试用）。nil 时用 time.Now。
func WithClock(fn func() time.Time) Option {
	return func(m *Module) { m.clock = fn }
}

// WithIntervals 覆盖两个定时节拍（测试用，避免等 60s）。
func WithIntervals(report, persist time.Duration) Option {
	return func(m *Module) {
		m.reportEvery = report
		m.persistEvery = persist
	}
}

// Module 跨服玩法模块。
//
// 无玩家态：所有数据在 Registry（进程内存）与三张表里。OnMessage 虽然
// 在玩家 Actor 内执行，但读写的是 Registry（自带锁），与下发 goroutine
// 共享同一份数据。
type Module struct {
	*module.Bound[module.NoState]

	center Center
	repo   *Repo
	logger *log.Logger

	cfg      Config
	serverID int32
	nodeID   string
	clock    func() time.Time

	reg *Registry

	reportEvery  time.Duration
	persistEvery time.Duration

	// seq relay_id 序号（sd:{node}:{seq}）。接收方按 (from_node, relay_id)
	// 去重，所以它必须单调且不重复——重启后从 0 开始也没关系，
	// 因为去重是「(来源, 序号)」二元组，重启后来源的时间线是新的。
	seq atomic.Int64

	// dedup 收到的改分通知去重（relay at-least-once，不去重就是重复扣分）。
	dedup deltaDedup
}

// New 创建模块。
//
// center 允许为 nil：无中心服的单机环境里报名会直接失败（回 code=2），
// 但界面查询、改分、镜像仍可用——模块不因为缺中心服就 panic。
func New(center Center, repo *Repo, logger *log.Logger, opts ...Option) *Module {
	m := &Module{
		center:       center,
		repo:         repo,
		logger:       logger,
		cfg:          DefaultConfig(),
		reportEvery:  DefaultReportInterval,
		persistEvery: DefaultPersistInterval,
	}
	// 先应用选项再建 Registry：serverID 要在构造镜像之前就位，否则
	// 「快照里谁是本服玩家」这个判断从一开始就是错的。
	for _, opt := range opts {
		opt(m)
	}
	m.reg = NewRegistry(m.serverID)

	m.Bound = module.Define[module.NoState]("crossplay").
		WithLogger(logger)
	module.Route(m.Bound, &gamev1.CrossPlayEnrollReq{}, m.onEnroll)
	module.Route(m.Bound, &gamev1.CrossPlayInfoReq{}, m.onInfo)
	return m
}

// Registry 暴露内存镜像（装配/测试用）。
func (m *Module) Registry() *Registry { return m.reg }

// Config 当前阶段配置（阶段计算在 phase.go）。
func (m *Module) Config() Config { return m.cfg.normalized() }

// Phase 返回 now 所处阶段。
func (m *Module) Phase() Phase {
	p, _, _ := m.cfg.PhaseAt(m.nowUnix())
	return p
}

// Start 启动后台任务：加载库 → 上报节拍 → 落库节拍。
//
// 加载失败不阻断启动：内存是空的，界面显示「未报名」，等下一次中心服
// 快照（10 分钟校准）会补回来。让进程起不来是更坏的选择——一个跨服玩法
// 的表读失败不该拖垮整个游戏服。
func (m *Module) Start(ctx context.Context) {
	if err := m.load(ctx); err != nil {
		m.logger.Warn("crossplay load failed, starting empty", "err", err)
	}

	go periodic.Run(ctx, periodic.Options{
		Name:     "crossplay-report",
		Interval: m.reportEvery,
		Fn:       m.reportTick,
		Logger:   m.logger,
	})
	go periodic.Run(ctx, periodic.Options{
		Name:     "crossplay-persist",
		Interval: m.persistEvery,
		Fn:       m.persistTick,
		Logger:   m.logger,
	})
}

// load 从三张表加载内存镜像。
//
// 没有 MySQL（repo 未就绪）时直接返回：单机演示环境下内存从空开始，
// 报名走中心服、分组靠下发，功能仍可用。
func (m *Module) load(ctx context.Context) error {
	if m.repo == nil || !m.repo.Ready() {
		return nil
	}

	enrolls, err := m.repo.ListEnrolls(ctx)
	if err != nil {
		return err
	}
	players, err := m.repo.ListPlayers(ctx)
	if err != nil {
		return err
	}
	groups, err := m.repo.ListGroups(ctx)
	if err != nil {
		return err
	}

	// players 先建，groups 里的本服玩家才能复用同一对象指针。
	ownPlayers := make([]*Entry, 0, len(players))
	for _, p := range players {
		ownPlayers = append(ownPlayers, &Entry{
			PlayerID:  p.PlayerId,
			GroupID:   p.GroupId,
			ServerID:  p.ServerId,
			Score:     p.Score,
			Power:     p.Power,
			UpdatedAt: p.UpdatedAt,
			Info:      jsonToInfo(p.Info),
		})
	}

	enrolled := make([]int64, 0, len(enrolls))
	for _, e := range enrolls {
		enrolled = append(enrolled, e.PlayerId)
		// 报名行比玩家行新时（报名后还没分组）用它补一个 players 条目：
		// 否则玩家报名后重启，界面会显示「未报名」。
		if _, exists := findPlayer(ownPlayers, e.PlayerId); !exists {
			ownPlayers = append(ownPlayers, &Entry{
				PlayerID:  e.PlayerId,
				ServerID:  e.ServerId,
				Power:     e.Power,
				UpdatedAt: e.EnrolledAt,
				Info:      jsonToInfo(e.Info),
			})
		}
	}

	members := make(map[int32][]*Entry, len(groups))
	for _, g := range groups {
		list, err := jsonToMembers(g.Players, g.Servers)
		if err != nil {
			m.logger.Warn("bad cross_group json, skipped", "group", g.GroupId, "err", err)
			continue
		}
		members[g.GroupId] = list
	}

	m.reg.Load(ownPlayers, enrolled, members)
	m.logger.Info("crossplay loaded", "players", len(ownPlayers),
		"enrolled", len(enrolled), "groups", len(members))
	return nil
}

// findPlayer 线性查找（启动加载，几千条，不值得建索引）。
func findPlayer(list []*Entry, playerID int64) (int, bool) {
	for i, e := range list {
		if e.PlayerID == playerID {
			return i, true
		}
	}
	return -1, false
}

// Flush 把内存刷进库（关服流程调用）。
//
// 分数只在内存里，宕机就丢——这是已确认的取舍（一次加分只影响 1 分，
// 为它做持久化不划算）。正常关服走这里，玩家数据的损失窗口就只剩
// 「两次落库之间宕机」。
func (m *Module) Flush(ctx context.Context) error {
	if m.repo == nil || !m.repo.Ready() {
		return nil
	}
	players := m.reg.AllPlayers()
	rows := make([]model.CrossPlayer, 0, len(players))
	for _, e := range players {
		if e.GroupID == 0 {
			continue // 未分组的不进玩家表，分组时由快照重建
		}
		// 展示信息序列化失败不阻断分数落库：分数是玩法数据，
		// 展示信息丢了下次报名还能补。
		info, err := infoToJSON(e.Info)
		if err != nil {
			m.logger.Warn("marshal cross info failed", "player", e.PlayerID, "err", err)
		}
		rows = append(rows, model.CrossPlayer{
			PlayerId:  e.PlayerID,
			GroupId:   e.GroupID,
			ServerId:  e.ServerID,
			Power:     e.Power,
			Score:     e.Score,
			Info:      info,
			UpdatedAt: e.UpdatedAt,
		})
	}
	if err := m.repo.SavePlayers(ctx, rows); err != nil {
		return err
	}
	return m.saveGroups(ctx, m.reg.AllGroups(), m.reg.AllPlayers())
}

// OnSave 实现 module.Module：玩家落库钩子。**这里必须什么都不做。**
//
// 框架对**每个下线的玩家**都会调一次 OnSave（player/actor.go 的
// savePlayer 里遍历所有模块）。本模块没有 Ext 槽位，玩家落库时没有任何
// 属于他的数据要写——积分是玩法数据，不随玩家快照走。
//
// 如果在这里全量刷进程内存，一万个玩家下线就是一万次全表 UPSERT，
// 而且全在同一批 SQL 的连接池上，会把正常落库拖死。进程级的刷盘是
// Flush，由关服流程（server.go 的 flush-crossplay 阶段）调一次。
func (m *Module) OnSave(context.Context) error { return nil }
