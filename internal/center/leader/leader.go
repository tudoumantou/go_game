// Package leader 中心服选主（设计文档 9.6）
//
// 中心服分两类职责：
//   - 无主（每个实例独立跑）：Report 接入、中转、排行榜写入；
//   - 有主（仅 leader 跑）：全服活动调度、赛季结算归档、全服进度里程碑判定。
//
// 选主用 etcd 的 concurrency.Election（基于租约 + 前缀比较），
// leader 失联时租约过期 → 其余实例自动抢主，回调通知上层切换状态。
//
// 协作关系：本包只依赖 etcd client，不反向依赖 center 的任何子系统；
// center 通过 onChange 回调把「成为主/失去主」接到归档/调度/里程碑的
// isLeader 开关上，依赖方向无环。IsLeader 用 atomic 无锁读，
// 供 30s 一次的 tick 循环高频查询。
package leader

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/backoff"
	"game_study/pkg/log"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DefaultKey 默认选主 key。
// 该 key 下挂竞选者条目（按创建 revision 排序比大小），换 key 等于换一套竞选组。
const DefaultKey = "/game_study/leader/center"

// Election 选主管理器。
//
// 基于 etcd concurrency.Election（租约 + 前缀比较）：抢到主的实例把 selfID
// 写入选主 key，租约到期自动让位。isLeader 用 atomic.Bool 暴露供无锁读取；
// 回调只在状态变化时触发，避免网络抖动导致上层反复切换。
//
// 字段分两组：
//   - 配置（New 之后不变）：client/key/selfID/ttl/logger/onChange；
//   - 运行时：mu 保护 session/election 的读写（LeaderID/Resign 与 campaign
//     并发访问）；isLeader 用 atomic 供高频无锁读取；stopOnce 保证 stopCh
//     只关一次——对已关闭的 channel 再 close 会直接 panic。
type Election struct {
	client   *clientv3.Client
	key      string
	selfID   string
	ttl      int64
	logger   *log.Logger
	onChange func(isLeader bool)

	mu       sync.Mutex
	session  *concurrency.Session
	election *concurrency.Election

	isLeader atomic.Bool
	stopOnce sync.Once
	stopCh   chan struct{}
}

// Config 选主配置。
//
// Key 为 etcd 选主 key（空用 DefaultKey）；SelfID 应稳定唯一（推荐
// StatefulSet Pod 名），etcd 侧才能把重启前后的竞选者视为同一个参与者。
// OnChange 在状态翻转时调用，可能在任意 goroutine 触发，实现方需自行保证线程安全。
type Config struct {
	Key      string
	SelfID   string
	TTL      int64 // 租约秒数，默认 10
	OnChange func(isLeader bool)
}

// New 创建选主管理器。
//
// Key/TTL 为空或非正时落默认值（DefaultKey / 10s）。返回的 Election 尚未
// 参与竞选，需调用 Run 才会抢主。
// stopCh 在 New 里就建好：Stop 可能在 Run 之前被调用（启动即关闭的场景），
// 迟一点创建会让 Stop 空转甚至 panic。
func New(client *clientv3.Client, cfg Config, logger *log.Logger) *Election {
	if cfg.Key == "" {
		cfg.Key = DefaultKey
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 10
	}
	return &Election{
		client:   client,
		key:      cfg.Key,
		selfID:   cfg.SelfID,
		ttl:      cfg.TTL,
		logger:   logger,
		onChange: cfg.OnChange,
		stopCh:   make(chan struct{}),
	}
}

// Run 参与选举并常驻（阻塞至 ctx 取消或 Stop）。
//
// 行为：抢到主 → 回调 true；失去主 → 回调 false 并重新排队。
//
// 竞选中断（session 过期/etcd 故障）时按指数退避重试，上限 30s：etcd
// 抖动期间不疯狂重连，恢复后又能及时抢回。ctx 取消或 Stop 时返回。
//
// 节奏交给 pkg/backoff。原先这里手写的版本在「翻倍前判上限」：cur=20s 时
// 翻到 40s 就不再判了，实际突破了注释里写的 30s——同一个循环写了三遍，
// 这一遍就是写错的那遍（另两处在 game 的 center/cross 客户端）。
func (e *Election) Run(ctx context.Context) error {
	b := backoff.New(time.Second, 30*time.Second)
	return backoff.Loop(ctx, e.stopCh, b, e.campaign,
		func(err error, wait time.Duration) {
			// ctx.Err() != nil 时 campaign 返回错误是预期内的（主动取消），
			// 记了只会误导排查（把正常退出当故障）。
			if ctx.Err() != nil {
				return
			}
			e.logger.Warn("leader campaign broke, retrying",
				"self", e.selfID, "backoff", wait.String(), "err", err)
		},
	)
}

// campaign 一次完整竞选（到失联为止）。
//
// 每轮新建 session（租约随 ctx 生命周期）：Campaign 成功即成为 leader，
// 随后阻塞等待三种结局——ctx 取消/Stop（主动让位）或 session.Done
// （租约过期被动下台）。session 过期不是错误而是正常轮转：交回控制权
// 由 Run 的退避重试循环接手。
//
// defer 顺序（LIFO）：后注册的 session.Close 先执行不到——实际是先跑
// setLeader(false)（上层先看到失主），函数返回前才释放 etcd 租约，
// 避免「回调还没发出就丢租约」的窗口。
func (e *Election) campaign(ctx context.Context) error {
	session, err := concurrency.NewSession(e.client, concurrency.WithTTL(int(e.ttl)), concurrency.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("new session: %w", err)
	}
	defer func() { _ = session.Close() }()

	e.mu.Lock()
	e.session = session
	e.election = concurrency.NewElection(session, e.key)
	election := e.election
	e.mu.Unlock()

	if err := election.Campaign(ctx, e.selfID); err != nil {
		return fmt.Errorf("campaign: %w", err)
	}

	e.setLeader(true)
	// defer 保证任何退出路径（含 panic 后的正常返回）都发出失主回调；
	// 先 true 后注册 false，LIFO 下 false 先执行，配对完整
	defer e.setLeader(false)

	// 阻塞至租约失效 / ctx 取消
	select {
	case <-ctx.Done():
		// 主动退出用 Background 做 Resign：ctx 已取消，让位请求必须还能
		// 发出去并等 etcd 响应，否则其他实例要等满一个 TTL 才能抢主
		_ = election.Resign(context.Background())
		return ctx.Err()
	case <-e.stopCh:
		_ = election.Resign(context.Background())
		return nil
	case <-session.Done():
		e.logger.Warn("leader session expired, stepping down", "self", e.selfID)
		return fmt.Errorf("session done")
	}
}

// IsLeader 当前是否为主。
// atomic.Bool.Load 无锁：归档/调度/里程碑的 tick 循环高频轮询，
// 加锁会把选主状态变成热点。读到的是「最近一次确认」的近似值，
// 短暂滞后（租约已过期但回调未到）由各任务的幂等设计兜底。
func (e *Election) IsLeader() bool { return e.isLeader.Load() }

// LeaderID 查询当前 leader（用于诊断）。
//
// 返回 etcd 选主 key 当前的值（即 leader 的 SelfID）。竞选未开始或尚无
// leader 时返回错误，调用方应把错误当作「未知」而非「无主」。
//
// 实现上先拷贝 election 指针再调用：etcd 的 Leader() 是阻塞 RPC，
// 持锁等待会把 Resign/Stop 全部堵住。
func (e *Election) LeaderID(ctx context.Context) (string, error) {
	e.mu.Lock()
	election := e.election
	e.mu.Unlock()
	if election == nil {
		return "", fmt.Errorf("election not started")
	}
	resp, err := election.Leader(ctx)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("no leader")
	}
	return string(resp.Kvs[0].Value), nil
}

// Resign 主动让位（优雅关闭时调用，让其他实例立即接管）。
//
// 先 Resign 再关 session：顺序反了会让租约直接过期，其他实例要等一个
// TTL 才能抢主，滚动重启期间的有主任务空窗更长。重复调用安全。
//
// 恒返回 nil：让位是尽力而为，关闭流程不应被它卡住（底层错误已由
// etcd client 内部记录）。
func (e *Election) Resign(ctx context.Context) error {
	e.mu.Lock()
	election := e.election
	session := e.session
	e.mu.Unlock()

	if election != nil {
		_ = election.Resign(ctx)
	}
	if session != nil {
		_ = session.Close()
	}
	e.setLeader(false)
	return nil
}

// Stop 停止参与选举。
// sync.Once 包住 close：Run 与外部可能并发调用，重复 close channel 会 panic。
func (e *Election) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
}

// setLeader 状态切换（只在变化时回调，避免抖动）。
//
// Swap 返回旧值：与目标相同说明是重复设置（重试路径常见），直接返回不
// 触发回调——上层（归档/调度）以回调为唯一切换信号，重复回调会导致
// 任务循环空转重启。
//
// 日志与回调都只发一次：etcd 抖动时状态可能高频翻转，去重是刚需；
// logger/onChange 都可为 nil（测试构造半成品 Election 的场景）。
func (e *Election) setLeader(v bool) {
	if e.isLeader.Swap(v) == v {
		return
	}
	if e.logger != nil {
		if v {
			e.logger.Info("became leader", "self", e.selfID)
		} else {
			e.logger.Info("lost leadership", "self", e.selfID)
		}
	}
	if e.onChange != nil {
		e.onChange(v)
	}
}
