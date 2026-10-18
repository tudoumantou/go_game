// Package leader 中心服选主（设计文档 9.6）
//
// 中心服分两类职责：
//   - 无主（每个实例独立跑）：Report 接入、中转、排行榜写入；
//   - 有主（仅 leader 跑）：全服活动调度、赛季结算归档、全服进度里程碑判定。
//
// 选主用 etcd 的 concurrency.Election（基于租约 + 前缀比较），
// leader 失联时租约过期 → 其余实例自动抢主，回调通知上层切换状态。
package leader

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/log"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// DefaultKey 默认选主 key。
const DefaultKey = "/game_study/leader/center"

// Election 选主管理器。
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
type Config struct {
	Key      string
	SelfID   string
	TTL      int64 // 租约秒数，默认 10
	OnChange func(isLeader bool)
}

// New 创建选主管理器。
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
func (e *Election) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.stopCh:
			return nil
		default:
		}

		err := e.campaign(ctx)
		if err != nil && ctx.Err() == nil {
			e.logger.Warn("leader campaign broke, retrying",
				"self", e.selfID, "backoff", backoff.String(), "err", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.stopCh:
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// campaign 一次完整竞选（到失联为止）。
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
	defer e.setLeader(false)

	// 阻塞至租约失效 / ctx 取消
	select {
	case <-ctx.Done():
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
func (e *Election) IsLeader() bool { return e.isLeader.Load() }

// LeaderID 查询当前 leader（用于诊断）。
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
func (e *Election) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
}

// setLeader 状态切换（只在变化时回调，避免抖动）。
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
