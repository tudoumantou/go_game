// internal/game/module/battle/battle.go —— 战斗模块（5001-5021，设计文档 4.9.3 / 10.3）
//
// Demo 期职责边界（服务端只做三件事）：
//  1. 通知发起：匹配凑人 → 向 CrossServer 申请建房 → 下发 BattleStartNotify；
//  2. 消息透传：BattleOpReport 原样转发到房间（不解析 body）；
//  3. 接收结果：BattleResultReport → 令牌校验 + 幂等 → 回 BattleSettleNotify。
//
// 战斗演算（技能/伤害/胜负）完全在客户端；服务端不做防作弊校验（后置到战斗 2.0）。

package battle

import (
	"sync"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/log"
)

type Module struct {
	// 基座是 Define[NoState]：战斗没有 Ext 槽位（会话/队列是本模块的
	// 自有内存态，随进程生死），但与签到/任务共用同一套接口管线。
	*module.Bound[module.NoState]

	cross  CrossCaller
	center CenterMatcher
	ranker RankReporter
	pusher Pusher
	logger *log.Logger

	mu sync.Mutex
	// sessions player_id → 当前战斗（玩家战斗中）
	sessions map[int64]*Session
	// queue 匹配队列（mode → 玩家列表）
	queue map[string][]int64
	// queued 玩家是否已在队列（防重复入队）
	queued map[int64]bool
	// timeout 单场战斗时限
	timeout time.Duration
}

// New 创建战斗模块。
//
// 注意 cross 不可为空（onMatch 直接调用其 Available）；ranker 后置注入（SetRanker），
// 因为排行上报是 P14 增量能力，demo 期允许缺失。
func New(cross CrossCaller, center CenterMatcher, pusher Pusher, logger *log.Logger) *Module {
	m := &Module{
		cross:    cross,
		center:   center,
		pusher:   pusher,
		logger:   logger,
		sessions: make(map[int64]*Session),
		queue:    make(map[string][]int64),
		queued:   make(map[int64]bool),
	}
	m.Bound = module.Define[module.NoState]("battle").WithLogger(logger)
	module.Handle(m.Bound, m.onMatch)
	// 战报是单向上报：客户端不等回包，服务端也没有响应协议可回。
	// 用 Handle 而不是 HandleR（后者要求声明响应）。
	m.Handle(codec.MsgBattleOpReport, m.onOpReport)
	module.Handle(m.Bound, m.onResultReport)
	return m
}

// InBattle 玩家是否在战斗中（供其他模块判断是否冻结）。
func (m *Module) InBattle(playerID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[playerID]
	return ok && !s.Settled
}

// QueueLen 当前队列人数（测试/指标用）。
func (m *Module) QueueLen(mode string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queue[mode])
}

// SessionOf 返回玩家当前战斗会话（测试用）。
func (m *Module) SessionOf(playerID int64) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[playerID]; ok {
		// 返回浅拷贝而非原指针：调用方拿到的是快照，
		// 不会与内部状态的并发修改产生数据竞争
		cp := *s
		return &cp
	}
	return nil
}

// battleTimeout 单场战斗时限（超时由 CrossServer 按平局兜底结算）。
func (m *Module) battleTimeout() time.Duration {
	if m.timeout > 0 {
		return m.timeout
	}
	return defaultBattleTimeout
}

// SetRanker 注入排行上报能力（P14）。
func (m *Module) SetRanker(r RankReporter) { m.ranker = r }

// SetBattleTimeout 覆盖默认战斗时限（组装层按配置注入）。
func (m *Module) SetBattleTimeout(d time.Duration) {
	if d > 0 {
		m.timeout = d
	}
}
