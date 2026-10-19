package activity

import (
	"sync"

	"game_study/pkg/activity"
	"game_study/pkg/gamedata"
)

// Window 一个活动实例的时间窗（由中心服下发）。
type Window struct {
	InstanceID string
	ActivityID int32
	StartAt    int64
	EndAt      int64
	Version    int64
}

// Registry 已下发的活动实例（进程级单例，非玩家状态）。
//
// 为什么允许为空：中心服消息可能还没到，也可能整个调度没部署。
// 读取方先查这里，查不到就按配置自己算——两条路算出的窗口必须一致，
// 因为用的是同一个 pkg/activity.Window。
type Registry struct {
	mu sync.RWMutex
	m  map[int32]Window
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{m: make(map[int32]Window)}
}

// Apply 应用一条下发；版本更旧或相同则忽略（乱序/重复消息幂等）。
func (r *Registry) Apply(w Window) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	cur, ok := r.m[w.ActivityID]
	if ok && w.Version <= cur.Version {
		return false
	}
	r.m[w.ActivityID] = w
	return true
}

// Get 读取某个活动的当前实例。
func (r *Registry) Get(activityID int32) (Window, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.m[activityID]
	return w, ok
}

// Len 已下发实例数（自检/日志用）。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.m)
}

// defaultRegistry 进程级注册表：调度消费者写入，活动模块读取。
var defaultRegistry = NewRegistry()

// DefaultRegistry 返回进程级注册表。
func DefaultRegistry() *Registry { return defaultRegistry }

// ActiveWindow 返回活动在 now 时是否开放，以及当前窗口。
//
// 优先用中心服下发的实例；没有下发时按配置周期计算。
// 两条路径都不依赖「消息恰好到达」，这是 R7.2 的核心要求。
func ActiveWindow(reg *Registry, activityID int32, now int64) (start, end int64, ok bool) {
	if reg != nil {
		if w, found := reg.Get(activityID); found {
			return w.StartAt, w.EndAt, activity.Active(w.StartAt, w.EndAt, now)
		}
	}
	cfg, found := gamedata.GetActivityPeriodCfg(int64(activityID))
	if !found {
		// 没有周期配置 = 不限制时间（配置未定义，不臆造窗口）。
		return 0, 0, true
	}
	start, end, _ = activity.Window(int32(cfg.Col1), now)
	return start, end, true // 自算窗口必然包含 now
}

// openNow 活动当前是否开放。
//
// 未下发实例时返回 true：此时按配置周期计算窗口，当前时刻必然落在某个周期内。
// 已下发且窗口过期才返回 false——这是「活动结束」的唯一判定来源。
func openNow(activityID int32, now int64) bool {
	w, ok := defaultRegistry.Get(activityID)
	if !ok {
		return true
	}
	return activity.Active(w.StartAt, w.EndAt, now)
}
