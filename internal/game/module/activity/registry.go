// internal/game/module/activity/registry.go —— 活动窗口注册表（R7.2）。
//
// 中心服调度把活动实例（窗口起止 + 版本号）下发到本进程；活动模块与
// 窗口自算路径共用同一份注册表，保证「下发」与「按配置自算」不打架。
//
// 架构职责：进程级单例，持有「中心服已下发的活动窗口」。
// 写入方是调度消费者（Kafka/长连接回调，多 goroutine），
// 读取方是各玩家 Actor 的协议处理（大量并发读）——读写比悬殊，
// 所以用 RWMutex 而不是 Mutex。
// Ext 槽位：无（进程级状态，不属于任何玩家）。
// 事件订阅：无（由调度消费者直接调 Apply）。
// 懒初始化：允许为空表——中心服消息未到或未部署调度时，
// 读取方降级为「视为开放」（见 openNow）。
package activity

import (
	"sync"

	"game_study/pkg/activity"
)

// Window 一个活动实例的时间窗（由中心服下发）。
//
// Version 是单调递增的版本号：乱序/重复消息靠它幂等
// （旧版本不覆盖新版本），不需要额外去重表。
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
//
// Go 知识点：sync.RWMutex 的「读多写少」优化——RLock 允许多个读者
// 并行，只有写（Lock）才互斥。本表的写只发生在调度消息到达时
// （分钟级），读发生在每个活动请求（秒级），用 RWMutex 正好。
type Registry struct {
	mu sync.RWMutex
	m  map[int32]Window
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{m: make(map[int32]Window)}
}

// Apply 应用一条下发；版本更旧或相同则忽略（乱序/重复消息幂等）。
//
// 返回值表示「是否真的发生了变更」：调用方（调度消费者）可据此打日志，
// 不需要自己再比对版本。
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
//
// Go 知识点：包级单例 var + init 期构造。与 writebehind 的模块注册表
// 同一模式——跨模块共享的进程级状态用包级变量承载，
// 避免层层传递；测试可用 NewRegistry 建独立实例隔离。
var defaultRegistry = NewRegistry()

// DefaultRegistry 返回进程级注册表。
func DefaultRegistry() *Registry { return defaultRegistry }

// openNow 活动当前是否开放。
//
// 未下发实例时返回 true：配置里的周期是「每天重复」的语义（如每天 20:00-21:00），
// 任何时刻都落在某个周期窗口内，不存在「今天整天不开」的状态。真正的「已结束」
// 只来自中心服下发的具体实例（EndAt 过后即关闭）——这是唯一的关闭判定来源。
func openNow(activityID int32, now int64) bool {
	w, ok := defaultRegistry.Get(activityID)
	if !ok {
		return true
	}
	return activity.Active(w.StartAt, w.EndAt, now)
}
