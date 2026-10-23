// Package gameplay 是"玩法插件"的实现集合：按地图类型分派的注册表，
// 以及主城/副本两个最小可玩实现。
//
// # 它在架构里的位置
//
//	MGR 只认 mgr.Gameplay 接口；具体玩法由本包提供，装配时注入 Deps.Gameplay。
//	一张图一个玩法实例：副本要记波次、主城要记人数，共享实例会互相串。
//
// # 错误语义
//
// 玩法回调里的 error 由 MGR 决定怎么处理（MgrInit 失败不开图，其余只记日志
// 不回滚）。所以本包不写"失败可以重试"的逻辑，把错误当成"这条业务没做成"。
package gameplay

import (
	"fmt"
	"sync"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/mgr"
)

// Registry 是"地图类型 → 玩法"的分派表（规格 7.3 的替代实现）。
//
// # 为什么用强类型 map 而不是事件总线
//
// 分派表只有"查一次"这一个动作，用 map 让"类型没注册"在查的那一刻显式
// 返回错误；换成 map[string]func(any) 的事件总线，拼错 key 的后果是
// 静默不触发，线上只能靠玩家反馈发现。
type Registry struct {
	mu sync.RWMutex
	// byType 地图类型 → 玩法。key 用 int32 与配置表的地图类型枚举同源。
	byType map[int32]mgr.Gameplay
}

// NewRegistry 构造空的注册表。
func NewRegistry() *Registry {
	return &Registry{byType: make(map[int32]mgr.Gameplay)}
}

// Register 注册一个地图类型的玩法。
//
// 重复注册返回 ErrAlreadyExists 而不是"后写覆盖"：覆盖会让两个人同时
// 改玩法时，谁最后启动谁生效，线上表现取决于部署顺序——这种不确定性
// 在开图期就炸出来，比运行期"玩法怎么偶尔不对"便宜得多。
func (r *Registry) Register(mapType int32, g mgr.Gameplay) error {
	if r == nil {
		return fmt.Errorf("%w: registry 为 nil", scenemap.ErrInvalidArgument)
	}
	if g == nil {
		return fmt.Errorf("%w: 地图类型 %d 的玩法为 nil", scenemap.ErrInvalidArgument, mapType)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byType == nil {
		r.byType = make(map[int32]mgr.Gameplay)
	}
	if _, dup := r.byType[mapType]; dup {
		return fmt.Errorf("%w: 地图类型 %d 已注册", scenemap.ErrAlreadyExists, mapType)
	}
	r.byType[mapType] = g
	return nil
}

// Lookup 按地图类型取玩法。
//
// # 有意偏离原实现（规格 11.13）
//
// 参考实现 版查不到地图类型时**默认按主城处理**，结果是"能进图、但所有
// 结算都走主城逻辑"——看起来正常，实际关卡进度、掉落、通关判定全不对，
// 而且不会报错。这里改成显式返回 ErrMapTypeMissing：开图期直接失败，
// 比运行期静默错算便宜得多。
func (r *Registry) Lookup(mapType int32) (mgr.Gameplay, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: registry 为 nil", scenemap.ErrInvalidArgument)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.byType[mapType]
	if !ok {
		return nil, fmt.Errorf("%w: 地图类型 %d", scenemap.ErrMapTypeMissing, mapType)
	}
	return g, nil
}

// Len 返回已注册的地图类型数（监控/测试用）。
func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byType)
}

// 编译期断言：Registry 的方法集与预期一致（防止签名漂移）。
var _ interface {
	Register(int32, mgr.Gameplay) error
	Lookup(int32) (mgr.Gameplay, error)
} = (*Registry)(nil)
