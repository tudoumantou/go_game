// Package module 游戏服业务模块接口（设计文档 17.2 模块标准结构）
package module

import "context"

// Module 业务功能模块统一接口（每个功能 = Init/OnMessage/OnSave/OnLogin/OnLogout）
type Module interface {
	// Name 模块名（日志/指标标签）
	Name() string
	// Init 初始化（注册消息处理、加载模块级配置）
	Init(ctx Context) error
	// OnMessage 协议处理（同一 Actor 内单线程调用，无锁直接读写 state）
	OnMessage(ctx Context, msgID uint32, body []byte) error
	// OnSave 持久化钩子（markDirty 的实体由 flusher 统一落库；此处处理模块级聚合数据）
	OnSave(ctx context.Context) error
	// OnLogin 玩家登录/预加载完成
	OnLogin(ctx Context) error
	// OnLogout 玩家下线（清理定时器、释放跨模块引用）
	OnLogout(ctx Context) error
}

// Context 模块上下文（P0 最小占位）。
// TODO(P3): 与 pkg/actor.Context 合并（ActorID 编址、Tell/Call 信箱投递、
// 定时器、TraceID 全链路），届时删除本占位类型，Module 接口签名不变。
type Context struct {
	ActorID  uint64 // 所属玩家 Actor 分片内 ID
	PlayerID int64
	TraceID  string // 全链路追踪（playerID + sessionID 贯穿五服务）
}

// Registry 模块注册表（P3 落地；P0 仅定义边界）
type Registry struct {
	modules []Module
}

// Register 注册模块（按 17.1 顺序：create/login/recharge/level/attr/equip/skill/bag/...）
func (r *Registry) Register(m Module) { r.modules = append(r.modules, m) }

// All 全部模块（服务器组装时初始化用）
func (r *Registry) All() []Module { return r.modules }
