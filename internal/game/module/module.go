// Package module 游戏服业务模块接口（设计文档 17.2 模块标准结构）
package module

import (
	"context"
	"database/sql"
	"fmt"

	"game_study/pkg/actor"
	"game_study/pkg/writebehind"
)

// Profile 角色基础信息。
type Profile = writebehind.Profile

// PlayerState 玩家内存状态接口（由 player.State 实现，模块通过它读写数据）。
type PlayerState interface {
	PlayerID() int64
	Profile() Profile
	ExpMax() int64
	GetAttr(key string) int64
	SetAttr(key string, val int64)
	AddBonus(key string, delta int64)
	ClearBonus()
	GetItemCount(itemID int32) int64
	Items() map[int32]int64
	AddItem(itemID int32, count int64)
	RemoveItem(itemID int32, count int64) bool
	MarkDirty(module string)
}

// Sender 下行发送能力（由游戏服组装层实现，写回网关）。
type Sender func(msgID uint32, body []byte) error

// ItemGrant 一次原子发奖中的道具增量。
type ItemGrant = writebehind.ItemGrant

// SigninState 是可选的玩家业务状态能力；查询/领取只访问所属 Actor。
type SigninState interface {
	SigninData() *writebehind.SigninData
	SetSigninData(*writebehind.SigninData)
}

// AddExpFunc 增加经验并按配置连续升级，返回是否升级与当前等级。
type AddExpFunc func(n int64) (levelUp bool, level int32)

// Event 玩家行为事件（模块间解耦：产生方 Emit，消费方 OnEvent）。
type Event struct {
	Type  string // use_item / kill / login / ...
	Key   string // 事件主体（道具 ID / 怪物 ID）
	Delta int64  // 增量（数量）
}

// EquipSnapshot 预加载的装备行（避免模块 OnLogin 再查一次库）。
type EquipSnapshot struct {
	UID          int64
	EquipID      int32
	EnhanceLevel int32
	Quality      int32
	Equipped     bool
}

// ActivitySnapshot 预加载的活动进度行。
type ActivitySnapshot struct {
	ActivityID int32
	Progress   int32
	RewardMask int64
}

// PrefetchSource 登录预加载结果（12.6：全部并行拉取，模块 OnLogin 直接读）。
//
// 实现方是 player.Repository。返回 nil 表示未预取（降级为模块自行查库），
// 模块必须同时支持两条路径。
type PrefetchSource interface {
	Equips() []EquipSnapshot
	Activities() []ActivitySnapshot
}

// Context 模块上下文：Actor 上下文 + 玩家状态 + 下行发送。
type Context struct {
	Actor        *actor.Context
	State        PlayerState
	Prefetch     PrefetchSource // 登录预加载结果（可能为 nil）
	TraceID      string
	Send         Sender
	Emit         func(evt Event)
	AddExp       AddExpFunc
	CacheEnabled bool // Redis 权威状态已接入，成功响应在缓存提交后发送。
	// CommitItems 在玩家 Actor 内将业务操作与背包奖励放入同一事务。
	// 只有事务提交成功后才修改内存；业务操作必须仅通过传入的 tx 写库。
	CommitItems func(context.Context, []ItemGrant, func(*sql.Tx) error) error
}

// Reply 向客户端回包（Send 未注入时静默忽略，便于单测）。
func (c *Context) Reply(msgID uint32, body []byte) error {
	if c.Send == nil {
		return nil
	}
	return c.Send(msgID, body)
}

// Module 业务功能模块统一接口（每个功能 = Init/OnMessage/OnSave/OnLogin/OnLogout）。
type Module interface {
	// Name 模块名（日志/指标标签）
	Name() string
	// MsgIDs 本模块处理的协议号（Registry 据此路由，避免全量广播）
	MsgIDs() []uint32
	// Init 初始化（注册消息处理、加载模块级配置）
	Init(ctx *Context) error
	// OnMessage 协议处理（同一 Actor 内单线程调用，无锁直接读写 state）
	OnMessage(ctx *Context, msgID uint32, body []byte) error
	// OnSave 持久化钩子（markDirty 的实体由 flusher 统一落库；此处处理模块级聚合数据）
	OnSave(ctx context.Context) error
	// OnLogin 玩家登录/预加载完成
	OnLogin(ctx *Context) error
	// OnLogout 玩家下线（清理定时器、释放跨模块引用）
	OnLogout(ctx *Context) error
}

// EventAware 可选接口：关心玩家行为事件的模块实现它。
type EventAware interface {
	OnEvent(ctx *Context, evt Event)
}

// Registry 模块注册表（含 msgID 路由索引）。
type Registry struct {
	modules []Module
	route   map[uint32]Module
}

// NewRegistry 创建注册表。
func NewRegistry() *Registry {
	return &Registry{route: make(map[uint32]Module)}
}

// ErrNoRoute 无模块处理该协议号。
var ErrNoRoute = fmt.Errorf("no module handles message")

// Register 注册模块（按 17.1 顺序：create/player/recharge/level/attr/equip/skill/bag/...）。
// 协议号冲突时 panic —— 属于编码错误，启动期暴露。
func (r *Registry) Register(m Module) {
	for _, id := range m.MsgIDs() {
		if old, ok := r.route[id]; ok {
			panic(fmt.Sprintf("msgID %d registered by both %s and %s", id, old.Name(), m.Name()))
		}
		r.route[id] = m
	}
	r.modules = append(r.modules, m)
}

// All 全部模块（服务器组装时初始化用）。
func (r *Registry) All() []Module { return r.modules }

// Dispatch 按 msgID 分发到对应模块。
func (r *Registry) Dispatch(ctx *Context, msgID uint32, body []byte) error {
	m, ok := r.route[msgID]
	if !ok {
		return fmt.Errorf("%w: %d", ErrNoRoute, msgID)
	}
	return m.OnMessage(ctx, msgID, body)
}

// DispatchEvent 把事件分发给所有 EventAware 模块。
func (r *Registry) DispatchEvent(ctx *Context, evt Event) {
	for _, m := range r.modules {
		if ea, ok := m.(EventAware); ok {
			ea.OnEvent(ctx, evt)
		}
	}
}

// InitAll 初始化全部模块。
func (r *Registry) InitAll(ctx *Context) error {
	for _, m := range r.modules {
		if err := m.Init(ctx); err != nil {
			return err
		}
	}
	return nil
}

// LoginAll 通知全部模块玩家上线。
func (r *Registry) LoginAll(ctx *Context) error {
	for _, m := range r.modules {
		if err := m.OnLogin(ctx); err != nil {
			return err
		}
	}
	return nil
}

// LogoutAll 通知全部模块玩家下线。
func (r *Registry) LogoutAll(ctx *Context) error {
	for _, m := range r.modules {
		if err := m.OnLogout(ctx); err != nil {
			return err
		}
	}
	return nil
}
