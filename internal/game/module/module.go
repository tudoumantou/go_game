// Package module 游戏服业务模块接口（设计文档 17.2 模块标准结构）
package module

import (
	"context"
	"fmt"

	"game_study/pkg/actor"
)

// Profile 角色基础信息。
type Profile struct {
	Name    string
	Level   int32
	Exp     int64
	Coin    int64
	SceneID int32
}

// PlayerState 玩家内存状态接口（由 player.State 实现，模块通过它读写数据）。
type PlayerState interface {
	PlayerID() int64
	Profile() Profile
	GetAttr(key string) int64
	SetAttr(key string, val int64)
	GetItemCount(itemID int32) int64
	Items() map[int32]int64
	AddItem(itemID int32, count int64)
	RemoveItem(itemID int32, count int64) bool
	MarkDirty(module string)
}

// Sender 下行发送能力（由游戏服组装层实现，写回网关）。
type Sender func(msgID uint32, body []byte) error

// Context 模块上下文：Actor 上下文 + 玩家状态 + 下行发送。
type Context struct {
	Actor   *actor.Context
	State   PlayerState
	TraceID string
	Send    Sender
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
