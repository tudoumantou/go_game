// Package module 玩法模块层的公共契约（设计文档 17.2 模块标准结构）。
//
// 每个玩法（背包/装备/任务/签到…）都是一个 Module：Init 注册协议与校验配置，
// OnMessage 在玩家 Actor 内单线程处理协议，OnLogin/OnLogout 挂接上下线钩子，
// OnSave 由 flusher 触达。模块与 MySQL 落库完全解耦——私有状态走玩家快照的
// Ext 槽位（writebehind.LoadExt/SaveExt），由 Persister 异步写回；只有关系型
// 数据（好友/公会/队伍）才在模块内直写事务。
package module

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/writebehind"
)

// ErrBagFull 背包已满：新增道具种类超过容量。模块用它作为「发奖被拒」的信号，
// 由玩家层回滚整条消息，保证不会出现扣了费却发不出货。
//
// 调用约定：发奖前先用 State.CanHold 预检；在 ApplyOnce 的 fn 里返回本错误时
// 不会占用业务键——玩家清空背包后可重新领取，不会被「已申请」永久卡死。
var ErrBagFull = errors.New("bag full")

// Profile 角色基础信息（writebehind.Profile 的别名）。
//
// 用类型别名而非新定义：模块看到的字段与落库结构完全一致，
// 省去 player 包与 module 包之间的循环依赖。
type Profile = writebehind.Profile

// PlayerState 玩家内存状态接口（由 player.State 实现，模块通过它读写数据）。
//
// 契约：所有方法只会在玩家 Actor 的单线程内被调用，实现方无需加锁；
// 跨 Actor 的读取一律走快照序列化通道，不直接持有本接口。
// 资产类改动（AddItem/AddBonus/SetAttr）只改内存，权威提交是整份快照的
// Redis CAS——任一改动失败则同一条消息内的全部改动一起回滚。
type PlayerState interface {
	// PlayerID 玩家 ID：快照主键，日志与业务键（biz key）都基于它派生。
	PlayerID() int64
	// Profile 角色基础信息快照（等级/经验/货币/场景归属等）。
	Profile() Profile
	// ExpMax 当前等级升到下一级所需经验（由等级配置决定）。
	ExpMax() int64
	// GetAttr 读取战斗属性最终值（基础值 + 各模块加成之和）。
	GetAttr(key string) int64
	// SetAttr 覆写属性基础值（升级重算时用）。
	SetAttr(key string, val int64)
	// AddBonus 增减某项属性的加成（装备/伙伴等派生项，只进内存不落库）。
	AddBonus(key string, delta int64)
	// ClearBonus 清空全部加成；登录重算前由玩家层统一调用。
	ClearBonus()
	// GetItemCount 某道具的持有数量（P6 堆叠模式：item_id 即占位 uid）。
	GetItemCount(itemID int32) int64
	// Items 全部道具的只读视图；返回内部 map，调用方不得直接修改。
	Items() map[int32]int64
	// AddItem 增加道具（只改内存，随玩家快照同一次 CAS 提交）。
	AddItem(itemID int32, count int64)
	// RemoveItem 减少道具；数量不足时返回 false 且不产生部分扣减。
	RemoveItem(itemID int32, count int64) bool
	// MarkDirty 标记模块状态已变，提示 flusher 关注；权威提交仍是快照 CAS。
	MarkDirty(module string)
	// SetSceneID 更新所在场景（0 表示不在场景中）并标脏。
	// 场景归属由场景 Actor 决定，模块只负责把它写回玩家状态。
	SetSceneID(sceneID int32)
	// Ext 读取模块私有状态；SetExt 覆盖并标脏。键必须是 writebehind.Persister.Name()。
	Ext(name string) json.RawMessage
	SetExt(name string, raw json.RawMessage)
	// CanHold 预检：这批道具是否都放得下（只统计新增种类，已有道具可叠加）。
	// 发奖前先预检，避免多物品奖励发到一半才发现满包。
	CanHold(grants []ItemGrant) bool
	// ClaimBiz 占用业务唯一键；返回 false 表示该键已应用过（幂等命中）。
	// 键随快照一起 CAS 并落库到 player_writebehind.applied_grants，重启后仍有效。
	ClaimBiz(key string) bool
	// HasBiz 只读查询业务键是否已应用。
	HasBiz(key string) bool
}

// Sender 下行发送能力（由游戏服组装层实现，写回网关）。
//
// 约定：返回 error 仅表示「本次发送失败」（连接已断等），模块记日志即可，
// 不重试也不缓存——客户端重连后会主动重新拉取权威状态。
type Sender func(msgID uint32, body []byte) error

// ItemGrant 一次原子发奖中的道具增量（writebehind.ItemGrant 别名）。
//
// 「原子」的含义：一批 Grant 与业务状态（领取事实/扣费）在同一次快照 CAS 里
// 提交，要么全部到账，要么全部回滚，不会出现发到一半。
type ItemGrant = writebehind.ItemGrant

// AddExpFunc 增加经验并按配置连续升级，返回是否升级与当前等级。
//
// 由玩家层注入（依赖倒置）：模块只声明「我要加经验」，升级规则、
// 等级上限与升级推送都由玩家层统一处理，避免每个模块各写一套。
type AddExpFunc func(n int64) (levelUp bool, level int32)

// Event 玩家行为事件（模块间解耦：产生方 Emit，消费方 OnEvent）。
//
// 例：背包 use_item、副本 copy_pass、商店 buy。任务/活动模块订阅后推进进度，
// 产生方不需要知道谁在听——新增消费方只改 Registry.DispatchEvent 的遍历。
type Event struct {
	Type  string // use_item / kill / login / ...
	Key   string // 事件主体（道具 ID / 怪物 ID）
	Delta int64  // 增量（数量）
}

// Context 模块上下文：Actor 上下文 + 玩家状态 + 下行发送。
//
// 每条玩家消息都会新建一个 Context（轻量值对象），字段由玩家层注入：
// 模块不直接依赖 player 包，只依赖这里声明的接口，便于单测替换。
type Context struct {
	Actor        *actor.Context  // 当前玩家 Actor 的上下文（Send 给其他 Actor）
	State        PlayerState     // 玩家内存状态（模块的主读写面）
	TraceID      string          // 链路追踪 ID（日志关联一次登录会话）
	Send         Sender          // 下行发送（nil 时 Reply 静默丢弃，便于单测）
	Emit         func(evt Event) // 事件广播（nil 表示无订阅方，产生方需判空）
	AddExp       AddExpFunc      // 加经验回调（nil 表示玩家层未注入，模块需判空）
	CacheEnabled bool            // Redis 权威状态已接入，成功响应在缓存提交后发送。
	// CommitItems 在玩家 Actor 内将业务操作与背包奖励放入同一事务。
	// 只有事务提交成功后才修改内存；业务操作必须仅通过传入的 tx 写库。
	CommitItems func(context.Context, []ItemGrant, func(*sql.Tx) error) error
	// Clock 取当前时间；nil 表示用系统时间。测试注入假时钟验证跨天/跨周边界，
	// 业务不再各自持有一个 now 字段。
	Clock func() time.Time
}

// Now 返回当前时间：优先注入的时钟，否则系统时间。
func (c *Context) Now() time.Time {
	if c.Clock != nil {
		return c.Clock()
	}
	return time.Now()
}

// Reply 向客户端回包（Send 未注入时静默忽略，便于单测）。
//
// 模块统一走这里而不是直接调 Send：测试里不注入 Send 也不会 panic，
// 生产里 Send 瞬时失败也不向上抛——协议处理已经成功，不该让整条消息报错。
func (c *Context) Reply(msgID uint32, body []byte) error {
	if c.Send == nil {
		return nil
	}
	return c.Send(msgID, body)
}

// ApplyOnce 幂等执行业务：key 已应用过则跳过 fn，返回 applied=false。
//
// 适用场景是「不可重复生效」的投递类业务（充值回调、Kafka 重投、离线补偿）。
// 键在 fn 成功后才占用 —— 一次配置错误或校验失败不能把业务键永久占死。
//
// 键、资产、模块状态最终都在同一次快照 CAS 里提交：Redis 失败则三者一起回滚，
// 不会出现「领取事实已记、奖励没到账」。主动领取类业务（任务/活动/副本）不需要
// 键，用业务状态里的「已领取」事实做幂等即可，避免 applied_grants 无限膨胀。
func (c *Context) ApplyOnce(key string, fn func() error) (applied bool, err error) {
	if c.State == nil {
		return false, fmt.Errorf("module context without player state")
	}
	if key == "" {
		return true, fn()
	}
	if c.State.HasBiz(key) {
		return false, nil
	}
	if err := fn(); err != nil {
		return false, err
	}
	c.State.ClaimBiz(key)
	return true, nil
}

// Module 业务功能模块统一接口（每个功能 = Init/OnMessage/OnSave/OnLogin/OnLogout）。
//
// 生命周期由 Registry 驱动：InitAll → LoginAll →（OnMessage 循环）→ LogoutAll → OnSave。
// 实现方通常是包级单例（New 返回 *Module），因为模块只持有配置与仓储，
// 玩家状态全部来自 Context.State——同一模块实例服务所有玩家。
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
//
// Go 知识点：这是「可选能力」惯用法——Registry 用类型断言
// （m.(EventAware)）探测，未实现的模块不会收到事件，
// 比在 Module 里塞一个空的 OnEvent 更省调用。
type EventAware interface {
	// OnEvent 消费一个玩家行为事件；在玩家 Actor 内单线程调用，可无锁读写 state。
	OnEvent(ctx *Context, evt Event)
}

// DiscardableAware 可选接口：声明某个请求在写缓存失败后能否直接丢弃内存改动。
//
// 返回 true 的前提是「重放该请求不会产生第二次副作用」：要么只读，要么有业务幂等键。
// 未实现该接口的模块按不可回滚处理（保守），失败时保留待提交快照而不是丢弃改动。
type DiscardableAware interface {
	// Discardable 判定该协议是否满足「重放无第二次副作用」。
	//
	// true 的处理方式：失败后不回滚内存，只标记重载——下一条消息开头
	// 从 Redis 整份恢复。false 的处理方式：保留待提交快照，下次补提
	// （不可重放的消息，如充值发货已在 MySQL 落定）。
	Discardable(msgID uint32) bool
}

// CacheFailureAware 可选接口：缓存不可用时给客户端一个有意义的错误响应。
//
// 未实现时由网关层统一回通用错误通知；实现了就由模块自己决定响应协议号与错误码，
// 避免在玩家层硬编码各模块的协议号。
type CacheFailureAware interface {
	// CacheFailureReply 返回模块自定义的缓存不可用响应；ok=false 时走网关通用错误。
	CacheFailureReply(msgID uint32) (respID uint32, body []byte, ok bool)
}

// StatelessAware 可选接口：声明某协议**不修改玩家状态**。
//
// 缓存权威模式下，玩家 Actor 默认对每条消息执行「clone State → 业务 →
// JSON 比对 → 有差异才 CAS」。声明为无状态后，该消息直接在 receive 路径
// 分发，跳过 clone 与两次 Marshal——即使 diff 为空不产生 Redis 写，
// 那份序列化开销也是白付的（State 含背包/属性/各模块 Ext，clone 不便宜）。
//
// 正确性责任：实现方保证处理器不调用 State 的任何写方法。误声明不会
// 造成数据错乱（状态照常随快照提交），只是那份改动要到下一次有状态消息
// 才落库——所以这是一个性能声明，不是一致性开关。
type StatelessAware interface {
	IsStateless(msgID uint32) bool
}

// Registry 模块注册表（含 msgID 路由索引）。
//
// 设计：modules 切片保序（Init/Login 按 17.1 的顺序），route map 提供
// O(1) 协议分发。注册表模式让组装层（server.go）只面对一个入口，
// 新增玩法 = 调一次 Register，不需要改分发代码。
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
//
// 错误用 %w 包装 ErrNoRoute：调用方可用 errors.Is 区分「没有路由」
// 与「模块内部错误」，前者通常是协议号配错，后者是业务 bug。
func (r *Registry) Dispatch(ctx *Context, msgID uint32, body []byte) error {
	m, ok := r.route[msgID]
	if !ok {
		return fmt.Errorf("%w: %d", ErrNoRoute, msgID)
	}
	return m.OnMessage(ctx, msgID, body)
}

// IsDiscardable 查询某协议失败后能否丢弃内存改动（未注册的协议一律不可丢弃）。
func (r *Registry) IsDiscardable(msgID uint32) bool {
	m, ok := r.route[msgID]
	if !ok {
		return false
	}
	rb, ok := m.(DiscardableAware)
	if !ok {
		return false
	}
	return rb.Discardable(msgID)
}

// CacheFailureReply 取得模块自定义的缓存不可用响应（未实现时返回 false）。
func (r *Registry) CacheFailureReply(msgID uint32) (uint32, []byte, bool) {
	m, ok := r.route[msgID]
	if !ok {
		return 0, nil, false
	}
	ca, ok := m.(CacheFailureAware)
	if !ok {
		return 0, nil, false
	}
	return ca.CacheFailureReply(msgID)
}

// IsStateless 查询某协议是否声明为无状态（未注册或未实现 StatelessAware 时返回 false）。
//
// 玩家 Actor 据此选择分发路径：true → 直接 receive（跳过快照流程），
// false → receiveCached（clone/CAS）。保守默认 false，误判只会多付开销，不会丢数据。
func (r *Registry) IsStateless(msgID uint32) bool {
	m, ok := r.route[msgID]
	if !ok {
		return false
	}
	sa, ok := m.(StatelessAware)
	if !ok {
		return false
	}
	return sa.IsStateless(msgID)
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
//
// 顺序与 LoginAll 相反：后登录的先下线（场景模块最后清理实体），
// 任一模块报错也继续跑完——下线路径不能半途中断。
func (r *Registry) LogoutAll(ctx *Context) error {
	for _, m := range r.modules {
		if err := m.OnLogout(ctx); err != nil {
			return err
		}
	}
	return nil
}
