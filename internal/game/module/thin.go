// internal/game/module/thin.go —— 薄模块基座（参考 框架 AutoCacheBuilder 的「预装配」思路）
//
// 解决什么问题：一个只有增删改查的业务模块（签到/玩家信息/属性占位…），
// 为了满足 module.Module 接口要写 7 个方法，其中大部分是样板：
// Rollbackable 恒 true、OnSave/OnLogin/OnLogout 空实现、
// CacheFailureReply 里按 msgID  switch 构造错误回包、failure() 再 switch 一次。
// 620 行的签到里，业务其实只占三分之一。
//
// Thin 把这些样板收敛到一处（仿 框架的泛型 AutoCacheBuilder：框架预装配
// 三级缓存管线，业务只配置 Key/Val 与读写函数）。业务模块嵌入 *Thin 后
// 只剩三件事：声明协议、写处理器、按需挂 Init/Login/Logout 钩子。
//
// 适用边界（重要）：
//   - Thin 默认把「已注册的协议」全部声明为可回滚（Rollbackable=true）。
//     前提是处理器的重放没有第二次副作用——只读，或业务状态自带幂等
//     （如签到按「今天已领」事实去重）。不满足的协议不要注册进 Thin，
//     或由外层类型自行实现 RollbackAware 覆盖嵌入方法。
//   - Thin 不持有任何玩家状态：模块仍是「单例 + Context.State 外置」形态。
//   - 需要 MySQL 直写事务的关系型模块（好友/公会/队伍）不建议用 Thin——
//     它们的复杂度在事务而不在接口样板， Thin 省不下什么。
package module

import (
	"context"
	"fmt"

	"game_study/pkg/log"
	"google.golang.org/protobuf/proto"
)

// ThinHandler 单协议的业务处理函数（模块只写这一层）。
//
// body 是原始协议字节：空消息体（如签到请求）可直接忽略；
// 需要字段的处理器自行 proto.Unmarshal——框架不替业务猜消息类型。
type ThinHandler func(ctx *Context, msgID uint32, body []byte) error

// RespFactory 按业务错误码构造响应消息。
//
// 用工厂而不是反射置 Code 字段：proto.Message 没有公共的 Code setter，
// 每个模块的响应类型又是不同的具体类型，工厂函数让模块自己决定
// 「哪个协议号、哪个错误码字段」的映射，框架保持零反射。
type RespFactory func(code int32) proto.Message

// Thin 「薄模块」基座：Module + RollbackAware + CacheFailureAware 的预装配实现。
//
// 用法：
//
//	m := &Module{}
//	m.Thin = module.NewThin("signin",
//		module.ThinLogger(logger),
//		module.ThinErrCode(codeSystem),
//		module.ThinRespOf(respOf),
//		module.ThinGuard(RequireCache),
//		module.ThinInit(m.initConfig),
//	)
//	m.Handle(MsgInfoReq, m.onInfo)   // 注册协议（Handle 由嵌入提升）
//	m.Handle(MsgClaimReq, m.onClaim)
//
// 生命周期钩子都是可选的：不挂 Login/Logout 就是空实现，
// 与手写 Module 时「用不到就 return nil」完全一致。
type Thin struct {
	name    string
	logger  *log.Logger
	errCode int32
	// respOf 请求协议 → （响应协议, 错误响应工厂）；nil 表示不实现
	// CacheFailureAware（走网关通用错误通知）。
	respOf func(msgID uint32) (uint32, RespFactory, bool)
	// guard 处理器前置校验（如「缓存权威路径必须就绪」）；
	// 返回 error 时按 errCode 回失败响应，不进入处理器。
	guard func(ctx *Context, msgID uint32) error

	initFn   func(ctx *Context) error
	loginFn  func(ctx *Context) error
	logoutFn func(ctx *Context) error
	// eventFn 事件消费钩子（nil 表示本模块不订阅事件）。
	eventFn func(ctx *Context, evt Event)

	handlers map[uint32]ThinHandler
	// ids 注册顺序的协议号（MsgIDs 返回稳定顺序，便于测试与日志）。
	ids []uint32
}

// ThinOption 薄模块配置项。
type ThinOption func(*Thin)

// ThinLogger 注入日志器（失败路径记告警）。
func ThinLogger(l *log.Logger) ThinOption { return func(t *Thin) { t.logger = l } }

// ThinErrCode 设置模块级系统错误码（Fail/缓存不可用响应的默认码）。
func ThinErrCode(code int32) ThinOption { return func(t *Thin) { t.errCode = code } }

// ThinRespOf 设置「请求协议 → 响应协议 + 错误响应工厂」映射。
//
// 不设置时 Thin 不实现 CacheFailureAware：Registry 探测不到该接口，
// 缓存不可用走网关通用错误通知——与不写该方法的模块行为一致。
func ThinRespOf(fn func(msgID uint32) (uint32, RespFactory, bool)) ThinOption {
	return func(t *Thin) { t.respOf = fn }
}

// ThinGuard 设置处理器前置校验（在分发前执行，失败即回错误码）。
//
// 典型用途：缓存权威模块声明「Redis 未就绪一律拒绝」
// （见 RequireCache）。Guard 返回的 error 会记入告警日志。
func ThinGuard(fn func(ctx *Context, msgID uint32) error) ThinOption {
	return func(t *Thin) { t.guard = fn }
}

// ThinInit 设置启动期初始化（配置校验等）。
func ThinInit(fn func(ctx *Context) error) ThinOption { return func(t *Thin) { t.initFn = fn } }

// ThinLogin 设置玩家上线钩子（登录礼包这类「OnLogin 侧写模块」用）。
func ThinLogin(fn func(ctx *Context) error) ThinOption { return func(t *Thin) { t.loginFn = fn } }

// ThinLogout 设置玩家下线钩子（默认空实现）。
func ThinLogout(fn func(ctx *Context) error) ThinOption { return func(t *Thin) { t.logoutFn = fn } }

// ThinOnEvent 设置事件消费钩子（实现 EventAware，任务/活动这类进度模块用）。
//
// 事件按 evt.Type 分发由模块自己做：框架不维护「事件类型 → 处理器」的注册表，
// 因为一个模块通常只关心两三个类型，一个 switch 比再引入一层间接更直白。
//
// 取舍：Thin 无条件实现 OnEvent（未挂钩子时为空），因此所有 Thin 模块都会
// 进入 Registry.DispatchEvent 的遍历。代价是每个事件多十几次空函数调用
// （几十纳秒），换来的是「模块无需为不订阅事件写任何代码」。
func ThinOnEvent(fn func(ctx *Context, evt Event)) ThinOption {
	return func(t *Thin) { t.eventFn = fn }
}

// ThinHandle 注册协议处理器（构造期选项形式）。
//
// 与实例方法 Handle 等价：选项形式适合「模块结构体先建好、
// 处理器是包级函数」的场景；处理器要引用业务模块方法时
// （m.onXxx）用 New 返回后的 Handle，否则拿不到 m。
func ThinHandle(msgID uint32, fn ThinHandler) ThinOption {
	return func(t *Thin) { t.Handle(msgID, fn) }
}

// NewThin 创建薄模块基座。
func NewThin(name string, opts ...ThinOption) *Thin {
	t := &Thin{name: name, handlers: make(map[uint32]ThinHandler)}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Handle 注册一个协议处理器（New 返回后调用，可引用业务模块的方法）。
//
// 重复注册同一协议直接 panic：这是编码错误，启动期暴露比线上静默
// 覆盖好——后注册的处理器会悄悄顶掉先注册的，排错成本极高。
func (t *Thin) Handle(msgID uint32, fn ThinHandler) {
	if _, dup := t.handlers[msgID]; dup {
		panic(fmt.Sprintf("module %s: msgID %d registered twice", t.name, msgID))
	}
	t.handlers[msgID] = fn
	t.ids = append(t.ids, msgID)
}

// Name 实现 module.Module。
func (t *Thin) Name() string { return t.name }

// MsgIDs 实现 module.Module（返回注册顺序，结果稳定）。
func (t *Thin) MsgIDs() []uint32 {
	out := make([]uint32, len(t.ids))
	copy(out, t.ids)
	return out
}

// Init 实现 module.Module（未挂钩子时无操作）。
func (t *Thin) Init(ctx *Context) error {
	if t.initFn == nil {
		return nil
	}
	return t.initFn(ctx)
}

// OnMessage 实现 module.Module：guard → 分发。
//
// 未注册的协议号返回错误而不是静默丢弃：Registry 只按 MsgIDs 路由，
// 走到这里说明路由表配错了，错误要让它在测试/监控里暴露。
func (t *Thin) OnMessage(ctx *Context, msgID uint32, body []byte) error {
	if t.guard != nil {
		if err := t.guard(ctx, msgID); err != nil {
			return t.Fail(ctx, msgID, t.errCode, err)
		}
	}
	fn, ok := t.handlers[msgID]
	if !ok {
		return fmt.Errorf("module %s: unexpected message %d", t.name, msgID)
	}
	return fn(ctx, msgID, body)
}

// OnSave 实现 module.Module：薄模块的私有状态一律走 Ext 槽位
// （随玩家快照 CAS 提交），没有模块级聚合数据需要在这里落库。
func (*Thin) OnSave(context.Context) error { return nil }

// OnLogin 实现 module.Module（未挂钩子时无操作）。
func (t *Thin) OnLogin(ctx *Context) error {
	if t.loginFn == nil {
		return nil
	}
	return t.loginFn(ctx)
}

// OnLogout 实现 module.Module（未挂钩子时无操作）。
func (t *Thin) OnLogout(ctx *Context) error {
	if t.logoutFn == nil {
		return nil
	}
	return t.logoutFn(ctx)
}

// OnEvent 实现 module.EventAware（未挂 ThinOnEvent 时为空操作）。
func (t *Thin) OnEvent(ctx *Context, evt Event) {
	if t.eventFn == nil {
		return
	}
	t.eventFn(ctx, evt)
}

// Rollbackable 实现 module.RollbackAware：已注册的协议默认全部可回滚。
//
// 契约（注册即承诺）：处理器的重放没有第二次副作用——只读，或业务状态
// 自带幂等（「今天已领」这类事实去重）。外部类型可覆盖本方法收紧策略。
func (t *Thin) Rollbackable(msgID uint32) bool {
	_, ok := t.handlers[msgID]
	return ok
}

// CacheFailureReply 实现 module.CacheFailureAware（respOf 未设置时 ok=false，
// Registry 探测不到，走网关通用错误通知）。
func (t *Thin) CacheFailureReply(msgID uint32) (uint32, []byte, bool) {
	if t.respOf == nil {
		return 0, nil, false
	}
	respID, factory, ok := t.respOf(msgID)
	if !ok {
		return 0, nil, false
	}
	raw, err := proto.Marshal(factory(t.errCode))
	if err != nil {
		return 0, nil, false
	}
	return respID, raw, true
}

// Reply 序列化并经 ctx.Reply 下发（Send 未注入时静默丢弃，便于单测）。
func (t *Thin) Reply(ctx *Context, respID uint32, msg proto.Message) error {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(respID, raw)
}

// Fail 记录告警并按 errCode 回失败响应（respOf 缺失时只记日志、向上返回错误）。
//
// 日志带 player_id/msg_id：失败响应对客户端只是一个错误码，
// 服务端必须能从日志反查是哪次请求、什么原因。
func (t *Thin) Fail(ctx *Context, msgID uint32, code int32, err error) error {
	if t.logger != nil && ctx != nil && ctx.State != nil {
		t.logger.Warn("module request failed",
			"module", t.name, "player_id", ctx.State.PlayerID(), "msg_id", msgID, "err", err)
	}
	if t.respOf == nil {
		return err
	}
	respID, factory, ok := t.respOf(msgID)
	if !ok {
		return err
	}
	return t.Reply(ctx, respID, factory(code))
}

// RequireCache 缓存权威前置校验：State 就绪且 Redis 权威路径已接入。
//
// 语义是「不可用则拒绝」而不是降级直写 MySQL：直写会绕过快照 CAS，
// 与缓存里的旧状态形成双写分叉。宁可回系统错误码，不可双写
// （与 signin 模块原有的注释口径一致）。
func RequireCache(ctx *Context, _ uint32) error {
	if ctx.State == nil || !ctx.CacheEnabled {
		return fmt.Errorf("cache-authoritative state unavailable")
	}
	return nil
}
