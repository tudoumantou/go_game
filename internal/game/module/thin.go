// internal/game/module/thin.go —— 薄模块基座（框架预装配接口样板，业务只写处理器）
//
// 解决什么问题：一个只有增删改查的业务模块（签到/玩家信息/属性占位…），
// 为了满足 module.Module 接口要写 7 个方法，其中大部分是样板：
// Discardable 恒 true、OnSave/OnLogin/OnLogout 空实现、
// CacheFailureReply 里按 msgID  switch 构造错误回包、failure() 再 switch 一次。
// 620 行的签到里，业务其实只占三分之一。
//
// Thin 把这些样板收敛到一处（框架预装配整条接口管线，业务只写处理器）。
// 业务模块嵌入 *Thin 后
// 只剩三件事：声明协议、写处理器、按需挂 Init/Login/Logout 钩子。
//
// 适用边界（重要）：
//   - Thin 默认把「已注册的协议」全部声明为可丢弃（Discardable=true）。
//     前提是处理器的重放没有第二次副作用——只读，或业务状态自带幂等
//     （如签到按「今天已领」事实去重）。不满足的协议不要注册进 Thin，
//     或由外层类型自行实现 DiscardableAware 覆盖嵌入方法。
//   - Thin 不持有任何玩家状态：模块仍是「单例 + Context.State 外置」形态。
//   - 需要 MySQL 直写事务的关系型模块（好友/公会/队伍）不建议用 Thin——
//     它们的复杂度在事务而不在接口样板， Thin 省不下什么。
//
// 与 Define 的分工（两者是同一个内核的两层门面，不是两套体系）：
//
//	Define[D]  状态是「一个结构体」→ 自动管 Ext 编解码（Repo.Get/Save）
//	Thin       状态不在 Ext（独立表 / 外部依赖注入）→ 只提供接口管线
//
// Define 内部就是 Stateful→Thin，所以 Thin 的能力它全有（含 module.Handle）。
// 选择依据只有一个：**要不要 Repo**。要就用 Define，不要就用 Thin；
// 纯只读代理则用 Define[NoState] + Stateless()。
package module

import (
	"context"
	"fmt"

	"game_study/pkg/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
//
// 多数模块不必写它——用 HandleR 声明「请求号 + 响应号 + 空原型」即可，
// 基座会按原型克隆并置 code 字段。ThinRespOf 只留给「一个请求回多种响应」
// 这类超出「一问一答」形态的场景。
type RespFactory func(code int32) proto.Message

// respSpec 一次协议注册的响应声明（HandleR 填入）。
type respSpec struct {
	respID    uint32
	prototype proto.Message
}

// Thin 「薄模块」基座：Module + DiscardableAware + CacheFailureAware 的预装配实现。
//
// 用法：
//
//	m := &Module{}
//	m.Thin = module.NewThin("signin",
//		module.ThinLogger(logger),
//		module.ThinErrCode(codeSystem),
//		module.ThinGuard(RequireCache),
//		module.ThinInit(m.initConfig),
//	)
//	m.HandleR(MsgInfoReq, MsgInfoResp, &gamev1.SigninInfoResp{}, m.onInfo)
//	m.HandleR(MsgClaimReq, MsgClaimResp, &gamev1.SigninClaimResp{}, m.onClaim)
//
// 生命周期钩子都是可选的：不挂 Login/Logout 就是空实现，
// 与手写 Module 时「用不到就 return nil」完全一致。
type Thin struct {
	name    string
	logger  *log.Logger
	errCode int32
	// resp 请求协议 → 响应声明（HandleR 注册时自动填入）。
	//
	// 绝大多数模块用它就够了：错误回包由基座按响应号 + 空原型自动构造，
	// 模块不必再写一个「按 msgID 分支的工厂函数」。
	resp map[uint32]respSpec
	// respOf 自定义映射（ThinRespOf）；设置后优先于 resp。
	//
	// 只留给「同一个请求可能回多种响应」这类少数情况；nil 表示不用。
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
// 优先于 HandleR 的自动推导，只用于「一个请求可能回多种响应」这类
// 一对多形态。普通一问一答用 HandleR 更省事：
//
//	m.HandleR(MsgInfoReq, MsgInfoResp, &gamev1.SigninInfoResp{}, m.onInfo)
//
// 不设置时 Thin 仍按 HandleR 的声明实现 CacheFailureAware；
// 两者都没有时 Registry 探测不到该接口，缓存不可用走网关通用错误通知。
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
	t := &Thin{
		name:     name,
		handlers: make(map[uint32]ThinHandler),
		resp:     make(map[uint32]respSpec),
	}
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

// HandleR 注册处理器，并声明它的响应协议。
//
// 相比 Handle 只多一条信息，省掉的是模块里那段「请求号 → 响应号 + 错误包
// 工厂」的 switch（signin 曾为此写 14 行）：
//
//	m.HandleR(MsgInfoReq, MsgInfoResp, &gamev1.SigninInfoResp{}, m.onInfo)
//
// prototype 必须是**零值原型**（&XxxResp{}）：基座用 proto.Clone 造新实例，
// 再按字段名设置 code。原型带默认值的话错误包会带上那些默认值。
//
// 响应没有 code 字段时仍会回包（只是不带错误码）——这比不回包好：
// 客户端至少知道请求失败了，而不是等到超时。
func (t *Thin) HandleR(reqID, respID uint32, prototype proto.Message, fn ThinHandler) {
	if prototype == nil {
		panic(fmt.Sprintf("module %s: HandleR(%d) nil prototype", t.name, reqID))
	}
	t.Handle(reqID, fn)
	t.resp[reqID] = respSpec{respID: respID, prototype: prototype}
}

// errorResp 按请求号构造错误回包（ThinRespOf 优先，其次 HandleR 声明）。
//
// ok=false 表示模块没声明响应：调用方只记日志、把 error 向上返回。
func (t *Thin) errorResp(msgID uint32, code int32) (uint32, proto.Message, bool) {
	if t.respOf != nil {
		respID, factory, ok := t.respOf(msgID)
		if !ok {
			return 0, nil, false
		}
		return respID, factory(code), true
	}
	spec, ok := t.resp[msgID]
	if !ok {
		return 0, nil, false
	}
	// code<=0 且响应带 code 字段：手里没有可用的业务码（模块没配
	// ThinErrCode）。此时回包会把 code=0 发出去，那正是「成功」——
	// 客户端会据此推进本地状态，而服务端一条都没执行。宁可只记日志、
	// 让上层走通用错误通知，也不回一个假的成功包。
	//
	// 响应本身没有 code 字段时不拦：那类协议本来就不带错误码，
	// 回一个空包至少让客户端触发回调，好过等到超时。
	if code <= 0 && hasCodeField(spec.prototype) {
		return 0, nil, false
	}
	return spec.respID, withCode(spec.prototype, code), true
}

// hasCodeField 响应是否带 int32 的 code 字段（决定零码时能不能回包）。
func hasCodeField(prototype proto.Message) bool {
	fd := prototype.ProtoReflect().Descriptor().Fields().ByName("code")
	return fd != nil && fd.Kind() == protoreflect.Int32Kind
}

// withCode 克隆零值原型并设置 code 字段（字段缺失时原样返回）。
//
// 这里用一次反射换掉每个模块的工厂函数：错误路径不在热路径上
// （一条消息最多一次），而工厂函数是每个模块都要写的样板。
func withCode(prototype proto.Message, code int32) proto.Message {
	out := proto.Clone(prototype)
	m := out.ProtoReflect()
	fd := m.Descriptor().Fields().ByName("code")
	if fd == nil || fd.Kind() != protoreflect.Int32Kind {
		return out
	}
	m.Set(fd, protoreflect.ValueOfInt32(code))
	return out
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

// Discardable 实现 module.DiscardableAware：已注册的协议默认全部可丢弃。
//
// 契约（注册即承诺）：处理器的重放没有第二次副作用——只读，或业务状态
// 自带幂等（「今天已领」这类事实去重）。外部类型可覆盖本方法收紧策略。
func (t *Thin) Discardable(msgID uint32) bool {
	_, ok := t.handlers[msgID]
	return ok
}

// CacheFailureReply 实现 module.CacheFailureAware（未声明响应时 ok=false，
// Registry 探测不到，走网关通用错误通知）。
func (t *Thin) CacheFailureReply(msgID uint32) (uint32, []byte, bool) {
	respID, msg, ok := t.errorResp(msgID, t.errCode)
	if !ok {
		return 0, nil, false
	}
	raw, err := proto.Marshal(msg)
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

// Fail 记录告警并按 code 回失败响应（未声明响应时只记日志、向上返回错误）。
//
// 日志带 player_id/msg_id：失败响应对客户端只是一个错误码，
// 服务端必须能从日志反查是哪次请求、什么原因。
func (t *Thin) Fail(ctx *Context, msgID uint32, code int32, err error) error {
	t.Warn(ctx, msgID, err)
	respID, msg, ok := t.errorResp(msgID, code)
	if !ok {
		return err
	}
	return t.Reply(ctx, respID, msg)
}

// Warn 记录一次处理失败（module/player/msg 三元组，便于反查是哪次请求）。
func (t *Thin) Warn(ctx *Context, msgID uint32, err error) {
	if t.logger != nil && ctx != nil && ctx.State != nil {
		t.logger.Warn("module request failed",
			"module", t.name, "player_id", ctx.State.PlayerID(), "msg_id", msgID, "err", err)
	}
}

// SetGuard 设置处理器前置校验（Define 的链式 API 使用）。
func (t *Thin) SetGuard(fn func(ctx *Context, msgID uint32) error) { t.guard = fn }

// SetInit 设置 Init 钩子（Define 的链式 API 使用）。
func (t *Thin) SetInit(fn func(ctx *Context) error) { t.initFn = fn }

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
