// internal/game/module/repo.go —— 模块声明、路由与状态门面
//
// 目标形态：模块只有「声明一行 + 路由若干行 + 业务函数」，
// 装配（日志/状态编解码/分发/生命周期）全部由基座完成。
//
//	var M = module.Define[Data]("signin").Guard(module.RequireCache)
//
//	func init() {
//		M.HandleRoute(&gamev1.SigninInfoReq{}, onInfo)
//		M.HandleRoute(&gamev1.SigninClaimReq{}, onClaim)
//	}
//
//	func onInfo(c *module.Ctx[Data]) error {
//		d, err := c.Repo.Get()          // 取状态
//		if err != nil { return c.Fail(&gamev1.SigninInfoResp{Code: int32(errcode.ErrSystem)}, err) }
//		return c.Reply(&gamev1.SigninInfoResp{...})   // 回什么由协议自己决定
//	}
//
// 注册时不声明响应协议：处理器可以回任意消息（或不回）。
package module

import (
	"fmt"
	"time"

	"game_study/pkg/codec"
	"game_study/pkg/log"
	"google.golang.org/protobuf/proto"
)

// Repo 是模块状态的读写门面。
//
//	Get    解码当前玩家的模块状态（首次访问返回空状态，不落库）
//	Save   编码回 Ext 槽位并标脏（由 flusher 异步落库）
//	Delete 清空槽位（下次 Get 回到空状态）
type Repo[D any] struct {
	s   *Stateful[D]
	ctx *Context
}

// Get 读取当前玩家的模块状态。
func (r *Repo[D]) Get() (*D, error) { return r.s.decode(r.ctx) }

// Save 写回模块状态（编码 + 标脏）。
func (r *Repo[D]) Save(d *D) error { return r.s.encode(r.ctx, d) }

// Delete 清空模块状态。
func (r *Repo[D]) Delete() error {
	r.ctx.State.SetExt(r.s.Name(), nil)
	return nil
}

// Ctx 业务处理器上下文：在 module.Context 之上挂类型化的 Repo。
//
// 嵌入 *Context 后，State/Send/Now 等能力仍可直接用；
// Reply 只收 proto 消息，协议号由基座按类型反查。
type Ctx[D any] struct {
	*Context
	Repo *Repo[D]

	msgID uint32 // 当前请求号（Fail 记录日志用）
	bound *Bound[D]
}

// MsgID 当前请求的协议号。
func (c *Ctx[D]) MsgID() uint32 { return c.msgID }

// Log 模块日志器（已带模块名字段）。
//
// 为什么从这里取而不是业务自己引用包级 M：模块的钩子（比如 WithEvent）
// 在包级变量初始化时就被 M 引用，钩子里再引用 M 会构成初始化循环，
// 编译期直接报错。
func (c *Ctx[D]) Log() *log.Logger { return c.bound.Logger() }

// Reply 回包：协议号由消息类型反查（codec.ProtoID），业务不写数字。
func (c *Ctx[D]) Reply(msg proto.Message) error {
	return c.bound.Stateful.Reply(c.Context, codec.ProtoID(msg), msg)
}

// Fail 记录失败并回 msg（错误码已经写在 msg 里）。
//
// 不回包失败响应是故意的：一个请求要回什么、回不回，由协议自己决定
// （推送/单向命令本来就没有响应）。需要回错误时把 Code 写进消息再传进来。
func (c *Ctx[D]) Fail(msg proto.Message, err error) error {
	c.bound.Warn(c.Context, c.msgID, err)
	return c.Reply(msg)
}

// Handler 业务处理器：只关心上下文，不看原始字节。
type Handler[D any] func(c *Ctx[D]) error

// Bound 由 Define 产出的模块：内嵌 Stateful，Module 接口（Name/MsgIDs/分发/
// 生命周期/可丢弃声明）仍由 Thin 预装配，业务只写 Handler。
type Bound[D any] struct {
	*Stateful[D]
	clock func() time.Time
}

// Define 声明模块：一行搞定名字、日志与状态解码。
//
// newFn 可选：给新玩家一个非零初始状态（如空切片而非 nil）；
// 不传则用 T 的零值。
func Define[D any](name string, newFn ...func() *D) *Bound[D] {
	var init func() *D
	if len(newFn) > 0 {
		init = newFn[0]
	}
	return &Bound[D]{Stateful: NewStateful(name, init, ThinLogger(log.New(name)))}
}

// Guard 设置处理器前置校验（如「缓存权威路径必须就绪」）。
func (b *Bound[D]) Guard(fn func(ctx *Context, msgID uint32) error) *Bound[D] {
	b.SetGuard(fn)
	return b
}

// OnInit 设置启动期初始化/配置校验钩子。
func (b *Bound[D]) OnInit(fn func(ctx *Context) error) *Bound[D] {
	b.SetInit(fn)
	return b
}

// SetClock 注入模块级时钟（测试专用；生产用系统时间）。
func (b *Bound[D]) SetClock(fn func() time.Time) *Bound[D] {
	b.clock = fn
	return b
}

// HandleRoute 注册一条路由：协议号由消息类型反查，回什么由处理器决定。
//
// 与旧 HandleR 的区别：不预先绑定响应协议，因此不限制「一问一答」——
// 处理器可以回任意消息、回多条，或者不回。
//
// 适用无参协议：请求的语义完全由「谁在发 + 服务端状态」决定（查询、
// 领取今天、上线下线这类）。需要请求参数的协议用 Route。
func (b *Bound[D]) HandleRoute(req proto.Message, h Handler[D]) {
	b.Handle(codec.ProtoID(req), func(ctx *Context, msgID uint32, _ []byte) error {
		if b.clock != nil && ctx.Clock == nil {
			ctx.Clock = b.clock
		}
		return h(b.newCtx(ctx, msgID))
	})
}

// Route 注册带请求体的路由：解包交给基座，业务只写「拿到 req 之后做什么」。
//
// 为什么是包级泛型函数而不是 Bound 的方法：Go 的方法不能有自己的类型参数，
// 「响应/请求类型随协议而变」只能靠包级函数推断（D 来自入参 b，R 来自 req）。
//
// 每次请求都新建一份消息实例再解包——直接解到 req 上会污染注册时传入的
// 那份，并发请求共用同一个对象即是数据竞争。
func Route[D any, R proto.Message](b *Bound[D], req R, h func(*Ctx[D], R) error) {
	b.Handle(codec.ProtoID(req), func(ctx *Context, msgID uint32, body []byte) error {
		if b.clock != nil && ctx.Clock == nil {
			ctx.Clock = b.clock
		}
		msg, ok := req.ProtoReflect().New().Interface().(R)
		if !ok {
			return fmt.Errorf("module %s: request type mismatch", b.Name())
		}
		if err := proto.Unmarshal(body, msg); err != nil {
			return fmt.Errorf("module %s: decode request: %w", b.Name(), err)
		}
		return h(b.newCtx(ctx, msgID), msg)
	})
}

// WithLogin 设置玩家上线钩子（加成重算这类「登录时补一刀」的模块用）。
//
// 名字不带 On 前缀：OnLogin 已被 Module 接口占用（生命周期入口），
// 这里是「注册钩子」而不是「实现接口方法」。
func (b *Bound[D]) WithLogin(fn func(*Ctx[D]) error) *Bound[D] {
	b.loginFn = func(ctx *Context) error { return fn(b.newCtx(ctx, 0)) }
	return b
}

// WithLogout 设置玩家下线钩子。
func (b *Bound[D]) WithLogout(fn func(*Ctx[D]) error) *Bound[D] {
	b.logoutFn = func(ctx *Context) error { return fn(b.newCtx(ctx, 0)) }
	return b
}

// WithEvent 设置事件消费钩子（任务/活动这类进度模块用）。
func (b *Bound[D]) WithEvent(fn func(*Ctx[D], Event)) *Bound[D] {
	b.eventFn = func(ctx *Context, evt Event) { fn(b.newCtx(ctx, 0), evt) }
	return b
}

// Logger 模块日志器（名字已作为固定字段带上，业务不必再传 logger）。
func (b *Bound[D]) Logger() *log.Logger { return b.logger }

// NewCtx 用给定上下文构造业务上下文（测试可直接调用处理器）。
func (b *Bound[D]) NewCtx(ctx *Context) *Ctx[D] { return b.newCtx(ctx, 0) }

func (b *Bound[D]) newCtx(ctx *Context, msgID uint32) *Ctx[D] {
	return &Ctx[D]{
		Context: ctx,
		Repo:    &Repo[D]{s: b.Stateful, ctx: ctx},
		msgID:   msgID,
		bound:   b,
	}
}

// 编译期断言：Bound 必须满足 Module 接口。
var _ Module = (*Bound[struct{}])(nil)
