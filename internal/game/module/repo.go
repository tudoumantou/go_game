// internal/game/module/repo.go —— 模块状态的 CRUD 门面（业务只写逻辑）
//
// 目标形态：业务处理器不再接触 Ext/JSON/标脏，只做三件事——
//
//	d, err := c.Repo.Get()      // 取状态
//	...改 d...                   // 业务规则
//	return c.Repo.Save(d)       // 存回去（基座负责编码与标脏）
//
// 与 Read/Mutate 闭包的关系：两者等价。闭包省一次显式 Save，但要求业务
// 理解「返回 true 才写回」；Repo 版本更直白，适合「取出来 → 校验 → 改 → 存」
// 这种线性流程。Choose one, 不要混用。
package module

import (
	"time"

	"game_study/internal/common/errcode"
	"game_study/pkg/codec"
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
// 嵌入 *Context 后，State/Send/Prefetch/Now 等能力仍可直接用；
// Reply/Fail 只收 proto 消息与错误码，协议号由基座按类型反查。
type Ctx[D any] struct {
	*Context
	Repo *Repo[D]

	msgID uint32 // 当前请求号（Fail 按它找失败回包模板）
	bound *Bound[D]
}

// MsgID 当前请求的协议号。
func (c *Ctx[D]) MsgID() uint32 { return c.msgID }

// Reply 回包：协议号由消息类型反查（codec.ProtoID），业务不写数字。
func (c *Ctx[D]) Reply(msg proto.Message) error {
	return c.bound.Stateful.Reply(c.Context, codec.ProtoID(msg), msg)
}

// Fail 记录告警并按错误码回失败响应（响应模板由注册时的 respID 决定）。
func (c *Ctx[D]) Fail(code errcode.Code, err error) error {
	return c.bound.Stateful.Fail(c.Context, c.msgID, int32(code), err)
}

// Handler 业务处理器：只关心上下文，不看原始字节。
type Handler[D any] func(c *Ctx[D]) error

// Bound 由 Bind 产出的模块：内嵌 Stateful，Module 接口（Name/MsgIDs/分发/
// 生命周期/可丢弃声明）仍由 Thin 预装配，业务只写 Handler。
type Bound[D any] struct {
	*Stateful[D]
	clock func() time.Time
}

// Bind 创建模块。newFn 给出新玩家的空状态（可为 nil，用零值）。
func Bind[D any](name string, newFn func() *D, opts ...ThinOption) *Bound[D] {
	return &Bound[D]{Stateful: NewStateful(name, newFn, opts...)}
}

// SetClock 注入模块级时钟（测试专用；生产用系统时间）。
//
// 注入后每个请求的 Context.Clock 为空时自动带上，业务用 c.Now() 取时间。
func (b *Bound[D]) SetClock(fn func() time.Time) { b.clock = fn }

// On 按协议号注册处理器：respID + prototype 用于失败时自动回包。
func (b *Bound[D]) On(reqID, respID uint32, prototype proto.Message, h Handler[D]) {
	b.HandleR(reqID, respID, prototype, func(ctx *Context, msgID uint32, _ []byte) error {
		if b.clock != nil && ctx.Clock == nil {
			ctx.Clock = b.clock
		}
		return h(b.newCtx(ctx, msgID))
	})
}

// OnMsg 按消息类型注册处理器：协议号从 proto 的 @msg 注解反查。
//
//	m.OnMsg(&gamev1.SigninInfoReq{}, &gamev1.SigninInfoResp{}, onInfo)
//
// 相比 On 不会写错号，也不会出现「请求/响应对不上」的拼写错误。
func (b *Bound[D]) OnMsg(req, resp proto.Message, h Handler[D]) {
	b.On(codec.ProtoID(req), codec.ProtoID(resp), resp, h)
}

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
