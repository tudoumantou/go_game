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
// Reply/Fail 被重载为 proto 版本，业务不再自己 Marshal。
type Ctx[D any] struct {
	*Context
	Repo *Repo[D]

	bound *Bound[D]
}

// Reply 序列化并回包（业务只面对 proto.Message）。
func (c *Ctx[D]) Reply(msgID uint32, msg proto.Message) error {
	return c.bound.Stateful.Reply(c.Context, msgID, msg)
}

// Fail 记录告警并按错误码回失败响应。
func (c *Ctx[D]) Fail(msgID uint32, code int32, err error) error {
	return c.bound.Stateful.Fail(c.Context, msgID, code, err)
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

// On 注册一个请求：respID + prototype 用于失败时自动回包。
func (b *Bound[D]) On(reqID, respID uint32, prototype proto.Message, h Handler[D]) {
	b.HandleR(reqID, respID, prototype, func(ctx *Context, _ uint32, _ []byte) error {
		if b.clock != nil && ctx.Clock == nil {
			ctx.Clock = b.clock
		}
		return h(b.NewCtx(ctx))
	})
}

// NewCtx 用给定上下文构造业务上下文（测试可直接调用处理器）。
func (b *Bound[D]) NewCtx(ctx *Context) *Ctx[D] {
	return &Ctx[D]{
		Context: ctx,
		Repo:    &Repo[D]{s: b.Stateful, ctx: ctx},
		bound:   b,
	}
}
