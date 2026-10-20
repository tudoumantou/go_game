// pkg/actor/context.go —— Actor 处理消息时的调用上下文
//
// 架构职责：把「当前处理的是谁的消息、来自谁、如何回复/再调度」打包
// 成一个对象传给 Receive，是 Actor 与外界交互的唯一入口。
// 协作关系：由 system.go 的 shard.process 在每条消息处理前新建；
// Send/Respond 转调 System.SendWithSender；ScheduleOnce 转调 System
// 的定时器（timerLoop）。
// 关键取舍：Context 每消息新建、不跨消息复用——消息间无隐式共享
// 状态，从根本上杜绝「上一条消息的 sender 污染下一条」类 bug。
package actor

import (
	"context"
	"time"
)

// Context 是 Actor 处理消息时的上下文。
//
// 每次 Receive 调用都会新建一个 Context（见 shard.process），只在当次
// 处理内有效：不要跨消息持有，也不要泄露给其他 goroutine。
// sender 为 nil 表示消息来自系统/定时器（无回复目标）。
type Context struct {
	// ctx 底层 context；当前固定为 context.Background()，见 Context() 注释。
	ctx     context.Context
	system  *System
	self    *PID
	sender  *PID
	message any
}

// PID 返回自身 PID。
//
// 零成本访问器：直接返回内部指针，调用方只读（改它等于伪造身份）。
func (c *Context) PID() *PID { return c.self }

// Sender 返回发送者 PID（可能为 nil）。
//
// nil 的两种来源：匿名发送（System.Send 不传 sender）与定时器/系统
// 消息。Respond 对 nil 静默跳过，业务可直接 Respond 无需判空；
// 但若业务逻辑要「仅当有发送者才回复」，仍需自行判空。
func (c *Context) Sender() *PID { return c.sender }

// Message 返回当前消息。
//
// 静态类型 any，业务按消息类型做 comma-ok 断言；框架不做类型检查
// 或路由——路由由外层 MsgID 完成，actor 信箱里只会收到发给它的消息。
func (c *Context) Message() any { return c.message }

// System 返回 ActorSystem。
//
// 用于在 Receive 内 Spawn 新 Actor（如给玩家拉起子 Actor）、或直接
// Send 给任意 PID（不限于回复 sender）。
func (c *Context) System() *System { return c.system }

// Context 返回底层 context.Context。
//
// 当前实现固定为 context.Background()：Actor 消息处理不随单条外部请求
// 取消（取消整条链路会牵连同 Actor 的其他消息）。需要超时的场景应在
// Actor 内部自建带 deadline 的 ctx。
// 设计原因：context 取消沿 goroutine 调用树向下传播，而 Actor 处理跑
// 在分片 goroutine 上、与请求方不在同一调用树，外部 cancel 传不进来；
// 强行传入反而会误伤同信箱排队的其他消息。这是「共享事件循环」模型
// 的必然取舍，不是遗漏。
func (c *Context) Context() context.Context { return c.ctx }

// Send 向目标 PID 发送消息。
//
// 错误语义与 System.Send 一致：信箱满返回 ErrMailboxFull（不挤掉旧
// 消息），目标已终止返回 ErrActorGone；调用方自行决定重试/拒绝/降级。
// Go 知识点：错误经 %w 包装，判定类别必须用 errors.Is，直接 == 比较
// 会漏掉包装层。
func (c *Context) Send(target *PID, msg any) error {
	return c.system.Send(target, msg)
}

// Respond 向发送者回复消息。
//
// sender 为 nil（系统/定时器消息）时静默返回 nil，不报错——
// 没有回复目标不是调用方错误；其余语义与 Send 相同。
// Go 知识点：sender 字段是类型化 *PID，c.sender == nil 判的是「指针
// 为 nil」；若哪天改成接口字段，就要小心 nil interface 陷阱（接口
// 持有具体类型与 nil 指针时 != nil 仍成立）。
func (c *Context) Respond(msg any) error {
	if c.sender == nil {
		return nil
	}
	return c.system.Send(c.sender, msg)
}

// ScheduleOnce 调度一次性延迟消息。
//
// 到期由 timerLoop 投递；信箱满时自动重试至多 timerRetryLimit 次，
// 仍失败只记日志丢弃——调用方无法感知失败，关键消息需自行确认送达。
// 计时精度为 timerLoop 的 tick（100ms），不要用于精确计时场景。
// 适用场景：结算缓冲、延迟顶号、冷却广播等秒级容忍的定时动作。
func (c *Context) ScheduleOnce(d time.Duration, target *PID, msg any) {
	c.system.ScheduleOnce(d, target, msg)
}
