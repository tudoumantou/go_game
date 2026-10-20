// pkg/actor/context.go —— Actor 处理上下文
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
func (c *Context) PID() *PID { return c.self }

// Sender 返回发送者 PID（可能为 nil）。
func (c *Context) Sender() *PID { return c.sender }

// Message 返回当前消息。
func (c *Context) Message() any { return c.message }

// System 返回 ActorSystem。
func (c *Context) System() *System { return c.system }

// Context 返回底层 context.Context。
//
// 当前实现固定为 context.Background()：Actor 消息处理不随单条外部请求
// 取消（取消整条链路会牵连同 Actor 的其他消息）。需要超时的场景应在
// Actor 内部自建带 deadline 的 ctx。
func (c *Context) Context() context.Context { return c.ctx }

// Send 向目标 PID 发送消息。
//
// 错误语义与 System.Send 一致：信箱满返回 ErrMailboxFull（不挤掉旧
// 消息），目标已终止返回 ErrActorGone；调用方自行决定重试/拒绝/降级。
func (c *Context) Send(target *PID, msg any) error {
	return c.system.Send(target, msg)
}

// Respond 向发送者回复消息。
//
// sender 为 nil（系统/定时器消息）时静默返回 nil，不报错——
// 没有回复目标不是调用方错误；其余语义与 Send 相同。
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
func (c *Context) ScheduleOnce(d time.Duration, target *PID, msg any) {
	c.system.ScheduleOnce(d, target, msg)
}
