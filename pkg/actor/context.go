// pkg/actor/context.go —— Actor 处理上下文
package actor

import (
	"context"
	"time"
)

// Context 是 Actor 处理消息时的上下文。
type Context struct {
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
func (c *Context) Context() context.Context { return c.ctx }

// Send 向目标 PID 发送消息。
func (c *Context) Send(target *PID, msg any) error {
	return c.system.Send(target, msg)
}

// Respond 向发送者回复消息。
func (c *Context) Respond(msg any) error {
	if c.sender == nil {
		return nil
	}
	return c.system.Send(c.sender, msg)
}

// ScheduleOnce 调度一次性延迟消息。
func (c *Context) ScheduleOnce(d time.Duration, target *PID, msg any) {
	c.system.ScheduleOnce(d, target, msg)
}
