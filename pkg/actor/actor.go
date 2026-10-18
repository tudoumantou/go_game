// pkg/actor/actor.go —— Actor 抽象
package actor

// PID Actor 进程标识符。
type PID struct {
	ID    string
	Shard int
}

// String 返回 PID 字符串表示。
func (p *PID) String() string { return p.ID }

// Actor 是参与者的行为接口。
type Actor interface {
	// Receive 处理消息；禁止在 Receive 中阻塞。
	Receive(ctx *Context)
}

// Behavior Actor 行为函数。
type Behavior func(ctx *Context)

// actorCell 是 Actor 的运行时封装。
type actorCell struct {
	pid    *PID
	actor  Actor
	mbox   *mailbox
	system *System
}

func newActorCell(pid *PID, actor Actor, mboxSize int, system *System) *actorCell {
	return &actorCell{
		pid:    pid,
		actor:  actor,
		mbox:   newMailbox(mboxSize),
		system: system,
	}
}

type envelope struct {
	sender  *PID
	message any
}
