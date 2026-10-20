// pkg/actor/actor.go —— Actor 抽象
package actor

// PID Actor 进程标识符。
//
// ID 是全局唯一名（如 "player:123"），同时作为分片路由键；
// Shard 是该 Actor 固定落到的分片序号——投递与终止都按 ID 哈希
// 路由到同一分片，保证同一 Actor 的消息在单 goroutine 上串行执行。
type PID struct {
	ID    string
	Shard int
}

// String 返回 PID 字符串表示。
func (p *PID) String() string { return p.ID }

// Actor 是参与者的行为接口。
//
// 并发约定：同一 Actor 的所有 Receive 调用都在其所属分片的单 goroutine
// 上串行发生，实现无需内部加锁；不同 Actor 之间才可能并发。
type Actor interface {
	// Receive 处理消息；禁止在 Receive 中阻塞。
	Receive(ctx *Context)
}

// Behavior Actor 行为函数。
//
// 与 Actor 接口等价的行为表示，便于用闭包定义简单参与者；
// 协约与 Actor.Receive 完全相同（不阻塞、单线程串行调用）。
type Behavior func(ctx *Context)

// actorCell 是 Actor 的运行时封装。
//
// pid/actor 由 Spawn 给定；mbox 与 system 在 newActorCell 里装配。
// cell 只归属一个分片，其 Receive 永远在该分片 goroutine 上被调用。
type actorCell struct {
	pid    *PID
	actor  Actor
	mbox   *mailbox
	system *System
}

// newActorCell 装配 Actor 运行时单元。
//
// mboxSize <= 0 时信箱退回默认 1024（兜底逻辑在 newMailbox；System
// 创建时通常已由 Config.MailboxSize 填好，不会走到默认值）。
func newActorCell(pid *PID, actor Actor, mboxSize int, system *System) *actorCell {
	return &actorCell{
		pid:    pid,
		actor:  actor,
		mbox:   newMailbox(mboxSize),
		system: system,
	}
}

// envelope 信箱中流转的一条消息。
//
// sender 为 nil 表示无发送者（定时器消息、系统消息），Context.Respond
// 会安全跳过；message 不做类型约束，由各 Actor 自行类型断言。
type envelope struct {
	sender  *PID
	message any
}
