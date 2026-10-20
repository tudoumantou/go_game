// pkg/actor/actor.go —— Actor 模型核心抽象
//
// 架构职责：定义「谁是一个 Actor」及其运行时装配（PID / Actor / Behavior /
// actorCell / envelope），是 pkg/actor 被业务直接接触的面——Spawn 一个
// Actor、拿到 PID，之后只跟 PID 打交道。
// 协作关系：
//   - mailbox.go：actorCell.mbox 持有的有界信箱，消息的实际缓冲区；
//   - system.go：System 把 cell 编入分片、驱动 goroutine 事件循环；
//   - context.go：Receive 调用时创建的上下文，携带 sender/message。
//
// 关键取舍：消息类型用 any（interface{} 的别名，Go 1.18+）而非泛型约束
// ——框架零依赖业务 proto，编译期解耦；类型安全交给各 Actor 自己做
// 断言（配合 comma-ok 模式防 panic）。
package actor

// PID Actor 进程标识符。
//
// ID 是全局唯一名（如 "player:123"），同时作为分片路由键；
// Shard 是该 Actor 固定落到的分片序号——投递与终止都按 ID 哈希
// 路由到同一分片，保证同一 Actor 的消息在单 goroutine 上串行执行。
// 全链路以 *PID 传递（Spawn 返回、Send/Terminate 接收）：指针语义
// 让「同一个 Actor」在各处是同一份标识，不会因值拷贝产生歧义。
type PID struct {
	ID    string
	Shard int
}

// String 返回 PID 字符串表示。
//
// 指针接收者：让 *PID 直接满足 fmt.Stringer，且避免值拷贝；
// Go 方法集规则——值类型 PID 的方法集**不含**指针接收者方法，
// 所以日志/错误信息里必须传 *PID 才会走到 String()。
func (p *PID) String() string { return p.ID }

// Actor 是参与者的行为接口。
//
// 并发约定：同一 Actor 的所有 Receive 调用都在其所属分片的单 goroutine
// 上串行发生，实现无需内部加锁；不同 Actor 之间才可能并发。
type Actor interface {
	// Receive 处理消息；禁止在 Receive 中阻塞。
	//
	// 「禁止阻塞」是硬契约而非风格建议：分片 goroutine 被一个 Actor
	// 卡住（同步 IO、sleep、等 channel），同分片其他 Actor 的消息会
	// 一起饿死——它们共享同一个事件循环。
	Receive(ctx *Context)
}

// Behavior Actor 行为函数。
//
// 与 Actor 接口等价的行为表示，便于用闭包定义简单参与者；
// 协约与 Actor.Receive 完全相同（不阻塞、单线程串行调用）。
// 闭包可直接捕获外部变量定义有状态行为，不必为每个小 Actor
// 单独建 struct。
type Behavior func(ctx *Context)

// actorCell 是 Actor 的运行时封装。
//
// pid/actor 由 Spawn 给定；mbox 与 system 在 newActorCell 里装配。
// cell 只归属一个分片，其 Receive 永远在该分片 goroutine 上被调用。
// 字段并发：Spawn/add 完成后四个字段均只读——分片循环只读它们，
// remove 通过 map 摘除而非改字段，因此无需额外同步。
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
// 返回指针而非值：cell 会被 mailbox、System、分片 map 交叉引用，
// 指针保证各处看到同一份状态；若按值传递，mbox 指针会各自为政，
// 消息投递与消费落到不同信箱上。
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
// Go 知识点：message 是 any（= interface{}），可承载任意值包括 nil；
// 消费侧断言务必用 comma-ok 模式（v, ok := msg.(T)），失败时得到
// 零值而非 panic；另注意「接口持有具体类型 + nil 指针」不等于 nil
// 接口——业务消息传指针类型时，判空与断言要分开写。
type envelope struct {
	sender  *PID
	message any
}
