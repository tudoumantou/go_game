// internal/game/player/commit_gate.go —— 提交通道（R2.5）
//
// 解决什么问题：缓存权威模式下，业务产生的下行必须等快照提交成功才能发出去，
// 否则客户端会收到「成功」但数据没落库（假成功）。原实现把这个策略写成
// Actor 上的一个 bufferReplies 布尔，散落在四处：
//
//	receiveCached 里手工置位/复位
//	send() 里判断后入队
//	onGrantItems / onSave 里各写一次 !bufferReplies 分支
//
// 业务模块虽然看不到这个字段，但消息处理流程被它切碎了。提交通道把这层
// 策略收成一个对象：业务照常发，是否延迟由窗口状态决定，提交成不成只影响
// 「刷出还是丢弃」。
package player

// stagedAction 暂存的一次下行动作（回包、Done 回执等）。
//
// 用 func() error 而不是具体类型：回包与 Done 回执的形态不同，但「提交成功后
// 才执行」的语义一致，闭包让通道不必认识每一种消息。
type stagedAction func() error

// commitGate 提交通道：窗口内的动作先暂存，Commit 刷出、Discard 丢弃。
//
// 只在玩家 Actor 的单线程内使用，无锁。
type commitGate struct {
	staging []stagedAction
	active  bool
}

// Begin 进入提交窗口：此后的 Send 只入队，不立即执行。
func (g *commitGate) Begin() {
	g.active = true
	g.staging = g.staging[:0]
}

// Active 是否处于提交窗口（调用方据此决定是否需要拷贝消息体）。
func (g *commitGate) Active() bool { return g.active }

// Send 窗口内入队；窗口外立即执行并返回错误。
//
// 窗口外保留错误返回：非缓存模式与纯下行消息不走提交通道，调用方仍能感知
// 发送失败。窗口内只能入队（错误要等 Commit 才知道），这是延迟发送的固有代价。
func (g *commitGate) Send(fn stagedAction) error {
	if !g.active {
		return fn()
	}
	g.staging = append(g.staging, fn)
	return nil
}

// Commit 依次执行全部暂存动作并退出窗口，返回执行错误（调用方记日志）。
//
// 发送失败不重试也不回滚：提交已经成功，数据是对的；客户端收不到回包会靠
// 超时重连拉全量，这比「因为一条推送失败而回滚一次成功提交」合理。
func (g *commitGate) Commit() []error {
	var errs []error
	for _, fn := range g.staging {
		if err := fn(); err != nil {
			errs = append(errs, err)
		}
	}
	g.reset()
	return errs
}

// Discard 丢弃全部暂存并退出窗口（提交失败时调用）。
//
// 丢弃的语义是「客户端收不到成功」：它会重试，而服务端不会出现
// 「回了成功但没落库」的假成功。
func (g *commitGate) Discard() { g.reset() }

func (g *commitGate) reset() {
	g.active = false
	g.staging = g.staging[:0]
}

// Pending 暂存动作数量（测试与观测用）。
func (g *commitGate) Pending() int { return len(g.staging) }
