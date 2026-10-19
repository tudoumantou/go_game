// Package actor Actor 运行时。
//
// 能力：有界信箱与严格背压（满则拒绝新消息，不静默挤掉旧消息）、按 playerID 分片的
// 单 goroutine 事件循环、Drain 等待在途消息处理完、时间轮定时器。
// 实现在 mailbox.go / system.go / actor.go。
package actor
