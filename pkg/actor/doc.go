// pkg/actor/doc.go —— 包级文档（实现在 mailbox.go / system.go / actor.go）

// Package actor Actor 运行时（游戏服进程内的业务执行内核）。
//
// 能力：有界信箱与严格背压（满则拒绝新消息，不静默挤掉旧消息）、按 playerID 分片的
// 单 goroutine 事件循环、Drain 等待在途消息处理完、时间轮定时器。
// 实现在 mailbox.go / system.go / actor.go。
//
// 架构位置：每玩家一个 Actor（playerID 为路由键），协议上行消息经网关
// 转发后由 System.Send 落到玩家 Actor 信箱，Receive 内串行改内存态，
// 再走 Redis/MySQL 写回；context.go 提供处理上下文。
// 协作关系：pkg/metrics 接收信箱深度/拒绝数/panic/处理耗时指标；
// 优雅关闭时由进程层编排「停外部入口 → Drain → FlushAll → Stop」。
//
// 关键设计取舍：
//   - 分片串行而非全局锁：actorID 经 fnv32a 取模落分片，同 ID 永远同
//     分片，Actor 内无锁；代价是单分片过载会拖慢片内所有玩家，用
//     per-Actor budget 轮转缓解饥饿；
//   - 有界信箱 + 拒绝新消息：满即 ErrMailboxFull 让调用方决策，换取
//     背压可见性；无界信箱只会把过载变成 OOM；
//   - wake 信号（cap 1）+ 100ms ticker：既保证新消息微秒级被处理，
//     又给不经 deliver 的路径（定时器投递）兜底。
package actor
