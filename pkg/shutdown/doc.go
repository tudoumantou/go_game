// Package shutdown 优雅关闭编排。
//
// Manager 按阶段顺序执行带超时的关闭步骤，收集并聚合错误——
// 关闭失败必须以非零退出码暴露，不能只打一行 Warn。
//
// 架构职责：定义关闭的阶段划分（DRAIN→DRAINING→FLUSH→STOP）与执行引擎，
// 各进程把关闭动作注册成 Step，由 Manager 统一编排。
// 协作关系：main 在收到 SIGTERM 后 cancel 根 ctx 并调 Manager.Run；
// 被编排的组件包括 net.TCPServer（DRAIN 阶段关 listener）、
// rpc.Server（STOP 阶段有界优雅停止）、mq.Producer（FLUSH 阶段）、
// registry cancel（DRAIN 阶段摘 etcd 注册）等。
// 设计取舍（两阶段顺序）：
//   - 先 DRAIN 拒新再 DRAINING 排空：关 listener/摘注册必须在等存量
//     之前——反序的话，等待期间新请求还在进来，永远等不到「零在途」。
//   - 再 FLUSH 再 STOP：组件（Kafka producer、DB）只能在「不再有新写入」
//     之后才能安全关闭；先停组件后排空，排空期的写入会直接失败。
//   - 单步失败不中断、错误聚合返回：跳过清理会让进程更难收场，但失败
//     必须体现在退出码上——「保存失败」不能被日志里一行 Warn 掩盖。
package shutdown
