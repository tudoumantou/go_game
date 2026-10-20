// Package log 结构化日志封装（基于 zap）。
//
// 架构职责：为 gateway/game/center/cross/login 五个进程提供唯一的日志入口——
// 级别过滤、JSON/文本编码、service 字段注入全部收敛在本包，业务代码只面对
// Logger 一个类型，不接触任何 zap 内部类型。
//
// 协作关系：各进程 main 入口用 log.New("svc") / log.NewDev("svc") 初始化，
// 优雅退出时经 shutdown.Manager 的 FLUSH 阶段调用 Sync() 兜底刷盘；
// 本包处于依赖图最底层，禁止 import 任何内部业务包（否则成环）。
//
// 设计取舍：选 zap 而非标准库 slog——高并发写入下 slog 的反射/分配开销偏大，
// 且键值对（Sugared）API 与既有调用点风格一致；代价是引入第三方依赖，
// 因此全项目只能经本包间接使用 zap，禁止业务包直接 import。
package log
