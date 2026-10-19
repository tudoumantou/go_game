// Package shutdown 优雅关闭编排。
//
// Manager 按阶段顺序执行带超时的关闭步骤，收集并聚合错误——
// 关闭失败必须以非零退出码暴露，不能只打一行 Warn。
package shutdown
