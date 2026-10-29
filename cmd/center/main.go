// cmd/center/main.go —— 中心服（CenterServer）进程启动入口（五进程中的协调者）
//
// 架构职责：中心服提供节点注册/发现、跨服匹配、消息中转、排行榜/统计，
// 以及仅 leader 执行的赛季归档、活动调度与全服里程碑（设计文档第 9 章）。
// 协作关系：启动顺序上中心服必须最先就绪（etcd → 中间件 → center → game/cross
// → gateway → login，19.5）——游戏服/跨服靠它完成注册与互相发现。
// 设计取舍：启动样板（配置 → 日志 → 信号 → 组装 → 启动 → 关闭预算）统一在
// pkg/bootstrap，本文件只声明「我是谁、默认读哪份配置、怎么组装」。
package main

import (
	"game_study/internal/center"
	"game_study/pkg/bootstrap"
	"game_study/pkg/config"
	"game_study/pkg/log"

	// automaxprocs：按 cgroup CPU 配额设置 GOMAXPROCS（K8s 限核时
	// 避免按宿主机核数开 P 导致 GC/调度抖动）；无限制时自动 no-op。
	_ "go.uber.org/automaxprocs"
)

// version 构建时 -ldflags "-X main.version=..." 注入。
// 必须留在 main 包里：-X 只能改写「对应 import path」的字符串变量，
// 搬进 pkg/bootstrap 之后这个注入点就失效了。
var version = "dev"

func main() {
	bootstrap.Run(bootstrap.Options{
		Name:          "center",
		DefaultConfig: "configs/center/center-dev.yaml",
		Version:       version,
		Build: func(cfg *config.Config, _ *log.Logger) (bootstrap.Server, error) {
			// New 只做纯内存组装、不返回 error；监听与中间件连接都在 Start 内完成。
			return center.New(cfg), nil
		},
	})
}
