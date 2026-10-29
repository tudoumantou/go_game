// cmd/cross/main.go —— 跨服节点（CrossServer）进程启动入口
//
// 架构职责：承载小组跨服战场——房间 Actor 的生命周期管理、战斗消息透传、
// 结算结果回推（设计文档第 10 章）。
// 协作关系：跨服节点不注册 etcd、不连网关（10.1），只被组内游戏服主动连接；
// 依赖中心服先完成启动与注册，游戏服经中心服 GroupUpdate 拿到跨服地址后才来建流。
// 设计取舍：启动样板统一在 pkg/bootstrap；跨服节点无 Redis/MySQL/Kafka 依赖，
// 是六个进程里唯一「Start 之前没有任何外部连接」的一个。
package main

import (
	"game_study/internal/cross"
	"game_study/pkg/bootstrap"
	"game_study/pkg/config"
	"game_study/pkg/log"

	// automaxprocs：按 cgroup CPU 配额设置 GOMAXPROCS（K8s 限核时
	// 避免按宿主机核数开 P 导致 GC/调度抖动）；无限制时自动 no-op。
	_ "go.uber.org/automaxprocs"
)

// version 构建时 -ldflags "-X main.version=..." 注入（-X 只能改写字符串变量）。
var version = "dev"

func main() {
	bootstrap.Run(bootstrap.Options{
		Name:          "cross",
		DefaultConfig: "configs/cross/cross-dev.yaml",
		Version:       version,
		Build: func(cfg *config.Config, _ *log.Logger) (bootstrap.Server, error) {
			return cross.New(cfg), nil
		},
	})
}
