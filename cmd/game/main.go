// cmd/game/main.go —— 游戏服（GameServer）进程启动入口
//
// 架构职责：承载玩家业务——Actor 系统跑地图/公会等有状态实体，gRPC 同时服务
// 网关（客户端会话）与中心服（Report 双向流），并经 Kafka 与其它进程交换消息。
// 协作关系：启动顺序 etcd → 中间件 → 中心服 → 游戏服 → 网关 → 登录服（19.5）；
// 游戏服起来后向中心服/etcd 注册，网关才能通过 resolver 发现并建流；
// 玩家状态经写穿透交 flusher 进程落库（Tier-2）。
// 设计取舍：启动样板统一在 pkg/bootstrap；游戏服是依赖最多（Redis/MySQL/
// Kafka/etcd 全接）、关闭链路最长（Actor 排空 → flusher 落库 → Producer 刷出）
// 的一个进程，其余五个都是它的子集。
package main

import (
	"game_study/internal/game"
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
		Name:          "game",
		DefaultConfig: "configs/game/game-dev.yaml",
		Version:       version,
		Build: func(cfg *config.Config, _ *log.Logger) (bootstrap.Server, error) {
			// 装配顺序即依赖顺序：先 Redis/MySQL/Kafka 句柄，再 Actor 系统，
			// 最后才是对外监听——全部在 Start 内完成，保证第一个客户端连上时
			// 内部已就绪。
			return game.New(cfg), nil
		},
	})
}
