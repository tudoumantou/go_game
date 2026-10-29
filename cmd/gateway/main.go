// cmd/gateway/main.go —— 网关服（GatewayServer）进程启动入口
//
// 架构职责：客户端唯一入口——TCP 长连接接入、会话管理、协议编解码，
// 并把客户端请求以 gRPC 流转发给游戏服（设计文档第 8 章）。
// 协作关系：启动顺序 etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5）；
// 网关起来后向 etcd 注册，客户端才能通过登录服拿到网关地址。
// 设计取舍：启动样板统一在 pkg/bootstrap。网关的关闭最急（要先把在线会话
// 断干净再退），所以它的 Shutdown 里「摘注册 → 断连」的顺序是硬约束。
package main

import (
	"game_study/internal/gateway"
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
		Name:          "gateway",
		DefaultConfig: "configs/gateway/gateway-dev.yaml",
		Version:       version,
		Build: func(cfg *config.Config, _ *log.Logger) (bootstrap.Server, error) {
			return gateway.New(cfg), nil
		},
	})
}
