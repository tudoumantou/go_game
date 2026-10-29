// cmd/login/main.go —— 登录服（LoginServer）进程启动入口
//
// 架构职责：账号鉴权、登录态签发与校验、选服推送（设计文档第 6 章），
// 对外暴露 HTTP 接口，对内经 Kafka 与游戏服交换发货/封禁等消息。
// 协作关系：启动顺序 etcd → 中间件 → 中心服 → 游戏服/跨服 → 网关 → 登录服（19.5），
// 登录服最后起：它要读网关/游戏服的注册信息才能回「该去哪个服」。
// 设计取舍：启动样板统一在 pkg/bootstrap。登录服的关闭顺序最讲究
// （停 HTTP → 停消费 → 刷 producer → 关 DB/Redis），因为支付回调走这条链路，
// 反序会丢单。
package main

import (
	"game_study/internal/login"
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
		Name:          "login",
		DefaultConfig: "configs/login/login-dev.yaml",
		Version:       version,
		Build: func(cfg *config.Config, _ *log.Logger) (bootstrap.Server, error) {
			return login.New(cfg), nil
		},
	})
}
