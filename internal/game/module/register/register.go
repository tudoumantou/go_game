// Package register 聚合各业务模块的持久化扩展点。
//
// 游戏服通过 server.go 直接导入模块，Persister 随包 init 生效；而独立落库进程
// （cmd/flusher）与集成测试不导入业务模块，需要显式导入本包，否则框架遇到
// 未注册的模块状态会拒绝落库 —— 这是刻意的：宁可整批回滚，也不静默丢数据。
//
// 新增玩法时在这里补一行空白导入即可，不需要改 writebehind 或 flusher 的实现。
package register

import (
	// 七日签到：player_signin / signin_claim。
	_ "game_study/internal/game/module/signin"
)
