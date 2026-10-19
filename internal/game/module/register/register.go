// Package register 聚合各业务模块的持久化扩展点。
//
// 游戏服通过 server.go 直接导入模块，Persister 随包 init 生效；而独立落库进程
// （cmd/flusher）与集成测试不导入业务模块，需要显式导入本包，否则框架遇到
// 未注册的模块状态会拒绝落库 —— 这是刻意的：宁可整批回滚，也不静默丢数据。
//
// 新增玩法时在这里补一行空白导入即可，不需要改 writebehind 或 flusher 的实现。
package register

import (
	// 七日签到：单表 player_signin，@flat 生成 FlatOps。
	_ "game_study/internal/game/module/signin"
	// 任务：状态整体进快照，player_quest 单行。
	_ "game_study/internal/game/module/quest"
	// 技能：等级进快照，player_skill 单行。
	_ "game_study/internal/game/module/skill"
	// 活动：进度与领取位图进快照，activity_record 单行。
	_ "game_study/internal/game/module/activity"
	// 副本：星级/首通进快照，copy_record 单行。
	_ "game_study/internal/game/module/copy"
	// 商店：购买次数进快照，shop_record 单行。
	_ "game_study/internal/game/module/shop"
	// 装备：实例进快照，player_equip 单行。
	_ "game_study/internal/game/module/equip"
)
