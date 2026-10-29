// Package loginreward —— 登录礼包（R3.2）
//
// 规则：按北京时间自然日，每天首次登录发放一次；连续登录天数对应
// login_reward.xml 的档位（col1），断签重置为 1，超出最大档按最大档发。
//
// 幂等靠状态本身：LastDay 与奖励在同一次快照 CAS 里提交，重连、换服、
// 进程重启都不会重复发。不占用 AppliedGrants —— 那是订单级业务键的地盘，
// 每天一个键会让快照无限膨胀（见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：登录时自动发奖（无客户端协议）。这是「OnLogin 侧写模块」的
// 范本——没有 MsgIDs，逻辑全部挂在登录钩子上。
// Ext 槽位：player_login（LastDay + Streak 两列）。
// 事件订阅：无。
// 配置表：login_reward.xml（col1=第几天，col2="item_id,count"）。
// 懒初始化：从未登录过返回零值 Data（LastDay=0 不等于任何自然日序号），
// 首次登录才产生第一行记录。
//
// 模块形态：module.Define[Data]（v3.17 迁移）。
//
// 迁移前是「嵌入 module.Thin + 自己持 now 字段 + 手工 LoadExt/SaveExt」的
// 一种独立形态：有 Ext 快照却不走 Repository，业务层看得见 writebehind。
// 当时绕开框架的直接诱因是 Bound.SetClock 只在路由路径生效，登录钩子里的
// c.Now() 拿不到注入时钟——注入点漏了一个，模块就自己长出一套。
// 修好 applyClock 之后本模块回到与其他模块一致的写法。
//
// 文件分工（docs/模块开发约定.md §2）：
//
//	module.go  本文件：模块声明与装配
//	types.go   状态结构
//	persist.go Ext 槽位 ↔ player_login 行 的转换
//	handler.go 发奖流程
package loginreward

import (
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "login"

// M 登录礼包模块的唯一实例。
//
// 没有路由（本模块不处理客户端协议），只有 Init 与 OnLogin 两个钩子。
// Init 在启动期校验配置：配置错误在这里暴露而不是玩家登录时——
// 启动期失败会中断发布，线上失败则影响每个登录玩家，失败要早、要响。
var M = module.Define[Data](stateKey).
	OnInit(validateConfig).
	WithLogin(onGrant)

// validateConfig 启动期校验配置：档位与奖励必须齐全且道具存在。
func validateConfig(*module.Context) error {
	if len(gamedata.LoginRewardCfgList()) == 0 {
		return fmt.Errorf("login_reward.xml is empty")
	}
	for _, cfg := range gamedata.LoginRewardCfgList() {
		if cfg.Col1 <= 0 {
			return fmt.Errorf("invalid login reward day %d", cfg.Col1)
		}
		if _, err := parseReward(cfg.Col2); err != nil {
			return fmt.Errorf("login reward %d: %w", cfg.Id, err)
		}
	}
	return nil
}
