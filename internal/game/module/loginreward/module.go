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
// 模块形态：嵌入 module.Thin 薄模块基座。本模块没有客户端协议
// （MsgIDs 为 nil），只挂 Init（配置校验）与 OnLogin（发奖）两个钩子。
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
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "login"

// Module 登录礼包模块（无协议：登录时自动发放）。
//
// 嵌入 *module.Thin：Module 接口与可选能力由基座预装配。
// logger 由本模块自持：Thin 内的 logger 只服务协议失败路径，
// 而本模块没有客户端协议，告警走自己的 warn。
type Module struct {
	*module.Thin
	logger *log.Logger
	now    func() time.Time // 时钟可注入：测试跨日/断签边界，生产用 time.Now
}

// New 的可选时钟用于测试跨日与断签；生产默认服务器时间。
//
// Go 知识点：clock ...func() time.Time 是「可选依赖」的惯用法——
// 可变参数 + nil 检查，比再提供一个 NewWithClock 更省 API 面积。
func New(logger *log.Logger, clock ...func() time.Time) *Module {
	m := &Module{logger: logger, now: time.Now}
	if len(clock) > 0 && clock[0] != nil {
		m.now = clock[0]
	}
	m.Thin = module.NewThin(stateKey,
		module.ThinLogger(logger),
		// 启动期校验配置可用：配置错误在这里暴露而不是玩家登录时——
		// 启动期失败会中断发布，线上失败则影响每个登录玩家，
		// 失败要早、要响。
		module.ThinInit(func(*module.Context) error {
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
		}),
		module.ThinLogin(m.grantOnLogin),
	)
	return m
}

// 保证 Module 满足 module.Module（Thin 已预实现全部方法）。
var _ module.Module = (*Module)(nil)
