// internal/game/module/attr/attr.go —— 属性模块
//
// 架构职责：战斗属性（hp/mp/atk/def/spd）的「计算面」。最终值 = 基础值
// （player_attr 表，升级时重算，随 Profile 快照提交）+ 加成（装备/伙伴模块
// 通过 State.AddBonus/ClearBonus 维护的派生值）。本模块只消费这个和，
// 把五维属性打包进 PlayerInfo 协议下发。
//
// Ext 槽位：无。加成是纯内存派生值，刻意不落库——每次登录由装备/伙伴
// 模块按快照重算，落库反而会造成重复累加。
// 事件订阅：无。属性是被动计算结果，不产生也不消费行为事件。
// 配置表：attr_formula.xml（P7）；P6 内置五维常量。
// 懒初始化：无私有状态，模块注册后即可用，不存在「首次操作才建行」。
//
// 模块形态：嵌入 module.Thin 薄模块基座。当前无协议（属性查询随
// PlayerInfo 下发），只挂登录/下线两个观测钩子；未来加属性协议时
// 在 New 里 m.Handle(msgID, fn) 即可，不动基座。
package attr

import (
	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// Module 属性模块。
//
// 与背包等模块不同，它只持有 logger：所有状态都在 PlayerState 上，
// 因此同一个实例可服务全部玩家（模块单例 + 玩家状态外置）。
type Module struct {
	*module.Thin
}

// New 创建属性模块。
func New(logger *log.Logger) *Module {
	m := &Module{}
	m.Thin = module.NewThin("attr",
		module.ThinLogger(logger),
		module.ThinLogin(func(ctx *module.Context) error {
			if logger != nil {
				logger.Debug("attr login", "player_id", ctx.State.PlayerID())
			}
			return nil
		}),
		// MarkDirty 是「提示」而非「命令」：权威提交仍是快照 CAS，
		// 这里标脏只是告诉 flusher 该玩家近期有变动，别跳过本轮。
		module.ThinLogout(func(ctx *module.Context) error {
			ctx.State.MarkDirty("attr")
			return nil
		}),
	)
	return m
}

// 保证 Module 满足 module.Module（Thin 已预实现全部方法）。
var _ module.Module = (*Module)(nil)
