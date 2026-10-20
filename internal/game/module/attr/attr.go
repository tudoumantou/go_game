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
package attr

import (
	"context"

	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// Module 属性模块。
//
// 与背包等模块不同，它只持有 logger：所有状态都在 PlayerState 上，
// 因此同一个实例可服务全部玩家（模块单例 + 玩家状态外置）。
type Module struct {
	logger *log.Logger
}

// New 创建属性模块。
func New(logger *log.Logger) *Module {
	return &Module{logger: logger}
}

// Name 实现 module.Module。
//
// 模块名同时是日志/指标标签；没有 Ext 槽位的模块（如本模块）名仅用于标识。
func (m *Module) Name() string { return "attr" }

// MsgIDs 实现 module.Module（属性查询随 PlayerInfo 下发，暂无独立协议）。
//
// 返回 nil 表示不注册任何协议路由——Registry.Register 对 nil 切片
// 直接跳过，不会建空索引项。
func (m *Module) MsgIDs() []uint32 { return nil }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	// TODO(P6): 属性查询/加点协议
	// 占位实现：参数置空是「显式声明不使用」的惯用法，
	// 避免未来加参数时静默改变语义（Go 的函数未使用参数本就合法）。
	_ = msgID
	_ = body
	return nil
}

// OnSave 实现 module.Module。
//
// 无模块级聚合数据：加成随玩家快照提交，基础值随 Profile 提交，
// flusher 不需要本模块额外落库。
func (m *Module) OnSave(ctx context.Context) error {
	_ = ctx
	return nil
}

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error {
	m.logger.Debug("attr login", "player_id", ctx.State.PlayerID())
	return nil
}

// OnLogout 实现 module.Module。
//
// MarkDirty 是「提示」而非「命令」：权威提交仍是快照 CAS，
// 这里标脏只是告诉 flusher 该玩家近期有变动，别跳过本轮。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}
