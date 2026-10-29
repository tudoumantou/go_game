// internal/game/module/team/team.go —— 组队模块（6201-6208）
//
// 架构职责：队伍的信息/创建/加入/退出，对应设计文档 4.9.3。
//  1. 信息：onInfo 查队伍 + 成员（联 player 表取昵称）；
//  2. 创建：onCreate 事务建队（队长自动入队）；
//  3. 加入/退出：onJoin/onLeave，人数上限在事务内加锁判定；
//  4. 掉线：OnLogout 自动退队——队伍是临时关系，防挂机占位。
//
// Ext 槽位：无。队伍数据全在 MySQL（team / team_member 两表），
// 与公会/好友同属「MySQL 直写模块」——不过 Persister 快照链路。
//
// 事件订阅：无。
//
// 配置表关系：team_limit.xml（第一行 Col1=队伍人数上限），
// 经 gamedata.TeamLimitCfgList() 读取，见 maxMembers。
//
// 懒初始化：不适用（无 Ext 行；队伍行由创建动作显式产生）。
//
// 跨切约定：仓储调用一律传 context.Background()——请求 ctx 在回包后
// 即取消，沿用会把这些 DB 读写误杀。
//
// 与公会模块的关键差异：公会解散是硬删（DELETE 行），队伍解散是
// 软删（status=2，查询统一过滤）——保留行供对账排查，且自增 id
// 不复用，旧客户端拿着旧 teamId 不会蒙到一支新队伍。
package team

import (
	"context"
	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// teamRepo 是模块对组队仓储的全部依赖（接口隔离）。
//
// 队伍的业务规则大多在 SQL 事务里（人数上限、队长解散），但「谁在队里
// 决定能不能创建」「仓储错误怎么翻译成业务码」这些编排逻辑在模块层，
// 需要能脱离 MySQL 验证。*Repo 隐式实现它，装配层不变。
type teamRepo interface {
	// Ready 数据库是否可用；无 DB 环境时模块降级回包（见 module.Shared）。
	Ready() bool
	FindByPlayer(ctx context.Context, playerID int64) (*Team, error)
	Create(ctx context.Context, leaderID int64, copyID int32) (int64, error)
	Join(ctx context.Context, teamID, playerID int64) error
	Leave(ctx context.Context, playerID int64) error
	Members(ctx context.Context, teamID int64) ([]Member, error)
}

// Module 组队模块。
//
// 无内存态、无锁：全部状态走 MySQL 事务，OnMessage 在玩家 Actor 内
// 串行执行；跨玩家的并发安全由数据库（唯一键 + FOR UPDATE）保证。

// Module 组队模块。
//
// 无内存态、无锁：全部状态走 MySQL 事务，OnMessage 在玩家 Actor 内
// 串行执行；跨玩家的并发安全由数据库（唯一键 + FOR UPDATE）保证。
type Module struct {
	*module.Thin

	// repo 仓储；nil 表示无 DB 环境，入口降级为「直接成功/不在队」回包
	repo   teamRepo
	logger *log.Logger
}

// ready 数据库是否可用。
//
// 两种「不可用」都要算：仓储压根没注入（无 DB 环境的模块级降级，repo 是
// nil 接口），以及仓储在但没配连接池（NewRepo(nil)，Ready() 为 false）。
// 只判前者会让后者变成 nil deref，只判后者会对 nil 接口调用方法——
// 所以这里是唯一一处同时判两者的地方，业务只写 if !m.ready()。
func (m *Module) ready() bool { return m.repo != nil && m.repo.Ready() }

// New 创建组队模块。
func New(repo teamRepo, logger *log.Logger) *Module {
	m := &Module{repo: repo, logger: logger}
	m.Thin = module.NewThin("team",
		module.ThinLogger(logger),
		module.ThinLogout(m.onLogout),
	)
	module.Handle(m.Thin, m.onInfo)
	module.Handle(m.Thin, m.onCreate)
	module.Handle(m.Thin, m.onJoin)
	module.Handle(m.Thin, m.onLeave)
	return m
}

// onLogout 掉线自动退队。
//
// 与公会的关键语义差异：公会掉线不退会（持久社交关系，防误退），
// 队伍掉线退队（临时关系，防挂机占位坑队友）——同一生命周期钩子
// 在不同模块承载相反的产品决策。
// 注意：这是与 Thin 的日志器、守卫同处一个基座的钩子，Thin 只负责
// 在 OnLogout 时调用它；错误被忽略——登出流程不能被 DB 抖动卡住。
func (m *Module) onLogout(ctx *module.Context) error {
	if !m.ready() {
		return nil
	}
	_ = m.repo.Leave(context.Background(), ctx.State.PlayerID())
	return nil
}

// onInfo 查询当前队伍信息；不在队回 int32(errcode.ErrTeamNotIn)。
//
// 两次查询（队伍 → 成员）：先查队伍，t==nil 时省掉第二次；
// 队伍上限 5 人，分两次查的代价可接受（合并成一条 JOIN 会把
// 「不在队」和「在队无成员」两种语义搅在一起）。
