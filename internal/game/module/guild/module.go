// internal/game/module/guild/guild.go —— 公会模块（6101-6108）
//
// 架构职责：公会的查询/创建/加入/退出，对应设计文档 4.9.3。
//  1. 信息：onInfo 按 player_id 反查所在公会；
//  2. 创建：onCreate 名校验 + 唯一性 + 事务建会（会长自动入会）；
//  3. 加入/退出：onJoin/onQuit，人数上限在事务内加锁判定。
//
// Ext 槽位：无。公会数据全在 MySQL（guild / guild_member 两表），
// 与好友/队伍同属「MySQL 直写模块」——不过 Persister 快照链路，
// 模块无内存态，OnMessage 在玩家 Actor 内串行执行也无需加锁。
//
// 事件订阅：无（公会变动不驱动任务/邮件等事件流）。
//
// 配置表关系：guild_level.xml（Col1=人数上限, Col2=公会等级），
// 经 gamedata.GuildLevelCfgList() 查表，见 maxMembers。
//
// 懒初始化：不适用（无 Ext 行；公会行由创建动作显式产生）。
//
// 跨切约定：本模块所有仓储调用一律传 context.Background()——
// 请求 ctx 在回包后即取消，沿用它会把这些 DB 读写误杀。
package guild

import (
	"context"
	"game_study/internal/game/module"
	"game_study/pkg/log"
)

// guildRepo 是模块对公会仓储的全部依赖（接口隔离）。
//
// 人数上限、会长解散的原子性在 SQL 事务里（归集成测试），但名字校验、
// 「一人一会」的前置判断、哨兵错误到业务码的翻译都在模块层，需要能
// 脱离 MySQL 验证。*Repo 隐式实现它，装配层不变。
type guildRepo interface {
	FindByPlayer(ctx context.Context, playerID int64) (*Guild, error)
	Create(ctx context.Context, name string, leaderID int64) (int64, error)
	Join(ctx context.Context, guildID, playerID int64) error
	Quit(ctx context.Context, playerID int64) error
}

// Module 公会模块。
//
// 无内存态、无锁：全部状态走 MySQL 事务，OnMessage 在玩家 Actor 内
// 串行执行；跨玩家的并发安全由数据库（唯一键 + FOR UPDATE）保证。

// Module 公会模块。
//
// 无内存态、无锁：全部状态走 MySQL 事务，OnMessage 在玩家 Actor 内
// 串行执行；跨玩家的并发安全由数据库（唯一键 + FOR UPDATE）保证。
type Module struct {
	*module.Thin

	// repo 仓储；nil 表示无 DB 环境，各入口降级为「直接成功」回包
	repo   guildRepo
	logger *log.Logger
}

// New 创建公会模块。

// New 创建公会模块。
func New(repo guildRepo, logger *log.Logger) *Module {
	m := &Module{repo: repo, logger: logger}
	m.Thin = module.NewThin("guild", module.ThinLogger(logger))
	module.Handle(m.Thin, m.onInfo)
	module.Handle(m.Thin, m.onCreate)
	module.Handle(m.Thin, m.onJoin)
	module.Handle(m.Thin, m.onQuit)
	return m
}

// OnSave 实现 module.Module。

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error { return nil }

// onInfo 查询所在公会；IsLeader 由 LeaderID 与本人比较得出。
//
// IsLeader 用公会行的 leader_id 内存态比较，不再查成员表 title：
// 会长身份以 leader_id 为权威（Quit 解散逻辑也认它），少一次查询
// 且两个判断口径一致，不会出现「title 是会长但 leader_id 是别人」的分歧。
