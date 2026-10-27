// types.go —— 队伍域的领域类型、错误与人数上限。
package team

import (
	"errors"

	"game_study/pkg/gamedata"
)

// maxMembers 队伍上限（team_limit.xml 第一行 col1；缺配置回退 5）。
//
// 与 guild.maxMembers 的差异：队伍上限是全局唯一配置（取第一行），
// 公会是按等级查表（Col2 匹配）。双层 if 先查存在再查有效性，
// 防御「表为空」和「首行 Col1=0」两种脏配置。
func maxMembers() int {
	if len(gamedata.TeamLimitCfgList()) > 0 {
		if n := gamedata.TeamLimitCfgList()[0].Col1; n > 0 {
			return int(n)
		}
	}
	return 5
}

// Member 队伍成员。

// Member 队伍成员。
type Member struct {
	// PlayerID 成员 player_id
	PlayerID int64
	// Name 昵称（LEFT JOIN player + COALESCE，玩家行缺失时回空串）
	Name string
	// Ready 是否已准备（开房 readiness，demo 期只存不校验）
	Ready bool
	// IsLeader 是否队长（由 leader_id 逐行折算，见 Members）
	IsLeader bool
}

// Team 队伍信息。
//
// 字段语义：Status 0=正常，2=已解散（软删，查询一律过滤）；
// CopyID 是队伍要打的副本配置 ID。

// Team 队伍信息。
//
// 字段语义：Status 0=正常，2=已解散（软删，查询一律过滤）；
// CopyID 是队伍要打的副本配置 ID。
type Team struct {
	// ID 队伍自增主键
	ID int64
	// LeaderID 队长 player_id（队长退队即触发解散）
	LeaderID int64
	// CopyID 目标副本配置 id（创建时指定，决定成员匹配意图）
	CopyID int32
	// Status 0=正常 2=已解散（软删标记，物理行保留）
	Status int32
}

// Repo 组队仓储。
//
// db 可为 nil（无 DB 环境）：模块层入口判空后按「降级成功/不在队」回包，
// 保证无依赖环境下协议演示不中断。

// 仓储错误。
//
// 哨兵 error：模块层只认 errors.Is，仓储层裸返回不 %w 包装，
// 保证哨兵链可读（与 guild 同一约定）。
var (
	ErrTeamNotExist = errors.New("team not exist")
	ErrTeamFull     = errors.New("team full")
	ErrAlreadyIn    = errors.New("already in team")
	ErrNotInTeam    = errors.New("not in team")
)

// teamRepo 是模块对组队仓储的全部依赖（接口隔离）。
//
// 队伍的业务规则大多在 SQL 事务里（人数上限、队长解散），但「谁在队里
// 决定能不能创建」「仓储错误怎么翻译成业务码」这些编排逻辑在模块层，
// 需要能脱离 MySQL 验证。*Repo 隐式实现它，装配层不变。
