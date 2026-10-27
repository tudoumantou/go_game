// types.go —— 公会域的领域类型、错误与人数上限。
//
// maxMembers 按公会等级算人数上限：这是玩法规则，store 的入会校验也要用，
// 所以放这里而不是 handler。
package guild

import (
	"errors"

	"game_study/pkg/gamedata"
)

// maxMembers 公会人数上限：按 guild_level.xml（col1=人数上限, col2=等级）取当前等级；
// 缺配置回退 50，避免配置漏项导致公会无法收人。
//
// 线性扫描而非建 map：配置表条目是个位数，且进程启动时整体载入一次，
// 这种数据量下遍历比维护索引更快也更简单（数据量小时别过度设计）。
// 缺配置回落是防御性默认：配置漏项时「少收人」远好过「一个不收」。
func maxMembers(level int32) int {
	for _, cfg := range gamedata.GuildLevelCfgList() {
		if int32(cfg.Col2) == level && cfg.Col1 > 0 {
			return int(cfg.Col1)
		}
	}
	return 50
}

// 职位。
//
// titleMember=0 的零值语义：guild_member 表不显式写 title 的行
// （数据库 DEFAULT 0）天然就是会员，无需显式赋值。

// 职位。
//
// titleMember=0 的零值语义：guild_member 表不显式写 title 的行
// （数据库 DEFAULT 0）天然就是会员，无需显式赋值。
const (
	titleMember int32 = 0
	titleLeader int32 = 1
)

// Guild 公会信息。
//
// 字段语义：MemberCount 是冗余计数（Join/Quit 时 ±1），人数上限按 Level 查
// guild_level.xml；Level 为公会等级，与成员等级无关。

// Guild 公会信息。
//
// 字段语义：MemberCount 是冗余计数（Join/Quit 时 ±1），人数上限按 Level 查
// guild_level.xml；Level 为公会等级，与成员等级无关。
type Guild struct {
	// ID 公会自增主键
	ID int64
	// Name 公会名（uk 唯一，重复创建被 1062 挡下转 ErrNameUsed）
	Name string
	// LeaderID 会长 player_id；IsLeader 判定以此列为准（不查成员表 title）
	LeaderID int64
	// Level 公会等级（决定人数上限，与成员个人等级无关）
	Level int32
	// MemberCount 冗余成员计数（非实时 count，异常路径靠 GREATEST 兜底）
	MemberCount int32
}

// Repo 公会仓储。
//
// db 可为 nil（无 DB 环境）：模块层所有入口判空后按「降级成功」回包，
// 保证无依赖环境下协议演示不中断。

// ErrNameUsed 公会名重复。
//
// 错误哨兵（sentinel）模式：仓储层用可比较的包级 error 表达「可预期失败」，
// 模块层用 errors.Is 判定后转业务码——两层职责分离，模块层不碰 SQL 细节。
var ErrNameUsed = errors.New("guild name used")

// Join 加入公会（事务：成员 + 人数 +1，超上限回滚）。
//
// FOR UPDATE 的必要性：member_count 的「读-判断-写」三步必须在同一事务内
// 锁住公会行，否则两个并发加入可能同时读到 39/40，双双通过判定造成超员。
// 上限判定失败时整个事务回滚——连成员行都不插，不会留下「人在公会、
// 计数没加」的错位。

// 仓储错误。
//
// 全部为哨兵 error：模块层只认 errors.Is，仓储层内部只 return 不包装
// （%w 包装会破坏哨兵链的可读性，这里保持裸返回）。
var (
	ErrGuildNotExist = errors.New("guild not exist")
	ErrGuildFull     = errors.New("guild full")
	ErrAlreadyIn     = errors.New("already in guild")
)

// Quit 退出公会（会长退出则解散）。
//
// 会长退出选「解散」而非「转让会长」：demo 期没有转让协议，
// 解散是最简且语义正确的选择（避免无人公会永久占位）。
// DELETE 顺序先成员后公会：满足外键依赖，也保证任一语句失败时
// 整个事务回滚，不会留下「公会没了、成员行还在」的孤儿数据。

// ErrNotInGuild 未加入公会。
var ErrNotInGuild = errors.New("not in guild")

// guildRepo 是模块对公会仓储的全部依赖（接口隔离）。
//
// 人数上限、会长解散的原子性在 SQL 事务里（归集成测试），但名字校验、
// 「一人一会」的前置判断、哨兵错误到业务码的翻译都在模块层，需要能
// 脱离 MySQL 验证。*Repo 隐式实现它，装配层不变。
