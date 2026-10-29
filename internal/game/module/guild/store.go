// store.go —— guild / guild_member 两张表的访问层。
//
// 建会、入会、退会都要在事务里同时改「公会行」和「成员行」，并且用
// SELECT ... FOR UPDATE 锁住公会行：member_count 是冗余计数，并发入会
// 不锁行就会算出同一个上限判断，人数冲破上线。
package guild

import (
	"context"
	"database/sql"
	"errors"

	"game_study/pkg/db"
	"game_study/pkg/model"
	"game_study/pkg/sqlstore"
)

// Repo 公会仓储。
//
// db 可为 nil（无 DB 环境）：模块层所有入口判空后按「降级成功」回包，
// 保证无依赖环境下协议演示不中断。
type Repo struct {
	*sqlstore.Base
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{Base: sqlstore.New(database, "guild")}
}

// FindByPlayer 查询玩家所在公会。
//
// 返回 (nil, nil) 表示「未入会」：把 sql.ErrNoRows 这个「正常无行」
// 翻译成 nil 而非错误，调用方只需判 g == nil，不用碰 errors.Is。
// JOIN + LIMIT 1：一个玩家正常只有一行成员记录，LIMIT 1 防脏数据多行时
// QueryRow 直接报错把查询打挂。
func (r *Repo) FindByPlayer(ctx context.Context, playerID int64) (*Guild, error) {
	const q = `SELECT g.id, g.name, g.leader_id, g.level, g.member_count
	           FROM guild g JOIN guild_member m ON m.guild_id = g.id
	           WHERE m.player_id = ? LIMIT 1`
	var g Guild
	err := r.SQL().QueryRowContext(ctx, q, playerID).Scan(&g.ID, &g.Name, &g.LeaderID, &g.Level, &g.MemberCount)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &g, nil
}

// Create 创建公会并把会长加入成员表（事务）。
//
// 两条 INSERT 必须同事务：只建公会行不插成员行，中间态下会长自己
// 都查不到这个公会；事务保证「要么都能用、要么都没发生」。
// 1062/Duplicate 字符串识别 → ErrNameUsed：database/sql 的 driver
// 不暴露「哪条唯一约束冲突」，只能匹配 MySQL 错误号文本——这是惯用法
// 但脆弱，换 driver/改字符集时要同步回来改这里。
func (r *Repo) Create(ctx context.Context, name string, leaderID int64) (int64, error) {
	var guildID int64
	err := r.Tx(ctx, func(tx *sql.Tx) error {
		res, err := model.InsertGuild(ctx, tx, &model.Guild{
			Name:        name,
			LeaderId:    leaderID,
			Level:       1,
			Exp:         0,
			Notice:      "",
			MemberCount: 1,
		})
		if err != nil {
			// 唯一键冲突 = 公会名重复（uk_name）。判定走结构化 1062，
			// 不再匹配 "Duplicate" 这类错误文本（换驱动版本会静默失效）。
			if db.IsDuplicate(err) {
				return ErrNameUsed
			}
			return err
		}
		guildID, err = res.LastInsertId()
		if err != nil {
			return err
		}
		_, err = model.InsertGuildMember(ctx, tx, &model.GuildMember{
			GuildId:  guildID,
			PlayerId: leaderID,
			Title:    titleLeader,
		})
		return err
	})
	return guildID, err
}

// ErrNameUsed 公会名重复。
//
// 错误哨兵（sentinel）模式：仓储层用可比较的包级 error 表达「可预期失败」，
// 模块层用 errors.Is 判定后转业务码——两层职责分离，模块层不碰 SQL 细节。

// Join 加入公会（事务：成员 + 人数 +1，超上限回滚）。
//
// FOR UPDATE 的必要性：member_count 的「读-判断-写」三步必须在同一事务内
// 锁住公会行，否则两个并发加入可能同时读到 39/40，双双通过判定造成超员。
// 上限判定失败时整个事务回滚——连成员行都不插，不会留下「人在公会、
// 计数没加」的错位。
func (r *Repo) Join(ctx context.Context, guildID, playerID int64) error {
	return r.Tx(ctx, func(tx *sql.Tx) error {
		// 锁公会行（生成的 @lockby:id）：member_count 的「读-判断-写」
		// 三步必须在同一事务内基于同一份读数，否则并发加入会双双通过判定
		g, err := model.LockGuildById(ctx, tx, guildID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrGuildNotExist
		}
		if err != nil {
			return err
		}
		if g.MemberCount >= int32(maxMembers(g.Level)) {
			return ErrGuildFull
		}
		if _, err := model.InsertGuildMember(ctx, tx, &model.GuildMember{
			GuildId:  guildID,
			PlayerId: playerID,
			Title:    titleMember,
		}); err != nil {
			// 唯一键冲突 = 已在该公会（uk_guild_player）。
			// 前置的 FindByPlayer 已挡掉绝大多数情况，这里是双端并发加入的兜底。
			if db.IsDuplicate(err) {
				return ErrAlreadyIn
			}
			return err
		}
		// 人数 +1：行已被 FOR UPDATE 锁住，读改写与表达式累加等价，
		// 且不依赖生成器支持表达式 SET。
		_, err = model.UpdateGuildMemberCountByPK(ctx, tx, &model.Guild{
			Id:          guildID,
			MemberCount: g.MemberCount + 1,
		})
		return err
	})
}

// 仓储错误。
//
// 全部为哨兵 error：模块层只认 errors.Is，仓储层内部只 return 不包装
// （%w 包装会破坏哨兵链的可读性，这里保持裸返回）。

// Quit 退出公会（会长退出则解散）。
//
// 会长退出选「解散」而非「转让会长」：demo 期没有转让协议，
// 解散是最简且语义正确的选择（避免无人公会永久占位）。
// DELETE 顺序先成员后公会：满足外键依赖，也保证任一语句失败时
// 整个事务回滚，不会留下「公会没了、成员行还在」的孤儿数据。
func (r *Repo) Quit(ctx context.Context, playerID int64) error {
	return r.Tx(ctx, func(tx *sql.Tx) error {
		member, err := model.FindGuildMemberByPlayerId(ctx, tx, playerID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotInGuild
		}
		if err != nil {
			return err
		}
		guildID, title := member.GuildId, member.Title
		if err := model.DeleteGuildMemberByPlayer(ctx, tx, playerID); err != nil {
			return err
		}
		if title == titleLeader {
			// 会长退出：解散公会（先删成员行再删公会行，满足外键依赖）
			if _, err := model.DeleteGuildMemberByGuildId(ctx, tx, guildID); err != nil {
				return err
			}
			_, err := model.DeleteGuildByPK(ctx, tx, guildID)
			return err
		}
		// 人数 -1：先锁公会行再读改写（原来用 GREATEST 表达式兜底，
		// 生成器不支持表达式 SET；加行锁后读改写同样不会丢更新，
		// 且 max(-1, 0) 保住了「人数不为负」这条数据库不变量）。
		g, err := model.LockGuildById(ctx, tx, guildID)
		if errors.Is(err, sql.ErrNoRows) {
			// 公会行已不存在（异常数据）：成员行删掉即达成目标，不再改计数
			return nil
		}
		if err != nil {
			return err
		}
		count := g.MemberCount - 1
		if count < 0 {
			count = 0
		}
		_, err = model.UpdateGuildMemberCountByPK(ctx, tx, &model.Guild{Id: guildID, MemberCount: count})
		return err
	})
}

// ErrNotInGuild 未加入公会。
