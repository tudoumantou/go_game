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
	"database/sql"
	"errors"
	"strings"
	"unicode/utf8"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
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
const (
	titleMember int32 = 0
	titleLeader int32 = 1
)

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
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

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
	err := r.db.QueryRowContext(ctx, q, playerID).Scan(&g.ID, &g.Name, &g.LeaderID, &g.Level, &g.MemberCount)
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
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`INSERT INTO guild (name, leader_id, level, exp, notice, member_count) VALUES (?, ?, 1, 0, '', 1)`,
			name, leaderID)
		if err != nil {
			if strings.Contains(err.Error(), "1062") || strings.Contains(err.Error(), "Duplicate") {
				return ErrNameUsed
			}
			return err
		}
		guildID, err = res.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO guild_member (guild_id, player_id, title) VALUES (?, ?, ?)`,
			guildID, leaderID, titleLeader)
		return err
	})
	return guildID, err
}

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
func (r *Repo) Join(ctx context.Context, guildID, playerID int64) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		var count, level int32
		if err := tx.QueryRowContext(ctx, `SELECT member_count, level FROM guild WHERE id = ? FOR UPDATE`, guildID).Scan(&count, &level); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrGuildNotExist
			}
			return err
		}
		if count >= int32(maxMembers(level)) {
			return ErrGuildFull
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO guild_member (guild_id, player_id, title) VALUES (?, ?, ?)`,
			guildID, playerID, titleMember); err != nil {
			// 唯一键冲突 = 已在该公会；靠 MySQL 1062 识别（driver 不暴露约束类型）。
			// 前置的 FindByPlayer 已挡掉绝大多数情况，这里是双端并发加入的兜底。
			if strings.Contains(err.Error(), "1062") {
				return ErrAlreadyIn
			}
			return err
		}
		// member_count + 1 用 SQL 表达式累加而非读出来算：
		// 行已被 FOR UPDATE 锁住，表达式累加不会丢失并发更新
		_, err := tx.ExecContext(ctx, `UPDATE guild SET member_count = member_count + 1 WHERE id = ?`, guildID)
		return err
	})
}

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
func (r *Repo) Quit(ctx context.Context, playerID int64) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		var guildID int64
		var title int32
		err := tx.QueryRowContext(ctx,
			`SELECT guild_id, title FROM guild_member WHERE player_id = ? LIMIT 1`, playerID).Scan(&guildID, &title)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotInGuild
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM guild_member WHERE player_id = ?`, playerID); err != nil {
			return err
		}
		if title == titleLeader {
			// 会长退出：解散公会
			if _, err := tx.ExecContext(ctx, `DELETE FROM guild_member WHERE guild_id = ?`, guildID); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `DELETE FROM guild WHERE id = ?`, guildID)
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE guild SET member_count = GREATEST(member_count - 1, 0) WHERE id = ?`, guildID)
		// GREATEST 兜底：并发退出时计数不会被减成负数。
		// member_count 是冗余计数，异常路径（手工改库/漏更新）可能已失真，
		// SQL 层兜底比信任应用层更稳——「人数不为负」是数据库 invariant。
		return err
	})
}

// ErrNotInGuild 未加入公会。
var ErrNotInGuild = errors.New("not in guild")

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
type Module struct {
	*module.Thin

	// repo 仓储；nil 表示无 DB 环境，各入口降级为「直接成功」回包
	repo   guildRepo
	logger *log.Logger
}

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
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error { return nil }

// onInfo 查询所在公会；IsLeader 由 LeaderID 与本人比较得出。
//
// IsLeader 用公会行的 leader_id 内存态比较，不再查成员表 title：
// 会长身份以 leader_id 为权威（Quit 解散逻辑也认它），少一次查询
// 且两个判断口径一致，不会出现「title 是会长但 leader_id 是别人」的分歧。
func (m *Module) onInfo(ctx *module.Context, _ *gamev1.GuildInfoReq, resp *gamev1.GuildInfoResp) error {
	// 无 DB 环境按「未入会」回：与 (nil, nil) 同码，客户端行为一致
	if m.repo == nil {
		resp.Code = int32(errcode.ErrGuildNotIn)
		return nil
	}
	g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if g == nil {
		resp.Code = int32(errcode.ErrGuildNotIn)
		return nil
	}
	resp.Code = int32(errcode.OK)
	resp.GuildId = g.ID
	resp.Name = g.Name
	resp.Level = g.Level
	resp.MemberCount = g.MemberCount
	resp.IsLeader = g.LeaderID == ctx.State.PlayerID()
	return nil
}

// onCreate 创建公会：名字按字符（而非字节）校验 2-16，中文名不会被误判。
//
// utf8.RuneCountInString 而非 len：中文名 2 字 = 6 字节，按字节校验会把
// 合法中文名误判超短/超长；TrimSpace 先去首尾空白再计数，
// "  龙  " 这类输入应按 1 字判非法而不是 4 字放行。
func (m *Module) onCreate(ctx *module.Context, req *gamev1.GuildCreateReq, resp *gamev1.GuildCreateResp) error {
	name := strings.TrimSpace(req.Name)
	if utf8.RuneCountInString(name) < 2 || utf8.RuneCountInString(name) > 16 {
		resp.Code = int32(errcode.ErrGuildNameIllegal)
		return nil
	}
	if m.repo == nil {
		// 无 DB 环境：降级直接成功（演示模式，guild_id 留 0）
		resp.Code = int32(errcode.OK)
		return nil
	}
	if g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if g != nil {
		// 已有公会再创建：int32(errcode.ErrGuildAlreadyIn)（一个人只能在一个公会）
		resp.Code = int32(errcode.ErrGuildAlreadyIn)
		return nil
	}
	guildID, err := m.repo.Create(context.Background(), name, ctx.State.PlayerID())
	if err != nil {
		// 哨兵错误转业务码；其余 err 是 DB 真故障，走 error 通道
		if errors.Is(err, ErrNameUsed) {
			resp.Code = int32(errcode.ErrGuildNameUsed)
			return nil
		}
		return err
	}
	resp.Code = int32(errcode.OK)
	resp.GuildId = guildID
	return nil
}

// onJoin 加入公会；仓储错误（不存在/满员/已加入）转业务码。
//
// 前置 FindByPlayer 挡掉「已有公会」的常见情况；switch 把三种哨兵
// 分别转码，default 是 DB 真故障走 error 通道。
// ErrGuildNotExist 复用 int32(errcode.ErrGuildNotIn)：协议未给「目标公会不存在」
// 定义单独码，复用 6101 避免客户端遇到不认识的码。
func (m *Module) onJoin(ctx *module.Context, req *gamev1.GuildJoinReq, resp *gamev1.GuildJoinResp) error {
	if m.repo == nil {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if g != nil {
		resp.Code = int32(errcode.ErrGuildAlreadyIn)
		return nil
	}
	if err := m.repo.Join(context.Background(), req.GuildId, ctx.State.PlayerID()); err != nil {
		switch {
		case errors.Is(err, ErrGuildNotExist):
			resp.Code = int32(errcode.ErrGuildNotIn)
		case errors.Is(err, ErrGuildFull):
			resp.Code = int32(errcode.ErrGuildFull)
		case errors.Is(err, ErrAlreadyIn):
			resp.Code = int32(errcode.ErrGuildAlreadyIn)
		default:
			return err
		}
		return nil
	}
	resp.Code = int32(errcode.OK)
	return nil
}

// onQuit 退出公会；会长退出即解散（仓储层删全员 + 删公会行）。
//
// 「解散」发生在仓储事务内：会长行删除后若后续 DELETE 失败，
// 整个事务回滚，不会出现「会长退了、公会还在」的半解散状态。
func (m *Module) onQuit(ctx *module.Context, _ *gamev1.GuildQuitReq, resp *gamev1.GuildQuitResp) error {
	if m.repo == nil {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if err := m.repo.Quit(context.Background(), ctx.State.PlayerID()); err != nil {
		if errors.Is(err, ErrNotInGuild) {
			resp.Code = int32(errcode.ErrGuildNotIn)
			return nil
		}
		return err
	}
	resp.Code = int32(errcode.OK)
	return nil
}
