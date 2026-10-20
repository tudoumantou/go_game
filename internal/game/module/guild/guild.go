// internal/game/module/guild/guild.go —— 公会模块（6101-6108）
package guild

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：6101/6102 信息，6103/6104 创建，6105/6106 加入，6107/6108 退出）。
const (
	msgIDInfoReq    = codec.MsgGuildInfoReq
	msgIDInfoResp   = codec.MsgGuildInfoResp
	msgIDCreateReq  = codec.MsgGuildCreateReq
	msgIDCreateResp = codec.MsgGuildCreateResp
	msgIDJoinReq    = codec.MsgGuildJoinReq
	msgIDJoinResp   = codec.MsgGuildJoinResp
	msgIDQuitReq    = codec.MsgGuildQuitReq
	msgIDQuitResp   = codec.MsgGuildQuitResp
)

// 业务码（4.10）。
const (
	codeOK          int32 = 0
	codeNotInGuild  int32 = 6101
	codeNameIllegal int32 = 6102
	codeNameUsed    int32 = 6103
	codeAlreadyIn   int32 = 6104
	codeGuildFull   int32 = 6105
)

// maxMembers 公会人数上限：按 guild_level.xml（col1=人数上限, col2=等级）取当前等级；
// 缺配置回退 50，避免配置漏项导致公会无法收人。
func maxMembers(level int32) int {
	for _, cfg := range gamedata.GuildLevelCfgList() {
		if int32(cfg.Col2) == level && cfg.Col1 > 0 {
			return int(cfg.Col1)
		}
	}
	return 50
}

// 职位。
const (
	titleMember int32 = 0
	titleLeader int32 = 1
)

// Guild 公会信息。
//
// 字段语义：MemberCount 是冗余计数（Join/Quit 时 ±1），人数上限按 Level 查
// guild_level.xml；Level 为公会等级，与成员等级无关。
type Guild struct {
	ID          int64
	Name        string
	LeaderID    int64
	Level       int32
	MemberCount int32
}

// Repo 公会仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

// FindByPlayer 查询玩家所在公会。
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
var ErrNameUsed = errors.New("guild name used")

// Join 加入公会（事务：成员 + 人数 +1，超上限回滚）。
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
			if strings.Contains(err.Error(), "1062") {
				return ErrAlreadyIn
			}
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE guild SET member_count = member_count + 1 WHERE id = ?`, guildID)
		return err
	})
}

// 仓储错误。
var (
	ErrGuildNotExist = errors.New("guild not exist")
	ErrGuildFull     = errors.New("guild full")
	ErrAlreadyIn     = errors.New("already in guild")
)

// Quit 退出公会（会长退出则解散）。
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
		return err
	})
}

// ErrNotInGuild 未加入公会。
var ErrNotInGuild = errors.New("not in guild")

// Module 公会模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建公会模块。
func New(repo *Repo, logger *log.Logger) *Module { return &Module{repo: repo, logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return "guild" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 {
	return []uint32{msgIDInfoReq, msgIDCreateReq, msgIDJoinReq, msgIDQuitReq}
}

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDInfoReq:
		return m.onInfo(ctx)
	case msgIDCreateReq:
		return m.onCreate(ctx, body)
	case msgIDJoinReq:
		return m.onJoin(ctx, body)
	case msgIDQuitReq:
		return m.onQuit(ctx)
	default:
		return fmt.Errorf("guild: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error { return nil }

// onInfo 查询所在公会；IsLeader 由 LeaderID 与本人比较得出。
func (m *Module) onInfo(ctx *module.Context) error {
	resp := &gamev1.GuildInfoResp{}
	if m.repo == nil {
		resp.Code = codeNotInGuild
		return m.reply(ctx, msgIDInfoResp, resp)
	}
	g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if g == nil {
		resp.Code = codeNotInGuild
		return m.reply(ctx, msgIDInfoResp, resp)
	}
	resp.Code = codeOK
	resp.GuildId = g.ID
	resp.Name = g.Name
	resp.Level = g.Level
	resp.MemberCount = g.MemberCount
	resp.IsLeader = g.LeaderID == ctx.State.PlayerID()
	return m.reply(ctx, msgIDInfoResp, resp)
}

// onCreate 创建公会：名字按字符（而非字节）校验 2-16，中文名不会被误判。
func (m *Module) onCreate(ctx *module.Context, body []byte) error {
	var req gamev1.GuildCreateReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode GuildCreateReq: %w", err)
	}
	resp := &gamev1.GuildCreateResp{}
	name := strings.TrimSpace(req.Name)
	if utf8.RuneCountInString(name) < 2 || utf8.RuneCountInString(name) > 16 {
		resp.Code = codeNameIllegal
		return m.reply(ctx, msgIDCreateResp, resp)
	}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDCreateResp, resp)
	}
	if g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if g != nil {
		resp.Code = codeAlreadyIn
		return m.reply(ctx, msgIDCreateResp, resp)
	}
	guildID, err := m.repo.Create(context.Background(), name, ctx.State.PlayerID())
	if err != nil {
		if errors.Is(err, ErrNameUsed) {
			resp.Code = codeNameUsed
			return m.reply(ctx, msgIDCreateResp, resp)
		}
		return err
	}
	resp.Code = codeOK
	resp.GuildId = guildID
	return m.reply(ctx, msgIDCreateResp, resp)
}

// onJoin 加入公会；仓储错误（不存在/满员/已加入）转业务码。
func (m *Module) onJoin(ctx *module.Context, body []byte) error {
	var req gamev1.GuildJoinReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode GuildJoinReq: %w", err)
	}
	resp := &gamev1.GuildJoinResp{}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDJoinResp, resp)
	}
	if g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if g != nil {
		resp.Code = codeAlreadyIn
		return m.reply(ctx, msgIDJoinResp, resp)
	}
	if err := m.repo.Join(context.Background(), req.GuildId, ctx.State.PlayerID()); err != nil {
		switch {
		case errors.Is(err, ErrGuildNotExist):
			resp.Code = codeNotInGuild
		case errors.Is(err, ErrGuildFull):
			resp.Code = codeGuildFull
		case errors.Is(err, ErrAlreadyIn):
			resp.Code = codeAlreadyIn
		default:
			return err
		}
		return m.reply(ctx, msgIDJoinResp, resp)
	}
	resp.Code = codeOK
	return m.reply(ctx, msgIDJoinResp, resp)
}

// onQuit 退出公会；会长退出即解散（仓储层删全员 + 删公会行）。
func (m *Module) onQuit(ctx *module.Context) error {
	resp := &gamev1.GuildQuitResp{}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDQuitResp, resp)
	}
	if err := m.repo.Quit(context.Background(), ctx.State.PlayerID()); err != nil {
		if errors.Is(err, ErrNotInGuild) {
			resp.Code = codeNotInGuild
			return m.reply(ctx, msgIDQuitResp, resp)
		}
		return err
	}
	resp.Code = codeOK
	return m.reply(ctx, msgIDQuitResp, resp)
}

// reply 序列化 proto 并经 ctx.Reply 下发。
func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
