// internal/game/module/team/team.go —— 组队模块（6201-6208）
package team

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：6201/6202 信息，6203/6204 创建，6205/6206 加入，6207/6208 退出）。
const (
	msgIDInfoReq    uint32 = 6201
	msgIDInfoResp   uint32 = 6202
	msgIDCreateReq  uint32 = 6203
	msgIDCreateResp uint32 = 6204
	msgIDJoinReq    uint32 = 6205
	msgIDJoinResp   uint32 = 6206
	msgIDLeaveReq   uint32 = 6207
	msgIDLeaveResp  uint32 = 6208
)

// 业务码（4.10）。
const (
	codeOK         int32 = 0
	codeNotInTeam  int32 = 6201
	codeAlreadyIn  int32 = 6202
	codeTeamNotExist int32 = 6203
	codeTeamFull   int32 = 6204
)

// maxMembers 队伍上限（P6 固定，P7 接 team_limit.xml）。
const maxMembers = 5

// Member 队伍成员。
type Member struct {
	PlayerID int64
	Name     string
	Ready    bool
	IsLeader bool
}

// Team 队伍信息。
type Team struct {
	ID       int64
	LeaderID int64
	CopyID   int32
	Status   int32
}

// Repo 组队仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

// FindByPlayer 查询玩家所在队伍。
func (r *Repo) FindByPlayer(ctx context.Context, playerID int64) (*Team, error) {
	const q = `SELECT t.id, t.leader_id, t.copy_id, t.status
	           FROM team t JOIN team_member m ON m.team_id = t.id
	           WHERE m.player_id = ? AND t.status != 2 LIMIT 1`
	var t Team
	err := r.db.QueryRowContext(ctx, q, playerID).Scan(&t.ID, &t.LeaderID, &t.CopyID, &t.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// Create 创建队伍并把队长加入。
func (r *Repo) Create(ctx context.Context, leaderID int64, copyID int32) (int64, error) {
	var teamID int64
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`INSERT INTO team (leader_id, copy_id, status) VALUES (?, ?, 0)`, leaderID, copyID)
		if err != nil {
			return err
		}
		teamID, err = res.LastInsertId()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO team_member (team_id, player_id, ready) VALUES (?, ?, 0)`, teamID, leaderID)
		return err
	})
	return teamID, err
}

// Join 加入队伍（事务：人数校验 + 插入，防并发超员）。
func (r *Repo) Join(ctx context.Context, teamID, playerID int64) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		var leaderID int64
		var status int32
		err := tx.QueryRowContext(ctx,
			`SELECT leader_id, status FROM team WHERE id = ? FOR UPDATE`, teamID).Scan(&leaderID, &status)
		if errors.Is(err, sql.ErrNoRows) || status == 2 {
			return ErrTeamNotExist
		}
		if err != nil {
			return err
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM team_member WHERE team_id = ?`, teamID).Scan(&count); err != nil {
			return err
		}
		if count >= maxMembers {
			return ErrTeamFull
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO team_member (team_id, player_id, ready) VALUES (?, ?, 0)`, teamID, playerID); err != nil {
			if strings.Contains(err.Error(), "1062") {
				return ErrAlreadyIn
			}
			return err
		}
		return nil
	})
}

// Leave 退出队伍（队长退出则解散）。
func (r *Repo) Leave(ctx context.Context, playerID int64) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		var teamID, leaderID int64
		err := tx.QueryRowContext(ctx,
			`SELECT t.id, t.leader_id FROM team t JOIN team_member m ON m.team_id = t.id
			 WHERE m.player_id = ? AND t.status != 2 LIMIT 1`, playerID).Scan(&teamID, &leaderID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotInTeam
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM team_member WHERE team_id = ? AND player_id = ?`, teamID, playerID); err != nil {
			return err
		}
		if leaderID == playerID {
			if _, err := tx.ExecContext(ctx, `DELETE FROM team_member WHERE team_id = ?`, teamID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE team SET status = 2 WHERE id = ?`, teamID); err != nil {
				return err
			}
		}
		return nil
	})
}

// Members 查询队伍成员（联 player 取名字）。
func (r *Repo) Members(ctx context.Context, teamID int64) ([]Member, error) {
	const q = `SELECT m.player_id, COALESCE(p.name, ''), m.ready, t.leader_id
	           FROM team_member m
	           JOIN team t ON t.id = m.team_id
	           LEFT JOIN player p ON p.id = m.player_id
	           WHERE m.team_id = ? ORDER BY m.id`
	rows, err := r.db.QueryContext(ctx, q, teamID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Member
	for rows.Next() {
		var m Member
		var leaderID int64
		if err := rows.Scan(&m.PlayerID, &m.Name, &m.Ready, &leaderID); err != nil {
			return nil, err
		}
		m.IsLeader = m.PlayerID == leaderID
		out = append(out, m)
	}
	return out, rows.Err()
}

// 仓储错误。
var (
	ErrTeamNotExist = errors.New("team not exist")
	ErrTeamFull     = errors.New("team full")
	ErrAlreadyIn    = errors.New("already in team")
	ErrNotInTeam    = errors.New("not in team")
)

// Module 组队模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建组队模块。
func New(repo *Repo, logger *log.Logger) *Module { return &Module{repo: repo, logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return "team" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 {
	return []uint32{msgIDInfoReq, msgIDCreateReq, msgIDJoinReq, msgIDLeaveReq}
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
	case msgIDLeaveReq:
		return m.onLeave(ctx)
	default:
		return fmt.Errorf("team: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module：掉线自动退队。
func (m *Module) OnLogout(ctx *module.Context) error {
	if m.repo == nil {
		return nil
	}
	_ = m.repo.Leave(context.Background(), ctx.State.PlayerID())
	return nil
}

func (m *Module) onInfo(ctx *module.Context) error {
	resp := &gamev1.TeamInfoResp{}
	if m.repo == nil {
		resp.Code = codeNotInTeam
		return m.reply(ctx, msgIDInfoResp, resp)
	}
	t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if t == nil {
		resp.Code = codeNotInTeam
		return m.reply(ctx, msgIDInfoResp, resp)
	}
	members, err := m.repo.Members(context.Background(), t.ID)
	if err != nil {
		return err
	}
	resp.Code = codeOK
	resp.TeamId = t.ID
	resp.CopyId = t.CopyID
	for _, mb := range members {
		resp.Members = append(resp.Members, &gamev1.TeamMemberInfo{
			PlayerId: mb.PlayerID,
			Name:     mb.Name,
			Ready:    mb.Ready,
			IsLeader: mb.IsLeader,
		})
	}
	return m.reply(ctx, msgIDInfoResp, resp)
}

func (m *Module) onCreate(ctx *module.Context, body []byte) error {
	var req gamev1.TeamCreateReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode TeamCreateReq: %w", err)
	}
	resp := &gamev1.TeamCreateResp{}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDCreateResp, resp)
	}
	if t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if t != nil {
		resp.Code = codeAlreadyIn
		return m.reply(ctx, msgIDCreateResp, resp)
	}
	teamID, err := m.repo.Create(context.Background(), ctx.State.PlayerID(), req.CopyId)
	if err != nil {
		return err
	}
	resp.Code = codeOK
	resp.TeamId = teamID
	return m.reply(ctx, msgIDCreateResp, resp)
}

func (m *Module) onJoin(ctx *module.Context, body []byte) error {
	var req gamev1.TeamJoinReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode TeamJoinReq: %w", err)
	}
	resp := &gamev1.TeamJoinResp{}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDJoinResp, resp)
	}
	if t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if t != nil {
		resp.Code = codeAlreadyIn
		return m.reply(ctx, msgIDJoinResp, resp)
	}
	if err := m.repo.Join(context.Background(), req.TeamId, ctx.State.PlayerID()); err != nil {
		switch {
		case errors.Is(err, ErrTeamNotExist):
			resp.Code = codeTeamNotExist
		case errors.Is(err, ErrTeamFull):
			resp.Code = codeTeamFull
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

func (m *Module) onLeave(ctx *module.Context) error {
	resp := &gamev1.TeamLeaveResp{}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDLeaveResp, resp)
	}
	if err := m.repo.Leave(context.Background(), ctx.State.PlayerID()); err != nil {
		if errors.Is(err, ErrNotInTeam) {
			resp.Code = codeNotInTeam
			return m.reply(ctx, msgIDLeaveResp, resp)
		}
		return err
	}
	resp.Code = codeOK
	return m.reply(ctx, msgIDLeaveResp, resp)
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
