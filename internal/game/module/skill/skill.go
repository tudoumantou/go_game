// internal/game/module/skill/skill.go —— 技能模块（3201-3206）
package skill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：3201/3202 列表，3203/3204 学习，3205/3206 升级）。
const (
	msgIDListReq     uint32 = 3201
	msgIDListResp    uint32 = 3202
	msgIDLearnReq    uint32 = 3203
	msgIDLearnResp   uint32 = 3204
	msgIDUpgradeReq  uint32 = 3205
	msgIDUpgradeResp uint32 = 3206
)

// 业务码（4.10）。
const (
	codeOK        int32 = 0
	codeNotExist  int32 = 3201
	codeLevelLow  int32 = 3202
	codeLearned   int32 = 3203
	codeNotLearned int32 = 3204
	codeNoResource int32 = 3205
)

// 升级消耗道具（钻石）。
const upgradeItemID int32 = 1001

// Entry 技能实例。
type Entry struct {
	SkillID int32
	Level   int32
}

// Repo 技能仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{db: database}
}

// ListByPlayer 查询玩家技能。
func (r *Repo) ListByPlayer(ctx context.Context, playerID int64) ([]Entry, error) {
	const q = `SELECT skill_id, level FROM player_skill WHERE player_id = ? ORDER BY skill_id`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, fmt.Errorf("list player_skill: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.SkillID, &e.Level); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Get 查询单个技能等级；未学习返回 ok=false。
func (r *Repo) Get(ctx context.Context, playerID int64, skillID int32) (int32, bool, error) {
	const q = `SELECT level FROM player_skill WHERE player_id = ? AND skill_id = ?`
	var level int32
	err := r.db.QueryRowContext(ctx, q, playerID, skillID).Scan(&level)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return level, true, nil
}

// Upsert 学习/更新技能等级（uk_player_skill 幂等）。
func (r *Repo) Upsert(ctx context.Context, playerID int64, skillID, level int32) error {
	const q = `INSERT INTO player_skill (player_id, skill_id, level) VALUES (?, ?, ?)
	           ON DUPLICATE KEY UPDATE level = VALUES(level)`
	_, err := r.db.ExecContext(ctx, q, playerID, skillID, level)
	if err != nil {
		return fmt.Errorf("upsert player_skill: %w", err)
	}
	return nil
}

// Module 技能模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建技能模块。
func New(repo *Repo, logger *log.Logger) *Module {
	return &Module{repo: repo, logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "skill" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDLearnReq, msgIDUpgradeReq} }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDLearnReq:
		return m.onLearn(ctx, body)
	case msgIDUpgradeReq:
		return m.onUpgrade(ctx, body)
	default:
		return fmt.Errorf("skill: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.SkillListResp{Code: codeOK}
	if m.repo == nil {
		return m.reply(ctx, msgIDListResp, resp)
	}
	list, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	for _, e := range list {
		name := ""
		if cfg, ok := gamedata.GetSkillCfg(int64(e.SkillID)); ok {
			name = cfg.Col1
		}
		resp.Skills = append(resp.Skills, &gamev1.SkillEntry{
			SkillId: e.SkillID,
			Name:    name,
			Level:   e.Level,
		})
	}
	return m.reply(ctx, msgIDListResp, resp)
}

func (m *Module) onLearn(ctx *module.Context, body []byte) error {
	var req gamev1.SkillLearnReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SkillLearnReq: %w", err)
	}
	resp := &gamev1.SkillLearnResp{}
	cfg, ok := gamedata.GetSkillCfg(int64(req.SkillId))
	if !ok {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDLearnResp, resp)
	}
	if ctx.State.Profile().Level < int32(cfg.Col2) {
		resp.Code = codeLevelLow
		return m.reply(ctx, msgIDLearnResp, resp)
	}
	if m.repo != nil {
		if _, learned, err := m.repo.Get(context.Background(), ctx.State.PlayerID(), req.SkillId); err != nil {
			return err
		} else if learned {
			resp.Code = codeLearned
			return m.reply(ctx, msgIDLearnResp, resp)
		}
		if err := m.repo.Upsert(context.Background(), ctx.State.PlayerID(), req.SkillId, 1); err != nil {
			return err
		}
	}
	resp.Code = codeOK
	return m.reply(ctx, msgIDLearnResp, resp)
}

func (m *Module) onUpgrade(ctx *module.Context, body []byte) error {
	var req gamev1.SkillUpgradeReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SkillUpgradeReq: %w", err)
	}
	resp := &gamev1.SkillUpgradeResp{}
	cfg, ok := gamedata.GetSkillCfg(int64(req.SkillId))
	if !ok {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDUpgradeResp, resp)
	}
	if m.repo == nil {
		resp.Code = codeNotLearned
		return m.reply(ctx, msgIDUpgradeResp, resp)
	}
	level, learned, err := m.repo.Get(context.Background(), ctx.State.PlayerID(), req.SkillId)
	if err != nil {
		return err
	}
	if !learned {
		resp.Code = codeNotLearned
		return m.reply(ctx, msgIDUpgradeResp, resp)
	}
	// 升级消耗：Col3 * 当前等级 钻石（Col3=0 表示免费）
	cost := int64(cfg.Col3) * int64(level)
	if cost > 0 {
		if ctx.State.GetItemCount(upgradeItemID) < cost {
			resp.Code = codeNoResource
			return m.reply(ctx, msgIDUpgradeResp, resp)
		}
		ctx.State.RemoveItem(upgradeItemID, cost)
	}
	next := level + 1
	if err := m.repo.Upsert(context.Background(), ctx.State.PlayerID(), req.SkillId, next); err != nil {
		return err
	}
	ctx.State.MarkDirty("bag")
	ctx.State.MarkDirty(m.Name())

	resp.Code = codeOK
	resp.Level = next
	return m.reply(ctx, msgIDUpgradeResp, resp)
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
