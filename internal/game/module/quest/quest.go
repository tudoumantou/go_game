// internal/game/module/quest/quest.go —— 任务模块（7101-7106）
package quest

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：7101/7102 列表，7103/7104 接取，7105/7106 领奖）。
const (
	msgIDListReq   uint32 = 7101
	msgIDListResp  uint32 = 7102
	msgIDAcceptReq uint32 = 7103
	msgIDAcceptResp uint32 = 7104
	msgIDClaimReq  uint32 = 7105
	msgIDClaimResp uint32 = 7106
)

// 业务码（4.10）。
const (
	codeOK           int32 = 0
	codeQuestNotExist int32 = 7101
	codeQuestAccepted int32 = 7102
	codeQuestUnfinished int32 = 7103
	codeQuestClaimed int32 = 7104
)

// 任务状态（与 DDL 注释一致）。
const (
	statusDoing   int32 = 0 // 进行中
	statusDone    int32 = 1 // 已完成（可领奖）
	statusClaimed int32 = 2 // 已领奖
)

// Record 玩家任务记录。
type Record struct {
	QuestID  int32
	Status   int32
	Progress int32
}

// Repo 任务仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{db: database}
}

// ListByPlayer 查询玩家全部任务记录。
func (r *Repo) ListByPlayer(ctx context.Context, playerID int64) (map[int32]*Record, error) {
	const q = `SELECT quest_id, status, progress FROM player_quest WHERE player_id = ?`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, fmt.Errorf("list player_quest: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int32]*Record)
	for rows.Next() {
		var rec Record
		if err := rows.Scan(&rec.QuestID, &rec.Status, &rec.Progress); err != nil {
			return nil, err
		}
		out[rec.QuestID] = &rec
	}
	return out, rows.Err()
}

// Upsert 写入/更新任务记录（uk_player_quest 幂等）。
func (r *Repo) Upsert(ctx context.Context, playerID int64, questID, status, progress int32) error {
	const q = `INSERT INTO player_quest (player_id, quest_id, status, progress) VALUES (?, ?, ?, ?)
	           ON DUPLICATE KEY UPDATE status = VALUES(status), progress = VALUES(progress)`
	_, err := r.db.ExecContext(ctx, q, playerID, questID, status, progress)
	if err != nil {
		return fmt.Errorf("upsert player_quest: %w", err)
	}
	return nil
}

// Module 任务模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建任务模块（repo 可为 nil：无 DB 环境降级为内存态不可用）。
func New(repo *Repo, logger *log.Logger) *Module {
	return &Module{repo: repo, logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "quest" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 {
	return []uint32{msgIDListReq, msgIDAcceptReq, msgIDClaimReq}
}

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDAcceptReq:
		return m.onAccept(ctx, body)
	case msgIDClaimReq:
		return m.onClaim(ctx, body)
	default:
		return fmt.Errorf("quest: unexpected msg %d", msgID)
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

// OnEvent 实现 module.EventAware：推进匹配类型的任务进度。
func (m *Module) OnEvent(ctx *module.Context, evt module.Event) {
	if m.repo == nil {
		return
	}
	recs, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return
	}
	for _, cfg := range gamedata.QuestCfgList {
		if cfg.Col2 != evt.Type {
			continue
		}
		// 未接取的任务不推进
		rec, ok := recs[int32(cfg.Id)]
		if !ok || rec.Status != statusDoing {
			continue
		}
		target := int32(cfg.Col3)
		next := rec.Progress + int32(evt.Delta)
		status := statusDoing
		if next >= target {
			next = target
			status = statusDone
		}
		if err := m.repo.Upsert(context.Background(), ctx.State.PlayerID(), int32(cfg.Id), status, next); err != nil {
			m.logger.Warn("quest progress update failed", "quest_id", cfg.Id, "err", err)
			continue
		}
		if status == statusDone {
			m.logger.Info("quest completed", "player_id", ctx.State.PlayerID(), "quest_id", cfg.Id)
		}
	}
}

// onList 任务列表（未接/进行中/可领奖/已领奖四态）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.QuestListResp{Code: codeOK}
	if m.repo == nil {
		return m.replyList(ctx, resp)
	}
	recs, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	for _, cfg := range gamedata.QuestCfgList {
		entry := &gamev1.QuestEntry{
			QuestId: int32(cfg.Id),
			Name:    cfg.Col1,
			Target:  int32(cfg.Col3),
			Status:  0, // 未接
		}
		if rec, ok := recs[int32(cfg.Id)]; ok {
			entry.Progress = rec.Progress
			switch rec.Status {
			case statusClaimed:
				entry.Status = 3
			case statusDone:
				entry.Status = 2
			default:
				if rec.Progress >= int32(cfg.Col3) {
					entry.Status = 2
				} else {
					entry.Status = 1
				}
			}
		}
		resp.Quests = append(resp.Quests, entry)
	}
	return m.replyList(ctx, resp)
}

// onAccept 接取任务。
func (m *Module) onAccept(ctx *module.Context, body []byte) error {
	var req gamev1.QuestAcceptReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode QuestAcceptReq: %w", err)
	}
	resp := &gamev1.QuestAcceptResp{}
	if _, ok := gamedata.GetQuestCfg(int64(req.QuestId)); !ok {
		resp.Code = codeQuestNotExist
		return m.reply(ctx, msgIDAcceptResp, resp)
	}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDAcceptResp, resp)
	}
	recs, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if rec, ok := recs[req.QuestId]; ok && rec.Status != statusClaimed {
		resp.Code = codeQuestAccepted
		return m.reply(ctx, msgIDAcceptResp, resp)
	}
	if err := m.repo.Upsert(context.Background(), ctx.State.PlayerID(), req.QuestId, statusDoing, 0); err != nil {
		return err
	}
	resp.Code = codeOK
	return m.reply(ctx, msgIDAcceptResp, resp)
}

// onClaim 领奖（经验 + 道具）。
func (m *Module) onClaim(ctx *module.Context, body []byte) error {
	var req gamev1.QuestClaimReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode QuestClaimReq: %w", err)
	}
	resp := &gamev1.QuestClaimResp{}
	cfg, ok := gamedata.GetQuestCfg(int64(req.QuestId))
	if !ok {
		resp.Code = codeQuestNotExist
		return m.reply(ctx, msgIDClaimResp, resp)
	}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDClaimResp, resp)
	}
	recs, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	rec, ok := recs[req.QuestId]
	if !ok {
		resp.Code = codeQuestUnfinished
		return m.reply(ctx, msgIDClaimResp, resp)
	}
	if rec.Status == statusClaimed {
		resp.Code = codeQuestClaimed
		return m.reply(ctx, msgIDClaimResp, resp)
	}
	if rec.Progress < int32(cfg.Col3) {
		resp.Code = codeQuestUnfinished
		return m.reply(ctx, msgIDClaimResp, resp)
	}

	// 发奖：经验 + 钻石（配置约定 target*100 经验、target*10 钻石）
	expGain := int64(cfg.Col3) * 100
	itemCount := int64(cfg.Col3) * 10
	ctx.State.AddItem(1001, itemCount)
	ctx.State.MarkDirty("bag")

	if err := m.repo.Upsert(context.Background(), ctx.State.PlayerID(), req.QuestId, statusClaimed, rec.Progress); err != nil {
		return err
	}

	resp.Code = codeOK
	resp.ExpGain = expGain
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{{
		ItemUid: 1001,
		ItemId:  1001,
		Count:   int32(ctx.State.GetItemCount(1001)),
		Change:  1,
	}}}); err == nil {
		resp.Rewards = raw
	}
	if err := m.reply(ctx, msgIDClaimResp, resp); err != nil {
		return err
	}
	// 发经验（Actor 内部会推送等级变化）
	if ctx.AddExp != nil {
		ctx.AddExp(expGain)
	}
	return nil
}

func (m *Module) replyList(ctx *module.Context, resp *gamev1.QuestListResp) error {
	return m.reply(ctx, msgIDListResp, resp)
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
