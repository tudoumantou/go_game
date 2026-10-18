// internal/game/module/activity/activity.go —— 活动模块（6501-6504）
package activity

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

// 协议号（4.9：6501/6502 列表，6503/6504 领奖）。
const (
	msgIDListReq  uint32 = 6501
	msgIDListResp uint32 = 6502
	msgIDClaimReq uint32 = 6503
	msgIDClaimResp uint32 = 6504
)

// 业务码（4.10）。
const (
	codeOK              int32 = 0
	codeActivityNotExist int32 = 6501
	codeNotFinished     int32 = 6502
	codeAlreadyClaimed  int32 = 6503
)

// 奖励道具（P6 固定：钻石 1001）。
const itemDiamond int32 = 1001

// Record 活动记录。
type Record struct {
	ActivityID int32
	Progress   int32
	RewardMask int64
}

// Repo 活动仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

// ListByPlayer 查询玩家全部活动记录。
func (r *Repo) ListByPlayer(ctx context.Context, playerID int64) (map[int32]*Record, error) {
	const q = `SELECT activity_id, progress, reward_mask FROM activity_record WHERE player_id = ?`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, fmt.Errorf("list activity_record: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int32]*Record)
	for rows.Next() {
		var rec Record
		if err := rows.Scan(&rec.ActivityID, &rec.Progress, &rec.RewardMask); err != nil {
			return nil, err
		}
		out[rec.ActivityID] = &rec
	}
	return out, rows.Err()
}

// Claim 领取奖励（事务 + FOR UPDATE：位图判定与置位原子，防并发重复领）。
// 返回 (是否已领过, 领后进度, 错误)。
func (r *Repo) Claim(ctx context.Context, playerID int64, activityID, target int32, bit uint) (bool, int32, error) {
	var claimed bool
	var progress int32
	mask := int64(1) << bit

	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		var progressDB, maskDB int64
		q := `SELECT progress, reward_mask FROM activity_record
		      WHERE player_id = ? AND activity_id = ? FOR UPDATE`
		serr := tx.QueryRowContext(ctx, q, playerID, activityID).Scan(&progressDB, &maskDB)
		switch {
		case errors.Is(serr, sql.ErrNoRows):
			// 无记录：进度 0，按未达标处理（由调用方在事务前已校验进度，此处兜底）
			progress = 0
			return nil
		case serr != nil:
			return serr
		}
		progress = int32(progressDB)
		if maskDB&mask != 0 {
			claimed = true
			return nil
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE activity_record SET reward_mask = reward_mask | ? WHERE player_id = ? AND activity_id = ?`,
			mask, playerID, activityID)
		return err
	})
	return claimed, progress, err
}

// UpsertProgress 写入活动进度（事件驱动，取最大值防回退）。
func (r *Repo) UpsertProgress(ctx context.Context, playerID int64, activityID, progress int32) error {
	const q = `INSERT INTO activity_record (player_id, activity_id, progress, reward_mask, joined_at)
	           VALUES (?, ?, ?, 0, NOW())
	           ON DUPLICATE KEY UPDATE progress = GREATEST(progress, VALUES(progress))`
	if _, err := r.db.ExecContext(ctx, q, playerID, activityID, progress); err != nil {
		return fmt.Errorf("upsert activity_record: %w", err)
	}
	return nil
}

// Module 活动模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建活动模块。
func New(repo *Repo, logger *log.Logger) *Module { return &Module{repo: repo, logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return "activity" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDClaimReq} }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDClaimReq:
		return m.onClaim(ctx, body)
	default:
		return fmt.Errorf("activity: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module：登录活动（activity.xml type=login）进度 +1。
//
// P12：优先用登录预加载的进度（省一次查询）。
// 修复：原实现在 cfg 循环内反复调 repo.ListByPlayer（每个活动查一次库），改为只查一次。
func (m *Module) OnLogin(ctx *module.Context) error {
	if m.repo == nil {
		return nil
	}

	progress := map[int32]int32{}
	if ctx.Prefetch != nil {
		for _, a := range ctx.Prefetch.Activities() {
			progress[a.ActivityID] = a.Progress
		}
	} else {
		recs, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
		if err != nil {
			return nil // 预加载失败不阻断登录，下一轮再补
		}
		for id, rec := range recs {
			progress[id] = rec.Progress
		}
	}

	for _, cfg := range gamedata.ActivityCfgList {
		if cfg.Col2 != "login" {
			continue
		}
		cur := progress[int32(cfg.Id)]
		if cur >= int32(cfg.Col3) {
			continue
		}
		if err := m.repo.UpsertProgress(context.Background(), ctx.State.PlayerID(), int32(cfg.Id), cur+1); err != nil {
			m.logger.Warn("activity login progress failed", "activity_id", cfg.Id, "err", err)
		}
	}
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error { return nil }

// OnEvent 实现 module.EventAware：把玩家行为事件累加为活动进度。
func (m *Module) OnEvent(ctx *module.Context, evt module.Event) {
	if m.repo == nil {
		return
	}
	for _, cfg := range gamedata.ActivityCfgList {
		if cfg.Col2 != evt.Type {
			continue
		}
		var progress int32
		recs, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
		if err != nil {
			return
		}
		if rec, ok := recs[int32(cfg.Id)]; ok {
			progress = rec.Progress
		}
		next := progress + int32(evt.Delta)
		if next > int32(cfg.Col3) {
			next = int32(cfg.Col3)
		}
		if err := m.repo.UpsertProgress(context.Background(), ctx.State.PlayerID(), int32(cfg.Id), next); err != nil {
			m.logger.Warn("activity progress update failed", "activity_id", cfg.Id, "err", err)
		}
	}
}

// onList 活动列表（配置 + 进度 + 是否已领）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.ActivityListResp{Code: codeOK}
	recs := map[int32]*Record{}
	if m.repo != nil {
		var err error
		recs, err = m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
		if err != nil {
			return err
		}
	}
	for _, cfg := range gamedata.ActivityCfgList {
		entry := &gamev1.ActivityEntry{
			ActivityId: int32(cfg.Id),
			Name:       cfg.Col1,
			Type:       cfg.Col2,
			Target:     int32(cfg.Col3),
		}
		if rec, ok := recs[int32(cfg.Id)]; ok {
			entry.Progress = rec.Progress
			entry.Claimed = rec.RewardMask&1 != 0
		}
		resp.Activities = append(resp.Activities, entry)
	}
	return m.reply(ctx, msgIDListResp, resp)
}

// onClaim 领奖（进度达标 + 位图防重复）。
func (m *Module) onClaim(ctx *module.Context, body []byte) error {
	var req gamev1.ActivityClaimReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode ActivityClaimReq: %w", err)
	}
	resp := &gamev1.ActivityClaimResp{}

	cfg, ok := gamedata.GetActivityCfg(int64(req.ActivityId))
	if !ok {
		resp.Code = codeActivityNotExist
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
	rec, hasRec := recs[req.ActivityId]
	if !hasRec || rec.Progress < int32(cfg.Col3) {
		resp.Code = codeNotFinished
		return m.reply(ctx, msgIDClaimResp, resp)
	}
	if rec.RewardMask&1 != 0 {
		resp.Code = codeAlreadyClaimed
		return m.reply(ctx, msgIDClaimResp, resp)
	}

	claimed, _, err := m.repo.Claim(context.Background(), ctx.State.PlayerID(), req.ActivityId, int32(cfg.Col3), 0)
	if err != nil {
		return err
	}
	if claimed {
		resp.Code = codeAlreadyClaimed
		return m.reply(ctx, msgIDClaimResp, resp)
	}

	// 发奖：target * 20 钻石
	gain := int64(cfg.Col3) * 20
	ctx.State.AddItem(itemDiamond, gain)
	ctx.State.MarkDirty("bag")

	resp.Code = codeOK
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{{
		ItemUid: int64(itemDiamond),
		ItemId:  itemDiamond,
		Count:   int32(ctx.State.GetItemCount(itemDiamond)),
		Change:  1,
	}}}); err == nil {
		resp.Rewards = raw
	}
	return m.reply(ctx, msgIDClaimResp, resp)
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
