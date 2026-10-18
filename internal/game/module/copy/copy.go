// internal/game/module/copy/copy.go —— 副本模块（6301-6304）
package copy

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

// 协议号（4.9：6301/6302 列表，6303/6304 结算）。
const (
	msgIDListReq    uint32 = 6301
	msgIDListResp   uint32 = 6302
	msgIDSettleReq  uint32 = 6303
	msgIDSettleResp uint32 = 6304
)

// 业务码（4.10）。
const (
	codeOK            int32 = 0
	codeCopyNotExist  int32 = 6301
	codeLevelNotEnough int32 = 6302
)

// Record 副本记录。
type Record struct {
	CopyID    int32
	Star      int32
	PassCount int32
	FirstPass bool
}

// Repo 副本仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

// ListByPlayer 查询玩家全部副本记录。
func (r *Repo) ListByPlayer(ctx context.Context, playerID int64) (map[int32]*Record, error) {
	const q = `SELECT copy_id, star, pass_count, first_pass FROM copy_record WHERE player_id = ?`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, fmt.Errorf("list copy_record: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int32]*Record)
	for rows.Next() {
		var rec Record
		if err := rows.Scan(&rec.CopyID, &rec.Star, &rec.PassCount, &rec.FirstPass); err != nil {
			return nil, err
		}
		out[rec.CopyID] = &rec
	}
	return out, rows.Err()
}

// Settle 通关结算落库（事务 + FOR UPDATE：首通判定与计数原子）。
// 返回是否首通。cb 在事务内被调用（用于同事务内发奖，P6 简化为事务外发奖，
// 故此处仅返回计算所需信息）。
func (r *Repo) Settle(ctx context.Context, playerID int64, copyID, star int32) (firstPass bool, err error) {
	err = r.db.Tx(ctx, func(tx *sql.Tx) error {
		var bestStar, passCount int32
		var first int8
		q := `SELECT star, pass_count, first_pass FROM copy_record
		      WHERE player_id = ? AND copy_id = ? FOR UPDATE`
		serr := tx.QueryRowContext(ctx, q, playerID, copyID).Scan(&bestStar, &passCount, &first)
		switch {
		case errors.Is(serr, sql.ErrNoRows):
			// 首次进入该副本：直接首通
			firstPass = true
			_, err := tx.ExecContext(ctx,
				`INSERT INTO copy_record (player_id, copy_id, star, pass_count, first_pass)
				 VALUES (?, ?, ?, 1, 1)`, playerID, copyID, star)
			return err
		case serr != nil:
			return serr
		}
		firstPass = first == 0
		if star > bestStar {
			bestStar = star
		}
		// first_pass 一旦置 1 不再回退（首通奖终身一次）
		newFirst := int8(0)
		if firstPass {
			newFirst = 1
		}
		_, err := tx.ExecContext(ctx,
			`UPDATE copy_record SET star = ?, pass_count = pass_count + 1,
			        first_pass = GREATEST(first_pass, ?)
			 WHERE player_id = ? AND copy_id = ?`,
			bestStar, newFirst, playerID, copyID)
		return err
	})
	return firstPass, err
}

// Module 副本模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建副本模块。
func New(repo *Repo, logger *log.Logger) *Module { return &Module{repo: repo, logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return "copy" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 {
	return []uint32{msgIDListReq, msgIDSettleReq}
}

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDSettleReq:
		return m.onSettle(ctx, body)
	default:
		return fmt.Errorf("copy: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error { return nil }

// onList 副本列表（全部配置 + 玩家记录）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.CopyListResp{Code: codeOK}
	var recs map[int32]*Record
	if m.repo != nil {
		var err error
		recs, err = m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
		if err != nil {
			return err
		}
	}
	level := ctx.State.Profile().Level
	for _, cfg := range gamedata.CopyCfgList {
		// 等级不足仍下发（客户端置灰展示），结算时强校验
		entry := &gamev1.CopyEntry{
			CopyId:   int32(cfg.Id),
			Name:     cfg.Col1,
			MinLevel: int32(cfg.Col2),
		}
		if rec, ok := recs[int32(cfg.Id)]; ok {
			entry.BestStar = rec.Star
			entry.PassCount = rec.PassCount
		}
		resp.Copies = append(resp.Copies, entry)
	}
	_ = level
	return m.reply(ctx, msgIDListResp, resp)
}

// onSettle 通关结算（首通发奖：exp + 首通道具）。
func (m *Module) onSettle(ctx *module.Context, body []byte) error {
	var req gamev1.CopySettleReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode CopySettleReq: %w", err)
	}
	resp := &gamev1.CopySettleResp{}

	cfg, ok := gamedata.GetCopyCfg(int64(req.CopyId))
	if !ok {
		resp.Code = codeCopyNotExist
		return m.reply(ctx, msgIDSettleResp, resp)
	}
	if ctx.State.Profile().Level < int32(cfg.Col2) {
		resp.Code = codeLevelNotEnough
		return m.reply(ctx, msgIDSettleResp, resp)
	}
	star := req.Star
	if star < 1 {
		star = 1
	}
	if star > 3 {
		star = 3
	}
	if m.repo == nil {
		resp.Code = codeOK
		return m.reply(ctx, msgIDSettleResp, resp)
	}

	firstPass, err := m.repo.Settle(context.Background(), ctx.State.PlayerID(), req.CopyId, star)
	if err != nil {
		return err
	}
	resp.Code = codeOK
	resp.FirstPass = firstPass

	// 通关经验：等级门槛 * 50（P7 接 copy.xml 掉落列）
	expGain := cfg.Col2 * 50
	if cfg.Col2 <= 0 {
		expGain = 50
	}
	resp.ExpGain = expGain

	// 首通发奖：按 copy_drop.xml 中 col1 = copy_id 的行发道具
	var changes []*gamev1.ItemChange
	if firstPass {
		for _, drop := range gamedata.CopyDropCfgList {
			if drop.Col1 != cfg.Id {
				continue
			}
			itemID := int32(drop.Col2)
			count := drop.Col3
			ctx.State.AddItem(itemID, count)
			changes = append(changes, &gamev1.ItemChange{
				ItemUid: int64(itemID),
				ItemId:  itemID,
				Count:   int32(ctx.State.GetItemCount(itemID)),
				Change:  1,
			})
		}
		if len(changes) > 0 {
			ctx.State.MarkDirty("bag")
			if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: changes}); err == nil {
				resp.Rewards = raw
			}
		}
	}

	if err := m.reply(ctx, msgIDSettleResp, resp); err != nil {
		return err
	}
	if ctx.AddExp != nil {
		ctx.AddExp(expGain)
	}
	// 事件：驱动任务/活动（kill 语义的副本通关）
	if ctx.Emit != nil {
		ctx.Emit(module.Event{Type: "copy_pass", Key: fmt.Sprintf("%d", cfg.Id), Delta: 1})
	}
	return nil
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
