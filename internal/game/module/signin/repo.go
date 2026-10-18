package signin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
)

var (
	errAlready      = errors.New("signin already claimed")
	errCycleChanged = errors.New("signin cycle changed; retry request")
)

type snapshotReward struct {
	ItemID int32 `json:"item_id"`
	Count  int64 `json:"count"`
}

type view struct {
	Start  int64
	Day    int32
	Claims map[int64][]module.ItemGrant
}

type Repo struct{ db *db.DB }

func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

// Load 不创建锚点：只有首次成功签到才开始角色周期。
func (r *Repo) Load(ctx context.Context, playerID, today int64) (*view, error) {
	var anchor int64
	err := r.db.QueryRowContext(ctx, `SELECT anchor_day FROM player_signin WHERE player_id = ?`, playerID).Scan(&anchor)
	if errors.Is(err, sql.ErrNoRows) {
		return &view{Start: today, Day: 1, Claims: make(map[int64][]module.ItemGrant)}, nil
	}
	if err != nil {
		return nil, err
	}
	start, day := cycleFor(anchor, today)
	v := &view{Start: start, Day: day, Claims: make(map[int64][]module.ItemGrant)}
	rows, err := r.db.QueryContext(ctx, `SELECT sign_day, rewards FROM signin_claim WHERE player_id = ? AND sign_day >= ? AND sign_day < ?`, playerID, start, start+7)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var signDay int64
		var raw string
		if err := rows.Scan(&signDay, &raw); err != nil {
			return nil, err
		}
		rewards, err := decodeRewards(raw)
		if err != nil {
			return nil, err
		}
		v.Claims[signDay] = rewards
	}
	return v, rows.Err()
}

// Claim 必须在玩家 Actor 的 CommitItems 事务内调用。
// 锚点行作为角色级锁，包含首次并发签到；唯一键提供每日领取的最终约束。
func (r *Repo) Claim(ctx context.Context, tx *sql.Tx, playerID, today, expectedStart int64, expectedDay int32, grants []module.ItemGrant) ([]module.ItemGrant, error) {
	if _, err := tx.ExecContext(ctx, `INSERT INTO player_signin (player_id, anchor_day) VALUES (?, ?) ON DUPLICATE KEY UPDATE player_id = VALUES(player_id)`, playerID, today); err != nil {
		return nil, err
	}
	var anchor int64
	if err := tx.QueryRowContext(ctx, `SELECT anchor_day FROM player_signin WHERE player_id = ? FOR UPDATE`, playerID).Scan(&anchor); err != nil {
		return nil, err
	}
	start, day := cycleFor(anchor, today)
	if day == 0 || start != expectedStart || day != expectedDay {
		return nil, errCycleChanged
	}
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT rewards FROM signin_claim WHERE player_id = ? AND sign_day = ? FOR UPDATE`, playerID, today).Scan(&raw)
	if err == nil {
		stored, err := decodeRewards(raw)
		if err != nil {
			return nil, err
		}
		return stored, errAlready
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	snapshot := make([]snapshotReward, 0, len(grants))
	for _, g := range grants {
		snapshot = append(snapshot, snapshotReward{ItemID: g.ItemID, Count: g.Count})
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO signin_claim (player_id, sign_day, cycle_start_day, day_index, rewards) VALUES (?, ?, ?, ?, ?)`, playerID, today, start, day, string(encoded))
	return grants, err
}

func decodeRewards(raw string) ([]module.ItemGrant, error) {
	var snapshot []snapshotReward
	if err := json.Unmarshal([]byte(raw), &snapshot); err != nil {
		return nil, fmt.Errorf("decode signin rewards: %w", err)
	}
	grants := make([]module.ItemGrant, 0, len(snapshot))
	for _, g := range snapshot {
		if g.ItemID <= 0 || g.Count <= 0 {
			return nil, fmt.Errorf("invalid stored signin reward")
		}
		grants = append(grants, module.ItemGrant{ItemID: g.ItemID, Count: g.Count})
	}
	if len(grants) == 0 {
		return nil, fmt.Errorf("empty stored signin rewards")
	}
	return grants, nil
}
