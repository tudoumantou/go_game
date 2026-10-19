package signin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"game_study/pkg/flush"
	"game_study/pkg/writebehind"
)

// stateKey 同时是 Snapshot.Ext 的键与落库归属的模块名。
const stateKey = "signin"

// Data 是签到的玩家私有状态：周期锚点 + 每个业务日的领取快照。
//
// JSON 字段与旧版内建结构完全一致，因此 Redis 中旧快照可以直接迁移进
// Ext["signin"]，升级不需要停机刷新热数据。
type Data struct {
	AnchorDay int64                             `json:"anchor_day"`
	Claims    map[int64][]writebehind.ItemGrant `json:"claims"`
}

// persister 把签到状态接入统一的快照、Redis 提交与独立落库。
type persister struct{}

// init 注册扩展点：只要 signin 包被业务代码导入，冷启动读取与落库就同时生效，
// 不会出现「协议已注册但数据不落库」的半接入状态。
func init() { writebehind.RegisterPersister(persister{}) }

func (persister) Name() string { return stateKey }

func (persister) Validate(raw json.RawMessage) error {
	var d Data
	if err := json.Unmarshal(raw, &d); err != nil {
		return fmt.Errorf("decode signin state: %w", err)
	}
	if d.AnchorDay <= 0 {
		return fmt.Errorf("invalid signin anchor %d", d.AnchorDay)
	}
	for day, rewards := range d.Claims {
		if day < d.AnchorDay || len(rewards) == 0 {
			return fmt.Errorf("invalid signin day %d", day)
		}
		for _, g := range rewards {
			if g.ItemID <= 0 || g.Count <= 0 {
				return fmt.Errorf("invalid signin reward on day %d", day)
			}
		}
	}
	return nil
}

// PersistBatch 在一个事务内写入本批玩家的锚点与领取流水。
func (persister) PersistBatch(ctx context.Context, tx *sql.Tx, rows []writebehind.ModuleRow) error {
	anchors := make([][]any, 0, len(rows))
	claims := make([][]any, 0)
	for _, row := range rows {
		var d Data
		if err := json.Unmarshal(row.Data, &d); err != nil {
			return fmt.Errorf("decode signin state for player %d: %w", row.PlayerID, err)
		}
		anchors = append(anchors, []any{row.PlayerID, d.AnchorDay})
		days := make([]int64, 0, len(d.Claims))
		for day := range d.Claims {
			days = append(days, day)
		}
		sort.Slice(days, func(i, j int) bool { return days[i] < days[j] })
		for _, day := range days {
			raw, err := json.Marshal(d.Claims[day])
			if err != nil {
				return fmt.Errorf("encode signin rewards for player %d day %d: %w", row.PlayerID, day, err)
			}
			start, _ := cycleFor(d.AnchorDay, day)
			claims = append(claims, []any{row.PlayerID, day, start, int32(day - start + 1), string(raw)})
		}
	}
	if err := upsertRows(ctx, tx, "player_signin", []string{"player_id", "anchor_day"}, anchors); err != nil {
		return err
	}
	return upsertRows(ctx, tx, "signin_claim", []string{"player_id", "sign_day", "cycle_start_day", "day_index", "rewards"}, claims)
}

// Load 冷启动读取：没有锚点行说明该角色从未签到。
func (persister) Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error) {
	var anchor int64
	err := tx.QueryRowContext(ctx, `SELECT anchor_day FROM player_signin WHERE player_id = ?`, playerID).Scan(&anchor)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	data := Data{AnchorDay: anchor, Claims: make(map[int64][]writebehind.ItemGrant)}
	rows, err := tx.QueryContext(ctx, `SELECT sign_day, rewards FROM signin_claim WHERE player_id = ?`, playerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var day int64
		var raw string
		if err := rows.Scan(&day, &raw); err != nil {
			return nil, err
		}
		var items []writebehind.ItemGrant
		if err := json.Unmarshal([]byte(raw), &items); err != nil {
			return nil, fmt.Errorf("decode signin rewards on day %d: %w", day, err)
		}
		data.Claims[day] = items
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// upsertRows 按行数与字节数双约束分批写入，复用 flush 的批量 SQL 生成。
func upsertRows(ctx context.Context, tx *sql.Tx, table string, cols []string, rows [][]any) error {
	for _, batch := range flush.Chunk(rows, flush.DefaultBatchRows, flush.DefaultBatchBytes) {
		query, args, err := flush.BuildBatchUpsert(table, cols, batch)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}
