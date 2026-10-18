package player

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"game_study/pkg/writebehind"
)

// LoadCached 只在热数据不存在时回源；Redis 故障或损坏时不使用旧 SQL 冒充最新状态。
func (r *Repository) LoadCached(ctx context.Context, id int64) (*State, int64, error) {
	record, err := r.cache.Load(ctx, id)
	if errors.Is(err, writebehind.ErrMiss) {
		cold, err := r.loadColdSnapshot(ctx, id)
		if err != nil {
			return nil, 0, err
		}
		record, err = r.cache.Seed(ctx, cold.State, cold.Version)
		if err != nil {
			return nil, 0, err
		}
	} else if err != nil {
		return nil, 0, err
	}
	return stateFromSnapshot(record.State), record.Version, nil
}

// SQL 只承担一致性冷启动读取，不参与在线签到查询、领取。
func (r *Repository) loadColdSnapshot(ctx context.Context, id int64) (writebehind.Record, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return writebehind.Record{}, err
	}
	defer tx.Rollback()
	out := writebehind.Record{State: writebehind.Snapshot{PlayerID: id, Items: make(map[int32]int64)}}
	var receipts sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT version, applied_grants FROM player_writebehind WHERE player_id = ?`, id).Scan(&out.Version, &receipts)
	if err == nil && receipts.Valid {
		if err := json.Unmarshal([]byte(receipts.String), &out.State.AppliedGrants); err != nil {
			return out, err
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	p := &out.State.Profile
	if err := tx.QueryRowContext(ctx, `SELECT name, level, exp, coin, scene_id FROM player WHERE id = ?`, id).Scan(&p.Name, &p.Level, &p.Exp, &p.Coin, &p.SceneID); err != nil {
		return out, err
	}
	a := &out.State.Attr
	err = tx.QueryRowContext(ctx, `SELECT career, hp, mp, atk, def, spd FROM player_attr WHERE player_id = ?`, id).Scan(&a.Career, &a.Hp, &a.Mp, &a.Atk, &a.Def, &a.Spd)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT item_id, count FROM player_item WHERE player_id = ?`, id)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var item int32
		var count int64
		if err := rows.Scan(&item, &count); err != nil {
			rows.Close()
			return out, err
		}
		out.State.Items[item] = count
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	var anchor int64
	err = tx.QueryRowContext(ctx, `SELECT anchor_day FROM player_signin WHERE player_id = ?`, id).Scan(&anchor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if err == nil {
		data := &writebehind.SigninData{AnchorDay: anchor, Claims: make(map[int64][]writebehind.ItemGrant)}
		rows, err := tx.QueryContext(ctx, `SELECT sign_day, rewards FROM signin_claim WHERE player_id = ?`, id)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var day int64
			var raw string
			if err := rows.Scan(&day, &raw); err != nil {
				rows.Close()
				return out, err
			}
			var items []writebehind.ItemGrant
			if err := json.Unmarshal([]byte(raw), &items); err != nil {
				rows.Close()
				return out, err
			}
			data.Claims[day] = items
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
		out.State.Signin = data
	}
	if err := out.State.Validate(); err != nil {
		return out, err
	}
	return out, tx.Commit()
}
