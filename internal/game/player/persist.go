// internal/game/player/persist.go —— 玩家数据加载与落库
package player

import (
	"context"
	"database/sql"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/model"
)

// Repository 玩家数据仓储。
type Repository struct {
	db *db.DB
}

// NewRepository 创建仓储。
func NewRepository(database *db.DB) *Repository {
	return &Repository{db: database}
}

// Load 从 MySQL 加载玩家状态（角色信息 + 属性 + 道具）。
func (r *Repository) Load(ctx context.Context, playerID int64) (*State, error) {
	state := NewState(playerID)

	// 角色基础信息
	row, err := model.FindPlayerByPK(ctx, r.db.DB, playerID)
	if err == nil {
		state.SetProfile(module.Profile{
			Name:    row.Name,
			Level:   row.Level,
			Exp:     row.Exp,
			Coin:    row.Coin,
			SceneID: row.SceneId,
		})
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("load player: %w", err)
	}

	// 属性
	attrRow, err := model.FindPlayerAttrByPK(ctx, r.db.DB, playerID)
	if err == nil {
		state.SetAttrStruct(Attr{
			Career: attrRow.Career,
			Hp:     attrRow.Hp,
			Mp:     attrRow.Mp,
			Atk:    attrRow.Atk,
			Def:    attrRow.Def,
			Spd:    attrRow.Spd,
		})
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("load attr: %w", err)
	}

	// 道具
	items, err := r.loadItems(ctx, playerID)
	if err != nil {
		return nil, err
	}
	for itemID, count := range items {
		state.AddItem(itemID, count)
	}
	return state, nil
}

func (r *Repository) loadItems(ctx context.Context, playerID int64) (map[int32]int64, error) {
	query := "SELECT item_id, count FROM player_item WHERE player_id = ?"
	rows, err := r.db.DB.QueryContext(ctx, query, playerID)
	if err != nil {
		return nil, fmt.Errorf("query items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int32]int64)
	for rows.Next() {
		var itemID int32
		var count int32
		if err := rows.Scan(&itemID, &count); err != nil {
			return nil, err
		}
		out[itemID] = int64(count)
	}
	return out, rows.Err()
}

// Save 落库玩家状态（事务：属性 + 道具）。
func (r *Repository) Save(ctx context.Context, state *State) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		playerID := state.PlayerID()
		a := state.Attr()

		// 角色基础信息（name 不在此更新，改名走独立流程）
		p := state.Profile()
		if _, err := tx.ExecContext(ctx,
			`UPDATE player SET level = ?, exp = ?, coin = ?, scene_id = ? WHERE id = ?`,
			p.Level, p.Exp, p.Coin, p.SceneID, playerID); err != nil {
			return fmt.Errorf("update player: %w", err)
		}

		// 属性 upsert（事务内执行，保证与道具同事务）
		_, err := tx.ExecContext(ctx,
			`INSERT INTO player_attr (player_id, career, hp, mp, atk, def, spd)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE career=VALUES(career), hp=VALUES(hp), mp=VALUES(mp),
			   atk=VALUES(atk), def=VALUES(def), spd=VALUES(spd)`,
			playerID, a.Career, a.Hp, a.Mp, a.Atk, a.Def, a.Spd)
		if err != nil {
			return fmt.Errorf("upsert attr: %w", err)
		}

		// 道具：先删后插（P4 简化，P6 改增量）
		if _, err := tx.ExecContext(ctx, "DELETE FROM player_item WHERE player_id = ?", playerID); err != nil {
			return fmt.Errorf("delete items: %w", err)
		}
		for itemID, count := range state.Items() {
			if count <= 0 {
				continue
			}
			if _, err := tx.ExecContext(ctx,
				"INSERT INTO player_item (player_id, item_id, count) VALUES (?, ?, ?)",
				playerID, itemID, count); err != nil {
				return fmt.Errorf("insert item: %w", err)
			}
		}
		return nil
	})
}
