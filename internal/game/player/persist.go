// internal/game/player/persist.go —— 玩家数据加载与落库
package player

import (
	"context"
	"database/sql"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// Repository 玩家数据仓储。
type Repository struct {
	db    *db.DB
	cache *writebehind.Store
}

func (r *Repository) SetCache(store *writebehind.Store) { r.cache = store }
func (r *Repository) CacheEnabled() bool                { return r != nil && r.cache != nil }

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
		// SeedItem 不标脏：预加载灌数据不应触发写回
		state.SeedItem(itemID, count)
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

// Save 落库玩家状态（事务：角色 + 属性 + 道具）。
//
// 道具从「先删后插全量重写」改为「只写变更项 + 合批 UPSERT」（12.4.4）：
//   - 只有 AddItem/RemoveItem 碰过的道具才写，50 格背包里通常只变 1-2 格；
//   - 多行合成一条 INSERT ... ON DUPLICATE KEY UPDATE（500 行/条）；
//   - 数量归零的道具合成一条 DELETE ... WHERE player_id=? AND item_id IN (...)。
//
// SQL 语句数从「1 + N（道具数）」降到「1 + 批次 + 批次」。
func (r *Repository) Save(ctx context.Context, state *State) error {
	if r.CacheEnabled() {
		return fmt.Errorf("direct player SQL save disabled: use cache and independent flusher")
	}
	dirty := state.DirtyItems()

	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
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

		return saveDirtyItems(ctx, tx, playerID, dirty)
	})
	if err != nil {
		return err // 失败保留脏标记，下轮重试
	}

	// 同一 Actor 线程内执行（onSave 走 Actor 消息），清标记无并发风险
	state.ClearItemsDirty()
	return nil
}

// saveDirtyItems 写本次变更的道具（合批 UPSERT + 合批 DELETE）。
func saveDirtyItems(ctx context.Context, tx *sql.Tx, playerID int64, dirty map[int32]int64) error {
	if len(dirty) == 0 {
		return nil
	}

	upserts := make([][]any, 0, len(dirty))
	deletes := make([]any, 0, len(dirty))
	for itemID, count := range dirty {
		if count <= 0 {
			deletes = append(deletes, itemID)
			continue
		}
		upserts = append(upserts, []any{playerID, itemID, count})
	}

	itemCols := []string{"player_id", "item_id", "count"}

	for _, batch := range flush.Chunk(upserts, flush.DefaultBatchRows, flush.DefaultBatchBytes) {
		q, args, err := flush.BuildBatchUpsert("player_item", itemCols, batch)
		if err != nil {
			return fmt.Errorf("build item upsert: %w", err)
		}
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("batch upsert items: %w", err)
		}
	}

	// 删除也合批：数量归零的道具一条语句删完
	const deleteChunk = flush.DefaultBatchRows
	for start := 0; start < len(deletes); start += deleteChunk {
		end := start + deleteChunk
		if end > len(deletes) {
			end = len(deletes)
		}
		q, args, err := flush.BuildBatchDeleteWhere("player_item", "player_id = ?",
			[]any{playerID}, "item_id", deletes[start:end])
		if err != nil {
			return fmt.Errorf("build item delete: %w", err)
		}
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("batch delete items: %w", err)
		}
	}
	return nil
}
