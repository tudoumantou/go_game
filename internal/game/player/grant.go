package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"game_study/internal/game/module"
)

// ErrCommitUnknown 表示 COMMIT 返回错误，数据库可能已提交，必须重载后再处理消息。
var ErrCommitUnknown = errors.New("transaction commit outcome unknown")

// CommitItems 由玩家 Actor 同步调用，业务事实与背包同事务持久化。
// 调用期间不会有该 Actor 的其他消息或 Write-Behind 保存交错执行。
func (r *Repository) CommitItems(ctx context.Context, state *State, grants []module.ItemGrant, business func(*sql.Tx) error) error {
	dirty := state.DirtyItems()
	counts := make(map[int32]int64)
	for _, grant := range grants {
		current, ok := counts[grant.ItemID]
		if !ok {
			current = state.GetItemCount(grant.ItemID)
		}
		if grant.ItemID <= 0 || grant.Count <= 0 || current < 0 || grant.Count > math.MaxInt32-current {
			return fmt.Errorf("invalid item grant: item=%d count=%d", grant.ItemID, grant.Count)
		}
		counts[grant.ItemID] = current + grant.Count
		dirty[grant.ItemID] = counts[grant.ItemID]
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM player WHERE id = ? FOR UPDATE`, state.PlayerID()).Scan(&id); err != nil {
		return err
	}
	if err := business(tx); err != nil {
		return err
	}
	if err := saveDirtyItems(ctx, tx, state.PlayerID(), dirty); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: %v", ErrCommitUnknown, err)
	}
	for id, count := range counts {
		state.SeedItem(id, count)
	}
	state.ClearItemsDirty()
	return nil
}

// ReloadItems 在未知提交后获取相同角色行锁，等待旧发奖事务结束再读取背包。
// 只替换背包，保留装备派生属性和其他模块的在线内存状态。
func (r *Repository) ReloadItems(ctx context.Context, state *State) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var id int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM player WHERE id = ? FOR UPDATE`, state.PlayerID()).Scan(&id); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT item_id, count FROM player_item WHERE player_id = ?`, state.PlayerID())
	if err != nil {
		return err
	}
	items := make(map[int32]int64)
	for rows.Next() {
		var itemID int32
		var count int64
		if err := rows.Scan(&itemID, &count); err != nil {
			rows.Close()
			return err
		}
		items[itemID] = count
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	state.mu.Lock()
	state.items = items
	clear(state.dirtyItems)
	state.mu.Unlock()
	return nil
}
