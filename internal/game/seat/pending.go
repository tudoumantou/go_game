// internal/game/seat/pending.go —— 离线座位奖励的耐久暂存
//
// 为什么需要它：座位释放（超时/被抢夺）时玩家往往已离线，玩家 Actor 不在
// 内存，GrantItemsMsg 无处投递。这里把「玩家、序号、档位、分钟数」落一张
// 小表，登录时由 seat 模块补发并清理。
//
// 为什么不用 writebehind：writebehind 存的是「已加载玩家的快照」，离线玩家
// 根本不加载；这张表是独立于玩家快照的投递队列，语义更接近 outbox。
package seat

import (
	"context"

	"game_study/pkg/db"
	pkgseat "game_study/pkg/seat"
)

// PendingStore 座位离线奖励表（seat_pending_reward）读写。
type PendingStore struct {
	db *db.DB
}

// NewPendingStore 创建离线奖励存储。
func NewPendingStore(database *db.DB) *PendingStore {
	return &PendingStore{db: database}
}

// Add 写入一笔挂起奖励。
//
// ON DUPLICATE KEY UPDATE：主键 (player_id, seq) 保证同一释放事件重复投递
// （跨服重推/Actor 重试）只留一行，不会重复发奖。
func (s *PendingStore) Add(ctx context.Context, playerID, seq int64, tier string, minutes int64) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO seat_pending_reward (player_id, seq, tier, minutes) VALUES (?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE tier = VALUES(tier), minutes = VALUES(minutes)",
		playerID, seq, tier, minutes)
	return err
}

// List 读取某玩家全部挂起奖励（按序号升序，保证补发顺序稳定）。
func (s *PendingStore) List(ctx context.Context, playerID int64) ([]pkgseat.PendingReward, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT seq, tier, minutes FROM seat_pending_reward WHERE player_id = ? ORDER BY seq", playerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []pkgseat.PendingReward
	for rows.Next() {
		var r pkgseat.PendingReward
		if err := rows.Scan(&r.Seq, &r.Tier, &r.Minutes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Delete 删除一笔已补发的挂起奖励。
func (s *PendingStore) Delete(ctx context.Context, playerID, seq int64) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM seat_pending_reward WHERE player_id = ? AND seq = ?", playerID, seq)
	return err
}
