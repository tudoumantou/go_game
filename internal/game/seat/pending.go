// internal/game/seat/pending.go —— 座位待结算奖励表（outbox）
//
// 为什么需要它：座位释放（超时/被抢夺/重启补结算）时玩家往往已离线，玩家
// Actor 不在内存；即使在线，「先把座位落库释放、再发奖」之间也可能崩溃。
// 因此结算统一走 outbox：
//
//	写本表（幂等）→ 座位置空并落库 → 尝试在线发奖 → 成功则删本行
//
// 任何一步崩溃，玩家下次登录由 seat 模块按 occupy_id 补发。
//
// 主键 (player_id, occupy_id)：occupy_id 是「本次占用实例」的 ID，重启后
// 重放同一笔结算仍是同一个 ID，天然幂等；用结算时刻当键则做不到——
// 重放会生成新时间戳，于是重复发奖。
package seat

import (
	"context"

	"game_study/pkg/db"
	pkgseat "game_study/pkg/seat"
)

// PendingStore 座位待结算奖励表读写。
type PendingStore struct {
	db *db.DB
}

// NewPendingStore 创建 outbox 存储。
func NewPendingStore(database *db.DB) *PendingStore {
	return &PendingStore{db: database}
}

// Add 写入一笔待结算奖励。
//
// ON DUPLICATE KEY UPDATE：同一占用实例重复投递（重启重放/跨服重推）只留一行。
func (s *PendingStore) Add(ctx context.Context, playerID, occupyID int64, tier pkgseat.Tier, minutes int64) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO seat_pending_reward (player_id, occupy_id, tier, minutes) VALUES (?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE tier = VALUES(tier), minutes = VALUES(minutes)",
		playerID, occupyID, int32(tier), minutes)
	return err
}

// List 读取某玩家全部待结算奖励（按占用实例升序，保证补发顺序稳定）。
func (s *PendingStore) List(ctx context.Context, playerID int64) ([]pkgseat.Pending, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT occupy_id, tier, minutes FROM seat_pending_reward WHERE player_id = ? ORDER BY occupy_id", playerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []pkgseat.Pending
	for rows.Next() {
		var (
			occupyID int64
			tier     int32
			minutes  int64
		)
		if err := rows.Scan(&occupyID, &tier, &minutes); err != nil {
			return nil, err
		}
		out = append(out, pkgseat.Pending{
			OccupyID: occupyID,
			Tier:     pkgseat.Tier(tier),
			Minutes:  minutes,
		})
	}
	return out, rows.Err()
}

// Delete 删除一笔已补发的待结算奖励。
func (s *PendingStore) Delete(ctx context.Context, playerID, occupyID int64) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM seat_pending_reward WHERE player_id = ? AND occupy_id = ?", playerID, occupyID)
	return err
}
