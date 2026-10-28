// internal/game/seat/store.go —— 房间状态持久化（一行 = 一个房间）
//
// 座位收在 seat_room.slots 里：房间是聚合根，读写粒度永远整房。
// 用 pkgseat.Slot 自己的 json tag 直接序列化，不再单独定义一套落库结构。
package seat

import (
	"context"
	"encoding/json"

	"game_study/pkg/db"
	pkgseat "game_study/pkg/seat"
)

// Store 房间状态仓储。
type Store struct {
	db *db.DB
}

// NewStore 创建仓储。
func NewStore(database *db.DB) *Store { return &Store{db: database} }

// EnsureRooms 幂等补齐固定房间行（高 1 / 中 3 / 低 6）。
func (s *Store) EnsureRooms(ctx context.Context, refs []pkgseat.SeatRef) error {
	for _, r := range refs {
		if _, err := s.db.ExecContext(ctx,
			"INSERT IGNORE INTO seat_room (tier, room_index, slots) VALUES (?, ?, '[]')",
			int32(r.Tier), r.RoomIndex); err != nil {
			return err
		}
	}
	return nil
}

// Load 读取全部房间（最多 10 行）。
//
// 单个房间 slots 解析失败就跳过：让这一个房间按空房启动，好过一条脏数据
// 导致所有房间都恢复不了。
func (s *Store) Load(ctx context.Context) ([]pkgseat.RoomSnapshot, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT tier, room_index, slots FROM seat_room")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []pkgseat.RoomSnapshot
	for rows.Next() {
		var (
			tier, roomIndex int32
			raw             string
		)
		if err := rows.Scan(&tier, &roomIndex, &raw); err != nil {
			return nil, err
		}
		var slots []pkgseat.Slot
		if raw != "" {
			// 必须解到 []Slot（int64 字段），不能走 map[string]any：
			// occupy_id 是纳秒级整数，经 float64 会被静默截断，
			// 结算幂等键失真会偶发多发奖励。
			if err := json.Unmarshal([]byte(raw), &slots); err != nil {
				continue
			}
		}
		out = append(out, pkgseat.RoomSnapshot{
			Tier:  pkgseat.Tier(tier),
			Index: int(roomIndex),
			Slots: slots,
		})
	}
	return out, rows.Err()
}

// Save 写回一个房间（只更新 slots）。
func (s *Store) Save(ctx context.Context, snap pkgseat.RoomSnapshot) error {
	raw, err := json.Marshal(snap.Slots)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO seat_room (tier, room_index, slots) VALUES (?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE slots = VALUES(slots)",
		int32(snap.Tier), snap.Index, string(raw))
	return err
}
