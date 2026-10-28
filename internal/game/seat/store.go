// internal/game/seat/store.go —— 房间与玩家数据的持久化
//
// 两张表，各司其职：
//   seat_room    房间维度（一行 = 一个房间的完整状态，座位收在 slots JSON）
//   seat_player  玩家维度（一行 = 一个玩家占着哪个房间的哪个座）
//
// seat_room 是权威；seat_player 是玩家视角的索引（按 player_id 一条 SQL
// 就能查占座，不用扫 JSON）。两者由 Manager.onChange 的同一份快照驱动，
// 不会各自演化出不一致的规则。
package seat

import (
	"context"
	"encoding/json"

	"game_study/pkg/db"
	pkgseat "game_study/pkg/seat"
)

// Store 抢位置持久化。
type Store struct {
	db *db.DB
}

// NewStore 创建仓储。
func NewStore(database *db.DB) *Store { return &Store{db: database} }

// EnsureRooms 幂等补齐固定房间行（高 1 / 中 3 / 低 6）。
func (s *Store) EnsureRooms(ctx context.Context, rooms []pkgseat.RoomSnapshot) error {
	for _, r := range rooms {
		if _, err := s.db.ExecContext(ctx,
			"INSERT IGNORE INTO seat_room (id, tier, room_index, slots) VALUES (?, ?, ?, '[]')",
			r.ID, int32(r.Tier), r.Index); err != nil {
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
	rows, err := s.db.QueryContext(ctx, "SELECT id, tier, room_index, slots FROM seat_room")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []pkgseat.RoomSnapshot
	for rows.Next() {
		var (
			id, tier, roomIndex int32
			raw                 string
		)
		if err := rows.Scan(&id, &tier, &roomIndex, &raw); err != nil {
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
			ID:    id,
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
		"INSERT INTO seat_room (id, tier, room_index, slots) VALUES (?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE slots = VALUES(slots)",
		snap.ID, int32(snap.Tier), snap.Index, string(raw))
	return err
}

// SavePlayer 写入/更新玩家的占座数据。
func (s *Store) SavePlayer(ctx context.Context, playerID int64, ref pkgseat.SeatRef, occupiedAt int64) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO seat_player (player_id, room_id, seat_index, occupied_at) VALUES (?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE room_id = VALUES(room_id), seat_index = VALUES(seat_index), "+
			"occupied_at = VALUES(occupied_at)",
		playerID, ref.RoomID, ref.SeatIndex, occupiedAt)
	return err
}

// DeletePlayer 删除玩家的占座数据。
//
// 必须带上 room_id 做条件：换座时「新房间写入」与「旧房间删除」可能交错，
// 只按 player_id 删会把刚写入的新行删掉，导致玩家数据凭空消失。
func (s *Store) DeletePlayer(ctx context.Context, playerID int64, roomID int32) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM seat_player WHERE player_id = ? AND room_id = ?", playerID, roomID)
	return err
}

// ---- 待补发奖励（seat_pending_reward）----

// AddPending 写入一笔待补发奖励；同一 occupy_id 重复写只留一行。
func (s *Store) AddPending(ctx context.Context, playerID, occupyID int64, tier pkgseat.Tier, minutes int64) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO seat_pending_reward (player_id, occupy_id, tier, minutes) VALUES (?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE tier = VALUES(tier), minutes = VALUES(minutes)",
		playerID, occupyID, int32(tier), minutes)
	return err
}

// ListPending 读取某玩家全部待补发奖励（按占用实例升序，补发顺序稳定）。
func (s *Store) ListPending(ctx context.Context, playerID int64) ([]pkgseat.Pending, error) {
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

// DeletePending 删除一笔已补发的奖励。
func (s *Store) DeletePending(ctx context.Context, playerID, occupyID int64) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM seat_pending_reward WHERE player_id = ? AND occupy_id = ?", playerID, occupyID)
	return err
}
