// internal/game/seat/store.go —— 房间与玩家数据的持久化
//
// 两张表，各司其职：
//
//	seat_room    房间维度（一行 = 一个房间的完整状态，座位收在 slots JSON）
//	seat_player  玩家维度（一行 = 一个玩家占着哪个房间的哪个座）
//
// seat_room 是权威；seat_player 是玩家视角的索引（按 player_id 一条 SQL
// 就能查占座，不用扫 JSON）。两者由同一轮变更驱动（房间 onChange / 座位
// onSeat），不会各自演化出不一致的规则；实际写入由 writer 合并成批。
//
// 仓储基座是 module.Shared（与公会/队伍/好友/聊天/邮件同源）：抢位置是
// game 进程内最后一个还自己写 `struct{ db *db.DB }` 的仓储，也是最后一个
// 「无 DB 怎么办」有独立答案的地方。座位现在没有跨表事务（两张表各写各的，
// 由房间快照兜底纠正），所以只用到基座的 Ready/SQL 两层。
package seat

import (
	"context"
	"encoding/json"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	pkgseat "game_study/pkg/seat"
)

// Store 抢位置持久化。
type Store struct {
	*module.Shared
}

// NewStore 创建仓储。
//
// database 为 nil 表示本进程不接数据库：调用方据此决定不挂载 Store
// （见 initSeat），而不是让 Store 自己「降级成什么都不做」。
func NewStore(database *db.DB) *Store {
	return &Store{Shared: module.NewShared(database, "seat")}
}

// EnsureRooms 幂等补齐固定房间行（高 1 / 中 3 / 低 6）。
//
// 先判 Ready：没有 DB 时返回 ErrNoDB 而不是 nil deref——起服日志里要能
// 看到「座位没有落库」这一句，而不是看到一个 panic 栈。
func (s *Store) EnsureRooms(ctx context.Context, rooms []pkgseat.RoomSnapshot) error {
	if !s.Ready() {
		return module.ErrNoDB
	}
	for _, r := range rooms {
		if _, err := s.DB().ExecContext(ctx,
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
	if !s.Ready() {
		return nil, module.ErrNoDB
	}
	rows, err := s.DB().QueryContext(ctx, "SELECT id, tier, room_index, slots FROM seat_room")
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
	if !s.Ready() {
		return module.ErrNoDB
	}
	raw, err := json.Marshal(snap.Slots)
	if err != nil {
		return err
	}
	_, err = s.DB().ExecContext(ctx,
		"INSERT INTO seat_room (id, tier, room_index, slots) VALUES (?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE slots = VALUES(slots)",
		snap.ID, int32(snap.Tier), snap.Index, string(raw))
	return err
}

// SavePlayer 写入/更新玩家的占座数据。
func (s *Store) SavePlayer(ctx context.Context, playerID int64, ref pkgseat.SeatRef, occupiedAt int64) error {
	if !s.Ready() {
		return module.ErrNoDB
	}
	_, err := s.DB().ExecContext(ctx,
		"INSERT INTO seat_player (player_id, room_id, seat_index, occupied_at) VALUES (?, ?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE room_id = VALUES(room_id), seat_index = VALUES(seat_index), "+
			"occupied_at = VALUES(occupied_at)",
		playerID, ref.RoomID, ref.SeatIndex, occupiedAt)
	return err
}

// DeletePlayer 删除玩家的占座数据。
//
// 条件必须带 room_id + seat_index：换座时「新座位写入」与「旧座位删除」
// 可能被合并进同一轮落库，只按 player_id 删会把刚写入的新行删掉，导致
// 玩家数据凭空消失（同房间换座时 room_id 也相同，只有 seat_index 能区分）。
func (s *Store) DeletePlayer(ctx context.Context, playerID int64, ref pkgseat.SeatRef) error {
	if !s.Ready() {
		return module.ErrNoDB
	}
	_, err := s.DB().ExecContext(ctx,
		"DELETE FROM seat_player WHERE player_id = ? AND room_id = ? AND seat_index = ?",
		playerID, ref.RoomID, ref.SeatIndex)
	return err
}
