// internal/game/seat/store.go —— 座位/房间状态的持久化读写（单服）
//
// 职责边界：只做「库表形状 ↔ 领域快照」的转换，不含业务判断。
// 房间 Actor 仍是权威：本文件既不参与占座裁决，也不决定结算时机，
// 只负责把 Actor 交出的快照写下去、把上次的快照读回来。
package seat

import (
	"context"
	"strings"

	"game_study/pkg/db"
	pkgseat "game_study/pkg/seat"
)

// SlotRow 一行座位状态（落库形状）。
type SlotRow struct {
	Tier       pkgseat.Tier
	RoomIndex  int
	SeatIndex  int
	State      int32
	Occupant   int64
	OccupyID   int64
	OccupiedAt int64 // 占用起始 unix 秒；空座为 0
}

// Store 座位/房间状态持久化。
type Store struct {
	db *db.DB
}

// NewStore 创建持久化层。
func NewStore(database *db.DB) *Store { return &Store{db: database} }

// EnsureRooms 幂等补齐固定房间行（高 1 / 中 3 / 低 6）。
//
// 用 INSERT IGNORE 而不是先查后插：并发启动多进程时也不会重复插入，
// 且不需要事务。
func (s *Store) EnsureRooms(ctx context.Context, refs []pkgseat.SeatRef) error {
	if len(refs) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT IGNORE INTO seat_room (tier, room_index) VALUES ")
	args := make([]any, 0, len(refs)*2)
	for i, r := range refs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?)")
		args = append(args, int32(r.Tier), r.RoomIndex)
	}
	_, err := s.db.ExecContext(ctx, b.String(), args...)
	return err
}

// LoadSlots 全量加载座位状态（最多 100 行，一次读完即可）。
func (s *Store) LoadSlots(ctx context.Context) ([]SlotRow, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT tier, room_index, seat_index, state, occupant, occupy_id, occupied_at FROM seat_slot")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SlotRow
	for rows.Next() {
		var (
			tier, roomIndex, seatIndex, state int32
			occupant, occupyID, occupiedAt   int64
		)
		if err := rows.Scan(&tier, &roomIndex, &seatIndex, &state, &occupant, &occupyID, &occupiedAt); err != nil {
			return nil, err
		}
		out = append(out, SlotRow{
			Tier:       pkgseat.Tier(tier),
			RoomIndex:  int(roomIndex),
			SeatIndex:  int(seatIndex),
			State:      state,
			Occupant:   occupant,
			OccupyID:   occupyID,
			OccupiedAt: occupiedAt,
		})
	}
	return out, rows.Err()
}

// SaveRoom 把一个房间的 10 个座位一次性 upsert。
//
// 整房一次往返而不是逐座写：房间就是落库的天然批次（10 行、主键相邻），
// 变更也总是整房快照一起产出，逐座写只会放大往返次数。
func (s *Store) SaveRoom(ctx context.Context, snap pkgseat.RoomSnapshot) error {
	if len(snap.Seats) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO seat_slot " +
		"(tier, room_index, seat_index, state, occupant, occupy_id, occupied_at) VALUES ")
	args := make([]any, 0, len(snap.Seats)*7)
	for i, sv := range snap.Seats {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString("(?,?,?,?,?,?,?)")
		args = append(args, int32(snap.Tier), snap.RoomIndex, sv.Index,
			int32(sv.State), sv.Occupant, sv.OccupyID, sv.OccupiedAt)
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE state=VALUES(state), occupant=VALUES(occupant), " +
		"occupy_id=VALUES(occupy_id), occupied_at=VALUES(occupied_at)")
	_, err := s.db.ExecContext(ctx, b.String(), args...)
	return err
}
