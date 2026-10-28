// internal/game/seat/store.go —— 房间/座位状态的持久化读写（单服）
//
// 职责边界：只做「库表形状 ↔ 领域快照」的转换，不含业务判断。
// 房间 Actor 仍是权威：本文件既不参与占座裁决，也不决定结算时机，
// 只负责把 Actor 交出的快照写下去、把上次的快照读回来。
//
// 为什么座位收在 seat_room.slots 而不是独立子表：座位是房间的组成部分，
// 读写粒度永远整房。一行 = 一个房间的完整状态，写入天然原子，也不会出现
// 「10 行写了 7 行就崩」的中间态。这与 player_equip.equips、player_quest.quests
// 的既有收纳方式一致。
package seat

import (
	"context"
	"encoding/json"

	"game_study/pkg/db"
	pkgseat "game_study/pkg/seat"
)

// slotJSON 是 seat_room.slots 里的单个座位形状。
//
// 必须解析到具体结构体，不能走 map[string]any：json 包把未知数字解成
// float64，而 occupy_id 是纳秒级 int64（约 1.7e18），超出 float64 的 53 位
// 精度会被静默截断——结算幂等键一旦失真就会重复发奖。结构体字段是 int64，
// 走的是整数解析，不会经过 float64。
type slotJSON struct {
	Index      int   `json:"index"`
	State      int32 `json:"state"`
	Occupant   int64 `json:"occupant"`
	OccupyID   int64 `json:"occupy_id"`
	OccupiedAt int64 `json:"occupied_at"`
}

// SlotState 座位状态（落库形状，不含剩余秒数等派生值）。
type SlotState struct {
	Index      int
	State      int32
	Occupant   int64
	OccupyID   int64
	OccupiedAt int64 // 占用起始 unix 秒；空座为 0
}

// RoomRow 一行房间状态（含其全部座位）。
type RoomRow struct {
	Tier      pkgseat.Tier
	RoomIndex int
	Status    int32
	Slots     []SlotState
}

// Store 房间/座位状态持久化。
type Store struct {
	db *db.DB
}

// NewStore 创建持久化层。
func NewStore(database *db.DB) *Store { return &Store{db: database} }

// EnsureRooms 幂等补齐固定房间行（高 1 / 中 3 / 低 6）。
//
// 用 INSERT IGNORE 而不是先查后插：多进程同时启动也不会重复插入，且不需要
// 事务。slots 必须显式给 '[]'——JSON 列没有（5.7 也不支持）DEFAULT。
func (s *Store) EnsureRooms(ctx context.Context, refs []pkgseat.SeatRef) error {
	if len(refs) == 0 {
		return nil
	}
	const empty = "[]"
	// 行数固定为 10，逐条执行即可，不做多值拼接（省不了多少往返，反而更绕）。
	for _, r := range refs {
		if _, err := s.db.ExecContext(ctx,
			"INSERT IGNORE INTO seat_room (tier, room_index, slots) VALUES (?, ?, ?)",
			int32(r.Tier), r.RoomIndex, empty); err != nil {
			return err
		}
	}
	return nil
}

// LoadRooms 全量加载房间与座位状态（10 行，一次读完）。
//
// 单个房间的 slots 解析失败时跳过该行：宁可让这一个房间以空状态启动，
// 也不要因为一条脏数据导致所有房间都恢复不了。
func (s *Store) LoadRooms(ctx context.Context) ([]RoomRow, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT tier, room_index, status, slots FROM seat_room")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RoomRow
	for rows.Next() {
		var (
			tier, roomIndex, status int32
			raw                     string
		)
		if err := rows.Scan(&tier, &roomIndex, &status, &raw); err != nil {
			return nil, err
		}
		slots, derr := decodeSlots(raw)
		if derr != nil {
			continue // 单房间脏数据：跳过，启动时按空房处理
		}
		out = append(out, RoomRow{
			Tier:      pkgseat.Tier(tier),
			RoomIndex: int(roomIndex),
			Status:    status,
			Slots:     slots,
		})
	}
	return out, rows.Err()
}

// encodeSlots 把房间快照编码成 slots JSON（按 index 升序，快照本就是有序的）。
func encodeSlots(snap pkgseat.RoomSnapshot) ([]byte, error) {
	arr := make([]slotJSON, 0, len(snap.Seats))
	for _, sv := range snap.Seats {
		arr = append(arr, slotJSON{
			Index:      sv.Index,
			State:      int32(sv.State),
			Occupant:   sv.Occupant,
			OccupyID:   sv.OccupyID,
			OccupiedAt: sv.OccupiedAt,
		})
	}
	return json.Marshal(arr)
}

// decodeSlots 解析 slots JSON。
//
// 解析目标是 []slotJSON（int64 字段），不是 map[string]any——后者会把数字
// 解成 float64，occupy_id 这类纳秒级整数会被静默截断。
func decodeSlots(raw string) ([]SlotState, error) {
	if raw == "" {
		return nil, nil
	}
	var arr []slotJSON
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil, err
	}
	out := make([]SlotState, 0, len(arr))
	for _, a := range arr {
		out = append(out, SlotState{
			Index:      a.Index,
			State:      a.State,
			Occupant:   a.Occupant,
			OccupyID:   a.OccupyID,
			OccupiedAt: a.OccupiedAt,
		})
	}
	return out, nil
}

// SaveRoom 把一个房间的完整状态写回（一行 upsert）。
//
// 只更新 slots，不动 status：status 是运营侧的开关，落库路径不该覆盖它。
func (s *Store) SaveRoom(ctx context.Context, snap pkgseat.RoomSnapshot) error {
	raw, err := encodeSlots(snap)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO seat_room (tier, room_index, slots) VALUES (?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE slots = VALUES(slots)",
		int32(snap.Tier), snap.RoomIndex, string(raw))
	return err
}
