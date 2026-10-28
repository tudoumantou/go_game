// internal/game/seat/store_test.go —— slots JSON 编解码的不变量
//
// 重点锁住「大整数不丢精度」：occupy_id 是纳秒级 int64（约 1.7e18），
// 一旦解析路径经过 float64（例如走 map[string]any），会被静默截断成相邻
// 整数——结算幂等键失真，重启重放就会重复发奖。这类错误不报错，只表现
// 为「偶尔多发一次奖励」，靠人工排查极难定位，所以用测试钉死。
package seat

import (
	"testing"

	"github.com/stretchr/testify/require"

	pkgseat "game_study/pkg/seat"
)

// TestSlotsRoundTripKeepsBigInt 往返编解码后，纳秒级 occupy_id 必须一字不差。
func TestSlotsRoundTripKeepsBigInt(t *testing.T) {
	const occID int64 = 1700000000123456789 // 约 1.7e18，超出 float64 的 2^53 精度

	snap := pkgseat.RoomSnapshot{
		Tier:      pkgseat.TierHigh,
		RoomIndex: 0,
		Seats: []pkgseat.SeatView{
			{Index: 0, State: pkgseat.SeatOccupied, Occupant: 123, OccupyID: occID, OccupiedAt: 1700000000},
			{Index: 1, State: pkgseat.SeatEmpty},
		},
	}

	raw, err := encodeSlots(snap)
	require.NoError(t, err)

	got, err := decodeSlots(string(raw))
	require.NoError(t, err)
	require.Len(t, got, 2)

	require.Equal(t, occID, got[0].OccupyID, "occupy_id 不能经 float64 丢精度")
	require.Equal(t, int64(123), got[0].Occupant)
	require.Equal(t, int64(1700000000), got[0].OccupiedAt)
	require.Equal(t, int32(pkgseat.SeatOccupied), got[0].State)

	require.Equal(t, 1, got[1].Index)
	require.Equal(t, int32(pkgseat.SeatEmpty), got[1].State)
	require.Zero(t, got[1].OccupyID)
}

// TestDecodeSlotsEmpty 空串代表「新房间」，不是错误。
func TestDecodeSlotsEmpty(t *testing.T) {
	got, err := decodeSlots("")
	require.NoError(t, err)
	require.Empty(t, got)
}

// TestDecodeSlotsBroken 脏 JSON 必须返回错误，由调用方跳过该房间。
func TestDecodeSlotsBroken(t *testing.T) {
	_, err := decodeSlots("{not-json")
	require.Error(t, err)
}

// TestEncodeSlotsKeepsOrder 快照本就按 index 升序，编码后顺序不能变。
func TestEncodeSlotsKeepsOrder(t *testing.T) {
	snap := pkgseat.RoomSnapshot{
		Tier:      pkgseat.TierLow,
		RoomIndex: 5,
		Seats: []pkgseat.SeatView{
			{Index: 0, State: pkgseat.SeatEmpty},
			{Index: 1, State: pkgseat.SeatOccupied, Occupant: 7, OccupyID: 1, OccupiedAt: 2},
		},
	}
	raw, err := encodeSlots(snap)
	require.NoError(t, err)

	got, err := decodeSlots(string(raw))
	require.NoError(t, err)
	require.Equal(t, 0, got[0].Index, "顺序必须与快照一致，便于直接读 JSON 排查")
	require.Equal(t, 1, got[1].Index)
}
