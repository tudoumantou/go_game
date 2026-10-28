// pkg/seat/seat_test.go —— 座位状态机不变量
//
// 不注入 PID（pid 为 nil 时不挂定时器），所以可直接传 nil ctx 调处理函数。
package seat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"game_study/pkg/log"
)

func newTestRoom(tier Tier) (*Room, *[]Release) {
	got := &[]Release{}
	r := NewRoom(tier, 0, log.New("seat-test", log.WithLevel("error")))
	r.SetOnRelease(func(rel Release) { *got = append(*got, rel) })
	return r, got
}

// 占 5 分钟后离开，按 5 分钟发奖。
func TestLeaveRewardsByMinutes(t *testing.T) {
	r, got := newTestRoom(TierHigh)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 0})
	r.seats[0].OccupiedAt = time.Now().Add(-5 * time.Minute)

	r.onLeave(&LeaveMsg{PlayerID: 1, SeatIndex: 0})

	require.Equal(t, SeatEmpty, r.seats[0].State)
	require.Len(t, *got, 1)
	require.Equal(t, int64(5), (*got)[0].Minutes)
	require.Equal(t, 5*RatePerMinute[TierHigh], (*got)[0].Amount)
	require.NotZero(t, (*got)[0].OccupyID)
}

// 抢夺：原主按已占分钟结算，新主接管且换新 occupy_id。
func TestSeizeSettlesPreviousOwner(t *testing.T) {
	r, got := newTestRoom(TierMid)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 3})
	oldID := r.seats[3].OccupyID
	r.seats[3].OccupiedAt = time.Now().Add(-3 * time.Minute)

	r.onOccupy(nil, &OccupyMsg{PlayerID: 2, SeatIndex: 3})

	require.Equal(t, int64(2), r.seats[3].Occupant)
	require.NotEqual(t, oldID, r.seats[3].OccupyID)
	require.Len(t, *got, 1)
	require.Equal(t, int64(1), (*got)[0].PlayerID)
	require.Equal(t, oldID, (*got)[0].OccupyID, "结算应带原主的占用实例 ID")
}

// 自己占自己：不重置计时、不发奖。
func TestSelfOccupyIdempotent(t *testing.T) {
	r, got := newTestRoom(TierLow)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 7, SeatIndex: 1})
	r.seats[1].OccupiedAt = time.Now().Add(-10 * time.Minute)

	r.onOccupy(nil, &OccupyMsg{PlayerID: 7, SeatIndex: 1})

	require.Empty(t, *got)
	require.WithinDuration(t, time.Now().Add(-10*time.Minute), r.seats[1].OccupiedAt, time.Second)
}

// 非本人离开不生效。
func TestLeaveByNonOwnerRejected(t *testing.T) {
	r, got := newTestRoom(TierLow)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 7, SeatIndex: 1})
	r.onLeave(&LeaveMsg{PlayerID: 8, SeatIndex: 1})
	require.Equal(t, int64(7), r.seats[1].Occupant)
	require.Empty(t, *got)
}

// 旧超时 tick 不得踢掉新占用者。
func TestStaleTimeoutIgnored(t *testing.T) {
	r, _ := newTestRoom(TierHigh)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 0})
	oldAt := r.seats[0].OccupiedAt

	r.onLeave(&LeaveMsg{PlayerID: 1, SeatIndex: 0})
	r.onOccupy(nil, &OccupyMsg{PlayerID: 2, SeatIndex: 0})

	r.onTimeout(&timeoutMsg{SeatIndex: 0, OccupiedAt: oldAt})
	require.Equal(t, int64(2), r.seats[0].Occupant, "旧 tick 必须作废")

	r.onTimeout(&timeoutMsg{SeatIndex: 0, OccupiedAt: r.seats[0].OccupiedAt})
	require.Equal(t, SeatEmpty, r.seats[0].State)
}

// 超过 1 小时按 60 分钟封顶。
func TestHoldLimitCaps(t *testing.T) {
	require.Equal(t, MaxRewardMinutes, OccupancyMinutes(time.Now().Add(-90*time.Minute), time.Now()))
}

// 重启恢复：已过 1h 的占用必须立即结算并让座。
func TestRestoreSettlesExpired(t *testing.T) {
	r, got := newTestRoom(TierHigh)
	r.onRestore(nil, &RestoreMsg{Slots: []Slot{
		{Index: 4, State: SeatOccupied, Occupant: 11, OccupyID: 9001, OccupiedAt: time.Now().Add(-90 * time.Minute).Unix()},
	}})

	require.Equal(t, SeatEmpty, r.seats[4].State)
	require.Len(t, *got, 1)
	require.Equal(t, MaxRewardMinutes, (*got)[0].Minutes)
	require.Equal(t, int64(9001), (*got)[0].OccupyID, "必须沿用原占用实例 ID")
}

// 重启恢复：未到期原样恢复，计时与 ID 都不重置。
func TestRestoreKeepsActive(t *testing.T) {
	r, got := newTestRoom(TierMid)
	start := time.Now().Add(-20 * time.Minute)
	r.onRestore(nil, &RestoreMsg{Slots: []Slot{
		{Index: 1, State: SeatOccupied, Occupant: 22, OccupyID: 9002, OccupiedAt: start.Unix()},
	}})

	require.Empty(t, *got)
	require.Equal(t, int64(22), r.seats[1].Occupant)
	require.Equal(t, int64(9002), r.seats[1].OccupyID)
	require.WithinDuration(t, start, r.seats[1].OccupiedAt, time.Second)
}

// 恢复时缺 occupy_id：补一个，避免幂等键为 0。
func TestRestoreFillsMissingID(t *testing.T) {
	r, _ := newTestRoom(TierHigh)
	r.onRestore(nil, &RestoreMsg{Slots: []Slot{
		{Index: 0, State: SeatOccupied, Occupant: 5, OccupyID: 0, OccupiedAt: time.Now().Add(-5 * time.Minute).Unix()},
	}})
	require.NotZero(t, r.seats[0].OccupyID)
}

// 脏数据（越界/空占用者）不得写入。
func TestRestoreIgnoresInvalid(t *testing.T) {
	r, got := newTestRoom(TierLow)
	r.onRestore(nil, &RestoreMsg{Slots: []Slot{
		{Index: -1, Occupant: 1, OccupyID: 1, OccupiedAt: time.Now().Unix()},
		{Index: SeatsPerRoom, Occupant: 2, OccupyID: 2, OccupiedAt: time.Now().Unix()},
		{Index: 0, Occupant: 0, OccupyID: 3, OccupiedAt: time.Now().Unix()},
	}})
	require.Empty(t, *got)
	require.Equal(t, SeatEmpty, r.seats[0].State)
}

func TestValidSeat(t *testing.T) {
	require.True(t, ValidSeat(TierHigh, 0, 9))
	require.True(t, ValidSeat(TierLow, 5, 0))
	require.False(t, ValidSeat(TierNone, 0, 0), "0 是非法档位")
	require.False(t, ValidSeat(TierHigh, 1, 0), "high 只有 1 间房")
	require.False(t, ValidSeat(TierLow, 0, SeatsPerRoom), "座号越界")
}

// RoomID 必须全局唯一且可反解——玩家数据（seat_player.room_id）靠它定位房间。
func TestRoomIDUniqueAndReversible(t *testing.T) {
	seen := make(map[int32]bool)
	for _, tier := range TierOrder {
		for i := 0; i < RoomCounts[tier]; i++ {
			id := RoomID(tier, i)
			require.False(t, seen[id], "room_id 重复：%d", id)
			seen[id] = true

			gotTier, gotIdx := SplitRoomID(id)
			require.Equal(t, tier, gotTier)
			require.Equal(t, i, gotIdx)
		}
	}
	require.Len(t, seen, 10, "高 1 + 中 3 + 低 6")
}

func TestNextSeqUnique(t *testing.T) {
	seen := map[int64]struct{}{}
	for i := 0; i < 1000; i++ {
		id := nextSeq()
		_, dup := seen[id]
		require.False(t, dup, "序号重复会导致发奖被幂等吞掉")
		seen[id] = struct{}{}
	}
}
