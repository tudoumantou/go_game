// pkg/seat/room_test.go —— 座位状态机关键不变量
//
// 直接调 Actor 的处理函数（测试与实现同包）：房间状态机是纯内存逻辑，
// 不依赖 actor 运行时；onOccupy/onRestore 只有在 pid 已注入时才用 ctx
// （挂定时器），测试不注入 PID，因此可安全传 nil。
package seat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"game_study/pkg/log"
)

func newTestRoom(tier Tier) (*Room, *[]ReleaseEvent) {
	got := &[]ReleaseEvent{}
	r := NewRoom(tier, 0, log.New("seat-test", log.WithLevel("error")))
	r.SetOnRelease(func(ev ReleaseEvent) { *got = append(*got, ev) })
	return r, got
}

// TestOccupyThenLeaveRewardsByMinutes 占用 5 分钟后主动离开，按 5 分钟发奖。
func TestOccupyThenLeaveRewardsByMinutes(t *testing.T) {
	r, got := newTestRoom(TierHigh)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 0})

	// 把起始时间拨回 5 分钟前（模拟已占用一段时间）。
	r.seats[0].OccupiedAt = time.Now().Add(-5 * time.Minute)
	r.onLeave(&LeaveMsg{PlayerID: 1, SeatIndex: 0})

	require.Equal(t, SeatEmpty, r.seats[0].State)
	require.Len(t, *got, 1)
	require.Equal(t, int64(1), (*got)[0].PlayerID)
	require.Equal(t, int64(5), (*got)[0].Minutes)
	require.Equal(t, 5*RatePerMinute[TierHigh], (*got)[0].Amount)
}

// TestOccupyAssignsOccupyID 占座必须生成非零的占用实例 ID（结算幂等键）。
func TestOccupyAssignsOccupyID(t *testing.T) {
	r, _ := newTestRoom(TierLow)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 2})
	require.NotZero(t, r.seats[2].OccupyID)
}

// TestSeizeSettlesPreviousOwner 抢夺：原主按已占分钟数结算，新主接管座位。
func TestSeizeSettlesPreviousOwner(t *testing.T) {
	r, got := newTestRoom(TierMid)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 3})
	firstOccupyID := r.seats[3].OccupyID
	r.seats[3].OccupiedAt = time.Now().Add(-3 * time.Minute)

	r.onOccupy(nil, &OccupyMsg{PlayerID: 2, SeatIndex: 3})

	require.Equal(t, SeatOccupied, r.seats[3].State)
	require.Equal(t, int64(2), r.seats[3].Occupant)
	require.NotEqual(t, firstOccupyID, r.seats[3].OccupyID, "换人必须换占用实例 ID")
	require.Len(t, *got, 1, "被顶掉的原主应结算一次")
	require.Equal(t, int64(1), (*got)[0].PlayerID)
	require.Equal(t, int64(3), (*got)[0].Minutes)
	require.Equal(t, firstOccupyID, (*got)[0].OccupyID, "结算事件应携带原主的占用实例 ID")
}

// TestSelfOccupyIsIdempotent 自己占自己：不改计时、不产生奖励。
func TestSelfOccupyIsIdempotent(t *testing.T) {
	r, got := newTestRoom(TierLow)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 7, SeatIndex: 1})
	r.seats[1].OccupiedAt = time.Now().Add(-10 * time.Minute)

	r.onOccupy(nil, &OccupyMsg{PlayerID: 7, SeatIndex: 1})

	require.Empty(t, *got)
	require.Equal(t, int64(7), r.seats[1].Occupant)
	// 计时未被重置：仍应是 10 分钟前。
	require.WithinDuration(t, time.Now().Add(-10*time.Minute), r.seats[1].OccupiedAt, time.Second)
}

// TestLeaveByNonOwnerRejected 非本人离开不生效。
func TestLeaveByNonOwnerRejected(t *testing.T) {
	r, got := newTestRoom(TierLow)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 7, SeatIndex: 1})

	r.onLeave(&LeaveMsg{PlayerID: 8, SeatIndex: 1})

	require.Equal(t, SeatOccupied, r.seats[1].State)
	require.Equal(t, int64(7), r.seats[1].Occupant)
	require.Empty(t, *got)
}

// TestStaleTimeoutIgnored 旧超时 tick 不得踢掉新占用者。
func TestStaleTimeoutIgnored(t *testing.T) {
	r, _ := newTestRoom(TierHigh)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 0})
	oldAt := r.seats[0].OccupiedAt

	// A 离开，B 立刻占用同一座位（B 的起始时间必然晚于 A）。
	r.onLeave(&LeaveMsg{PlayerID: 1, SeatIndex: 0})
	r.onOccupy(nil, &OccupyMsg{PlayerID: 2, SeatIndex: 0})

	// A 的旧超时到点：必须作废，不能把 B 踢掉。
	r.onTimeout(&timeoutMsg{SeatIndex: 0, OccupiedAt: oldAt})
	require.Equal(t, SeatOccupied, r.seats[0].State)
	require.Equal(t, int64(2), r.seats[0].Occupant)

	// B 自己的超时到点：正常释放。
	r.onTimeout(&timeoutMsg{SeatIndex: 0, OccupiedAt: r.seats[0].OccupiedAt})
	require.Equal(t, SeatEmpty, r.seats[0].State)
}

// TestHoldLimitCapsMinutes 超过 1 小时按 60 分钟封顶。
func TestHoldLimitCapsMinutes(t *testing.T) {
	require.Equal(t, MaxRewardMinutes, OccupancyMinutes(time.Now().Add(-90*time.Minute), time.Now()))
}

// TestRestoreSettlesExpiredSeat 重启恢复：已过 1h 的占用必须立即结算并让座。
func TestRestoreSettlesExpiredSeat(t *testing.T) {
	r, got := newTestRoom(TierHigh)

	r.onRestore(nil, &RestoreMsg{Slots: []RestoredSlot{{
		SeatIndex:  4,
		Occupant:   11,
		OccupyID:   9001,
		OccupiedAt: time.Now().Add(-90 * time.Minute), // 已超时 30 分钟
	}}})

	require.Equal(t, SeatEmpty, r.seats[4].State, "过期占用必须让座")
	require.Len(t, *got, 1)
	require.Equal(t, int64(11), (*got)[0].PlayerID)
	require.Equal(t, MaxRewardMinutes, (*got)[0].Minutes, "按 1h 封顶结算")
	require.Equal(t, int64(9001), (*got)[0].OccupyID, "结算必须沿用原占用实例 ID")
	require.Equal(t, MaxRewardMinutes*RatePerMinute[TierHigh], (*got)[0].Amount)
}

// TestRestoreKeepsActiveSeat 重启恢复：未到期的占用原样恢复，计时不重置。
func TestRestoreKeepsActiveSeat(t *testing.T) {
	r, got := newTestRoom(TierMid)
	start := time.Now().Add(-20 * time.Minute)

	r.onRestore(nil, &RestoreMsg{Slots: []RestoredSlot{{
		SeatIndex:  1,
		Occupant:   22,
		OccupyID:   9002,
		OccupiedAt: start,
	}}})

	require.Empty(t, *got, "未到期不应结算")
	require.Equal(t, SeatOccupied, r.seats[1].State)
	require.Equal(t, int64(22), r.seats[1].Occupant)
	require.Equal(t, int64(9002), r.seats[1].OccupyID, "占用实例 ID 必须保留，否则重启后结算幂等失效")
	require.WithinDuration(t, start, r.seats[1].OccupiedAt, time.Second, "计时不能被重置")
}

// TestRestoreIgnoresInvalidSeat 脏数据（越界座号/空占用者）不得写入房间。
func TestRestoreIgnoresInvalidSeat(t *testing.T) {
	r, got := newTestRoom(TierLow)
	r.onRestore(nil, &RestoreMsg{Slots: []RestoredSlot{
		{SeatIndex: -1, Occupant: 1, OccupyID: 1, OccupiedAt: time.Now()},
		{SeatIndex: SeatsPerRoom, Occupant: 2, OccupyID: 2, OccupiedAt: time.Now()},
		{SeatIndex: 0, Occupant: 0, OccupyID: 3, OccupiedAt: time.Now()},
	}})
	require.Empty(t, *got)
	require.Equal(t, SeatEmpty, r.seats[0].State)
}

// TestRestoreFillsMissingOccupyID 库里缺占用实例 ID 时必须补一个。
//
// 不补的话结算幂等键是 0，同一玩家所有奖励共用一个 BizID，只发得出第一笔。
func TestRestoreFillsMissingOccupyID(t *testing.T) {
	r, _ := newTestRoom(TierHigh)
	r.onRestore(nil, &RestoreMsg{Slots: []RestoredSlot{{
		SeatIndex:  0,
		Occupant:   5,
		OccupyID:   0, // 历史数据缺 ID
		OccupiedAt: time.Now().Add(-5 * time.Minute),
	}}})
	require.NotZero(t, r.seats[0].OccupyID)
}
