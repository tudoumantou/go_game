// pkg/seat/room_test.go —— 座位状态机关键不变量
//
// 直接调 Actor 的处理函数（测试与实现同包）：房间状态机是纯内存逻辑，
// 不依赖 actor 运行时；onOccupy 只有在 pid 已注入时才用 ctx（挂超时），
// 测试不注入 PID，因此可安全传 nil。
package seat

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"game_study/pkg/log"
)

func newTestRoom(tier Tier) (*Room, *[]Reward) {
	var got []Reward
	r := NewRoom(tier, 0, func(rw Reward) { got = append(got, rw) }, log.New("seat-test", log.WithLevel("error")))
	return r, &got
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
	require.Equal(t, int64(5*RatePerMinute[TierHigh]), (*got)[0].Amount)
}

// TestSeizeSettlesPreviousOwner 抢夺：原主按已占分钟数结算，新主接管座位。
func TestSeizeSettlesPreviousOwner(t *testing.T) {
	r, got := newTestRoom(TierMid)
	r.onOccupy(nil, &OccupyMsg{PlayerID: 1, SeatIndex: 3})
	r.seats[3].OccupiedAt = time.Now().Add(-3 * time.Minute)

	r.onOccupy(nil, &OccupyMsg{PlayerID: 2, SeatIndex: 3})

	require.Equal(t, SeatOccupied, r.seats[3].State)
	require.Equal(t, int64(2), r.seats[3].Occupant)
	require.Len(t, *got, 1, "被顶掉的原主应结算一次")
	require.Equal(t, int64(1), (*got)[0].PlayerID)
	require.Equal(t, int64(3), (*got)[0].Minutes)
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
	require.Equal(t, int64(MaxRewardMinutes), OccupancyMinutes(time.Now().Add(-90*time.Minute), time.Now()))
}
