// internal/game/seat/manager_test.go —— 换座顺序与失败路径
//
// 这里的 Manager 只用到 rooms / seats / logger，没有 Actor 系统：
// 投递由 deliver 参数注入，所以无需起 actor.System 就能验证顺序。
package seat

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"game_study/pkg/log"
	pkgseat "game_study/pkg/seat"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	logger := log.New("seat-test", log.WithLevel("error"))
	high := pkgseat.RoomID(pkgseat.TierHigh, 0)
	mid := pkgseat.RoomID(pkgseat.TierMid, 0)
	return &Manager{
		logger: logger,
		rooms: map[int32]*pkgseat.Room{
			high: pkgseat.NewRoom(pkgseat.TierHigh, 0, logger),
			mid:  pkgseat.NewRoom(pkgseat.TierMid, 0, logger),
		},
	}
}

// 新座投递失败时，旧座必须原样保留。
//
// 回归背景：早期实现是「先放旧座、再占新座」，新座投递失败（邮箱满 /
// 房间已摘）时旧座已经释放，玩家两头空。
func TestSwitchSeatKeepsOldSeatWhenOccupyFails(t *testing.T) {
	m := newTestManager(t)
	oldRef := pkgseat.SeatRef{RoomID: pkgseat.RoomID(pkgseat.TierHigh, 0), SeatIndex: 2}
	m.seats.Store(int64(301), oldRef)

	deliver := func(roomID int32, msg any) error {
		if _, ok := msg.(*pkgseat.LeaveMsg); ok {
			t.Fatalf("不应向旧座投递释放消息（新座都没占上）: room=%d", roomID)
		}
		return errors.New("mailbox full")
	}

	ok := m.switchSeat(pkgseat.RoomID(pkgseat.TierMid, 0), 0, 301, deliver)
	require.False(t, ok, "投递失败必须返回 false")

	got, found := m.Seat(301)
	require.True(t, found, "旧座不能被清掉")
	require.Equal(t, oldRef, got)
}

// 成功换座：先占新座、再释放旧座。
func TestSwitchSeatOccupiesNewBeforeReleasingOld(t *testing.T) {
	m := newTestManager(t)
	high := pkgseat.RoomID(pkgseat.TierHigh, 0)
	mid := pkgseat.RoomID(pkgseat.TierMid, 0)
	m.seats.Store(int64(301), pkgseat.SeatRef{RoomID: high, SeatIndex: 2})

	var calls []string
	deliver := func(roomID int32, msg any) error {
		kind := "occupy"
		if _, ok := msg.(*pkgseat.LeaveMsg); ok {
			kind = "leave"
		}
		calls = append(calls, kind+":"+strconv.Itoa(int(roomID)))
		return nil
	}

	require.True(t, m.switchSeat(mid, 0, 301, deliver))
	require.Equal(t, []string{"occupy:" + strconv.Itoa(int(mid)), "leave:" + strconv.Itoa(int(high))}, calls)
}

// 占的正是当前座位时不应产生多余的释放消息（幂等）。
func TestSwitchSeatSameSeatSkipsRelease(t *testing.T) {
	m := newTestManager(t)
	high := pkgseat.RoomID(pkgseat.TierHigh, 0)
	m.seats.Store(int64(301), pkgseat.SeatRef{RoomID: high, SeatIndex: 2})

	var leaves int
	deliver := func(_ int32, msg any) error {
		if _, ok := msg.(*pkgseat.LeaveMsg); ok {
			leaves++
		}
		return nil
	}

	require.True(t, m.switchSeat(high, 2, 301, deliver))
	require.Zero(t, leaves, "同一座位不应释放")
}
