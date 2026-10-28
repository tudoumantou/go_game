// pkg/seat/occupancy_test.go —— 一人一座登记与座位坐标校验的不变量
//
// 覆盖四个容易回归的点：
//  1. 坐标校验必须拦住未知档位与越界（跨服模式靠它避免「静默失败」）；
//  2. 换座要能把旧座交还给调用方去结算；
//  3. 旧座的延迟释放回调不能误删新登记；
//  4. 无奖励的释放也必须清登记，否则玩家会永久占不了新座。
package seat

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"game_study/pkg/log"
)

func TestValidSeat(t *testing.T) {
	require.True(t, ValidSeat(TierHigh, 0, 0))
	require.True(t, ValidSeat(TierHigh, 0, SeatsPerRoom-1))
	require.True(t, ValidSeat(TierMid, 2, 5))
	require.True(t, ValidSeat(TierLow, 5, 9))

	require.False(t, ValidSeat("hign", 0, 0), "未知档位")
	require.False(t, ValidSeat(TierHigh, 1, 0), "high 只有 1 间房")
	require.False(t, ValidSeat(TierMid, 3, 0), "mid 只有 3 间房")
	require.False(t, ValidSeat(TierLow, -1, 0), "负房号")
	require.False(t, ValidSeat(TierLow, 0, -1), "负座号")
	require.False(t, ValidSeat(TierLow, 0, SeatsPerRoom), "座号越界")
}

func TestClaimReturnsOldSeatOnSwitch(t *testing.T) {
	o := NewOccupancy()
	a := SeatRef{Tier: TierHigh, RoomIndex: 0, SeatIndex: 1}
	b := SeatRef{Tier: TierLow, RoomIndex: 2, SeatIndex: 3}

	_, had := o.Claim(7, a)
	require.False(t, had, "首次占座没有旧座")

	old, had := o.Claim(7, b)
	require.True(t, had)
	require.Equal(t, a, old, "换座应把旧座交还调用方去结算")

	cur, ok := o.Get(7)
	require.True(t, ok)
	require.Equal(t, b, cur)
}

func TestClaimSameSeatIsNotSwitch(t *testing.T) {
	o := NewOccupancy()
	a := SeatRef{Tier: TierMid, RoomIndex: 0, SeatIndex: 0}
	o.Claim(7, a)

	_, had := o.Claim(7, a)
	require.False(t, had, "同座重复占用不算换座，否则会多发一次 Leave")
}

func TestReleaseOnlyWhenRefMatches(t *testing.T) {
	o := NewOccupancy()
	old := SeatRef{Tier: TierHigh, RoomIndex: 0, SeatIndex: 1}
	newer := SeatRef{Tier: TierLow, RoomIndex: 0, SeatIndex: 2}
	o.Claim(7, newer)

	// 旧座的释放回调晚到：不能把玩家新占的座位登记删掉。
	o.Release(7, old)
	cur, ok := o.Get(7)
	require.True(t, ok, "旧座回调不得清除新登记")
	require.Equal(t, newer, cur)

	o.Release(7, newer)
	_, ok = o.Get(7)
	require.False(t, ok)
}

func TestRoomReleaseNotifiesOccupancy(t *testing.T) {
	var (
		gotPlayer int64
		gotRef    SeatRef
	)
	r := NewRoom(TierLow, 2, nil, log.New("seat-test", log.WithLevel("error")))
	r.SetOnRelease(func(playerID int64, ref SeatRef) {
		gotPlayer, gotRef = playerID, ref
	})

	r.onOccupy(nil, &OccupyMsg{PlayerID: 9, SeatIndex: 4})
	r.onLeave(&LeaveMsg{PlayerID: 9, SeatIndex: 4})

	require.Equal(t, int64(9), gotPlayer)
	require.Equal(t, SeatRef{Tier: TierLow, RoomIndex: 2, SeatIndex: 4}, gotRef)
}

func TestRoomReleaseNotifiesEvenWithoutReward(t *testing.T) {
	called := 0
	r := NewRoom(TierLow, 0, nil, log.New("seat-test", log.WithLevel("error")))
	r.SetOnRelease(func(int64, SeatRef) { called++ })

	// 只占了几毫秒：不足 1 分钟没有奖励，但登记必须清。
	r.onOccupy(nil, &OccupyMsg{PlayerID: 9, SeatIndex: 0})
	r.onLeave(&LeaveMsg{PlayerID: 9, SeatIndex: 0})
	require.Equal(t, 1, called, "无奖励也要清登记，否则玩家永远占不了新座")
}

func TestNextSeqUnique(t *testing.T) {
	const n = 2000
	seen := make([]int64, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			seen[i] = nextSeq()
		}(i)
	}
	close(start)
	wg.Wait()

	m := make(map[int64]struct{}, n)
	for _, s := range seen {
		_, dup := m[s]
		require.False(t, dup, "序号重复会导致发奖被幂等吞掉：%d", s)
		m[s] = struct{}{}
	}
}
