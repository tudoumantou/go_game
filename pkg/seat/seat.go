// pkg/seat/seat.go —— 抢位置领域模型（档位 / 座位 / 快照 / 结算）
//
// 只保留三个数据形状：
//   Seat    房间 Actor 内部的内存态
//   Slot    对外的扁平快照（查询协议、落库 JSON、重启恢复共用）
//   Release 一次释放的结算入参
// 没有 View/Row/JSON 多套镜像：落库和协议用同一个 Slot，靠 json tag 直接序列化。
package seat

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Tier 档位。0 保留为非法值（协议默认值即 0，入口校验直接拦下）。
type Tier int32

const (
	TierNone Tier = 0
	TierHigh Tier = 1
	TierMid  Tier = 2
	TierLow  Tier = 3
)

// String 仅供日志使用。
func (t Tier) String() string {
	switch t {
	case TierHigh:
		return "high"
	case TierMid:
		return "mid"
	case TierLow:
		return "low"
	}
	return "none"
}

const (
	SeatsPerRoom           = 10
	HoldLimit              = time.Hour
	MaxRewardMinutes int64 = 60
	RewardItem       int32 = 1001 // 钻石
)

var (
	RoomCounts    = map[Tier]int{TierHigh: 1, TierMid: 3, TierLow: 6}
	TierOrder     = []Tier{TierHigh, TierMid, TierLow}
	RatePerMinute = map[Tier]int64{TierHigh: 10, TierMid: 5, TierLow: 2}
)

// SeatState 座位状态。
type SeatState int32

const (
	SeatEmpty    SeatState = 0
	SeatOccupied SeatState = 1
)

// Seat 座位（内存态，只在房间 Actor 内读写，无锁）。
type Seat struct {
	Index      int
	State      SeatState
	Occupant   int64
	OccupyID   int64 // 本次占用实例 ID：结算幂等键
	OccupiedAt time.Time
}

// Slot 座位快照：查询协议、落库 JSON、重启恢复共用。
//
// OccupiedAt 用 unix 秒而不是 time.Time：落库省事，JSON 也不会变成
// RFC3339 长字符串；倒计时由查询端按当前时间现算。
type Slot struct {
	Index      int       `json:"index"`
	State      SeatState `json:"state"`
	Occupant   int64     `json:"occupant"`
	OccupyID   int64     `json:"occupy_id"`
	OccupiedAt int64     `json:"occupied_at"`
}

// RoomSnapshot 房间快照（一行 = 一个房间的完整状态）。
type RoomSnapshot struct {
	Tier  Tier
	Index int
	Slots []Slot
}

// Release 一次座位释放（结算入参）。
type Release struct {
	PlayerID  int64
	Tier      Tier
	RoomIndex int
	SeatIndex int
	OccupyID  int64
	Minutes   int64
	Amount    int64 // 0 表示不足 1 分钟、不发奖
}

// SeatRef 座位引用（一人一座登记）。
type SeatRef struct {
	Tier      Tier
	RoomIndex int
	SeatIndex int
}

// Pending 待补发奖励（outbox 行）。
type Pending struct {
	OccupyID int64
	Tier     Tier
	Minutes  int64
}

// OccupancyMinutes 已占分钟数（向下取整，1h 封顶）。
func OccupancyMinutes(occupiedAt, now time.Time) int64 {
	if occupiedAt.IsZero() || !now.After(occupiedAt) {
		return 0
	}
	mins := int64(now.Sub(occupiedAt) / time.Minute)
	if mins > MaxRewardMinutes {
		mins = MaxRewardMinutes
	}
	return mins
}

// RewardOf 奖励 = 档位单价 × 分钟数。
func RewardOf(tier Tier, minutes int64) int64 {
	if minutes <= 0 {
		return 0
	}
	return RatePerMinute[tier] * minutes
}

// RoomName 房间 Actor 名：seat:<tier>:<index>（兼作落库分组键）。
func RoomName(tier Tier, index int) string {
	return fmt.Sprintf("seat:%d:%d", tier, index)
}

// ValidSeat 校验座位坐标；档位传 0 时 RoomCounts 查不到，一并拦下。
func ValidSeat(tier Tier, roomIndex, seatIndex int) bool {
	n, ok := RoomCounts[tier]
	return ok && roomIndex >= 0 && roomIndex < n &&
		seatIndex >= 0 && seatIndex < SeatsPerRoom
}

// seqClock 进程内单调发号器，生成 occupy_id。
var seqClock atomic.Int64

// nextSeq 返回进程内唯一的占用实例 ID。
//
// 以纳秒为基：重启后一定大于历史值，不会和已落库的旧 ID 撞上；CAS 兜底
// 同一纳秒的并发调用。
func nextSeq() int64 {
	for {
		old := seqClock.Load()
		now := time.Now().UnixNano()
		if now <= old {
			now = old + 1
		}
		if seqClock.CompareAndSwap(old, now) {
			return now
		}
	}
}
