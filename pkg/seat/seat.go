// pkg/seat/seat.go —— 抢位置房间的共享领域模型
//
// 为什么放在 pkg/ 而不是 internal/：单服（internal/game/seat）与跨服
// （internal/cross/seat）必须共用同一套「座位状态机 + 奖励算法」，而
// internal/ 禁止跨服务 import。放到 pkg/ 后两端只注入不同的「奖励发射器」，
// 状态机与数值规则只此一份，不会出现单服/跨服两套逻辑漂移。
//
// 依赖方向：本包只依赖 pkg/actor、pkg/log，不反向依赖任何 internal/。
package seat

import (
	"fmt"
	"time"
)

// 档位。
const (
	TierHigh = "high"
	TierMid  = "mid"
	TierLow  = "low"
)

// 房间规格与规则常量。
const (
	// SeatsPerRoom 每个房间的座位数。
	SeatsPerRoom = 10
	// HoldLimit 单次占用的最长持有时间：到点自动结算并让座。
	HoldLimit = time.Hour
	// MaxRewardMinutes 计奖分钟数上限（1 小时），防止卡座刷奖励。
	MaxRewardMinutes int64 = 60
	// RewardItem 奖励道具：钻石（与 activity 模块保持一致）。
	RewardItem int32 = 1001
)

var (
	// RoomCounts 每档房间数：高 1 / 中 3 / 低 6。
	RoomCounts = map[string]int{TierHigh: 1, TierMid: 3, TierLow: 6}
	// TierOrder 遍历顺序（map 无序，查询/初始化用固定顺序保证结果稳定）。
	TierOrder = []string{TierHigh, TierMid, TierLow}
	// RatePerMinute 每档每分钟奖励数量（可被组装层按配置覆盖）。
	RatePerMinute = map[string]int64{TierHigh: 10, TierMid: 5, TierLow: 2}
)

// SeatState 座位状态。
type SeatState int32

const (
	SeatEmpty    SeatState = 0
	SeatOccupied SeatState = 1
)

// Seat 单个座位。字段只被房间 Actor 串行读写（无锁）。
type Seat struct {
	Index      int
	State      SeatState
	Occupant   int64     // 当前占用者；空座为 0
	Node       string    // 占用者所在游戏服节点（跨服回推奖励用；单服为空）
	OccupiedAt time.Time // 本次占用起始时间
}

// OccupancyMinutes 本次占用已满的分钟数（向下取整，1h 封顶）。
//
// 用「结束时间 - 起始时间」现算而不是累加 tick：Actor 重启/定时器抖动
// 都不会让分钟数漂移，且与「1 小时硬上限」天然一致。
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

// RewardOf 奖励数量 = 档位每分钟单价 × 分钟数。
func RewardOf(tier string, minutes int64) int64 {
	if minutes <= 0 {
		return 0
	}
	return RatePerMinute[tier] * minutes
}

// RoomName 房间 Actor 的稳定命名：seat:<tier>:<index>。
//
// 名字即路由：单服玩家模块按名字拿 PID，跨服管理器按名字索引房间。
// 固定名字让「重启后同一间房仍能被找到」这件事不依赖任何额外注册表。
func RoomName(tier string, index int) string {
	return fmt.Sprintf("seat:%s:%d", tier, index)
}

// SeatView 座位只读快照（协议层直接映射）。
type SeatView struct {
	Index     int
	State     SeatState
	Occupant  int64
	OccupiedAt int64 // 占用起始 unix 秒；空座为 0。查询端据此实时算倒计时
	RemainSec int64 // 距 1h 超时的剩余秒数；空座为 0（快照生成时的值）
}

// RoomSnapshot 房间只读快照。
type RoomSnapshot struct {
	Tier      string
	RoomIndex int
	Seats     []SeatView
}

// Reward 一次座位释放产生的奖励事件。
//
// Amount 在房间侧算好一并带出，避免单服/跨服各自再算一遍导致数值不一致。
// Seq 是释放事件的唯一序号（幂等键）：在线发奖用它做 GrantItemsMsg.BizID，
// 离线挂起用它做表主键，重投不会二次发奖。
type Reward struct {
	PlayerID int64
	Tier     string
	Minutes  int64
	Amount   int64
	Seq      int64
	Node     string // 单服为空；跨服为玩家所在游戏服节点
}

// PendingReward 离线挂起的一笔座位奖励。
type PendingReward struct {
	Seq     int64
	Tier    string
	Minutes int64
}
