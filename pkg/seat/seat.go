// pkg/seat/seat.go —— 抢位置房间的共享领域模型
//
// 为什么放在 pkg/ 而不是 internal/：单服（internal/game/seat）与跨服
// （internal/cross/seat）必须共用同一套「座位状态机 + 奖励算法」，而
// internal/ 禁止跨服务 import。放到 pkg/ 后两端只注入不同的「释放处理器」，
// 状态机与数值规则只此一份，不会出现单服/跨服两套逻辑漂移。
//
// 依赖方向：本包只依赖 pkg/actor、pkg/log，不反向依赖任何 internal/，
// 也不碰数据库——持久化由组装层（internal/game/seat）负责，房间状态机
// 保持纯内存、可单测。
package seat

import (
	"fmt"
	"time"
)

// Tier 房间档位。
//
// 用数字而不是字符串，三个理由：
//  1. 协议与落库都省带宽：int32/TINYINT 对比变长字符串；
//  2. 0 天然表示「非法/未填」——proto 默认值就是 0，玩家少传或传错会被
//     入口校验直接挡掉，不存在 "hign" 这种拼写错误只能靠运行期兜底；
//  3. 排序/比较/索引都比字符串便宜，日志里再用 String() 转回可读名。
type Tier int32

const (
	TierNone Tier = 0 // 非法值：未指定档位（协议默认值，必须被校验拦下）
	TierHigh Tier = 1 // 高
	TierMid  Tier = 2 // 中
	TierLow  Tier = 3 // 低
)

// String 返回可读档位名（仅用于日志/报错；协议与存储一律用数字）。
func (t Tier) String() string {
	switch t {
	case TierHigh:
		return "high"
	case TierMid:
		return "mid"
	case TierLow:
		return "low"
	default:
		return "none"
	}
}

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
	RoomCounts = map[Tier]int{TierHigh: 1, TierMid: 3, TierLow: 6}
	// TierOrder 遍历顺序（map 无序，查询/初始化用固定顺序保证结果稳定）。
	TierOrder = []Tier{TierHigh, TierMid, TierLow}
	// RatePerMinute 每档每分钟奖励数量（可被组装层按配置覆盖）。
	RatePerMinute = map[Tier]int64{TierHigh: 10, TierMid: 5, TierLow: 2}
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
	OccupyID   int64     // 本次占用实例 ID（落库与结算幂等的锚点）
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
func RewardOf(tier Tier, minutes int64) int64 {
	if minutes <= 0 {
		return 0
	}
	return RatePerMinute[tier] * minutes
}

// RoomName 房间 Actor 的稳定命名：seat:<tier>:<index>。
//
// 名字即路由：单服玩家模块按名字找房间，跨服管理器按名字索引房间；
// 持久化也以它做「房间 → 落库行」的分组键。
func RoomName(tier Tier, index int) string {
	return fmt.Sprintf("seat:%d:%d", tier, index)
}

// SeatView 座位只读快照（协议层直接映射，也是落库的数据源）。
type SeatView struct {
	Index      int
	State      SeatState
	Occupant   int64
	OccupyID   int64 // 本次占用实例 ID；空座为 0
	OccupiedAt int64 // 占用起始 unix 秒；空座为 0。查询端据此实时算倒计时
	RemainSec  int64 // 距 1h 超时的剩余秒数；空座为 0（快照生成时的值）
}

// RoomSnapshot 房间只读快照。
type RoomSnapshot struct {
	Tier      Tier
	RoomIndex int
	Seats     []SeatView
}

// ReleaseEvent 一次座位释放（座位让出 + 应得奖励）。
//
// Amount 在房间侧算好一并带出，避免单服/跨服各自再算一遍导致数值不一致；
// 0 表示不足 1 分钟、不发奖（座位仍然释放）。
// OccupyID 是本次占用实例 ID，作为结算幂等键：在线发奖的 BizID、outbox 的
// 主键都用它，重启后重放同一笔结算不会重复发奖。
type ReleaseEvent struct {
	PlayerID  int64
	Tier      Tier
	RoomIndex int
	SeatIndex int
	OccupyID  int64
	Minutes   int64
	Amount    int64
	Node      string // 单服为空；跨服为玩家所在游戏服节点
}

// RestoredSlot 重启时从库里恢复出来的一条座位占用。
type RestoredSlot struct {
	SeatIndex  int
	Occupant   int64
	OccupyID   int64
	Node       string
	OccupiedAt time.Time
}

// PendingReward 待结算/待补发的一笔座位奖励（outbox 行）。
type PendingReward struct {
	OccupyID int64
	Tier     Tier
	Minutes  int64
}
