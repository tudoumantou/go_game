// pkg/seat/occupancy.go —— 一人一座的全局占座登记 + 座位坐标校验 + 序号发号器
//
// 为什么需要「全局」登记：房间是 10 个独立 Actor，各自只看得见自己那 10 个座。
// 若只在房间内判重，玩家仍可跨房间各占一座（三档合计 100 座），按座叠加奖励，
// 「抢位置」就退化成「包场」。因此把「玩家 → 当前座位」的登记提到房间之外，
// 由管理器在投递占用消息前读写；房间侧通过 onRelease 回调在释放时清理。
//
// 进程内语义：单服（internal/game/seat）与跨服（internal/cross/seat）各自在
// 自己的进程里持有一份 Occupancy——单服的房间在本进程，跨服的房间在 cross
// 进程，两者天然不会互相干扰，不需要落 Redis 或 MySQL。
package seat

import (
	"sync"
	"sync/atomic"
	"time"
)

// SeatRef 一个座位引用（档位 + 房号 + 座号）。
type SeatRef struct {
	Tier      Tier
	RoomIndex int
	SeatIndex int
}

// ValidSeat 校验座位坐标是否落在已配置的房间/座位范围内。
//
// 服务端必须自己校验，而不是依赖「查不到房间就返回 nil」兜底：
// 跨服模式下操作是发给 cross 节点的，房间不存在时对端只会静默丢弃，
// 玩家却收到「成功」回包——表现为点了没反应。入口处拦住才是正解。
//
// 档位传 0（未填/填错）时 RoomCounts 查不到，直接判非法。
func ValidSeat(tier Tier, roomIndex, seatIndex int) bool {
	n, ok := RoomCounts[tier]
	if !ok {
		return false
	}
	return roomIndex >= 0 && roomIndex < n && seatIndex >= 0 && seatIndex < SeatsPerRoom
}

// Occupancy 进程内「一人一座」登记表。
//
// 只记录玩家当前占用的唯一座位；不记录历史、不做 TTL——座位释放时由
// onRelease 回调精确清理，比超时淘汰更可靠。
type Occupancy struct {
	mu   sync.Mutex
	refs map[int64]SeatRef
}

// NewOccupancy 创建登记表。
func NewOccupancy() *Occupancy {
	return &Occupancy{refs: make(map[int64]SeatRef)}
}

// Claim 把玩家的座位登记为目标座位，返回其原先的座位（若有）。
//
// 返回值是「自动换座」的实现点：调用方拿到旧座位后应先向旧房间投递
// LeaveMsg（触发旧座结算），再投递新的 OccupyMsg。
//
// 同一座位重复 Claim 时 had 返回 false，避免把「自己占自己」误判成换座、
// 发出一次多余的 Leave。
func (o *Occupancy) Claim(playerID int64, ref SeatRef) (old SeatRef, had bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	old, had = o.refs[playerID]
	o.refs[playerID] = ref
	if had && old == ref {
		return SeatRef{}, false
	}
	return old, had
}

// Release 释放登记：仅当当前登记仍等于 ref 时才删除。
//
// 必须比对 ref：换座场景下「旧房间的释放回调」会晚于「新登记」到达，
// 不比对就会把玩家新占的座位登记误删，导致其可以再占第二座。
func (o *Occupancy) Release(playerID int64, ref SeatRef) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if cur, ok := o.refs[playerID]; ok && cur == ref {
		delete(o.refs, playerID)
	}
}

// Get 查询玩家当前座位。
func (o *Occupancy) Get(playerID int64) (SeatRef, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	ref, ok := o.refs[playerID]
	return ref, ok
}

// seqClock 进程内全局单调发号器。
//
// 为什么不用「每房间各自取纳秒」：不同房间在同一纳秒各产生一条释放事件时，
// 序号会相同；而序号是发奖幂等键（BizID / 挂起表主键），相同意味着第二笔
// 被当成重复投递吞掉——玩家少发奖。这里用 CAS 保证整个进程内严格唯一。
var seqClock atomic.Int64

// nextSeq 返回进程内严格递增的唯一序号（以纳秒为基，保重启后不会回退到
// 历史值附近，避免与 applied_grants 里的旧键撞车）。
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
