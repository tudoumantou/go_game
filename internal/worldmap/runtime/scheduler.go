package runtime

import (
	"container/heap"
	"fmt"

	"game_study/internal/worldmap/domain"
)

// DueQueue 是场景的到期事件最小堆（文档 9.2 / 18.1 scheduler.go）。
//
// 为什么用 container/heap 而不是有序切片或 time.Timer：
//   - 有序切片插入 O(n)，热点场景每次行军都要插一条，退化为 O(n²)；
//   - 每个事件一个 time.Timer 会随部队数量增长产生大量定时器，
//     且改速/召回后旧 timer 需要精确 Stop，漏一个就是幽灵事件。
//
// 单个堆 + 每轮取堆顶，天然只关心"最早一件"，与场景循环的
// 批量预算模型匹配（每轮最多处理 N 条）。
//
// 取消采用懒删除：改速/召回/战斗提前结束时，旧事件仍留在堆里，
// 只把它的关联键记进 cancelled。取出时发现已取消就丢弃。
// 这样做的好处是 O(1) 取消、不需要在堆里查找元素下标（显式删除
// 要维护 index，任何一处漏改就会破坏堆序）。
type DueQueue struct {
	items     []domain.DueEvent
	cancelled map[string]struct{}
}

// NewDueQueue 创建空堆。
func NewDueQueue() *DueQueue {
	return &DueQueue{cancelled: make(map[string]struct{})}
}

// Len 实现 heap.Interface。
func (q *DueQueue) Len() int { return len(q.items) }

// Less 实现 heap.Interface：严格复用 domain.DueLess，
// 保证调度顺序与恢复补处理顺序完全一致（文档 9.2 的三元组）。
func (q *DueQueue) Less(i, j int) bool { return domain.DueLess(q.items[i], q.items[j]) }

// Swap 实现 heap.Interface。
func (q *DueQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }

// Push 实现 heap.Interface。参数是 any 是标准库约定，
// 断言失败直接 panic：这只可能是调用方 bug，静默忽略会让事件永久丢失。
func (q *DueQueue) Push(x any) {
	ev, ok := x.(domain.DueEvent)
	if !ok {
		panic(fmt.Sprintf("DueQueue: 非 DueEvent 入堆: %T", x))
	}
	q.items = append(q.items, ev)
}

// Pop 实现 heap.Interface。
func (q *DueQueue) Pop() any {
	n := len(q.items)
	ev := q.items[n-1]
	q.items = q.items[:n-1]
	return ev
}

// Schedule 插入事件。同一 (kind, 关联键) 重复插入是允许的：
// 后续由 Cancel 或取出时的幂等键判重兜住，不在入堆时去重——
// 入堆去重要遍历，而重复事件本身是低频的（同一部队改速多次）。
func (q *DueQueue) Schedule(ev domain.DueEvent) {
	heap.Push(q, ev)
}

// Cancel 标记某关联键的事件作废（懒删除）。key 由 moveKey /
// battleKey / sessionKey / refreshKey 构造。
func (q *DueQueue) Cancel(key string) {
	if key == "" {
		return
	}
	q.cancelled[key] = struct{}{}
}

// Peek 返回堆顶事件而不弹出（用于判断"是否已到期"）。
func (q *DueQueue) Peek() (domain.DueEvent, bool) {
	q.dropCancelledHead()
	if len(q.items) == 0 {
		return domain.DueEvent{}, false
	}
	return q.items[0], true
}

// PopDue 弹出所有 due_at_ms <= nowMS 的事件，最多 limit 条。
// 返回的切片按 DueLess 升序，调用方可以直接按序处理。
func (q *DueQueue) PopDue(nowMS int64, limit int) []domain.DueEvent {
	if limit <= 0 {
		return nil
	}
	var out []domain.DueEvent
	for len(out) < limit {
		q.dropCancelledHead()
		if len(q.items) == 0 {
			break
		}
		if q.items[0].DueAtMS > nowMS {
			break
		}
		out = append(out, heap.Pop(q).(domain.DueEvent))
	}
	return out
}

// NextDueAt 返回最早未取消事件的到期时刻；空堆返回 (0,false)。
// 场景循环据此计算"还要睡多久"，避免忙轮询。
func (q *DueQueue) NextDueAt() (int64, bool) {
	q.dropCancelledHead()
	if len(q.items) == 0 {
		return 0, false
	}
	return q.items[0].DueAtMS, true
}

// Len 返回有效事件数（含未清理的已取消事件）。
func (q *DueQueue) Size() int { return len(q.items) }

// dropCancelledHead 丢弃堆顶所有已取消事件。
// 只在堆顶做，因为懒删除的语义就是"等到它成为最早事件时再丢"。
func (q *DueQueue) dropCancelledHead() {
	for len(q.items) > 0 {
		if _, dead := q.cancelled[eventRef(q.items[0])]; !dead {
			return
		}
		heap.Pop(q)
	}
}

// eventRef 把事件映射为取消键。必须与 Cancel 的入参同构，
// 因此集中在这里构造，避免两处字符串格式漂移。
func eventRef(ev domain.DueEvent) string {
	switch ev.Kind {
	case domain.DueKindMoveArrive, domain.DueKindMoveBoundary:
		return moveRef(ev.MoveID)
	case domain.DueKindBattleTimeout:
		return battleRef(ev.BattleID)
	case domain.DueKindCollectEnd:
		return sessionRef(ev.SessionID)
	case domain.DueKindRefresh:
		// 走 domain.RefreshKey 而不是本地再拼一份：这个键同时被
		// manager 当作幂等 RequestID（domain/collect.go:196 的注释说明
		// 「键相同即已创建，重试不重复抽奖」）。两处各拼一遍时，改一处
		// 漏一处不会报错——只会让补点槽位的去重键与幂等键对不上，
		// 表现为「定时刷新偶尔不触发」，查起来没有任何线索。
		return domain.RefreshKey(ev.WorldID, ev.RichBlockID, ev.RefreshGeneration, ev.RefreshSlot)
	default:
		return ""
	}
}

func moveRef(moveID string) string {
	if moveID == "" {
		return ""
	}
	return "move:" + moveID
}

func battleRef(battleID string) string {
	if battleID == "" {
		return ""
	}
	return "battle:" + battleID
}

func sessionRef(sessionID string) string {
	if sessionID == "" {
		return ""
	}
	return "session:" + sessionID
}
