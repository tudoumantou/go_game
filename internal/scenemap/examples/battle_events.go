package examples

import (
	"sync"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/battle"
)

// BattleEvent 是一条战斗结果事件及其收件人。
//
// 收件人是"目标主人/击杀者主人"，不是目标本身：宠物被打，事件发给主人。
type BattleEvent struct {
	To  scenemap.UUID
	Evt any
}

// BattleEventRecorder 记录 battle.Notify 投递的结果事件。
//
// 并发：Notify 在 MGR goroutine 里同步调用，测试在另一个 goroutine 里读，
// 所以必须加锁。与 RecordingSink 同样的取舍：锁只保护一次切片追加。
type BattleEventRecorder struct {
	mu   sync.Mutex
	list []BattleEvent
}

// NewBattleEventRecorder 构造记录器。
func NewBattleEventRecorder() *BattleEventRecorder {
	return &BattleEventRecorder{}
}

// Add 记录一条事件（实现 battle.Options.Notify 的形状）。
func (r *BattleEventRecorder) Add(to scenemap.UUID, evt any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.list = append(r.list, BattleEvent{To: to, Evt: evt})
}

// All 返回全部事件副本。
func (r *BattleEventRecorder) All() []BattleEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]BattleEvent, len(r.list))
	copy(out, r.list)
	return out
}

// Len 返回事件条数。
func (r *BattleEventRecorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.list)
}

// ResultEvents 返回技能结算事件（按记录顺序）。
func (r *BattleEventRecorder) ResultEvents() []battle.SkillResultEvent {
	var out []battle.SkillResultEvent
	for _, e := range r.All() {
		if ev, ok := e.Evt.(battle.SkillResultEvent); ok {
			out = append(out, ev)
		}
	}
	return out
}

// MonsterDeaths 返回怪物死亡事件。
func (r *BattleEventRecorder) MonsterDeaths() []battle.MonDeadEvent {
	var out []battle.MonDeadEvent
	for _, e := range r.All() {
		if ev, ok := e.Evt.(battle.MonDeadEvent); ok {
			out = append(out, ev)
		}
	}
	return out
}

// Reset 清空记录。
func (r *BattleEventRecorder) Reset() {
	r.mu.Lock()
	r.list = nil
	r.mu.Unlock()
}
