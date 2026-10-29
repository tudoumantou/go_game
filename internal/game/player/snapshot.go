// internal/game/player/snapshot.go —— State ↔ writebehind.Snapshot 转换
//
// 架构职责：两个方向的转换。
//   - 出：State.Field/Fields 按字段序列化（提交时只写变化的字段）；
//     State.snapshot 构造整份快照（pending 补提、落库需要完整形态）。
//   - 入：stateFromSnapshot 把读到的快照还原成 State。
//
// 协作关系：cache_actor.go 的提交流程用 Field/Fields（R2.5-B 起不再为了
// 写一个字段先复制整份状态）；cache_load.go 用 stateFromSnapshot 还原。
//
// 设计取舍：snapshot() 里的深拷贝是必要的——落库与 pending 都要一份
// 与内存解耦的副本，否则后续消息改动会污染已经在途的记录；而常规提交
// 不需要副本，所以走 Field 现取现序列化。
package player

import (
	"encoding/json"
	"fmt"
	"strings"

	"game_study/pkg/writebehind"
)

// cloneExt 深拷贝模块私有状态，避免 Actor 回滚时与快照共享底层数组。
//
// Go 知识点：append(json.RawMessage(nil), raw...) 分配新数组再拷贝，
// 得到的是副本而非别名；直接 out[name] = raw 会让两个 map 指向同一块
// 内存，一方写入另一方「见鬼」。
func cloneExt(src map[string]json.RawMessage) map[string]json.RawMessage {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]json.RawMessage, len(src))
	for name, raw := range src {
		out[name] = append(json.RawMessage(nil), raw...)
	}
	return out
}

// snapshot 产出当前状态的持久化视图（RLock 下读取，外部拿到的是副本）。
//
// 只含持久化字段：Profile/Attr/Items/Ext/AppliedGrants——即与 DB 列一一
// 对应的部分（player / player_attr / player_item / 模块 Persister /
// player_writebehind.applied_grants）；bonus（装备加成）与脏标记不落库。
// Field 实现 writebehind.FieldSource：按字段现取序列化。
//
// 这是「只写变化字段」的最后一块拼图：调用方不必先构造整份 Snapshot
// （100 件道具约 4.3µs），省掉的是纯粹的复制。
//
// 持读锁期间做 json.Marshal：写方要拿写锁，因此序列化出来的是一致快照；
// 代价是这段时间写方会等（一条消息一次，微秒级）。
func (s *State) Field(name string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch name {
	case writebehind.FieldProfile:
		return json.Marshal(s.profile)
	case writebehind.FieldAttr:
		return json.Marshal(s.attr)
	case writebehind.FieldItems:
		return json.Marshal(s.items)
	case writebehind.FieldGrants:
		return json.Marshal(s.appliedGrants)
	}
	mod, ok := strings.CutPrefix(name, writebehind.FieldExtPrefix)
	if !ok {
		return nil, fmt.Errorf("player: unknown snapshot field %q", name)
	}
	raw := s.ext[mod]
	if len(raw) == 0 {
		return nil, nil // 该模块没有状态
	}
	return raw, nil
}

// Fields 实现 writebehind.FieldSource：全部字段名（整份写入用）。
//
// 用于旧格式迁移（legacy）与「HasChanged 为真但字段标记为空」的兜底。
func (s *State) Fields() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(writebehind.CoreFields)+len(s.ext))
	out = append(out, writebehind.CoreFields[:]...)
	for name := range s.ext {
		out = append(out, writebehind.FieldExtPrefix+name)
	}
	return out
}

// clone 深拷贝一份状态，供落库线程独占使用。
//
// 为什么需要：A 类同步落库（充值）原本在 Actor 内跑事务，一个 5s 的
// DB 抖动会把同分片的其它玩家一起卡住。改成工作线程执行后，必须给它
// 一份与 Actor 解耦的副本——否则 Actor 继续处理后续消息，落库读到的是
// 被改到一半的状态（脏写），还会撞上 map 并发读写。
//
// 与 snapshot() 的区别：snapshot 只带持久化字段，clone 额外保留脏标记
// 与 appliedGrants——SaveForce 依赖它们决定「写哪些道具/模块」以及幂等键。
// dirtyHook 刻意不复制：副本不是活的 Actor 状态，不应再触发 flusher。
func (s *State) clone() *State {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := &State{
		playerID: s.playerID,
		profile:  s.profile,
		attr:     s.attr,
		baseAttr: s.baseAttr,
		bonus:    s.bonus,
		ext:      cloneExt(s.ext),
		changed:  s.changed,
		items:    make(map[int32]int64, len(s.items)),
		dirty:    make(map[string]bool, len(s.dirty)),
		// 容量按来源给足，避免落库线程扩容时再分配。
		dirtyItems:  make(map[int32]bool, len(s.dirtyItems)),
		dirtyFields: make(map[string]bool, len(s.dirtyFields)),
	}
	for id, count := range s.items {
		out.items[id] = count
	}
	if len(s.appliedGrants) > 0 {
		out.appliedGrants = make(map[string]bool, len(s.appliedGrants))
		for key, v := range s.appliedGrants {
			out.appliedGrants[key] = v
		}
	}
	for name, v := range s.dirty {
		out.dirty[name] = v
	}
	for id, v := range s.dirtyItems {
		out.dirtyItems[id] = v
	}
	for field, v := range s.dirtyFields {
		out.dirtyFields[field] = v
	}
	if len(s.removedExt) > 0 {
		out.removedExt = make(map[string]bool, len(s.removedExt))
		for name, v := range s.removedExt {
			out.removedExt[name] = v
		}
	}
	return out
}

func (s *State) snapshot() writebehind.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := writebehind.Snapshot{PlayerID: s.playerID, Profile: s.profile, Attr: s.attr, Items: make(map[int32]int64, len(s.items)), Ext: cloneExt(s.ext)}
	for id, count := range s.items {
		out.Items[id] = count
	}
	if s.appliedGrants != nil {
		out.AppliedGrants = make(map[string]bool, len(s.appliedGrants))
		for key, v := range s.appliedGrants {
			out.AppliedGrants[key] = v
		}
	}
	return out
}

// stateFromSnapshot 用快照还原一个全新 State。
//
// 注意 baseAttr 也初始化为快照的 Attr：落库值就是「职业基础属性」，
// 等级成长与 bonus 由登录流程重算（state.go RecalcAttrs / ClearBonus），
// 不从快照恢复。
func stateFromSnapshot(snapshot writebehind.Snapshot) *State {
	s := NewState(snapshot.PlayerID)
	s.profile = snapshot.Profile
	s.attr = snapshot.Attr
	s.baseAttr = snapshot.Attr
	s.ext = cloneExt(snapshot.Ext)
	if s.ext == nil {
		s.ext = make(map[string]json.RawMessage)
	}
	if snapshot.AppliedGrants != nil {
		s.appliedGrants = make(map[string]bool, len(snapshot.AppliedGrants))
		for key, v := range snapshot.AppliedGrants {
			s.appliedGrants[key] = v
		}
	}
	for id, count := range snapshot.Items {
		s.items[id] = count
	}
	return s
}
