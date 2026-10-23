// internal/game/player/snapshot.go —— State ↔ writebehind.Snapshot 转换与深拷贝
//
// 架构职责：把内存 State 序列化成可提交/可落库的 Snapshot，以及反向还原；
// clone()  additionally 保留派生属性与脏标记，供提交失败时整体回退。
//
// 协作关系：cache_actor.go 的「克隆 → diff → CAS」流程完全建立在本文件的
// 三个函数上；cache_load.go 用 stateFromSnapshot 把读到的快照还原成 State。
//
// 设计取舍：
//   - 为什么必须深拷贝而不是共享引用：回滚靠的是「换回 previous 指针」，
//     若 Ext 的 json.RawMessage（底层是 []byte）与快照共享底层数组，
//     回滚后业务再改同一个数组，previous 会被悄悄改写，回滚形同虚设；
//   - 为什么 diff 只比较快照 JSON：字段级比对要为每个新字段加代码，
//     JSON 字节比对自动覆盖所有字段；Ext 里模块私有状态的键序由
//     encoding/json 保证稳定（map 键排序输出），不会误报差异。
package player

import (
	"encoding/json"

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
