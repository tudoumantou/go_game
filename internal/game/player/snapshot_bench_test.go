// internal/game/player/snapshot_bench_test.go —— 热路径基线（R2.5 改造前）
//
// 量化对象：一条有状态消息在缓存权威模式下的固定开销，由三部分组成——
//  1. clone()：为回滚备份整份状态（内部 snapshot + 还原，两次 map 分配）；
//  2. 提交前的前后对比：两次 json.Marshal + bytes.Equal；
//  3. Commit：真正写 Redis 的那一次序列化。
//
// 建立基线后，R2.5 的 S3（去 clone 与全量比对）应能把 1、2 两步归零，
// 只留第 3 步的一次序列化。
package player

import (
	"bytes"
	"encoding/json"
	"testing"

	"game_study/internal/game/module"
)

// benchState 构造一个「中等玩家」的状态：100 件道具 + 5 个模块 Ext。
//
// 规模参考：正式服在线玩家的典型量级（背包 50-200 件、模块 Ext 4-8 个）。
func benchState() *State {
	s := NewState(42)
	s.SetProfile(module.Profile{Name: "bench_player", Level: 60, Exp: 12345, Coin: 987654, SceneID: 1001})
	s.SetAttrStruct(Attr{Career: 1, Hp: 8000, Mp: 3000, Atk: 1200, Def: 800, Spd: 150})
	for i := int32(0); i < 100; i++ {
		s.SeedItem(10000+i, int64(i*13+1))
	}
	// 5 个模块 Ext，每个约 200-400 字节，接近真实签到/任务/装备进度
	exts := map[string]string{
		"signin":      `{"days":[1,2,3,4,5],"rewards":{"1001":700,"1002":350}}`,
		"quest":       `{"records":{"1":{"progress":3,"claimed":false},"2":{"progress":10,"claimed":true}}}`,
		"equip":       `{"equips":[{"uid":1,"equip_id":1001,"enhance":3,"quality":2,"equipped":true}]}`,
		"activity":    `{"records":{"101":{"progress":5},"102":{"progress":2}}}`,
		"loginreward": `{"day":3,"streak":7,"last":"2026-10-22"}`,
	}
	for name, raw := range exts {
		s.SetExt(name, json.RawMessage(raw))
	}
	return s
}

// benchmarkClone 复刻 R2.5 之前的 clone 实现（只为让 Legacy 对照可运行）。
// 生产代码里已删除——失败路径改为整份重载，不需要备份。
func benchmarkClone(s *State) *State {
	out := stateFromSnapshot(s.snapshot())
	s.mu.RLock()
	defer s.mu.RUnlock()
	out.bonus = s.bonus
	out.dirtyHook = s.dirtyHook
	for name, dirty := range s.dirty {
		out.dirty[name] = dirty
	}
	for id, dirty := range s.dirtyItems {
		out.dirtyItems[id] = dirty
	}
	return out
}

// BenchmarkStateClone 回滚备份的成本（R2.5 后生产路径已无此开销）。
func BenchmarkStateClone(b *testing.B) {
	s := benchState()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = benchmarkClone(s)
	}
}

// BenchmarkSnapshotMarshal 单次快照序列化（Commit 里那一次，不可省）。
func BenchmarkSnapshotMarshal(b *testing.B) {
	s := benchState()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := json.Marshal(s.snapshot())
		if err != nil {
			b.Fatal(err)
		}
		if len(raw) == 0 {
			b.Fatal("empty snapshot")
		}
	}
}

// BenchmarkMessageOverhead_Legacy 改造前路径的开销复刻（R2.5 之前）。
//
// 保留它只用于对照：代码里已经不再走这条路（clone + 两次序列化 + 字节比较），
// 它的数字说明「当初为什么值得改」。
//
// 完整复刻 receiveCached 的准备工作与提交判定：
//
//	clone → marshal(previous.snapshot) → 业务（模拟一次加道具）
//	→ marshal(state.snapshot) → bytes.Equal
//
// 这是 R2.5 要砍掉的部分：改造后应只剩「业务 + 一次 snapshot 序列化」。
func BenchmarkMessageOverhead_Legacy(b *testing.B) {
	s := benchState()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		previous := benchmarkClone(s)
		before, err := json.Marshal(previous.snapshot())
		if err != nil {
			b.Fatal(err)
		}
		s.AddItem(1001, 1) // 模拟业务改动
		after, err := json.Marshal(s.snapshot())
		if err != nil {
			b.Fatal(err)
		}
		if bytes.Equal(before, after) {
			b.Fatal("改动未被检出")
		}
	}
}

// BenchmarkMessageOverhead_Dirty R2.5 目标形态的预估：一次快照序列化，无 clone。
func BenchmarkMessageOverhead_Changed(b *testing.B) {
	s := benchState()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.ClearChanged()
		s.AddItem(1001, 1)
		if !s.HasChanged() {
			b.Fatal("markDirty 未生效")
		}
		if _, err := json.Marshal(s.snapshot()); err != nil {
			b.Fatal(err)
		}
	}
}
