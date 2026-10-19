// state_test.go —— 业务键幂等语义（R2.1）
package player

import (
	"testing"

	"game_study/pkg/writebehind"
)

// TestClaimBiz 占用一次后重复占用返回 false，空键不参与幂等。
func TestClaimBiz(t *testing.T) {
	s := NewState(1)
	if !s.ClaimBiz("recharge:order-1") {
		t.Fatal("first claim must succeed")
	}
	if s.ClaimBiz("recharge:order-1") {
		t.Fatal("duplicate claim must be rejected")
	}
	if !s.HasBiz("recharge:order-1") {
		t.Fatal("business key not recorded")
	}
	if s.HasBiz("recharge:order-2") {
		t.Fatal("unknown key reported as applied")
	}
	// 空键表示不启用幂等，不应污染 applied_grants。
	if !s.ClaimBiz("") {
		t.Fatal("empty key must always succeed")
	}
	if len(s.snapshot().AppliedGrants) != 1 {
		t.Fatalf("empty key recorded: %v", s.snapshot().AppliedGrants)
	}
}

// TestClaimBizSurvivesSnapshot 业务键必须随快照往返，否则重启后幂等失效。
func TestClaimBizSurvivesSnapshot(t *testing.T) {
	s := NewState(7)
	s.ClaimBiz("mail:42")
	snap := s.snapshot()
	back := stateFromSnapshot(snap)
	if !back.HasBiz("mail:42") {
		t.Fatal("business key lost across snapshot round trip")
	}
	if back.ClaimBiz("mail:42") {
		t.Fatal("restored key should still block duplicates")
	}
}

// TestCapacityFromConfig 容量 = 初始值 + 等级 * 每级扩容（bag_capacity.xml）。
func TestCapacityFromConfig(t *testing.T) {
	s := NewState(1)
	s.mu.Lock()
	s.profile.Level = 10
	s.mu.Unlock()
	// 配置：col1=60 初始，col2=5 每级 → 10 级 = 110
	if got := s.Capacity(); got != 110 {
		t.Fatalf("capacity = %d, want 110", got)
	}
}

// TestCanHoldCountsNewKindsOnly 只有新增种类受容量限制，已有道具可继续叠加。
func TestCanHoldCountsNewKindsOnly(t *testing.T) {
	s := NewState(1)
	s.mu.Lock()
	s.profile.Level = 0 // 容量 60
	// 塞满 60 种
	for i := int32(1); i <= 60; i++ {
		s.items[1000+i] = 1
	}
	s.mu.Unlock()

	// 已有道具继续叠加：允许
	if !s.CanHold([]writebehind.ItemGrant{{ItemID: 1001, Count: 100}}) {
		t.Fatal("stacking existing item must be allowed when bag is full")
	}
	// 新增第 61 种：拒绝
	if s.CanHold([]writebehind.ItemGrant{{ItemID: 9999, Count: 1}}) {
		t.Fatal("new item kind must be rejected when bag is full")
	}
	// 批量里重复的新道具只算一种：59 种 + 去重后 1 种 = 60，刚好放得下。
	s.mu.Lock()
	delete(s.items, 1060)
	s.mu.Unlock()
	if !s.CanHold([]writebehind.ItemGrant{{ItemID: 9999, Count: 1}, {ItemID: 9999, Count: 2}}) {
		t.Fatal("duplicate new item should count once")
	}
}

// TestCanHoldRejectsOverCapacity 超过容量的一次性批量发放整体拒绝。
func TestCanHoldRejectsOverCapacity(t *testing.T) {
	s := NewState(1)
	grants := make([]writebehind.ItemGrant, 0, 100)
	for i := int32(1); i <= 100; i++ {
		grants = append(grants, writebehind.ItemGrant{ItemID: 1000 + i, Count: 1})
	}
	if s.CanHold(grants) {
		t.Fatal("batch exceeding capacity must be rejected")
	}
}

// TestRecalcAttrsGrowth 等级成长按 attr_formula 叠加，重复重算不累加。
func TestRecalcAttrsGrowth(t *testing.T) {
	s := NewState(1)
	s.mu.Lock()
	s.baseAttr = Attr{Hp: 1000, Atk: 50}
	s.profile.Level = 1
	s.mu.Unlock()
	s.RecalcAttrs()
	if got := s.GetAttr("hp"); got != 1000 {
		t.Fatalf("lv1 hp = %d, want 1000", got)
	}

	s.mu.Lock()
	s.profile.Level = 11 // 10 级成长：hp +20*10=200, atk +3*10=30
	s.mu.Unlock()
	s.RecalcAttrs()
	s.RecalcAttrs() // 再算一次，不能重复累加
	if got := s.GetAttr("hp"); got != 1200 {
		t.Fatalf("lv11 hp = %d, want 1200", got)
	}
	if got := s.GetAttr("atk"); got != 80 {
		t.Fatalf("lv11 atk = %d, want 80", got)
	}
}

// TestRecalcAttrsKeepsBonus 在线升级不清掉已生效的装备加成。
func TestRecalcAttrsKeepsBonus(t *testing.T) {
	s := NewState(1)
	s.mu.Lock()
	s.baseAttr = Attr{Hp: 1000, Atk: 50}
	s.profile.Level = 1
	s.mu.Unlock()
	s.AddBonus("atk", 10)
	s.RecalcAttrs()
	if got := s.GetAttr("atk"); got != 60 {
		t.Fatalf("atk = %d, want 50 base + 10 bonus", got)
	}
}
