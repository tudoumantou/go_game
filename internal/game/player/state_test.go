// state_test.go —— 业务键幂等语义（R2.1）
package player

import (
	"testing"
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
