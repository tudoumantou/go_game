// context_test.go —— ApplyOnce 幂等语义（R2.1）
package module

import (
	"encoding/json"
	"errors"
	"testing"
)

// bizState 是 PlayerState 的最小实现，只关心业务键与道具。
type bizState struct {
	items map[int32]int64
	biz   map[string]bool
}

func newBizState() *bizState {
	return &bizState{items: map[int32]int64{}, biz: map[string]bool{}}
}

func (s *bizState) PlayerID() int64               { return 1 }
func (s *bizState) Profile() Profile              { return Profile{} }
func (s *bizState) ExpMax() int64                 { return 0 }
func (s *bizState) GetAttr(string) int64          { return 0 }
func (s *bizState) SetAttr(string, int64)         {}
func (s *bizState) AddBonus(string, int64)        {}
func (s *bizState) ClearBonus()                   {}
func (s *bizState) GetItemCount(id int32) int64   { return s.items[id] }
func (s *bizState) Items() map[int32]int64        { return s.items }
func (s *bizState) AddItem(id int32, count int64) { s.items[id] += count }
func (s *bizState) RemoveItem(id int32, count int64) bool {
	if s.items[id] < count {
		return false
	}
	s.items[id] -= count
	return true
}
func (s *bizState) MarkDirty(string)               {}
func (s *bizState) Ext(string) json.RawMessage     { return nil }
func (s *bizState) SetExt(string, json.RawMessage) {}
func (s *bizState) CanHold([]ItemGrant) bool       { return true }
func (s *bizState) ClaimBiz(key string) bool {
	if key == "" {
		return true
	}
	if s.biz[key] {
		return false
	}
	s.biz[key] = true
	return true
}
func (s *bizState) HasBiz(key string) bool { return s.biz[key] }

// TestApplyOnceIdempotent 同键只执行一次，第二次直接跳过。
func TestApplyOnceIdempotent(t *testing.T) {
	st := newBizState()
	ctx := &Context{State: st}
	calls := 0
	fn := func() error {
		calls++
		st.AddItem(1001, 10)
		return nil
	}
	applied, err := ctx.ApplyOnce("recharge:order-1", fn)
	if err != nil || !applied {
		t.Fatalf("first apply: applied=%v err=%v", applied, err)
	}
	applied, err = ctx.ApplyOnce("recharge:order-1", fn)
	if err != nil || applied {
		t.Fatalf("duplicate apply: applied=%v err=%v", applied, err)
	}
	if calls != 1 || st.GetItemCount(1001) != 10 {
		t.Fatalf("side effects repeated: calls=%d count=%d", calls, st.GetItemCount(1001))
	}
}

// TestApplyOnceFailureDoesNotClaimKey 业务失败不能占键，否则一次配置错误就永久锁死。
func TestApplyOnceFailureDoesNotClaimKey(t *testing.T) {
	st := newBizState()
	ctx := &Context{State: st}
	boom := errors.New("config missing")
	if _, err := ctx.ApplyOnce("mail:7", func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("expected business error, got %v", err)
	}
	if st.HasBiz("mail:7") {
		t.Fatal("failed apply must not claim the key")
	}
	// 修好配置后重试可以正常生效。
	applied, err := ctx.ApplyOnce("mail:7", func() error {
		st.AddItem(1001, 1)
		return nil
	})
	if err != nil || !applied || st.GetItemCount(1001) != 1 {
		t.Fatalf("retry after fix: applied=%v err=%v count=%d", applied, err, st.GetItemCount(1001))
	}
}

// TestApplyOnceEmptyKeySkipsIdempotency 无键业务直接执行，不写 applied_grants。
func TestApplyOnceEmptyKeySkipsIdempotency(t *testing.T) {
	st := newBizState()
	ctx := &Context{State: st}
	for i := 0; i < 2; i++ {
		applied, err := ctx.ApplyOnce("", func() error {
			st.AddItem(1001, 1)
			return nil
		})
		if err != nil || !applied {
			t.Fatalf("empty key apply: applied=%v err=%v", applied, err)
		}
	}
	if len(st.biz) != 0 {
		t.Fatalf("empty key polluted applied_grants: %v", st.biz)
	}
	if st.GetItemCount(1001) != 2 {
		t.Fatalf("empty key must run every time, count=%d", st.GetItemCount(1001))
	}
}

// TestApplyOnceWithoutState Context 缺少玩家状态时明确报错，不静默成功。
func TestApplyOnceWithoutState(t *testing.T) {
	ctx := &Context{}
	if _, err := ctx.ApplyOnce("k", func() error { return nil }); err == nil {
		t.Fatal("missing state must be an error")
	}
}
