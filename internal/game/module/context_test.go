// context_test.go —— ApplyOnce 幂等语义（R2.1）
package module

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
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
func (s *bizState) SetSceneID(int32)               {}
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
	require.NoError(t, err, fmt.Sprintf("first apply: applied=%v err=%v", applied, err))
	require.True(t, applied)
	applied, err = ctx.ApplyOnce("recharge:order-1", fn)
	require.False(t, err != nil || applied, fmt.Sprintf("duplicate apply: applied=%v err=%v", applied, err))
	require.EqualValues(t, 1, calls, fmt.Sprintf("side effects repeated: calls=%d count=%d", calls, st.GetItemCount(1001)))
	require.EqualValues(t, 10, st.GetItemCount(1001))
}

// TestApplyOnceFailureDoesNotClaimKey 业务失败不能占键，否则一次配置错误就永久锁死。
func TestApplyOnceFailureDoesNotClaimKey(t *testing.T) {
	st := newBizState()
	ctx := &Context{State: st}
	boom := errors.New("config missing")
	_, err := ctx.ApplyOnce("mail:7", func() error { return boom })
	require.False(t, !errors.Is(err, boom), fmt.Sprintf("expected business error, got %v", err))
	require.False(t, st.HasBiz("mail:7"), "failed apply must not claim the key")
	// 修好配置后重试可以正常生效。
	applied, err := ctx.ApplyOnce("mail:7", func() error {
		st.AddItem(1001, 1)
		return nil
	})
	require.NoError(t, err, fmt.Sprintf("retry after fix: applied=%v err=%v count=%d", applied, err, st.GetItemCount(1001)))
	require.True(t, applied)
	require.EqualValues(t, 1, st.GetItemCount(1001))
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
		require.NoError(t, err, fmt.Sprintf("empty key apply: applied=%v err=%v", applied, err))
		require.True(t, applied)
	}
	require.Len(t, st.biz, 0)
	require.EqualValues(t, 2, st.GetItemCount(1001))
}

// TestApplyOnceWithoutState Context 缺少玩家状态时明确报错，不静默成功。
func TestApplyOnceWithoutState(t *testing.T) {
	ctx := &Context{}
	_, err := ctx.ApplyOnce("k", func() error { return nil })
	require.Error(t, err, "missing state must be an error")
}
