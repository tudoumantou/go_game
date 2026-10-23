// internal/game/module/spend_test.go —— 扣费路径：余额判定、免费边界、防御分支。
package module

import (
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

// TestSpendDeducts 余额充足时扣减，且返回 nil。
func TestSpendDeducts(t *testing.T) {
	st := newGrantState(10, map[int32]int64{1001: 100})
	if err := Spend(st, 1001, 30); err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, 70, st.items[1001])
}

// TestSpendRejectsShortfallWithoutSideEffect 余额不足时不扣、不改状态。
func TestSpendRejectsShortfallWithoutSideEffect(t *testing.T) {
	st := newGrantState(10, map[int32]int64{1001: 10})
	if err := Spend(st, 1001, 11); !errors.Is(err, ErrNotEnough) {
		t.Fatalf("err = %v, want ErrNotEnough", err)
	}
	require.EqualValues(t, 10, st.items[1001])
	// 恰好够：边界值必须放行。
	if err := Spend(st, 1001, 10); err != nil {
		t.Fatalf("余额刚好够却被拒: %v", err)
	}
	require.EqualValues(t, 0, st.items[1001])
}

// TestSpendFreeIsNoop 消耗为 0（或负数配置）视为免费：不报错、不写状态。
func TestSpendFreeIsNoop(t *testing.T) {
	st := newGrantState(10, map[int32]int64{1001: 5})
	for _, cost := range []int64{0, -1} {
		if err := Spend(st, 1001, cost); err != nil {
			t.Fatalf("cost=%d 应免费放行: %v", cost, err)
		}
	}
	require.EqualValues(t, 5, st.items[1001])
}

// TestSpendRemoveFailureIsNotEnough RemoveItem 返回 false（校验后状态又变了）
// 必须报余额不足，而不是静默当作扣费成功。
func TestSpendRemoveFailureIsNotEnough(t *testing.T) {
	st := newGrantState(10, map[int32]int64{1001: 100})
	st.removeFails = true
	if err := Spend(st, 1001, 10); !errors.Is(err, ErrNotEnough) {
		t.Fatalf("err = %v, want ErrNotEnough", err)
	}
	require.EqualValues(t, 100, st.items[1001])
}
