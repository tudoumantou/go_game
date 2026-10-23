// internal/game/module/grant_test.go —— 发奖路径：原子性、溢出防护、容量预检。
package module

import (
	"errors"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

// grantState 记录背包内容的测试状态；容量按「道具种类数」算，与真实 State 同语义。
type grantState struct {
	thinState
	items map[int32]int64
	cap   int
	// removeFails 让 RemoveItem 假装失败（模拟「查过之后状态又变了」）。
	removeFails bool
}

func newGrantState(cap int, init map[int32]int64) *grantState {
	return &grantState{items: init, cap: cap}
}

func (s *grantState) GetItemCount(id int32) int64 { return s.items[id] }

func (s *grantState) AddItem(id int32, n int64) { s.items[id] += n }

func (s *grantState) RemoveItem(id int32, n int64) bool {
	if s.removeFails || n <= 0 || s.items[id] < n {
		return false
	}
	s.items[id] -= n
	if s.items[id] == 0 {
		delete(s.items, id)
	}
	return true
}

func (s *grantState) CanHold(grants []ItemGrant) bool {
	kinds := 0
	for _, n := range s.items {
		if n > 0 {
			kinds++
		}
	}
	seen := map[int32]bool{}
	for _, g := range grants {
		if g.Count <= 0 || g.ItemID <= 0 || s.items[g.ItemID] > 0 || seen[g.ItemID] {
			continue
		}
		seen[g.ItemID] = true
	}
	return kinds+len(seen) <= s.cap
}

// TestGrantAppliesAndReports 正常发放：存量累加、返回发放后存量与增量。
func TestGrantAppliesAndReports(t *testing.T) {
	st := newGrantState(10, map[int32]int64{1001: 5})
	changes, err := Grant(st, []ItemGrant{
		{ItemID: 1001, Count: 3},
		{ItemID: 1002, Count: 7},
	})
	require.NoError(t, err)
	if st.items[1001] != 8 || st.items[1002] != 7 {
		t.Fatalf("背包内容不符: %+v", st.items)
	}
	require.Len(t, changes, 2)
	// 顺序与入参一致，Count 是发放后存量而非本次数量。
	require.EqualValues(t, (BagChange{ItemID: 1001, Count: 8, Delta: 3}), changes[0])
	require.EqualValues(t, (BagChange{ItemID: 1002, Count: 7, Delta: 7}), changes[1])
}

// TestGrantRejectsBagFullWithoutSideEffect 满包时拒绝，且一个道具都不加。
func TestGrantRejectsBagFullWithoutSideEffect(t *testing.T) {
	// 容量 1，已有 1 种，再发新种类必然超。
	st := newGrantState(1, map[int32]int64{1001: 5})
	_, err := Grant(st, []ItemGrant{{ItemID: 2001, Count: 1}})
	if !errors.Is(err, ErrBagFull) {
		t.Fatalf("err = %v, want ErrBagFull", err)
	}
	if _, ok := st.items[2001]; ok {
		t.Fatalf("满包拒绝后不应写入: %+v", st.items)
	}

	// 同种道具追加种类数不变，不应被容量挡住。
	if _, err := Grant(st, []ItemGrant{{ItemID: 1001, Count: 1}}); err != nil {
		t.Fatalf("同种道具追加被误拒: %v", err)
	}
	require.EqualValues(t, 6, st.items[1001])
}

// TestGrantRejectsOverflowWithoutSideEffect 存量接近 int32 上限时拒绝整批。
//
// 这条防护原先只有 signin 有：其他模块直接把 int64 转 int32 塞进协议，
// 越界后客户端看到负数。
func TestGrantRejectsOverflowWithoutSideEffect(t *testing.T) {
	st := newGrantState(10, map[int32]int64{1001: math.MaxInt32 - 1, 2001: 0})
	_, err := Grant(st, []ItemGrant{
		{ItemID: 2001, Count: 5}, // 这一项本身合法
		{ItemID: 1001, Count: 2}, // 这一项会溢出
	})
	if !errors.Is(err, ErrItemLimit) {
		t.Fatalf("err = %v, want ErrItemLimit", err)
	}
	// 原子性：前一项也不能被写入。
	if st.items[2001] != 0 || st.items[1001] != math.MaxInt32-1 {
		t.Fatalf("溢出拒绝后状态被改: %+v", st.items)
	}
}

// TestGrantIgnoresEmptyEntries 空配置（非正 ID/数量）按不发处理，不算失败。
func TestGrantIgnoresEmptyEntries(t *testing.T) {
	st := newGrantState(10, map[int32]int64{})
	changes, err := Grant(st, []ItemGrant{
		{ItemID: 0, Count: 5},
		{ItemID: 1001, Count: 0},
		{ItemID: -1, Count: 5},
	})
	if err != nil || changes != nil {
		t.Fatalf("空配置应返回 nil,nil: %+v %v", changes, err)
	}
	require.Len(t, st.items, 0)
}

// TestPrecheckGrantDoesNotMutate 预检只读：商店路径靠它做到「先验后扣」。
func TestPrecheckGrantDoesNotMutate(t *testing.T) {
	st := newGrantState(2, map[int32]int64{1001: 10})
	grants := []ItemGrant{{ItemID: 2001, Count: 3}}
	if err := PrecheckGrant(st, grants); err != nil {
		t.Fatalf("预检失败: %v", err)
	}
	if st.items[2001] != 0 || st.items[1001] != 10 {
		t.Fatalf("预检改了状态: %+v", st.items)
	}

	// 满包时预检报错，且同样不改状态。
	full := newGrantState(1, map[int32]int64{1001: 1, 2001: 1})
	if err := PrecheckGrant(full, []ItemGrant{{ItemID: 3001, Count: 1}}); !errors.Is(err, ErrBagFull) {
		t.Fatalf("err = %v, want ErrBagFull", err)
	}
}

// TestToItemChanges 协议映射：ItemUid 占位、Count 取发放后存量、Change 恒 1。
func TestToItemChanges(t *testing.T) {
	if got := ToItemChanges(nil); got != nil {
		t.Fatalf("nil 输入应返回 nil: %v", got)
	}
	got := ToItemChanges([]BagChange{{ItemID: 1001, Count: 8, Delta: 3}})
	require.Len(t, got, 1)
	c := got[0]
	if c.ItemUid != 1001 || c.ItemId != 1001 || c.Count != 8 || c.Change != 1 {
		t.Fatalf("字段映射不符: %+v", c)
	}
}
