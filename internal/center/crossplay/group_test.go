package crossplay

import (
	"fmt"
	"testing"
)

// makeMembers 造 n 个玩家，Power 从 n 递减到 1，ServerID 轮流分配。
func makeMembers(n int) []Member {
	out := make([]Member, n)
	for i := 0; i < n; i++ {
		out[i] = Member{
			PlayerID: int64(i + 1),
			ServerID: int32(i%30 + 1),
			Power:    int64(n - i),
			Score:    InitialScore,
		}
	}
	return out
}

func sumPower(g Group) int64 {
	var s int64
	for _, m := range g.Members {
		s += m.Power
	}
	return s
}

// TestDistributeSnake 验证蛇形落位：第 1 名进 G1、第 2 名进 G2、第 4 名回 G3。
func TestDistributeSnake(t *testing.T) {
	groups := Distribute(makeMembers(300))
	if len(groups) != 3 {
		t.Fatalf("组数 = %d, 期望 3", len(groups))
	}
	for i, g := range groups {
		if g.ID != int32(i+1) {
			t.Errorf("第 %d 组 ID = %d, 期望 %d", i, g.ID, i+1)
		}
		if len(g.Members) != GroupSize {
			t.Errorf("组 %d 人数 = %d, 期望 %d", g.ID, len(g.Members), GroupSize)
		}
	}

	// round0: 300→G1, 299→G2, 298→G3
	// round1: 297→G3, 296→G2, 295→G1  （反向）
	// round2: 294→G1, 293→G2, 292→G3
	want := map[int32][]int64{
		1: {300, 295, 294},
		2: {299, 296, 293},
		3: {298, 297, 292},
	}
	for gid, powers := range want {
		got := groups[gid-1].Members
		for i, w := range powers {
			if got[i].Power != w {
				t.Errorf("组 %d 第 %d 位 Power = %d, 期望 %d", gid, i, got[i].Power, w)
			}
		}
	}
}

// TestDistributeBalancedPower 验证组间实力均衡。
//
// 这条是最重要的断言：切片式分组（前 100 一组）会让第 1 组与最后一组的
// Power 总和差出上万，而蛇形应控制在几十以内。
func TestDistributeBalancedPower(t *testing.T) {
	groups := Distribute(makeMembers(250))
	if len(groups) != 3 {
		t.Fatalf("组数 = %d, 期望 3", len(groups))
	}
	lo, hi := sumPower(groups[0]), sumPower(groups[0])
	for _, g := range groups {
		s := sumPower(g)
		if s < lo {
			lo = s
		}
		if s > hi {
			hi = s
		}
	}
	// 250 人 power 250..1，单组总和约 10000；允许 200 的偏差（≈2 名玩家的差距）。
	// 切片式分组的极差会是 1 万以上，用 200 就能把两种实现区分开。
	if hi-lo > 200 {
		t.Errorf("组间 Power 总和极差 = %d (lo=%d hi=%d)，超过 200", hi-lo, lo, hi)
	}
}

// TestDistributeSizeImbalanceAtMostOne 任意人数下，各组人数最多差 1。
func TestDistributeSizeImbalanceAtMostOne(t *testing.T) {
	for _, n := range []int{1, 2, 99, 100, 101, 199, 200, 201, 250, 999, 1000, 1001} {
		groups := Distribute(makeMembers(n))
		wantGroups := (n + GroupSize - 1) / GroupSize
		if len(groups) != wantGroups {
			t.Fatalf("n=%d 组数 = %d, 期望 %d", n, len(groups), wantGroups)
		}
		lo, hi := len(groups[0].Members), len(groups[0].Members)
		for _, g := range groups {
			if l := len(g.Members); l < lo {
				lo = l
			} else if l > hi {
				hi = l
			}
		}
		if hi-lo > 1 {
			t.Errorf("n=%d 人数极差 = %d (lo=%d hi=%d)", n, hi-lo, lo, hi)
		}
	}
}

// TestDistributeCoversEveryone 不重不漏：每人恰好出现在一个组里。
func TestDistributeCoversEveryone(t *testing.T) {
	const n = 1234
	groups := Distribute(makeMembers(n))
	seen := make(map[int64]int32, n)
	for _, g := range groups {
		for _, m := range g.Members {
			if prev, ok := seen[m.PlayerID]; ok {
				t.Fatalf("玩家 %d 同时出现在组 %d 和组 %d", m.PlayerID, prev, g.ID)
			}
			seen[m.PlayerID] = g.ID
		}
	}
	if len(seen) != n {
		t.Errorf("覆盖 %d 人, 期望 %d", len(seen), n)
	}
}

// TestDistributeDoesNotMutateInput 分组不能改调用方的切片顺序。
func TestDistributeDoesNotMutateInput(t *testing.T) {
	in := []Member{
		{PlayerID: 1, Power: 10},
		{PlayerID: 2, Power: 30},
		{PlayerID: 3, Power: 20},
	}
	before := make([]Member, len(in))
	copy(before, in)

	Distribute(in)

	for i := range in {
		if in[i] != before[i] {
			t.Fatalf("第 %d 项被修改: %+v → %+v", i, before[i], in[i])
		}
	}
}

// TestDistributeDeterministic 同 Power 时按 PlayerID 升序，两次结果必须一致。
func TestDistributeDeterministic(t *testing.T) {
	in := make([]Member, 150)
	for i := range in {
		in[i] = Member{PlayerID: int64(150 - i), Power: 100} // Power 全同，ID 倒序造噪音
	}
	a, b := Distribute(in), Distribute(in)
	for gi := range a {
		for mi := range a[gi].Members {
			if a[gi].Members[mi].PlayerID != b[gi].Members[mi].PlayerID {
				t.Fatalf("组 %d 第 %d 位不一致: %d vs %d",
					gi, mi, a[gi].Members[mi].PlayerID, b[gi].Members[mi].PlayerID)
			}
		}
	}
	// 同 Power 全 ID 升序后，第 1 组第一人应是最小 ID。
	if got := a[0].Members[0].PlayerID; got != 1 {
		t.Errorf("同 Power 未按 ID 升序: 首位 = %d, 期望 1", got)
	}
}

func TestDistributeEmpty(t *testing.T) {
	if got := Distribute(nil); got != nil {
		t.Errorf("空输入应返回 nil, 得到 %v", got)
	}
}

// TestDistributeTailGroupStillOpens 尾组不足 100 也照样开班（已确认不并组）。
func TestDistributeTailGroupStillOpens(t *testing.T) {
	groups := Distribute(makeMembers(250))
	if len(groups) != 3 {
		t.Fatalf("组数 = %d, 期望 3", len(groups))
	}
	for _, g := range groups {
		if len(g.Members) == 0 {
			t.Errorf("组 %d 为空 —— 尾组应照样开", g.ID)
		}
	}
}

func TestClampScore(t *testing.T) {
	cases := []struct{ in, want int64 }{
		{1000, 1000},
		{601, 601},
		{600, 600},
		{599, 600},
		{0, 600},
		{-5, 600},
	}
	for _, c := range cases {
		if got := ClampScore(c.in); got != c.want {
			t.Errorf("ClampScore(%d) = %d, 期望 %d", c.in, got, c.want)
		}
	}
}

func BenchmarkDistribute100k(b *testing.B) {
	members := makeMembers(100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Distribute(members)
	}
}

// ExampleDistribute 展示输出结构：3 人不足一组，只有一个组。
func ExampleDistribute() {
	in := makeMembers(3)
	groups := Distribute(in)
	for _, g := range groups {
		fmt.Printf("组%d:", g.ID)
		for _, m := range g.Members {
			fmt.Printf(" %d(%d)", m.PlayerID, m.Power)
		}
		fmt.Println()
	}
	// Output:
	// 组1: 1(3) 2(2) 3(1)
}
