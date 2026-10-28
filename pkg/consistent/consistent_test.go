// pkg/consistent/consistent_test.go —— 哈希环的稳定性与均匀性
package consistent

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func ids(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("game-%d", i))
	}
	return out
}

// 同一个 key 必须永远落到同一个节点；环的形状与节点传入顺序无关。
func TestGetIsDeterministic(t *testing.T) {
	a := New([]string{"game-2", "game-0", "game-1"})
	b := New([]string{"game-0", "game-1", "game-2"})

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("account:%d", i)
		na, ok := a.Get(key)
		require.True(t, ok)
		nb, _ := b.Get(key)
		require.Equal(t, na, nb, "节点顺序不同不应改变结果")
	}
}

// 分布式均匀：3 个节点、12000 个 key，每个节点应拿到 1/3 左右。
func TestDistributionIsBalanced(t *testing.T) {
	r := New(ids(3))
	counts := map[string]int{}
	const total = 12000
	for i := 0; i < total; i++ {
		node, ok := r.Get(fmt.Sprintf("account:%d", i))
		require.True(t, ok)
		counts[node]++
	}
	require.Len(t, counts, 3, "每个节点都应分到 key")
	for node, c := range counts {
		share := float64(c) / total
		require.InDelta(t, 1.0/3, share, 0.05, "节点 %s 占比 %.3f 偏离过大", node, share)
	}
}

// 扩缩容只迁移「该节点负责的那一份」——这是选一致性哈希的全部理由。
func TestRemoveNodeMovesOnlyItsShare(t *testing.T) {
	before := New(ids(4))
	after := New(ids(3)) // 去掉 game-3

	var moved, total int
	const n = 8000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("account:%d", i)
		nb, _ := before.Get(key)
		na, _ := after.Get(key)
		total++
		if nb != na {
			moved++
			// 只有原本落在被摘节点上的 key 才允许换节点
			require.Equal(t, "game-3", nb, "不该动到其它节点的归属")
		}
	}
	share := float64(moved) / float64(total)
	require.InDelta(t, 0.25, share, 0.06, "摘 1/4 节点应只迁移约 1/4 的 key，实际 %.3f", share)
}

// 取模法作为对照：同样摘一个节点，几乎全量迁移。这里只断言一致性哈希
// 明显更少，防止以后有人把它改回取模。
func TestConsistentHashBeatsModulo(t *testing.T) {
	const n = 4000
	modMoved := 0
	for i := 0; i < n; i++ {
		key := i
		if key%4 != key%3 {
			modMoved++
		}
	}
	ringMoved := 0
	before, after := New(ids(4)), New(ids(3))
	for i := 0; i < n; i++ {
		a, _ := before.Get(fmt.Sprintf("account:%d", i))
		b, _ := after.Get(fmt.Sprintf("account:%d", i))
		if a != b {
			ringMoved++
		}
	}
	require.Less(t, ringMoved, modMoved/2, "一致性哈希的迁移量应远小于取模")
}

// 空环／空 ID 不应 panic，返回 false 让调用方走「无可用节点」分支。
func TestEmptyRing(t *testing.T) {
	var nilRing *Ring
	_, ok := nilRing.Get("k")
	require.False(t, ok)

	e := New(nil)
	_, ok = e.Get("k")
	require.False(t, ok)
	require.Zero(t, e.Len())

	// 空串 ID 被忽略，不能变成环上的一个「无名节点」
	dirty := New([]string{"", "game-0"})
	node, ok := dirty.Get("k")
	require.True(t, ok)
	require.Equal(t, "game-0", node)
}
