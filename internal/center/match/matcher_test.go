// internal/center/match/matcher_test.go —— 匹配池测试
package match

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// enqueue 辅助：入队并返回结果。
func enqueue(m *Matcher, node string, playerID int64, elo int32) []*Result {
	return m.Enqueue(&Ticket{NodeID: node, PlayerID: playerID, Mode: DefaultMode, Elo: elo})
}

func TestEnqueuePairSameBucket(t *testing.T) {
	m := New(1, []string{"cross-01:9300"}, time.Minute)

	res := enqueue(m, "game-01", 1001, 1200)
	require.Nil(t, res, fmt.Sprintf("first enqueue should not match, got %d results", len(res)))
	waiting, _ := m.Stats()
	require.EqualValues(t, 1, waiting, fmt.Sprintf("expect 1 waiting, got %d", waiting))

	res = enqueue(m, "game-02", 1002, 1250) // 同桶（1200/200=6）
	require.Len(t, res, 2)

	// 两条结果指向同一房间，令牌各不相同
	require.NotEqualValues(t, 0, res[0].RoomID, fmt.Sprintf("room id mismatch: %d vs %d", res[0].RoomID, res[1].RoomID))
	require.EqualValues(t, res[1].RoomID, res[0].RoomID)
	require.NotEqualValues(t, res[1].Players[0].Token, res[0].Players[0].Token, "each player must get its own token")
	// 双方互为对手
	require.EqualValues(t, 1002, res[0].Players[0].PeerID, fmt.Sprintf("peer mismatch: %+v / %+v", res[0].Players[0], res[1].Players[0]))
	require.EqualValues(t, 1001, res[1].Players[0].PeerID)
	// 跨服地址已分配
	require.NotEqualValues(t, "", res[0].CrossAddr, "cross addr must be assigned")
	waiting, _ = m.Stats()
	require.EqualValues(t, 0, waiting, fmt.Sprintf("queue should be empty after match, got %d", waiting))
}

func TestEnqueueDifferentBucketNoImmediateMatch(t *testing.T) {
	m := New(1, []string{"cross-01:9300"}, time.Minute)

	_ = enqueue(m, "game-01", 1001, 100)     // bucket 0
	res := enqueue(m, "game-02", 1002, 5000) // bucket 25
	require.Nil(t, res)
	waiting, buckets := m.Stats()
	require.EqualValues(t, 2, waiting, fmt.Sprintf("expect 2 waiting in 2 buckets, got %d/%d", waiting, buckets))
	require.EqualValues(t, 2, buckets)
}

func TestDuplicateEnqueueIgnored(t *testing.T) {
	m := New(1, []string{"cross-01:9300"}, time.Minute)

	_ = enqueue(m, "game-01", 1001, 1000)
	_ = enqueue(m, "game-01", 1001, 1000) // 同一玩家重复入队
	waiting, _ := m.Stats()
	require.EqualValues(t, 1, waiting, fmt.Sprintf("duplicate enqueue must be ignored, got %d waiting", waiting))
}

func TestSweepRelaxesEloAfterTimeout(t *testing.T) {
	m := New(1, []string{"cross-01:9300"}, 50*time.Millisecond)
	// 注入可控时钟：用虚拟时间跨过排队超时，不真的睡 120ms。
	base := time.Now()
	clock := base
	m.now = func() time.Time { return clock }

	_ = enqueue(m, "game-01", 1001, 100)  // bucket 0
	_ = enqueue(m, "game-02", 1002, 5000) // bucket 25
	clock = base.Add(120 * time.Millisecond)

	// 超时后 sweep 应跨桶配对（只产出 1 条 —— 每次 sweep 配对一对中的一个）
	results := m.Sweep()
	require.NotEmpty(t, results, "sweep should pair timed-out tickets across buckets")
	// 反复 sweep 直到队列清空（每次 sweep 可能只处理一个等待者）
	for i := 0; i < 3 && len(results) < 2; i++ {
		clock = clock.Add(20 * time.Millisecond)
		results = append(results, m.Sweep()...)
	}
	require.False(t, len(results) < 2, fmt.Sprintf("expect both players matched after sweep, got %d results", len(results)))
	waiting, _ := m.Stats()
	require.EqualValues(t, 0, waiting, fmt.Sprintf("queue should be drained, got %d", waiting))
}

func TestCancelRemovesTicket(t *testing.T) {
	m := New(1, []string{"cross-01:9300"}, time.Minute)

	_ = enqueue(m, "game-01", 1001, 1000)
	require.False(t, !m.Cancel("game-01", 1001), "cancel should report removal")
	waiting, _ := m.Stats()
	require.EqualValues(t, 0, waiting, fmt.Sprintf("expect empty queue, got %d", waiting))
	require.False(t, m.Cancel("game-01", 1001), "cancel on missing ticket should return false")
}

func TestNoCrossNodeNoMatch(t *testing.T) {
	m := New(1, nil, time.Minute) // 无可用跨服节点

	_ = enqueue(m, "game-01", 1001, 1000)
	res := enqueue(m, "game-02", 1002, 1000)
	require.Nil(t, res)
	// 两人都应放回队列（不能丢）
	waiting, _ := m.Stats()
	require.EqualValues(t, 2, waiting, fmt.Sprintf("both tickets must stay queued, got %d", waiting))
}

func TestRoomIDUniquePerInstance(t *testing.T) {
	// 不同实例号必须产出不同 room_id（避免集群串房）
	m1 := New(1, []string{"cross-01:9300"}, time.Minute)
	m2 := New(2, []string{"cross-01:9300"}, time.Minute)

	_ = enqueue(m1, "game-01", 1, 100)
	r1 := enqueue(m1, "game-01", 2, 100)
	_ = enqueue(m2, "game-02", 3, 100)
	r2 := enqueue(m2, "game-02", 4, 100)

	require.NotEmpty(t, r1, "both matchers should produce results")
	require.NotEmpty(t, r2)
	require.NotEqualValues(t, r2[0].RoomID, r1[0].RoomID, fmt.Sprintf("room ids from different instances must differ, both %d", r1[0].RoomID))
}
