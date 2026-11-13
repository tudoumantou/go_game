package crossplay

import (
	"testing"
)

// newLoadedStore 建一个 3 组 × 若干人的 Store，ServerID 轮流 1..3。
func newLoadedStore(t *testing.T) *Store {
	t.Helper()
	s := NewStore()
	s.Load(Distribute(makeMembers(250))) // 3 组
	if got, want := s.Len(), 250; got != want {
		t.Fatalf("Len = %d, 期望 %d", got, want)
	}
	return s
}

func TestStoreApplyScore(t *testing.T) {
	s := newLoadedStore(t)

	m, ok, changed := s.ApplyScore(1, 1010, 1770000000)
	if !ok {
		t.Fatal("已分组的玩家应命中")
	}
	if !changed {
		t.Fatal("1000 → 1010 应判定为变化")
	}
	if m.Score != 1010 {
		t.Errorf("Score = %d, 期望 1010", m.Score)
	}
	if m.GroupID == 0 {
		t.Error("返回的成员应带组号")
	}
	if m.UpdatedAt != 1770000000 {
		t.Errorf("UpdatedAt = %d", m.UpdatedAt)
	}

	// 读回来必须是同一个值（写的是切片元素，读的是深拷贝）。
	var found bool
	for _, mem := range s.Group(m.GroupID) {
		if mem.PlayerID == 1 {
			found = true
			if mem.Score != 1010 {
				t.Errorf("读回 Score = %d, 期望 1010", mem.Score)
			}
		}
	}
	if !found {
		t.Fatal("组内找不到玩家 1")
	}
}

// TestStoreApplyScoreIdempotent 重复上报同分不算变化，避免无谓触发下发。
func TestStoreApplyScoreIdempotent(t *testing.T) {
	s := newLoadedStore(t)

	if _, _, changed := s.ApplyScore(1, 1000, 100); changed {
		t.Fatal("首次写入同分不应算变化（初始就是 1000）")
	}
	s.ApplyScore(1, 990, 100)
	if _, _, changed := s.ApplyScore(1, 990, 200); changed {
		t.Error("同一分值重复上报不应算变化")
	}
	if _, _, changed := s.ApplyScore(1, 980, 300); !changed {
		t.Error("分值真的变了却没算变化")
	}
}

func TestStoreApplyScoreUnknownPlayer(t *testing.T) {
	s := newLoadedStore(t)
	if _, ok, _ := s.ApplyScore(999999, 1000, 1); ok {
		t.Error("未分组的玩家不应命中")
	}
}

// TestStoreApplyScoreClamps 低于下限的分数被截断，而不是写进负数。
func TestStoreApplyScoreClamps(t *testing.T) {
	s := newLoadedStore(t)
	m, ok, _ := s.ApplyScore(1, 100, 1)
	if !ok || m.Score != ScoreFloor {
		t.Errorf("Score = %d, 期望截断到 %d", m.Score, ScoreFloor)
	}
	m, _, _ = s.ApplyScore(1, -999, 2)
	if m.Score != ScoreFloor {
		t.Errorf("负分应截断到 %d, 得到 %d", ScoreFloor, m.Score)
	}
}

func TestStoreServerOf(t *testing.T) {
	s := newLoadedStore(t)
	for pid := int64(1); pid <= 250; pid++ {
		want := int32((pid-1)%30 + 1)
		if got := s.ServerOf(pid); got != want {
			t.Fatalf("玩家 %d 的 ServerOf = %d, 期望 %d", pid, got, want)
		}
	}
	if got := s.ServerOf(999999); got != 0 {
		t.Errorf("未分组玩家 ServerOf = %d, 期望 0", got)
	}
}

// TestStoreLoadReplaces 整体重载后旧数据不残留。
func TestStoreLoadReplaces(t *testing.T) {
	s := newLoadedStore(t)
	s.ApplyScore(1, 990, 1)

	s.Load(Distribute(makeMembers(100))) // 只剩 1 组
	if got := s.Len(); got != 100 {
		t.Errorf("重载后 Len = %d, 期望 100", got)
	}
	if _, ok, _ := s.ApplyScore(250, 1000, 1); ok {
		t.Error("重载后不应还能找到旧的玩家 250")
	}
	if len(s.Groups()) != 1 {
		t.Errorf("重载后组数 = %d, 期望 1", len(s.Groups()))
	}
}

func TestStoreSnapshot(t *testing.T) {
	s := newLoadedStore(t)
	ids := s.Groups()
	snap := s.Snapshot(ids[0])
	if snap == nil {
		t.Fatal("快照不应为 nil")
	}
	if !snap.Snapshot {
		t.Error("快照必须标记 Snapshot=true")
	}
	if len(snap.Players) != len(s.Group(ids[0])) {
		t.Errorf("快照人数 = %d, 期望 %d", len(snap.Players), len(s.Group(ids[0])))
	}
	for _, p := range snap.Players {
		if p.Power == 0 {
			t.Errorf("快照必须带 Power（分组排序依据），玩家 %d 为 0", p.PlayerId)
		}
	}
	if s.Snapshot(9999) != nil {
		t.Error("不存在的组应返回 nil")
	}
}

// TestPushBuilderGroupsByServer 不同服的玩家进不同报文，同服不同组进同一条报文。
func TestPushBuilderGroupsByServer(t *testing.T) {
	b := NewPushBuilder()
	// 玩家 1（服1）的变化要发给服 2 和服 3，自己的服不发
	b.AddTo(2, Member{PlayerID: 1, GroupID: 1, ServerID: 1, Score: 1001})
	b.AddTo(3, Member{PlayerID: 1, GroupID: 1, ServerID: 1, Score: 1001})
	// 玩家 2（服2）的变化要发给服 1
	b.AddTo(1, Member{PlayerID: 2, GroupID: 1, ServerID: 2, Score: 999})
	// 服 1 的另一个组也变了
	b.AddTo(1, Member{PlayerID: 3, GroupID: 2, ServerID: 1, Score: 1005})

	msgs := b.Messages()
	if len(msgs) != 3 {
		t.Fatalf("报文数 = %d, 期望 3（三个服各一条）", len(msgs))
	}

	s1 := msgs[1]
	if s1 == nil || len(s1.Groups) != 2 {
		t.Fatalf("服1 的报文应含 2 个组, 得到 %+v", s1)
	}
	// 组号升序
	if s1.Groups[0].GroupId != 1 || s1.Groups[1].GroupId != 2 {
		t.Errorf("组未按组号升序: %d, %d", s1.Groups[0].GroupId, s1.Groups[1].GroupId)
	}
	if len(s1.Groups[0].Players) != 1 || s1.Groups[0].Players[0].PlayerId != 2 {
		t.Errorf("服1 组1 内容错误: %+v", s1.Groups[0].Players)
	}

	s2 := msgs[2]
	if s2 == nil || len(s2.Groups) != 1 || s2.Groups[0].Players[0].PlayerId != 1 {
		t.Errorf("服2 报文错误: %+v", s2)
	}
}

// TestPushBuilderSnapshotIncludesOwnPlayers 快照**必须**把收件方自己的玩家也发回去。
//
// 这条曾经是反的（快照也排除自己），当时的理由是「本地是权威，别覆盖」。
// 但它带来一个更严重的后果：**游戏服没有其他途径知道自己的分组**。
// 分组结果只写在中心服，快照是唯一的通道；滤掉自己的人，那个玩家在本服
// 就永远是「已报名、未分组」，界面一直转圈到活动结束。
//
// 正确性靠接收端分工：快照里的本服玩家只采纳归属（group_id / power），
// 分数以本地为准。归属只能听中心服的，分数只能听本服的——两件事。
func TestPushBuilderSnapshotIncludesOwnPlayers(t *testing.T) {
	s := newLoadedStore(t)
	ids := s.Groups()
	members := s.Group(ids[0])

	servers := s.ServersOf(ids[0])
	if len(servers) < 2 {
		t.Fatalf("这一组只涉及 %d 个服，测试前提不成立", len(servers))
	}

	b := NewPushBuilder()
	for _, sid := range servers {
		b.AddSnapshotTo(sid, ids[0], members)
	}
	msgs := b.Messages()

	if len(msgs) != len(servers) {
		t.Errorf("报文数 = %d, 期望 %d", len(msgs), len(servers))
	}
	for sid, push := range msgs {
		if len(push.Groups) != 1 || !push.Groups[0].Snapshot {
			t.Errorf("服 %d 快照标记错误: %+v", sid, push.Groups)
			continue
		}
		if want := len(members); len(push.Groups[0].Players) != want {
			t.Errorf("服 %d 收到 %d 人, 期望 %d（整组成员）", sid, len(push.Groups[0].Players), want)
		}
	}
}

func countServer(members []Member, sid int32) int {
	n := 0
	for _, m := range members {
		if m.ServerID == sid {
			n++
		}
	}
	return n
}

func TestPushBuilderEmpty(t *testing.T) {
	b := NewPushBuilder()
	if !b.Empty() {
		t.Error("新建的构建器应为空")
	}
	b.AddTo(1, Member{PlayerID: 1, GroupID: 1, ServerID: 1})
	if b.Empty() {
		t.Error("加过变化项后不应为空")
	}
}
