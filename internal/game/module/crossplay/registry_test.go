package crossplay

import (
	"testing"
)

// newEntry 造一个条目，测试里少写几个字段。
func newEntry(pid int64, sid int32, score int64) *Entry {
	return &Entry{PlayerID: pid, ServerID: sid, Score: score, Power: score}
}

// TestAddScoreSharedPointer 本服玩家改分后，组镜像里那一项必须同步变。
//
// 这是「共享指针」的全部收益：界面排行读的是组镜像，上报读的是 players，
// 两处指向同一对象才不会出现「自己界面上自己永远是 1000 分」。
func TestAddScoreSharedPointer(t *testing.T) {
	r := NewRegistry(1)
	r.Load([]*Entry{newEntry(1, 1, 1000)},
		[]int64{1},
		map[int32][]*Entry{7: {newEntry(1, 1, 1000)}})

	e, changed, ok := r.AddScore(1, 1, 1700000000)
	if !ok || !changed {
		t.Fatalf("加分失败: ok=%v changed=%v", ok, changed)
	}
	if e.Score != 1001 {
		t.Fatalf("返回值分数 = %d, 期望 1001", e.Score)
	}

	snap := r.GroupSnapshot(7)
	if len(snap) != 1 || snap[0].Score != 1001 {
		t.Fatalf("组镜像未同步: %+v", snap)
	}
	// 必须还是同一个对象（不是值拷贝）
	if got, _ := r.Player(1); got.Score != 1001 {
		t.Fatalf("players 侧分数 = %d, 期望 1001", got.Score)
	}
}

// TestAddScoreFloor 扣到下限后不再扣，且不产生脏标记。
func TestAddScoreFloor(t *testing.T) {
	r := NewRegistry(1)
	r.Load([]*Entry{newEntry(1, 1, ScoreFloor)}, nil, nil)

	e, changed, ok := r.AddScore(1, -1, 1700000000)
	if !ok {
		t.Fatal("玩家应在册")
	}
	if changed {
		t.Error("已在下限，changed 应为 false")
	}
	if e.Score != ScoreFloor {
		t.Errorf("分数 = %d, 期望 %d", e.Score, ScoreFloor)
	}
	if n := r.DirtyCount(); n != 0 {
		t.Errorf("没变化不该标脏，脏数 = %d", n)
	}

	// 加分别人还能加（下限只管扣）
	if _, changed, _ := r.AddScore(1, 5, 1700000001); !changed {
		t.Error("下限之上仍可加分")
	}
}

// TestAddScoreUnknownPlayer 不是本服玩家就改不动（数据归属铁律）。
func TestAddScoreUnknownPlayer(t *testing.T) {
	r := NewRegistry(1)
	if _, changed, ok := r.AddScore(99, 1, 0); ok || changed {
		t.Error("不在册的玩家不应能改分")
	}
}

// TestApplySnapshotKeepsLocalScore 快照不能覆盖本服玩家的分数。
//
// 这是最关键的一条：本地刚加了分（1001），中心服手上还是 60s 前的 1000。
// 如果整条覆盖，这一分会被抹掉，而且下一轮上报又把 1000 推上去，
// 形成自我强化的「加分被吞」。
func TestApplySnapshotKeepsLocalScore(t *testing.T) {
	r := NewRegistry(1)
	r.Load([]*Entry{newEntry(1, 1, 1000)}, []int64{1}, nil)
	r.AddScore(1, 1, 1700000000) // 本地 1001

	// 中心服推来 1000（旧值），同时带别人的分
	r.ApplySnapshot(7, []*Entry{
		newEntry(1, 1, 1000),
		newEntry(2, 2, 990),
	}, 1700000060)

	own, _ := r.Player(1)
	if own.Score != 1001 {
		t.Errorf("本服玩家分数被中心服覆盖: %d, 期望 1001（本地权威）", own.Score)
	}
	if own.GroupID != 7 {
		t.Errorf("归属没采纳: group=%d, 期望 7", own.GroupID)
	}
	other, isOwn := r.Participant(2)
	if isOwn || other.Score != 990 {
		t.Errorf("别服玩家 = (%+v, own=%v), 期望分数 990 且非本服", other, isOwn)
	}
}

// TestApplySnapshotAdoptsScoreForUnknown 本地没有记录时采纳中心服的分数。
//
// 这正是「进程重启后本地是旧的、中心服是新的」场景：快照必须能把分数补回来，
// 否则重启过的服会一直显示旧分直到该玩家下次被打。
func TestApplySnapshotAdoptsScoreForUnknown(t *testing.T) {
	r := NewRegistry(1) // 空镜像，模拟重启后没加载到数据
	r.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1234)}, 1700000060)

	own, ok := r.Player(1)
	if !ok {
		t.Fatal("快照里的本服玩家应被登记进 players（否则界面永远显示未分组）")
	}
	if own.Score != 1234 || own.GroupID != 7 {
		t.Errorf("采纳失败: %+v", own)
	}
	if !r.IsEnrolled(1) {
		t.Error("已分组应视为已报名")
	}
}

// TestApplySnapshotPrunesOthers 快照是完整名单：不在名单里的别服玩家要删掉。
// 本服玩家不删（可能刚被分到别的组，删了会显示「未分组」）。
func TestApplySnapshotPrunesOthers(t *testing.T) {
	r := NewRegistry(1)
	r.Load([]*Entry{newEntry(1, 1, 1000)}, nil, map[int32][]*Entry{
		7: {newEntry(1, 1, 1000), newEntry(2, 2, 900), newEntry(3, 3, 800)},
	})

	r.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000), newEntry(2, 2, 900)}, 0)

	if _, ok := r.Participant(3); ok {
		t.Error("不在快照里的别服玩家应被清除")
	}
	if _, ok := r.Participant(1); !ok {
		t.Error("本服玩家不能因为不在快照里就被删")
	}
}

// TestApplyDeltaSkipsOwn 增量绝不能改本服玩家（发送端已滤，这里是兜底）。
func TestApplyDeltaSkipsOwn(t *testing.T) {
	r := NewRegistry(1)
	r.Load([]*Entry{newEntry(1, 1, 1000)}, nil, map[int32][]*Entry{7: {newEntry(1, 1, 1000)}})
	r.AddScore(1, 1, 100) // 本地 1001

	r.ApplyDelta(7, []*Entry{newEntry(1, 1, 500), newEntry(2, 2, 777)})

	if e, _ := r.Player(1); e.Score != 1001 {
		t.Errorf("增量覆盖了本服分数: %d, 期望 1001", e.Score)
	}
	if e, _ := r.Participant(2); e.Score != 777 {
		t.Errorf("别服增量未生效: %d, 期望 777", e.Score)
	}
}

// TestTakeDirtyAndRestore 取走即清空；失败回填不倒退时间戳。
func TestTakeDirtyAndRestore(t *testing.T) {
	r := NewRegistry(1)
	r.Load([]*Entry{newEntry(1, 1, 1000), newEntry(2, 1, 1000)}, nil, nil)
	r.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000), newEntry(2, 1, 1000)}, 0)

	r.AddScore(1, 3, 100)
	r.AddScore(2, 4, 200)

	batch := r.TakeDirty(0)
	if len(batch) != 2 {
		t.Fatalf("取到 %d 条, 期望 2", len(batch))
	}
	if n := r.DirtyCount(); n != 0 {
		t.Fatalf("取走后应为空, 剩 %d", n)
	}

	// 模拟上报失败回填
	r.RestoreDirty(batch)
	if n := r.DirtyCount(); n != 2 {
		t.Fatalf("回填后应有 2 条, 得到 %d", n)
	}

	// 回填不能把「之后又变化」的时间戳改小
	r.TakeDirty(0)
	r.AddScore(1, 1, 999) // 更新的变化
	r.RestoreDirty([]Entry{{PlayerID: 1, UpdatedAt: 100}})
	batch2 := r.TakeDirty(0)
	if len(batch2) != 1 || batch2[0].UpdatedAt != 999 {
		t.Errorf("回填覆盖了更新的时间戳: %+v", batch2)
	}
}

// TestTakeDirtyDropsUngrouped 未分组的变化不上报（中心服定位不到组）。
func TestTakeDirtyDropsUngrouped(t *testing.T) {
	r := NewRegistry(1)
	r.Enroll(newEntry(1, 1, 0)) // 报名，未分组
	r.SetScore(1, 5, 100)

	if batch := r.TakeDirty(0); len(batch) != 0 {
		t.Errorf("未分组的玩家不应进上报队列: %+v", batch)
	}
	if n := r.DirtyCount(); n != 0 {
		t.Errorf("脏标记应被清掉（等分组快照重建），剩 %d", n)
	}
}

// TestCancelRemovesEverything 取消报名清掉全部痕迹。
func TestCancelRemovesEverything(t *testing.T) {
	r := NewRegistry(1)
	r.Enroll(newEntry(1, 1, 0))
	if !r.IsEnrolled(1) {
		t.Fatal("报名后应已报名")
	}
	r.Cancel(1)
	if r.IsEnrolled(1) {
		t.Error("取消后不应还是已报名")
	}
	if _, ok := r.Participant(1); ok {
		t.Error("取消后不应还能查到")
	}
}

// TestRankTieBreak 同分按 player_id 升序，与中心服分组算法口径一致。
func TestRankTieBreak(t *testing.T) {
	r := NewRegistry(1)
	members := []*Entry{
		newEntry(5, 1, 1000),
		newEntry(3, 1, 1000),
		newEntry(9, 1, 1100),
	}
	r.ApplySnapshot(7, members, 0)

	// 9 分最高 → 第 1；3 与 5 同分，3 在前 → 第 2、第 3
	if rank, size := r.RankOf(9); rank != 1 || size != 3 {
		t.Errorf("玩家9 名次 = (%d/%d), 期望 1/3", rank, size)
	}
	if rank, _ := r.RankOf(3); rank != 2 {
		t.Errorf("玩家3 名次 = %d, 期望 2（同分按 id 升序）", rank)
	}
	if rank, _ := r.RankOf(5); rank != 3 {
		t.Errorf("玩家5 名次 = %d, 期望 3", rank)
	}
}
