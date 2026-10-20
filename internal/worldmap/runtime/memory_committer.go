package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"game_study/internal/worldmap/domain"
)

// MemoryCommitter 是内存事务替身。
//
// ⚠️ 仅用于 M1 的领域/运行时测试与本地闭环，生产必须替换为 MySQL 实现
// （文档 M2）。它没有事务隔离、没有崩溃恢复、没有唯一键约束，
// 唯一目的是让 SceneLoop 的「规划 → 提交 → 应用结果 → 推进 revision」
// 这条链路可以被单测完整驱动。
//
// 虽然只是替身，仍然严格做深拷贝：如果替身偷懒共享 map，
// 测试会通过而生产会数据竞争——替身必须和真实实现守同一套纪律。
type MemoryCommitter struct {
	mu       sync.Mutex
	objects  map[domain.ObjectKey]domain.ObjectSnapshot
	receipts map[domain.CommandKey]domain.CommandReceipt
	battles  map[string]domain.BattleRecord
	delivery map[string]domain.Delivery
	revs     map[domain.SceneID]uint64

	// 故障注入（测试用）。
	failNext    int
	failErr     error
	unknownNext int

	commits int64
}

// NewMemoryCommitter 创建内存提交器。
func NewMemoryCommitter() *MemoryCommitter {
	return &MemoryCommitter{
		objects:  make(map[domain.ObjectKey]domain.ObjectSnapshot),
		receipts: make(map[domain.CommandKey]domain.CommandReceipt),
		battles:  make(map[string]domain.BattleRecord),
		delivery: make(map[string]domain.Delivery),
		revs:     make(map[domain.SceneID]uint64),
	}
}

// FailNext 让接下来 n 次提交返回可重试错误（模拟 SQL 死锁/超时回滚）。
func (m *MemoryCommitter) FailNext(n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNext = n
	m.failErr = err
}

// UnknownNext 让接下来 n 次提交返回「结果未知」（模拟 COMMIT 回包丢失）。
// 结果未知时**不应用**任何写入，场景据此进入冻结/解析流程。
func (m *MemoryCommitter) UnknownNext(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unknownNext = n
}

// Commit 实现 Committer。
func (m *MemoryCommitter) Commit(ctx context.Context, plan *domain.MutationPlan) domain.CommitResult {
	if err := ctx.Err(); err != nil {
		return domain.CommitResult{Err: err}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failNext > 0 {
		m.failNext--
		err := m.failErr
		if err == nil {
			err = errors.New("worldmap: 注入的提交失败")
		}
		return domain.CommitResult{Err: err}
	}
	if m.unknownNext > 0 {
		m.unknownNext--
		// Unknown 语义：事务可能已提交也可能没有，调用方不得视为成功，
		// 也不得直接重试（必须先解析）。
		return domain.CommitResult{Unknown: true}
	}

	// 幂等：同键同参直接返回已提交，不同参是客户端错误。
	if plan.CommandReceipt != nil {
		key := plan.CommandReceipt.Key
		if existing, ok := m.receipts[key]; ok {
			if existing.PayloadHash != plan.CommandReceipt.PayloadHash {
				return domain.CommitResult{Err: fmt.Errorf("%w: 键 %v 已用于其他请求", domain.ErrIdempotencyConflict, key)}
			}
			rev := m.revForKey(key)
			return domain.CommitResult{
				Committed:      true,
				SceneRevisions: []domain.SceneRevision{{SceneID: plan.AffectedSceneRevisions[0].SceneID, Revision: rev}},
			}
		}
	}

	// 应用对象写入。
	for _, snap := range plan.ObjectsUpsert {
		m.objects[snap.Key] = cloneObjectSnapshot(snap)
	}
	for _, key := range plan.ObjectsTombstone {
		if obj, ok := m.objects[key]; ok {
			obj.Deleted = true
			m.objects[key] = obj
		}
	}
	for _, b := range plan.BattleUpsert {
		cp := b
		m.battles[b.BattleID] = cp
	}
	for _, d := range plan.DeliveryCreate {
		cp := d
		if d.Cargo != nil {
			cp.Cargo = make(map[int32]int64, len(d.Cargo))
			for k, v := range d.Cargo {
				cp.Cargo[k] = v
			}
		}
		m.delivery[d.DeliveryID] = cp
	}

	// 推进 revision：每个受影响场景 +1。
	var revs []domain.SceneRevision
	for _, sr := range plan.AffectedSceneRevisions {
		m.revs[sr.SceneID] = sr.Revision + 1
		revs = append(revs, domain.SceneRevision{SceneID: sr.SceneID, Revision: m.revs[sr.SceneID]})
	}

	// 回执最后写：它是「这条命令已生效」的提交标记，
	// 先写回执再写对象会让"回执存在但对象没落库"的中间态可被读到。
	if plan.CommandReceipt != nil {
		m.receipts[plan.CommandReceipt.Key] = *plan.CommandReceipt
	}
	m.commits++

	versions := make([]domain.ObjectVersion, 0, len(plan.ObjectsUpsert))
	for _, snap := range plan.ObjectsUpsert {
		versions = append(versions, domain.ObjectVersion{Key: snap.Key, Version: snap.Version})
	}
	return domain.CommitResult{Committed: true, SceneRevisions: revs, ObjectVersions: versions}
}

// revForKey 返回命令所属场景的当前 revision。
func (m *MemoryCommitter) revForKey(key domain.CommandKey) uint64 {
	// 内存替身不维护「命令 → 场景」映射，直接返回全局最大 revision；
	// 真实实现从 map_command.scene_id 读取。
	var max uint64
	for _, r := range m.revs {
		if r > max {
			max = r
		}
	}
	return max
}

// Object 返回提交器内的对象（测试断言用）。
func (m *MemoryCommitter) Object(key domain.ObjectKey) (domain.ObjectSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, ok := m.objects[key]
	return cloneObjectSnapshot(obj), ok
}

// Receipt 返回回执（测试断言幂等用）。
func (m *MemoryCommitter) Receipt(key domain.CommandKey) (domain.CommandReceipt, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.receipts[key]
	return r, ok
}

// Objects 返回全部对象副本（按需排序由调用方处理）。
func (m *MemoryCommitter) Objects() []domain.ObjectSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.ObjectSnapshot, 0, len(m.objects))
	for _, obj := range m.objects {
		out = append(out, cloneObjectSnapshot(obj))
	}
	return out
}

// Commits 返回成功提交次数。
func (m *MemoryCommitter) Commits() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.commits
}

// Revision 返回某场景当前 revision。
func (m *MemoryCommitter) Revision(scene domain.SceneID) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.revs[scene]
}

// cloneObjectSnapshot 深拷贝对象快照。所有指针字段与内部 map 都要复制：
// 只要漏一个 map，两个 goroutine 就会共享同一个底层哈希表。
func cloneObjectSnapshot(s domain.ObjectSnapshot) domain.ObjectSnapshot {
	cp := s
	if s.Troop != nil {
		cp.Troop = s.Troop.Clone()
	}
	if s.City != nil {
		c := *s.City
		cp.City = &c
	}
	if s.Resource != nil {
		r := *s.Resource
		cp.Resource = &r
	}
	return cp
}
