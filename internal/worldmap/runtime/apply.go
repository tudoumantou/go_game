package runtime

import (
	"sort"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
)

// apply.go 负责「把已提交的计划应用到场景内存镜像」。
//
// 这是单写者模型的落点：只有 SceneLoop 的循环 goroutine 调用这里的函数，
// 因此内存 map 不需要任何锁。反过来，任何从外部（TxWorker、AOI、
// 测试 goroutine）读写这些 map 的行为都会破坏模型——所以字段全部私有，
// 对外只暴露深拷贝的快照。
//
// 应用顺序是有语义的，不能随便调换：
//  1. 对象主体（Troop/City/Resource）先落，因为采集会话、战斗记录都引用它们；
//  2. 会话状态再落，因为结束会话时要读资源余量做校验；
//  3. 战斗与队列最后落，队列依赖战斗的目标归属。
func (s *SceneLoop) applyPlan(plan *domain.MutationPlan) {
	if plan == nil {
		return
	}
	for _, snap := range plan.ObjectsUpsert {
		cp := cloneObjectSnapshot(snap)
		s.objects[cp.Key] = cp
		switch {
		case cp.Troop != nil:
			s.troops[cp.Key.ID] = cp.Troop
		case cp.City != nil:
			s.cities[cp.Key.ID] = cp.City
		case cp.Resource != nil:
			s.resources[cp.Key.ID] = cp.Resource
		}
	}
	for _, key := range plan.ObjectsTombstone {
		obj, ok := s.objects[key]
		if !ok {
			continue
		}
		obj.Deleted = true
		s.objects[key] = obj
		delete(s.troops, key.ID)
		delete(s.cities, key.ID)
		delete(s.resources, key.ID)
	}

	// 采集会话：开始与结算。
	for i := range plan.CollectStart {
		sess := plan.CollectStart[i]
		cp := sess
		s.sessions[cp.ResourceID] = &cp
	}
	for _, ch := range plan.CollectSettle {
		for resID, sess := range s.sessions {
			if sess.ID != ch.SessionID {
				continue
			}
			sess.Status = domain.CollectStatusSettled
			sess.SettledQty = ch.SettledQty
			delete(s.sessions, resID)
			if res, ok := s.resources[resID]; ok {
				res.ActiveCollectID = ""
			}
			break
		}
	}

	// 战斗记录：按目标索引（每目标同时最多一场活动战斗）。
	for i := range plan.BattleUpsert {
		b := plan.BattleUpsert[i]
		cp := b
		s.battles[cp.TargetID] = &cp
	}

	// 队列：显式变更优先。
	//
	// 为什么需要 queueExplicit 这个开关：M1 的历史数据用
	// "PENDING 战斗记录 + m1:queued 标记"表示排队成员，而新数据用
	// map_battle_queue。如果每条计划都无条件 rebuildQueues，那么一条
	// 与队列无关的命令提交后，队列就会被"从战斗记录推断"的结果覆盖，
	// 显式维护的 FIFO 立刻丢失。反过来，只要还没见过显式变更，
	// 就必须继续按旧格式推断，否则老世界恢复后队列是空的。
	if len(plan.QueueEnqueue) > 0 || len(plan.QueueDequeue) > 0 {
		s.queueExplicit = true
		s.applyQueueChanges(plan)
	} else if !s.queueExplicit {
		s.rebuildQueues()
	}

	// 回执。
	if plan.CommandReceipt != nil {
		s.receipts[plan.CommandReceipt.Key] = *plan.CommandReceipt
	}

	// 到期事件：从新状态推导，而不是由规划器显式返回。
	// 好处是"状态即事实"：重启后用同一套推导重建定时堆（文档 6.2），
	// 不需要持久化定时器本身。
	s.scheduleFromPlan(plan)
}

// applyQueueChanges 把计划里的队列变更落到内存视图。
//
// 顺序固定为"先出队、再入队"：结果命令会在同一个计划里先移除队首、
// 再让下一名开战；如果反过来，同一个部队可能先入队再被删掉，
// 或者 Seq 排序时把新的队首排到了被删除的成员后面。
//
// 去重按 (TargetID,TroopID)：map_battle_queue 上有
// UNIQUE(world,target,troop)，同一支部队重复入队会让事务失败。
// 内存视图先挡住，避免"规划重试"变成一次注定失败的提交。
func (s *SceneLoop) applyQueueChanges(plan *domain.MutationPlan) {
	for _, de := range plan.QueueDequeue {
		q := s.queues[de.TargetID]
		kept := make([]domain.QueueEntry, 0, len(q))
		for _, e := range q {
			if e.TroopID == de.TroopID {
				continue
			}
			kept = append(kept, e)
		}
		if len(kept) == 0 {
			delete(s.queues, de.TargetID)
			continue
		}
		s.queues[de.TargetID] = kept
	}
	for _, en := range plan.QueueEnqueue {
		q := s.queues[en.TargetID]
		dup := false
		for _, e := range q {
			if e.TroopID == en.TroopID {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		q = append(q, domain.QueueEntry{TargetID: en.TargetID, TroopID: en.TroopID, Seq: en.Seq})
		// 按 Seq 排序：入队顺序才是 FIFO 的真源，插入顺序不是
		// （恢复/重放时插入顺序可能来自 SQL 行序或 map 遍历）。
		sort.SliceStable(q, func(i, j int) bool { return q[i].Seq < q[j].Seq })
		s.queues[en.TargetID] = q
	}
}

// rebuildQueues 从战斗记录重建排队视图（M1 历史数据兼容路径）。
//
// M1 没有 map_battle_queue 表，队列成员在持久层表现为
// Status=PENDING 且 AppliedOperationID="m1:queued" 的战斗记录
// （见 application/arrive.go 的兼容标记说明）。重建按 DeadlineMS、
// BattleID 排序，保证同一份数据两次重建得到同样的顺序——map 遍历
// 顺序随机，不排序就会"每次重启队列顺序都不一样"。
//
// 新命令不再产生这种记录；一旦见过显式队列变更，本函数就不再被调用
// （applyPlan 的 queueExplicit 开关）。
func (s *SceneLoop) rebuildQueues() {
	queues := make(map[domain.ObjectID][]domain.QueueEntry)
	for _, b := range s.battles {
		if b.Status != domain.BattleStatusPending {
			continue
		}
		if b.AppliedOperationID != queuedMarker {
			continue
		}
		queues[b.TargetID] = append(queues[b.TargetID], domain.QueueEntry{
			TargetID: b.TargetID,
			TroopID:  b.AttackerID,
			// 序号取 DeadlineMS：M1 用它近似入队时间；同一毫秒内
			// 再用 TroopID 兜底（下方 sort），保证重建顺序唯一。
			Seq: b.DeadlineMS,
		})
	}
	for target, q := range queues {
		sort.SliceStable(q, func(i, j int) bool {
			if q[i].Seq != q[j].Seq {
				return q[i].Seq < q[j].Seq
			}
			return q[i].TroopID < q[j].TroopID
		})
		queues[target] = q
	}
	s.queues = queues
}

// scheduleFromPlan 为计划里产生的新状态安排到期事件。
func (s *SceneLoop) scheduleFromPlan(plan *domain.MutationPlan) {
	for _, snap := range plan.ObjectsUpsert {
		t := snap.Troop
		if t == nil || t.Move == nil {
			continue
		}
		if t.Phase != domain.TroopPhaseMarching && t.Phase != domain.TroopPhaseReturning {
			continue
		}
		s.heap.Schedule(domain.DueEvent{
			WorldID:  s.worldID,
			DueAtMS:  t.Move.EndAtMS,
			Kind:     domain.DueKindMoveArrive,
			ObjectID: t.Header.ID,
			MoveID:   t.Move.MoveID,
		})
	}
	for i := range plan.CollectStart {
		sess := plan.CollectStart[i]
		s.heap.Schedule(domain.DueEvent{
			WorldID:   s.worldID,
			DueAtMS:   sess.EndAtMS,
			Kind:      domain.DueKindCollectEnd,
			ObjectID:  sess.ResourceID,
			SessionID: sess.ID,
		})
	}
	// 战斗超时只给"队首"安排：同一目标上的 PENDING 记录里先到期的
	// 那一条才是当前正在打的战斗。为什么不是全部 PENDING：历史数据
	// 里排队成员也是 PENDING（M1 标记），给它们排超时会在队首还没结束
	// 时就触发另一场超时裁决，把队列顺序彻底打乱。
	//
	// 用 map 聚合"每目标最早的一条"，聚合结果与遍历顺序无关
	// （比较键是 DeadlineMS+BattleID 全序），因此可以放心 range。
	heads := make(map[domain.ObjectID]domain.BattleRecord, len(plan.BattleUpsert))
	for i := range plan.BattleUpsert {
		b := plan.BattleUpsert[i]
		if b.Status != domain.BattleStatusPending {
			continue
		}
		if b.AppliedOperationID == queuedMarker {
			continue
		}
		cur, ok := heads[b.TargetID]
		if !ok || b.DeadlineMS < cur.DeadlineMS ||
			(b.DeadlineMS == cur.DeadlineMS && b.BattleID < cur.BattleID) {
			heads[b.TargetID] = b
		}
	}
	for _, b := range heads {
		s.heap.Schedule(domain.DueEvent{
			WorldID:  s.worldID,
			DueAtMS:  b.DeadlineMS,
			Kind:     domain.DueKindBattleTimeout,
			ObjectID: b.TargetID,
			BattleID: b.BattleID,
		})
	}
}

// queuedMarker 标记「排队占位」的战斗记录。
//
// 这是 M1 的历史约定：MutationPlan 还没有队列字段时，排队成员写成
// 一条 Status=PENDING 且 AppliedOperationID="m1:queued" 的战斗记录。
// 现在队列有了显式字段（QueueEnqueue/QueueDequeue → map_battle_queue），
// 新数据不再产生这种记录；本常量只服务于**兼容读取**（rebuildQueues
// 与 scheduleFromPlan 跳过排队占位）。字符串必须与 application 包
// 的 m1QueuedBattleMarker 保持一致——两份常量是历史包袱，删掉任一份
// 都要先确认线上没有带标记的存量记录。
const queuedMarker = "m1:queued"

// snapshotForPlanner 用已提交状态构造只读快照。
//
// 每次都新建快照而不是缓存：规划器可能持有它直到计划提交完成，
// 复用同一个实例会让"快照被后续状态污染"，违反「快照 = 某一时刻」的定义。
// 对象数量在单场景量级（千级），重建成本远低于一次 SQL 提交。
func (s *SceneLoop) snapshotForPlanner(nowMS int64) *application.SceneSnapshot {
	objects := make([]domain.ObjectSnapshot, 0, len(s.objects))
	for _, obj := range s.objects {
		if obj.Deleted {
			continue
		}
		objects = append(objects, cloneObjectSnapshot(obj))
	}
	sessions := make([]*domain.CollectSession, 0, len(s.sessions))
	for _, sess := range s.sessions {
		cp := *sess
		sessions = append(sessions, &cp)
	}
	receipts := make([]domain.CommandReceipt, 0, len(s.receipts))
	for _, r := range s.receipts {
		receipts = append(receipts, r)
	}
	battles := make([]*domain.BattleRecord, 0, len(s.battles))
	for _, b := range s.battles {
		if b.Status != domain.BattleStatusPending {
			continue
		}
		cp := *b
		battles = append(battles, &cp)
	}
	// 队列投影：按 Seq 升序转成"攻击方 ID 列表"。规划器只关心先后
	// 顺序，Seq 是持久化字段，不属于快照的命令契约。
	//
	// 先复制再排序：s.queues 是场景内部状态，快照必须与它完全隔离
	// （文档 4.2），直接 sort 内部切片会在快照构造期间改动业务状态。
	queues := make(map[domain.ObjectID][]domain.ObjectID, len(s.queues))
	for target, entries := range s.queues {
		if len(entries) == 0 {
			continue
		}
		ordered := make([]domain.QueueEntry, len(entries))
		copy(ordered, entries)
		sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Seq < ordered[j].Seq })
		ids := make([]domain.ObjectID, 0, len(ordered))
		for _, e := range ordered {
			ids = append(ids, e.TroopID)
		}
		queues[target] = ids
	}
	return application.NewSceneSnapshot(application.SnapshotOptions{
		SceneID:      s.sceneID,
		Epoch:        s.epoch,
		Revision:     s.revision,
		Topo:         s.topo,
		NowMS:        nowMS,
		Objects:      objects,
		Sessions:     sessions,
		Receipts:     receipts,
		Queues:       queues,
		Battles:      battles,
		BlockedCells: s.blockedCells(),
	})
}

// blockedCells 返回场景内静态阻挡格。
//
// M1 还没有地形配置表（configs/gamedata/xml/map_*.xml 属于 M2），
// 因此返回空集：所有格子可通行。接口先留好，接配置时只改这里。
func (s *SceneLoop) blockedCells() [][2]int64 { return nil }
