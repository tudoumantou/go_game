package runtime

import (
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
	s.rebuildQueues()

	// 回执。
	if plan.CommandReceipt != nil {
		s.receipts[plan.CommandReceipt.Key] = *plan.CommandReceipt
	}

	// 到期事件：从新状态推导，而不是由规划器显式返回。
	// 好处是"状态即事实"：重启后用同一套推导重建定时堆（文档 6.2），
	// 不需要持久化定时器本身。
	s.scheduleFromPlan(plan)
}

// rebuildQueues 从战斗记录重建排队视图。
//
// M1 没有 map_battle_queue 表，队列成员在持久层表现为
// Status=PENDING 且 AppliedOperationID="m1:queued" 的战斗记录
// （见 application/arrive.go 的约定）。重建按 DeadlineMS、BattleID
// 排序，保证同一份数据两次重建得到同样的顺序。
func (s *SceneLoop) rebuildQueues() {
	queues := make(map[domain.ObjectID][]domain.ObjectID)
	for _, b := range s.battles {
		if b.Status != domain.BattleStatusPending {
			continue
		}
		if b.AppliedOperationID != queuedMarker {
			continue
		}
		queues[b.TargetID] = append(queues[b.TargetID], b.AttackerID)
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
	for i := range plan.BattleUpsert {
		b := plan.BattleUpsert[i]
		if b.Status != domain.BattleStatusPending {
			continue
		}
		// 排队中的战斗记录不是真正的战斗（只是占位），不安排超时；
		// 只有队首那场才需要超时裁决。
		if b.AppliedOperationID == queuedMarker {
			continue
		}
		s.heap.Schedule(domain.DueEvent{
			WorldID:  s.worldID,
			DueAtMS:  b.DeadlineMS,
			Kind:     domain.DueKindBattleTimeout,
			ObjectID: b.TargetID,
			BattleID: b.BattleID,
		})
	}
}

// queuedMarker 标记「排队占位」的战斗记录（M1 约定，M2 由队列表替代）。
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
	queues := make(map[domain.ObjectID][]domain.ObjectID, len(s.queues))
	for k, v := range s.queues {
		queues[k] = append([]domain.ObjectID(nil), v...)
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
