package application

import (
	"game_study/internal/worldmap/domain"
)

// SceneSnapshot 是规划器唯一的只读输入：某一时刻、某个场景的已提交状态。
//
// 不变量（文档 18.2 / 4.2）：
//   - 快照构造时对全部对象做深拷贝，之后与 SceneLoop 的内存完全隔离；
//     规划器把快照里的字段（Cargo map、Move.Path 切片）原样放进
//     MutationPlan 交给 TxWorker 时，不会与场景循环后续的修改产生数据竞争。
//   - 快照只包含「已提交」状态。pending 事务的结果不在其中，所以规划器
//     永远不会基于未落库的假设做决策。
//   - 快照不提供写入方法。需要修改状态的路径只有一条：生成 MutationPlan。
//
// 结构体字段全部私有，通过方法访问——泄露内部 map 会让调用方
// 绕过深拷贝纪律（Go 的 map 是引用类型，返回 map 就等于交出写权限）。
type SceneSnapshot struct {
	sceneID  domain.SceneID
	epoch    uint64
	revision uint64
	topo     domain.Topology
	nowMS    int64

	objects      map[domain.ObjectKey]domain.ObjectSnapshot
	troops       map[domain.ObjectID]*domain.Troop
	cities       map[domain.ObjectID]*domain.City
	resources    map[domain.ObjectID]*domain.ResourceNode
	sessions     map[domain.ObjectID]*domain.CollectSession // key = resource_id（每资源最多一个活动会话）
	sessionsByID map[string]*domain.CollectSession
	receipts     map[domain.CommandKey]domain.CommandReceipt
	queues       map[domain.ObjectID][]domain.ObjectID    // key = 目标，value = 排队攻击方
	battles      map[domain.ObjectID]*domain.BattleRecord // key = 目标，value = 进行中战斗
	blocked      map[cellKey]struct{}
}

// cellKey 是静态阻挡格的键。用数组而不是 struct，
// 因为 [2]int64 可比较、内存布局紧凑，作 map 键比 struct 略快。
type cellKey [2]int64

// SnapshotOptions 是构造快照的参数。用 Options 而不是长参数列表：
// 后续加入战斗队列、移交记录等字段时，不需要改所有调用点。
type SnapshotOptions struct {
	SceneID  domain.SceneID
	Epoch    uint64
	Revision uint64
	Topo     domain.Topology
	NowMS    int64

	// Objects 场景内的对象快照（调用方保证已按对象种类填好 City/Troop/Resource）。
	Objects []domain.ObjectSnapshot
	// Sessions 活动的采集会话（已结算的不要传：快照只关心可继续操作的状态）。
	Sessions []*domain.CollectSession
	// Receipts 已存在的命令回执，用于幂等重放。
	Receipts []domain.CommandReceipt
	// Queues map[目标ID][]攻击方ID，按 enqueue_seq 升序。
	Queues map[domain.ObjectID][]domain.ObjectID
	// Battles 进行中的战斗（PENDING）。
	Battles []*domain.BattleRecord
	// BlockedCells 静态阻挡格（地形/建筑），坐标是格。
	BlockedCells [][2]int64
}

// NewSceneSnapshot 构造只读快照。所有入参都会被拷贝，
// 调用方在构造之后修改自己的切片不会影响快照。
func NewSceneSnapshot(opts SnapshotOptions) *SceneSnapshot {
	s := &SceneSnapshot{
		sceneID:      opts.SceneID,
		epoch:        opts.Epoch,
		revision:     opts.Revision,
		topo:         opts.Topo,
		nowMS:        opts.NowMS,
		objects:      make(map[domain.ObjectKey]domain.ObjectSnapshot, len(opts.Objects)),
		troops:       make(map[domain.ObjectID]*domain.Troop),
		cities:       make(map[domain.ObjectID]*domain.City),
		resources:    make(map[domain.ObjectID]*domain.ResourceNode),
		sessions:     make(map[domain.ObjectID]*domain.CollectSession, len(opts.Sessions)),
		sessionsByID: make(map[string]*domain.CollectSession, len(opts.Sessions)),
		receipts:     make(map[domain.CommandKey]domain.CommandReceipt, len(opts.Receipts)),
		queues:       make(map[domain.ObjectID][]domain.ObjectID, len(opts.Queues)),
		battles:      make(map[domain.ObjectID]*domain.BattleRecord, len(opts.Battles)),
		blocked:      make(map[cellKey]struct{}, len(opts.BlockedCells)),
	}
	for _, obj := range opts.Objects {
		cp := obj
		if obj.Troop != nil {
			cp.Troop = obj.Troop.Clone()
			s.troops[obj.Key.ID] = cp.Troop
		}
		if obj.City != nil {
			c := *obj.City
			cp.City = &c
			s.cities[obj.Key.ID] = cp.City
		}
		if obj.Resource != nil {
			r := *obj.Resource
			cp.Resource = &r
			s.resources[obj.Key.ID] = cp.Resource
		}
		s.objects[obj.Key] = cp
	}
	for _, sess := range opts.Sessions {
		cp := *sess
		s.sessions[cp.ResourceID] = &cp
		s.sessionsByID[cp.ID] = &cp
	}
	for _, r := range opts.Receipts {
		s.receipts[r.Key] = r
	}
	for target, queue := range opts.Queues {
		s.queues[target] = append([]domain.ObjectID(nil), queue...)
	}
	for _, b := range opts.Battles {
		cp := *b
		s.battles[cp.TargetID] = &cp
	}
	for _, c := range opts.BlockedCells {
		s.blocked[c] = struct{}{}
	}
	return s
}

func (s *SceneSnapshot) SceneID() domain.SceneID { return s.sceneID }
func (s *SceneSnapshot) Epoch() uint64           { return s.epoch }
func (s *SceneSnapshot) Revision() uint64        { return s.revision }
func (s *SceneSnapshot) Topo() domain.Topology   { return s.topo }
func (s *SceneSnapshot) NowMS() int64            { return s.nowMS }

// Object 返回对象快照的副本。副本里的指针仍指向快照内部的
// Troop/City/Resource——调用方只应读取，不应修改；写操作必须走计划。
// 这里不做二次深拷贝是性能取舍：规划器每次命令都要查若干对象，
// 全量深拷贝会把每条命令的分配放大十倍以上。
func (s *SceneSnapshot) Object(key domain.ObjectKey) (domain.ObjectSnapshot, bool) {
	obj, ok := s.objects[key]
	return obj, ok
}

// Troop 返回部队的深拷贝（含 Cargo map 与 Move.Path）。
// 部队是唯一可能被规划器修改后放进计划的类型，必须独立副本。
func (s *SceneSnapshot) Troop(id domain.ObjectID) (*domain.Troop, bool) {
	t, ok := s.troops[id]
	if !ok {
		return nil, false
	}
	return t.Clone(), true
}

// City 返回城池副本。
func (s *SceneSnapshot) City(id domain.ObjectID) (*domain.City, bool) {
	c, ok := s.cities[id]
	if !ok {
		return nil, false
	}
	cp := *c
	return &cp, true
}

// Resource 返回资源点副本。
func (s *SceneSnapshot) Resource(id domain.ObjectID) (*domain.ResourceNode, bool) {
	r, ok := s.resources[id]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

// ActiveSessionOn 返回某资源点上的活动采集会话。第一版每资源最多一个，
// 因此不返回切片——返回切片会让调用方误以为需要处理多个会话。
func (s *SceneSnapshot) ActiveSessionOn(resourceID domain.ObjectID) (*domain.CollectSession, bool) {
	sess, ok := s.sessions[resourceID]
	if !ok {
		return nil, false
	}
	cp := *sess
	return &cp, true
}

// SessionByID 按会话 ID 查（结算命令只带会话 ID）。
func (s *SceneSnapshot) SessionByID(id string) (*domain.CollectSession, bool) {
	sess, ok := s.sessionsByID[id]
	if !ok {
		return nil, false
	}
	cp := *sess
	return &cp, true
}

// SessionOnAnyResource 返回部队正在进行的采集会话（用于召回时判断
// COLLECTING 阶段是否真有一致的数据，而不是只信 Troop.Phase）。
func (s *SceneSnapshot) SessionOnAnyResource(troopID domain.ObjectID) (*domain.CollectSession, bool) {
	for _, sess := range s.sessions {
		if sess.TroopID == troopID && sess.Status == domain.CollectStatusActive {
			cp := *sess
			return &cp, true
		}
	}
	return nil, false
}

// Receipt 返回命令回执（幂等重放）。
func (s *SceneSnapshot) Receipt(key domain.CommandKey) (domain.CommandReceipt, bool) {
	r, ok := s.receipts[key]
	return r, ok
}

// TroopsOfPlayer 返回该玩家在场景内的全部部队（含 CLOSED）。
// 槽位冲突检查需要看到 CLOSED 之外的每一支部队：SETTLING 仍占槽位，
// 漏掉它会让玩家在同一槽位同时存在两支"活着"的部队。
func (s *SceneSnapshot) TroopsOfPlayer(playerID int64) []*domain.Troop {
	var out []*domain.Troop
	for _, t := range s.troops {
		if t.PlayerID == playerID {
			out = append(out, t.Clone())
		}
	}
	return out
}

// QueuedOn 返回目标上的排队攻击方（按入队顺序）。
func (s *SceneSnapshot) QueuedOn(targetID domain.ObjectID) []domain.ObjectID {
	return append([]domain.ObjectID(nil), s.queues[targetID]...)
}

// ActiveBattleOn 返回目标上进行中的战斗。
func (s *SceneSnapshot) ActiveBattleOn(targetID domain.ObjectID) (*domain.BattleRecord, bool) {
	b, ok := s.battles[targetID]
	if !ok {
		return nil, false
	}
	cp := *b
	return &cp, true
}

// Blocked 报告格是否静态阻挡。第一版只有地形/建筑阻挡，
// 部队之间不互相阻挡（否则会出现"谁先谁后通行"的排队问题）。
func (s *SceneSnapshot) Blocked(x, y int64) bool {
	_, ok := s.blocked[cellKey{x, y}]
	return ok
}

// ResourcesOfBlock 返回某富饶块上现有资源点（刷新时用于重算数量，
// 富饶计数不独立落库——文档 10.5）。
func (s *SceneSnapshot) ResourcesOfBlock(blockID int64) []*domain.ResourceNode {
	var out []*domain.ResourceNode
	for _, r := range s.resources {
		if r.RichBlockID == blockID {
			cp := *r
			out = append(out, &cp)
		}
	}
	return out
}

// AllResources 返回场景内全部资源点副本。
func (s *SceneSnapshot) AllResources() []*domain.ResourceNode {
	out := make([]*domain.ResourceNode, 0, len(s.resources))
	for _, r := range s.resources {
		cp := *r
		out = append(out, &cp)
	}
	return out
}

// AllTroops 返回场景内全部部队副本。
func (s *SceneSnapshot) AllTroops() []*domain.Troop {
	out := make([]*domain.Troop, 0, len(s.troops))
	for _, t := range s.troops {
		out = append(out, t.Clone())
	}
	return out
}
