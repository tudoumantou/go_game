package crossplay

import (
	"sort"
	"sync"

	internalv2 "game_study/pkg/pb/internalv2"
)

// loc 玩家在内存分组里的坐标（组号 + 组内下标）。
//
// 存下标而不是指针：Load 会整体替换分组切片，指针在全量重载后失效；
// 下标在 Load 时重建，只要不跨 Load 使用就是稳定的。
type loc struct {
	groupID int32
	idx     int
}

// Store 活动期分组状态的唯一内存副本（进程级单例）。
//
// 谁写：收积分上报的并发 goroutine（多个游戏服各一条流）；
// 谁读：下发窗口（单 goroutine）、玩家查询、GM。
// 因此用 RWMutex —— 写是「改一个玩家的一个字段」，读是「整组快照」，
// 读远多于写。
//
// 为什么不放 Actor：积分是玩法数据不是玩家存档，离线玩家也要能被改分。
// 走 Actor 意味着为了改一个数先把玩家三张表全量加载，跨服活动里离线占多数，
// 代价不成比例。
type Store struct {
	mu    sync.RWMutex
	byID  map[int32]*Group
	index map[int64]loc
}

// NewStore 创建空 Store。
func NewStore() *Store {
	return &Store{
		byID:  make(map[int32]*Group),
		index: make(map[int64]loc),
	}
}

// Load 用一次分组结果整体替换内存（分组完成 / center 重启后从库重载）。
//
// 整体替换而不是增量合并：分组结果是全量的，合并反而会留下上一版的残影。
func (s *Store) Load(groups []Group) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.byID = make(map[int32]*Group, len(groups))
	s.index = make(map[int64]loc, len(groups)*GroupSize)
	for i := range groups {
		src := groups[i]
		g := &Group{ID: src.ID}
		g.Members = make([]Member, len(src.Members))
		copy(g.Members, src.Members)
		s.byID[g.ID] = g
		for j := range g.Members {
			g.Members[j].GroupID = g.ID
			s.index[g.Members[j].PlayerID] = loc{groupID: g.ID, idx: j}
		}
	}
}

// ApplyScore 写入一个玩家的积分，返回更新后的成员（含组号）。
//
// ok=false 表示这个玩家不在分组里（未报名、或被分到别的 instance），
// 调用方应丢弃该条上报而不是报错——跨服活动期间报名表已清，
// center 只认分组表，认不出来就是脏数据。
//
// changed=false 表示分值确实没变（重复上报）：调用方据此跳过下发，
// 否则一条没变化的重发报文会白白触发一次全组下发。
//
// 分数按 ScoreFloor 收敛，越界值直接截断而不是拒绝：越界意味着上游算错，
// 但拒绝写入会让 center 与游戏服长期不一致，截断至少能收敛到合法区间。
func (s *Store) ApplyScore(playerID, score, updatedAt int64) (m Member, ok, changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, exists := s.index[playerID]
	if !exists {
		return Member{}, false, false
	}
	g, exists := s.byID[l.groupID]
	if !exists || l.idx >= len(g.Members) {
		// 索引与分组切片不同步（不应发生）：按未知玩家处理，等全量重载修回来。
		return Member{}, false, false
	}

	cur := &g.Members[l.idx]
	score = ClampScore(score)
	if cur.Score == score {
		return *cur, true, false
	}
	cur.Score = score
	cur.UpdatedAt = updatedAt
	return *cur, true, true
}

// ServersOf 返回某组涉及的区服（升序去重）。
//
// 用途：下发时算出「这条变化要送给哪几个服」。数据源仍是分组快照里的
// ServerID，不额外维护 server → group 的索引表。
func (s *Store) ServersOf(groupID int32) []int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.byID[groupID]
	if !ok {
		return nil
	}
	seen := make(map[int32]struct{}, 8)
	out := make([]int32, 0, 8)
	for _, m := range g.Members {
		if _, dup := seen[m.ServerID]; dup {
			continue
		}
		seen[m.ServerID] = struct{}{}
		out = append(out, m.ServerID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Member 返回某玩家的当前值（含组号）；未分组返回 false。
func (s *Store) Member(playerID int64) (Member, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l, ok := s.index[playerID]
	if !ok {
		return Member{}, false
	}
	g, ok := s.byID[l.groupID]
	if !ok || l.idx >= len(g.Members) {
		return Member{}, false
	}
	return g.Members[l.idx], true
}

// Group 返回某组的成员快照（深拷贝，调用方可随意读）。
func (s *Store) Group(id int32) []Member {
	s.mu.RLock()
	defer s.mu.RUnlock()

	g, ok := s.byID[id]
	if !ok {
		return nil
	}
	out := make([]Member, len(g.Members))
	copy(out, g.Members)
	return out
}

// Groups 返回全部组号的升序列表。
func (s *Store) Groups() []int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := make([]int32, 0, len(s.byID))
	for id := range s.byID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// All 返回全部组的深拷贝（center 重启时落库用，或 GM 导出）。
func (s *Store) All() []Group {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]Group, 0, len(s.byID))
	for _, g := range s.byID {
		cp := Group{ID: g.ID, Members: make([]Member, len(g.Members))}
		copy(cp.Members, g.Members)
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Len 当前参与分组的玩家总数。
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.index)
}

// ServerOf 返回某玩家所属区服；未分组返回 0。
//
// 用途：跨服分数变更时查「这个玩家归哪个服管」。数据源是分组快照，
// 不额外维护一份 playerID → serverID 的映射表 —— 两份信息迟早会对不上。
func (s *Store) ServerOf(playerID int64) int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.index[playerID]
	if !ok {
		return 0
	}
	g, ok := s.byID[l.groupID]
	if !ok || l.idx >= len(g.Members) {
		return 0
	}
	return g.Members[l.idx].ServerID
}

// Snapshot 生成某组的下发快照（含 Power，用于首次下发与校准）。
func (s *Store) Snapshot(groupID int32) *internalv2.CrossGroupDelta {
	members := s.Group(groupID)
	if members == nil {
		return nil
	}
	d := &internalv2.CrossGroupDelta{
		GroupId:  groupID,
		Snapshot: true,
		Players:  make([]*internalv2.CrossScoreEntry, 0, len(members)),
	}
	for _, m := range members {
		d.Players = append(d.Players, &internalv2.CrossScoreEntry{
			GroupId:   groupID,
			PlayerId:  m.PlayerID,
			Score:     m.Score,
			Power:     m.Power,
			UpdatedAt: m.UpdatedAt,
			ServerId:  m.ServerID,
		})
	}
	return d
}

// PushBuilder 累积一个下发窗口内要发给各服的变化项，按服聚合。
//
// 为什么按服聚合而不是按组推：一期 1000 组，一组的变化要送到组内约 30 个服，
// 按组推就是 30000 条报文/分钟（500 条/秒），接收端每条都要过 oneof 解码并抢
// 一次队列锁，tickLoop 会被打满。按服聚合后是 30 条/分钟，语义完全不变。
//
// 非并发安全：一个下发窗口内由单 goroutine 使用。
type PushBuilder struct {
	perServer map[int32]map[int32]*internalv2.CrossGroupDelta // serverID → groupID → delta
}

// NewPushBuilder 创建空构建器。
func NewPushBuilder() *PushBuilder {
	return &PushBuilder{perServer: make(map[int32]map[int32]*internalv2.CrossGroupDelta)}
}

// AddTo 把一个变化项加进指定服的包。
//
// 调用方负责「不给玩家自己的服发」：接收方本地是该玩家的权威，收到自己的
// 旧值会把它刚加的分覆盖掉（本地 1001 ← center 的 1000），下一轮上报又把
// 1000 报回去，形成「加分被吞掉」的循环。在中心服这一侧排除，接收方就
// 不需要再做判断。
func (b *PushBuilder) AddTo(serverID int32, m Member) {
	d := b.deltaFor(serverID, m.GroupID, false)
	d.Players = append(d.Players, &internalv2.CrossScoreEntry{
		GroupId:   m.GroupID,
		PlayerId:  m.PlayerID,
		Score:     m.Score,
		UpdatedAt: m.UpdatedAt,
		ServerId:  m.ServerID,
	})
}

// AddSnapshotTo 把整组成员加进该服的包（**包括该服自己的玩家**）。
//
// 快照用于分组完成与 10 分钟校准：一次带上 Power（分组排序依据），
// 接收方据此直接替换本地镜像的这一组，而不是逐条合并。
//
// 与 Add（增量）的区别就在这里：增量**不发**收件方自己的玩家，快照**要发**。
// 理由不是「多带几个字段无所谓」，而是**收件方没有别的途径知道自己的分组**：
//
//	分组结果只存在于中心服，游戏服要知道「我的玩家 7 在组 3」只有这一条路。
//	如果快照也把自己的人滤掉，那个玩家在本服永远是「已报名、未分组」，
//	界面会一直显示等待分组，直到活动结束。
//
// 那为什么不怕覆盖本地新值？因为接收端对快照里的**本服玩家**只采纳归属
// （group_id / power）与「本地没有时才采纳分数」，本地有记录就以本地为准
// （见游戏服 crossplay/registry.go 的 applySnapshot）。归属与分数在这里
// 是两件事：归属只能听中心服的，分数只能听本服的。
func (b *PushBuilder) AddSnapshotTo(serverID int32, groupID int32, members []Member) {
	for _, m := range members {
		d := b.deltaFor(serverID, groupID, true)
		d.Players = append(d.Players, &internalv2.CrossScoreEntry{
			GroupId:   groupID,
			PlayerId:  m.PlayerID,
			Score:     m.Score,
			Power:     m.Power,
			UpdatedAt: m.UpdatedAt,
			ServerId:  m.ServerID,
		})
	}
}

func (b *PushBuilder) deltaFor(serverID, groupID int32, snapshot bool) *internalv2.CrossGroupDelta {
	byGroup, ok := b.perServer[serverID]
	if !ok {
		byGroup = make(map[int32]*internalv2.CrossGroupDelta)
		b.perServer[serverID] = byGroup
	}
	d, ok := byGroup[groupID]
	if !ok {
		d = &internalv2.CrossGroupDelta{GroupId: groupID, Snapshot: snapshot}
		byGroup[groupID] = d
	}
	return d
}

// Messages 返回「一个服一条」的报文集合。
func (b *PushBuilder) Messages() map[int32]*internalv2.CrossGroupDeltaPush {
	out := make(map[int32]*internalv2.CrossGroupDeltaPush, len(b.perServer))
	for sid, byGroup := range b.perServer {
		push := &internalv2.CrossGroupDeltaPush{Groups: make([]*internalv2.CrossGroupDelta, 0, len(byGroup))}
		for _, d := range byGroup {
			push.Groups = append(push.Groups, d)
		}
		// 组号升序：报文内容稳定，便于对比与测试（map 遍历顺序随机）。
		sort.Slice(push.Groups, func(i, j int) bool { return push.Groups[i].GroupId < push.Groups[j].GroupId })
		out[sid] = push
	}
	return out
}

// Empty 本窗口没有任何变化。
func (b *PushBuilder) Empty() bool { return len(b.perServer) == 0 }
