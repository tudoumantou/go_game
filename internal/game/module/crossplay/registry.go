// registry.go —— 进程级内存镜像（项目约定的 registry 槽位：数据不属于某个玩家）。
//
// 为什么是进程级单例而不是 Ext 槽位：积分是玩法数据，不是玩家存档。
// 玩家离线也能被打、被改分，所以「玩家不在内存」不能成为「改不了分」的理由；
// 反过来，如果挂在 Ext 上，打一个离线玩家就要把他的背包/任务/装备全量
// 加载进来只为一个字段——活动期几万次攻击就是几万次全量加载。
//
// 并发模型：三处入口并发访问同一个 Registry——
//
//	① 玩家 Actor（handler.go，读自己的状态、点报名）
//	② 中心服下发 goroutine（callback.go，改别服玩家的分）
//	③ 上报/落库定时器（delivery.go，取脏集合）
//
// 所以全部方法自带锁，调用方不需要也不应该再套一层。
package crossplay

import (
	"sync"
)

// Registry 玩法数据的内存镜像。
type Registry struct {
	mu sync.RWMutex

	// serverID 本区区服编号。
	//
	// 判断「这个玩家是不是本服人」只能靠它：快照里每个成员都带 server_id，
	// 而 players 表里可能还没有这个玩家（重启后没加载到、或刚分好组）。
	// 没有它的话，快照到达时会把自己人当别服人处理——那个玩家在本服
	// 永远不在 players 里，界面一直显示「未分组」。
	serverID int32

	// players 本服玩家的玩法数据（权威）。值指针与 groups 里同一玩家的条目
	// **共享同一个对象**：改分只写一次，界面排行与上报数据自动一致。
	players map[int64]*Entry

	// byID 全部参与者（本服 + 别服），指向 players / groups 里同一个对象。
	//
	// 单独一张表是因为「攻击目标」既可能是本服玩家也可能是别服玩家，
	// 而 groups 是按组切的，从 playerID 找组需要遍历全部组。byID 让
	// 「查任意一个参与者」变成 O(1)——攻击是热路径，一秒钟可能几千次。
	byID map[int64]*Entry

	// groups 组镜像：group_id → 全组成员（含别服玩家）。
	//
	// 别服玩家的条目只在这里，来自中心服下发——本服不写它，也无权写。
	groups map[int32]*GroupView

	// enrolled 已报名、尚未分组。分组信息到达后从这里移除，转入 players。
	enrolled map[int64]struct{}

	// dirty 待上报中心服的玩家（playerID → 最后变化时刻）。
	//
	// 用 map 而不是队列：同一玩家 60s 内变十次，只需要报最后那个值
	// （上报的是绝对值）。队列会让中心服收到十个中间态，且要自己去重。
	dirty map[int64]int64
}

// NewRegistry 创建空镜像。serverID 是本区区服编号（0 表示单区/未配置，
// 此时所有快照成员都按「别服」处理，本服玩家只能靠 players 表识别）。
func NewRegistry(serverID int32) *Registry {
	return &Registry{
		serverID: serverID,
		players:  make(map[int64]*Entry),
		byID:     make(map[int64]*Entry),
		groups:   make(map[int32]*GroupView),
		enrolled: make(map[int64]struct{}),
		dirty:    make(map[int64]int64),
	}
}

// isOwnServer 判断一个快照成员是不是本服玩家。
//
// 两个条件取或：报文里声明的 server_id 是本服，或者 players 里已经有他
// （报名时登记过）。后者是兜底——serverID 配置漏填（0）时仍能正确识别。
func (r *Registry) isOwnServer(m *Entry) bool {
	if _, ok := r.players[m.PlayerID]; ok {
		return true
	}
	return r.serverID != 0 && m.ServerID == r.serverID
}

// Load 用库里的数据初始化镜像（进程启动时调用一次）。
//
// 顺序要求：先 players 再 groups。因为 groups 里的本服玩家必须复用 players
// 的对象指针，反过来的话会建出两份对象，之后改分只改一份，界面读另一份。
func (r *Registry) Load(players []*Entry, enrolled []int64, groupMembers map[int32][]*Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, e := range players {
		if e == nil || e.PlayerID == 0 {
			continue
		}
		r.players[e.PlayerID] = e
		r.byID[e.PlayerID] = e
	}
	for _, pid := range enrolled {
		r.enrolled[pid] = struct{}{}
	}

	for gid, members := range groupMembers {
		g := &GroupView{ID: gid, entries: make(map[int64]*Entry, len(members))}
		r.groups[gid] = g
		for _, m := range members {
			if m == nil || m.PlayerID == 0 {
				continue
			}
			// 本服玩家：复用 players 的对象，保证「改一次两边都在」
			if own, ok := r.players[m.PlayerID]; ok {
				own.GroupID = gid
				if own.Power == 0 {
					own.Power = m.Power
				}
				g.entries[own.PlayerID] = own
				r.byID[own.PlayerID] = own
				continue
			}
			m.GroupID = gid
			g.entries[m.PlayerID] = m
			r.byID[m.PlayerID] = m
		}
	}
}

// Enroll 登记一次报名成功（中心服已裁决通过）。
func (r *Registry) Enroll(e *Entry) {
	if e == nil || e.PlayerID == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	e.GroupID = 0 // 报名期还没有分组
	r.players[e.PlayerID] = e
	r.byID[e.PlayerID] = e
	// 分组信息尚未到达，先挂在「已报名未分组」里。
	// 两者都置：players 是查询与改分的入口，enrolled 是界面区分
	// 「已报名」与「已分组」的标志。
	r.enrolled[e.PlayerID] = struct{}{}
}

// Cancel 取消报名（中心服已裁决通过）。
//
// 已分组的玩家不能取消（报名期才允许取消，那时不可能有分组），
// 所以这里不需要处理「从组镜像里摘掉」的复杂情况；真出现了也只是
// 留一个孤儿条目，等下一次快照整体替换掉。
func (r *Registry) Cancel(playerID int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.players, playerID)
	delete(r.enrolled, playerID)
	delete(r.dirty, playerID)
	// 取消只发生在报名期，那时不可能有分组，所以 byID 里也不会有
	// 「别处共享的同一对象」，直接删是安全的。
	delete(r.byID, playerID)
}

// IsEnrolled 是否已报名（含已分组）。
func (r *Registry) IsEnrolled(playerID int64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if _, ok := r.enrolled[playerID]; ok {
		return true
	}
	e, ok := r.players[playerID]
	return ok && e.GroupID > 0
}

// Player 返回本服玩家的条目副本。
func (r *Registry) Player(playerID int64) (Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.players[playerID]
	if !ok {
		return Entry{}, false
	}
	return *e, true
}

// Participant 查任意一个参与者（本服或别服）。
//
// 攻击热路径要用它：A 服要打 B 服玩家，得先知道那个人在哪个服
// （改分只能由他自己服写，报文要发给他的服）。isOwn=false 的条目
// 本服无权修改，只能发通知。
func (r *Registry) Participant(playerID int64) (e Entry, isOwn bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cur, ok := r.byID[playerID]
	if !ok {
		return Entry{}, false
	}
	_, isOwn = r.players[playerID]
	return *cur, isOwn
}

// RankOf 返回玩家在组内的名次与组内人数（带锁版本，供界面调用）。
func (r *Registry) RankOf(playerID int64) (rank, size int32) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.players[playerID]
	if !ok || e.GroupID == 0 {
		return 0, 0
	}
	g, ok := r.groups[e.GroupID]
	if !ok {
		return 0, 0
	}
	return g.Rank(playerID), int32(g.Size())
}

// GroupSnapshot 组内成员快照（按积分降序），供界面展示排行。
func (r *Registry) GroupSnapshot(groupID int32) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	g, ok := r.groups[groupID]
	if !ok {
		return nil
	}
	return g.Snapshot()
}

// AddScore 改一个本服玩家的分，返回改后条目与「是否真的变了」。
//
// 唯一的改分入口：cross_player 的数据、组镜像里那一项、脏标记，三样一起动。
// 收敛成一个函数是因为分散写必然漏一处——漏了「组镜像里那一项」，玩家在
// 自己界面的排行里永远停在初始分；漏了脏标记，这次加分就永远同步不出去。
//
// 返回 changed=false 有两种情况，调用方要区分对待：
//   - 分数已在下限，扣不动（不能算失败，攻击本身是成功的）
//   - 玩家不是本服玩家 / 未分组（这是调用错误）
func (r *Registry) AddScore(playerID, delta, now int64) (Entry, bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	e, ok := r.players[playerID]
	if !ok {
		return Entry{}, false, false
	}
	old := e.Score
	e.Score = clampScore(e.Score + delta)
	if e.Score == old {
		// 到下限了，分数没变。不标脏：上报一个没变的值只会让中心服
		// 白跑一次合并，还可能触发一次全组下发（ApplyScore 的 changed 判定）。
		return *e, false, true
	}
	e.UpdatedAt = now
	r.dirty[playerID] = now
	// e 与 groups[gid].entries[pid] 是同一个对象，组镜像里的那一项
	// 跟着一起变了——这就是「共享指针」的全部收益。
	return *e, true, true
}

// SetScore 直接设置分数（结算、GM 修分用），语义同 AddScore。
func (r *Registry) SetScore(playerID, score, now int64) (Entry, bool, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.players[playerID]
	if !ok {
		return Entry{}, false, false
	}
	old := e.Score
	e.Score = clampScore(score)
	if e.Score == old {
		return *e, false, true
	}
	e.UpdatedAt = now
	r.dirty[playerID] = now
	return *e, true, true
}

// ApplySnapshot 用整组快照替换本地镜像（分组完成、10 分钟校准）。
//
// 这是**归属信息的唯一来源**：分组结果只写在中心服，如果不是这条通道，
// 本服玩家永远停在「已报名、未分组」。
//
// 对本服玩家与别服玩家的处理不同，这是本方法的核心：
//
//	别服玩家 → 整条覆盖（分数、战力、时间都以中心服为准）
//	本服玩家 → 只采纳归属（group_id / power）；分数**本地为准**
//
// 为什么本服分数不能被覆盖：本地刚加的分，中心服手上是 60s 前的快照。
// 覆盖回去之后，下一轮上报又把旧值推上去，这一分就永久消失了——而且
// 会自我强化。反过来，本服分数没本地记录时（进程重启过）要用中心服的：
// 那正是「重启后本地旧、中心服新」的场景。
func (r *Registry) ApplySnapshot(groupID int32, members []*Entry, now int64) {
	if groupID == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	g, ok := r.groups[groupID]
	if !ok {
		g = &GroupView{ID: groupID, entries: make(map[int64]*Entry, len(members))}
		r.groups[groupID] = g
	}

	seen := make(map[int64]struct{}, len(members))
	for _, m := range members {
		if m == nil || m.PlayerID == 0 {
			continue
		}
		seen[m.PlayerID] = struct{}{}
		m.GroupID = groupID

		if r.isOwnServer(m) {
			// 本服玩家：归属听中心服的，分数听本服的。
			// 本地没记录时（重启过、或首次分组）直接采纳中心服的分——
			// 那一刻中心服就是唯一真相。
			own, ok := r.players[m.PlayerID]
			if !ok {
				own = &Entry{PlayerID: m.PlayerID, Score: m.Score, UpdatedAt: m.UpdatedAt}
				r.players[m.PlayerID] = own
			}
			own.ServerID = m.ServerID
			own.GroupID = groupID
			own.Power = m.Power
			delete(r.enrolled, m.PlayerID) // 已分组，不再是「待分组」
			g.entries[m.PlayerID] = own
			r.byID[m.PlayerID] = own
			continue
		}
		// 别服玩家：整条覆盖。保留已有对象以维持 map 里的指针稳定，
		// 只拷字段——界面可能正拿着它读。
		if cur, ok := r.byID[m.PlayerID]; ok {
			*cur = *m
			g.entries[m.PlayerID] = cur
			continue
		}
		g.entries[m.PlayerID] = m
		r.byID[m.PlayerID] = m
	}

	// 快照是「这一组的完整名单」：不在名单里的条目要删掉（重新分组、
	// 有人退出）。但**本服玩家**要留着——他可能是刚被分到别组的，
	// 等那一组的快照到达时会重新挂上；现在就删会让玩家查到自己「未分组」。
	for pid := range g.entries {
		if _, ok := seen[pid]; ok {
			continue
		}
		if _, isOwn := r.players[pid]; isOwn {
			continue
		}
		delete(g.entries, pid)
		delete(r.byID, pid)
	}
	g.dirty = true
}

// ApplyDelta 合并一批增量（活动期的常规下发）。
//
// 增量里**不含本服玩家**（中心服组装时就滤掉了），所以这里不需要
// 「跳过本服玩家」的判断。把过滤放在发送端而不是接收端，是因为接收端
// 一旦漏掉一个分支，就会用旧值覆盖本服刚加的分；发送端只有一个地方。
func (r *Registry) ApplyDelta(groupID int32, members []*Entry) {
	if groupID == 0 || len(members) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	g, ok := r.groups[groupID]
	if !ok {
		// 还没收到过快照就来增量：先建组，快照到了会补齐。
		// 不丢弃是因为增量里可能有本组最活跃的那几个人，丢了界面就停住。
		g = &GroupView{ID: groupID, entries: make(map[int64]*Entry, len(members))}
		r.groups[groupID] = g
	}
	for _, m := range members {
		if m == nil || m.PlayerID == 0 {
			continue
		}
		m.GroupID = groupID
		if _, isOwn := r.players[m.PlayerID]; isOwn || (r.serverID != 0 && m.ServerID == r.serverID) {
			continue // 兜底：即便发送端漏了，也不许覆盖本服权威
		}
		if cur, ok := r.byID[m.PlayerID]; ok {
			*cur = *m
			g.entries[m.PlayerID] = cur
			continue
		}
		g.entries[m.PlayerID] = m
		r.byID[m.PlayerID] = m
	}
	g.dirty = true
}

// TakeDirty 取走待上报的玩家快照并清空脏集合。
//
// 「取走 + 回填」而不是「只读不清」：清空之后玩家再变分会重新标脏，
// 所以不会丢；如果不允许清，上报失败一次就会永远卡在同一批人上。
// 上报失败时由 RestoreDirty 把这一批重新标脏。
func (r *Registry) TakeDirty(limit int) []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.dirty) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(r.dirty) {
		limit = len(r.dirty)
	}
	out := make([]Entry, 0, limit)
	for pid := range r.dirty {
		if len(out) >= limit {
			break
		}
		e, ok := r.players[pid]
		if !ok || e.GroupID == 0 {
			// 未分组或已取消：没有 group_id 中心服无法定位，直接丢弃。
			// 这不是丢数据——分组完成后 ApplySnapshot 会用中心服的值重建。
			delete(r.dirty, pid)
			continue
		}
		out = append(out, *e)
		delete(r.dirty, pid)
	}
	return out
}

// RestoreDirty 上报失败时把一批玩家重新标脏。
//
// 用 max 而不是直接写：这批数据是 t0 的快照，如果 t1 又改过分，
// 脏时间已经是 t1，写回 t0 会让「最后变化时刻」倒退（只影响观测，
// 但让人对不上日志）。
func (r *Registry) RestoreDirty(entries []Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range entries {
		cur, ok := r.dirty[e.PlayerID]
		if ok && cur >= e.UpdatedAt {
			continue
		}
		r.dirty[e.PlayerID] = e.UpdatedAt
	}
}

// DirtyCount 待上报人数（指标/测试用）。
func (r *Registry) DirtyCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.dirty)
}

// Len 本服玩家条目数。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.players)
}

// GroupDirty 列出需要落库的组号并清标记。
//
// cross_group 在游戏服是**恢复副本**不是权威（权威在中心服），所以不需要
// 每次变化都写：重启后读它只是为了「活动期重启能立刻显示排行」，晚几分钟
// 落库没有任何业务影响。
func (r *Registry) GroupDirty() []int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int32
	for gid, g := range r.groups {
		if !g.dirty {
			continue
		}
		out = append(out, gid)
		g.dirty = false
	}
	return out
}

// AllGroups 全部组镜像快照（落库用）。
func (r *Registry) AllGroups() map[int32][]Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[int32][]Entry, len(r.groups))
	for gid, g := range r.groups {
		out[gid] = g.Snapshot()
	}
	return out
}

// AllPlayers 全部本服玩家快照（落库用）。
func (r *Registry) AllPlayers() []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Entry, 0, len(r.players))
	for _, e := range r.players {
		out = append(out, *e)
	}
	return out
}

// EnrolledIDs 已报名未分组的玩家（落库用）。
func (r *Registry) EnrolledIDs() []int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]int64, 0, len(r.enrolled))
	for pid := range r.enrolled {
		out = append(out, pid)
	}
	return out
}
