// callback.go —— 中心服下行回调：积分下发、跨服改分通知、发起攻击。
//
// 这些函数在中心服客户端的 recv goroutine 里执行（不是玩家 Actor），
// 所以要自己加锁——Registry 的方法自带锁，本文件不需要再套。
package crossplay

import (
	"errors"
	"fmt"
	"sync"

	internalv2 "game_study/pkg/pb/internalv2"
)

// 玩法错误。
var (
	// ErrNotActive 不在活动期（此时改分不生效）。
	ErrNotActive = errors.New("crossplay: not in active phase")
	// ErrNotParticipant 玩家不在玩法里（没报名/没分组）。
	ErrNotParticipant = errors.New("crossplay: player not participating")
	// ErrSelfTarget 攻击目标是自己（没有意义，且会平白消耗道具）。
	ErrSelfTarget = errors.New("crossplay: cannot attack self")
	// ErrNoServer 目标玩家的归属服未知（分组数据不全）。
	ErrNoServer = errors.New("crossplay: target server unknown")
)

// deltaDedupTTL 改分通知的去重窗口。
//
// relay 是 at-least-once 语义（中心服有重试队列），同一笔改分会重发。
// 窗口取 10 分钟：远超一次重试的实际跨度（中心服最多退避到 6 秒），
// 又不会让内存无限涨——10 分钟内最多几万条。
const deltaDedupTTL = 10 * 60

// OnGroupDelta 处理中心服下发的积分（已按本服裁剪，一条报文含本服涉及的多个组）。
//
// 快照与增量走两条不同的路径，这个区分不能省：
//
//	snapshot=true  → 整组替换：名单对齐中心服，顺带补上本服玩家的分组归属
//	snapshot=false → 合并：只改报文里那几个人的分数
//
// 前者是「本服玩家知道自己在哪组」的唯一来源（分组结果只写在中心服），
// 后者是活动期的常规同步（60s 一次，只含别服玩家）。
func (m *Module) OnGroupDelta(push *internalv2.CrossGroupDeltaPush) {
	if push == nil {
		return
	}
	now := m.nowUnix()
	for _, g := range push.GetGroups() {
		if g == nil || g.GetGroupId() == 0 {
			continue
		}
		members := make([]*Entry, 0, len(g.GetPlayers()))
		for _, p := range g.GetPlayers() {
			if p == nil || p.GetPlayerId() == 0 {
				continue
			}
			members = append(members, &Entry{
				PlayerID:  p.GetPlayerId(),
				GroupID:   g.GetGroupId(),
				ServerID:  p.GetServerId(),
				Score:     p.GetScore(),
				Power:     p.GetPower(),
				UpdatedAt: p.GetUpdatedAt(),
			})
		}
		if len(members) == 0 {
			continue
		}
		if g.GetSnapshot() {
			m.reg.ApplySnapshot(g.GetGroupId(), members, now)
			continue
		}
		m.reg.ApplyDelta(g.GetGroupId(), members)
	}
}

// OnScoreDelta 处理别的服请求本服改某个玩家的分（传的是差值，不是绝对值）。
//
// 铁律：谁的分谁改。A 服手上的 B 服分数是 60s 前的快照，直接覆盖会抹掉
// B 服本地刚产生的增量，所以只能传差值。
//
// 去重必须做：relay 是 at-least-once，同一笔改分会重发，不去重就是重复加分。
// 键是 (from_node, relay_id)——只按 relay_id 不够，两个服可能生成同样的序号。
//
// 这里**不碰 Actor**：积分是玩法数据，玩家离线也照改。这正是「积分不能放
// Ext 槽位」的实际收益——打一个离线玩家不需要把他的背包/任务/装备全量加载。
func (m *Module) OnScoreDelta(d *internalv2.CrossScoreDelta) {
	if d == nil || d.GetPlayerId() == 0 || d.GetDelta() == 0 {
		return
	}
	now := m.nowUnix()
	if !m.claimDelta(d.GetFromNode(), d.GetRelayId(), now) {
		m.logger.Debug("duplicate cross delta ignored",
			"from", d.GetFromNode(), "relay_id", d.GetRelayId(), "player", d.GetPlayerId())
		return
	}

	e, changed, ok := m.reg.AddScore(d.GetPlayerId(), d.GetDelta(), now)
	if !ok {
		// 目标不在本服数据里：可能他压根没报名，或本服数据丢了。
		// 不重试——重试也不会变对，反而会一直占着失败队列。
		m.logger.Warn("cross delta for unknown player",
			"from", d.GetFromNode(), "player", d.GetPlayerId(), "delta", d.GetDelta())
		return
	}
	if !changed {
		// 到下限了，扣不动。上游攻击方仍然 +1，这是已确认的口子。
		m.logger.Debug("cross delta clamped at floor",
			"player", e.PlayerID, "score", e.Score)
	}
}

// Attack 发起一次攻击：本服攻击者 +1、目标 −1。
//
// 调用方（具体玩法）负责前置校验与**扣道具**，本函数只管改分。顺序铁律是
// 「扣道具 → 加分 → 发通知」：反过来会出现「B 已经扣了分，A 的道具没扣成功」。
//
// 分两种情况：
//
//	同服目标 → 直接改，不经过中心服（少一次往返，也不会因为中心服抖动丢分）
//	别服目标 → 发差值通知，由对方服自己改
//
// 返回攻击者改后的分数，方便调用方直接回包。
func (m *Module) Attack(attackerID, targetID int64) (int64, error) {
	now := m.nowUnix()
	if !m.cfg.inActive(now) {
		return 0, ErrNotActive
	}
	if attackerID == targetID {
		return 0, ErrSelfTarget
	}

	target, isOwn := m.reg.Participant(targetID)
	if target.PlayerID == 0 {
		return 0, ErrNotParticipant
	}

	// 先给攻击者加分：这一步是本服权威，不会失败（除非他没参赛）。
	attacker, changed, ok := m.reg.AddScore(attackerID, 1, now)
	if !ok {
		return 0, ErrNotParticipant
	}
	if !changed {
		// 攻击者已经在上限之外（本玩法没有上限，理论上到不了）。
		// 走到这里说明分数被 clamp 了，仍然继续——攻击本身是成功的。
		m.logger.Debug("attacker score unchanged", "player", attackerID, "score", attacker.Score)
	}

	if isOwn {
		// 同服：直接扣，两个人都走本地权威
		if _, _, ok := m.reg.AddScore(targetID, -1, now); !ok {
			return attacker.Score, ErrNotParticipant
		}
		return attacker.Score, nil
	}

	// 别服：发差值。失败时攻击者的分已经加了、目标没扣，账不平——但这个
	// 偏差只有 1 分，且下一轮上报会把攻击者的新分同步给全组。为它回滚
	// 攻击者的分更糟：玩家已经消耗了道具。
	if target.ServerID == 0 {
		return attacker.Score, ErrNoServer
	}
	if m.center == nil || !m.center.Ready() {
		return attacker.Score, ErrNoServer
	}
	if err := m.center.SendCrossDelta(&internalv2.CrossScoreDelta{
		RelayId:    m.nextDeltaID(),
		PlayerId:   targetID,
		Delta:      -1,
		GroupId:    target.GroupID,
		FromPlayer: attackerID,
		ToServer:   target.ServerID, // 游戏服只给业务标识，路由由中心服反查
	}); err != nil {
		m.logger.Warn("send cross delta failed",
			"target", targetID, "server", target.ServerID, "err", err)
	}
	return attacker.Score, nil
}

// nextDeltaID 生成稳定的去重键。
//
// 格式 sd:{node}:{seq}：node 区分来源，seq 单调。重启后从 0 开始也没问题，
// 因为去重是 (来源, 序号) 二元组，重启后那条时间线是新的。
func (m *Module) nextDeltaID() string {
	return fmt.Sprintf("sd:%s:%d", m.nodeID, m.seq.Add(1))
}

// deltaDedup 改分通知的去重集合。
//
// 用 map + 定期清扫而不是 LRU：命中率极高（99% 是首次），不值得为它维护
// 链表；过期条目按批清掉，摊还成本可以忽略。
type deltaDedup struct {
	mu   sync.Mutex
	seen map[string]int64
}

// claim 标记一个键；返回 true 表示首次（可以继续处理）。
func (d *deltaDedup) claim(key string, now int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.seen == nil {
		d.seen = make(map[string]int64, 256)
	}
	if exp, ok := d.seen[key]; ok && now < exp {
		return false
	}
	d.seen[key] = now + deltaDedupTTL

	// 清扫：超过 8192 条才扫，避免每条消息都遍历一遍 map。
	if len(d.seen) > 8192 {
		for k, exp := range d.seen {
			if now >= exp {
				delete(d.seen, k)
			}
		}
	}
	return true
}

// claimDelta 组合键并去重。
func (m *Module) claimDelta(fromNode, relayID string, now int64) bool {
	if relayID == "" {
		// 没有幂等键就没法去重。放行比丢弃好：丢一条改分是实打实的
		// 少扣 1 分，重复扣分有下限兜着（不会变负），后果更轻。
		return true
	}
	return m.dedup.claim(fromNode+"|"+relayID, now)
}
