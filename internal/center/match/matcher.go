// Package match 跨服匹配池（设计文档 9.3 / 10.3）
//
// 职责：按 (group, mode, elo) 收集匹配请求，凑够人后分配 room_id + battle_token，
// 并通过 MatchResult 下发给各发起方游戏服。
//
// 设计取舍：
//   - 匹配池纯内存（中心服不持久化玩家业务数据，见 9.1）；中心服重启后队列清空，
//     客户端重发 MatchReq 重建即可；
//   - elo 分桶：先同桶内配，超时（MatchTimeout）后放宽到相邻桶，避免高段位永远配不上；
//   - room_id 用「中心服实例号 << 40 | 秒级时间戳 << 8 | 自增」拼装，保证跨实例唯一。
//
// 协作关系：matcher 只被 center 的 Server 调用（Enqueue/Sweep/SetCrossAddrs），
// 自身不知道 gRPC/Redis 的存在——匹配结果由 Server 经 Report 流下发，
// 可用跨服地址也由 Server 从 GroupUpdate 同步过来；纯内存结构便于单测。
package match

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// DefaultMode 默认玩法。
const DefaultMode = "arena"

// DefaultPlayers 单场人数（demo 1v1）。
const DefaultPlayers = 2

// DefaultTimeout 匹配超时后放宽 elo 桶。
const DefaultTimeout = 10 * time.Second

// EloBucket elo 分桶宽度（200 分一档）。
// 宽度是「精度 × 池子浓度」的折中：桶内分差小到体验可接受，
// 桶内人数又多到能凑齐一局；过细则长期凑不齐，过粗则水平悬殊。
const EloBucket = 200

// Ticket 一个等待中的匹配请求。
//
// NodeID 是结果回传目标（发起方游戏服）；Snapshot 是对手快照的数据源，
// 成局时塞进 MatchResult 让双方入房前互见；Enqueued 是放宽匹配的时间基准，
// 入队时统一补齐（调用方漏填时用 time.Now，避免零值导致永远「超时」）。
type Ticket struct {
	NodeID   string
	PlayerID int64
	Mode     string
	Elo      int32
	Snapshot []byte
	Enqueued time.Time
}

// Result 匹配成功的结果（一位玩家一条）。
//
// 一名玩家一条而不是一局一条：同局玩家可能分布在不同游戏服上，
// 每条只需带「我」的 token 与「对手」信息，接收方按 PlayerId 对号入座。
type Result struct {
	RoomID    uint64
	Mode      string
	CrossAddr string
	Players   []ResultPlayer
}

// ResultPlayer 单个参战者的结果。
// PeerID/PeerSnap 是对手信息（1v1 语义）：入房前双方互见初始状态；
// 多人玩法需把这两个字段扩展为列表。
type ResultPlayer struct {
	NodeID   string
	PlayerID int64
	Token    string
	PeerID   int64
	PeerSnap []byte
}

// Matcher 匹配器。
//
// 并发模型：一把 mu 保护全部队列——入队/取消/扫描都是纯内存操作、
// 临界区极短，粗粒度换来的收益是「配对逻辑无需考虑并发交错」的简单正确性。
// 读多写少的场景（Stats/查询）也没有拆 RWMutex：一次匹配就几十微秒，
// 锁竞争可忽略。instanceID/crossAddrs/tokenKey/timeout 初始化后只读
// （crossAddrs 例外，由 SetCrossAddrs 在锁内替换）。
type Matcher struct {
	instanceID uint32
	crossAddrs []string

	mu       sync.Mutex
	queues   map[string][]*Ticket // key = group|mode|bucket
	seq      uint64
	tokenKey []byte

	timeout time.Duration
}

// New 创建匹配器。
//   - instanceID：中心服实例序号（0~255），用于拼装全局唯一 room_id；
//   - crossAddrs：可分配的跨服节点地址（组内）。
//
// instanceID 掩到 8 位（& 0xFF）：room_id 只给它留了 8 bit，
// 超范围的位会污染时间戳段。tokenKey 取 crypto/rand；读取失败时退回
// 时间派生——令牌防伪造强度稍降，但好过启动失败（房间纯内存，重启即失效）。
func New(instanceID uint32, crossAddrs []string, timeout time.Duration) *Matcher {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		binary.BigEndian.PutUint64(key, uint64(time.Now().UnixNano()))
	}
	return &Matcher{
		instanceID: instanceID & 0xFF,
		crossAddrs: crossAddrs,
		queues:     make(map[string][]*Ticket),
		tokenKey:   key,
		timeout:    timeout,
	}
}

// SetCrossAddrs 更新可用跨服节点（GroupUpdate 后调用）。
// 拷贝一份再存（append([]string(nil), ...)）：调用方可能复用传入切片，
// 直接持有会被外部后续改动带偏。
func (m *Matcher) SetCrossAddrs(addrs []string) {
	m.mu.Lock()
	m.crossAddrs = append([]string(nil), addrs...)
	m.mu.Unlock()
}

// Enqueue 入队并尝试匹配。返回本次触发的结果（可能为空）。
//
// 调用方（center Server）拿到结果后逐条下发；返回空表示「已入队，等 Sweep」。
// 入队即尝试同桶配对：两人先后到达时第二次入队立即成局，不必等定时器；
// 入队路径也做一次超时放宽：等待超时的老 ticket 可能一直躺在队里没人叫醒。
func (m *Matcher) Enqueue(t *Ticket) []*Result {
	if t == nil || t.PlayerID == 0 {
		return nil
	}
	if t.Mode == "" {
		t.Mode = DefaultMode
	}
	if t.Enqueued.IsZero() {
		t.Enqueued = time.Now()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 去重：同一玩家同一模式只保留一个 ticket
	// 线性扫队列而不是再维护一张索引 map：单桶凑够人就清空，
	// 额外表的建立/清理成本大于收益
	key := m.key(t.Mode, t.Elo)
	for _, old := range m.queues[key] {
		if old.PlayerID == t.PlayerID && old.NodeID == t.NodeID {
			return nil
		}
	}

	m.queues[key] = append(m.queues[key], t)

	// 同桶配
	if res := m.tryPairLocked(key); res != nil {
		return res
	}
	// 放宽到相邻桶（等待超过 timeout 的老 ticket）
	// 仅当本 ticket 自己已等超 timeout：刚入队的新 ticket 无权把别人
	// 拉出同桶，否则新人每次都「降维」匹配老队列，公平性失衡
	if time.Since(t.Enqueued) > m.timeout {
		return m.relaxLocked(t)
	}
	return nil
}

// Cancel 取消匹配（玩家下线/主动取消）。
//
// 遍历所有桶而不是按 key 直达：调用方不一定记得玩家的 elo（下线时
// 通常只有 node+player），全桶扫描代价可接受（总队列 = 等待人数，很小）。
// q[:0] 原地过滤复用底层数组，避免每次取消都分配新切片。
func (m *Matcher) Cancel(nodeID string, playerID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	removed := false
	for k, q := range m.queues {
		out := q[:0]
		for _, t := range q {
			if t.PlayerID == playerID && t.NodeID == nodeID {
				removed = true
				continue
			}
			out = append(out, t)
		}
		if len(out) == 0 {
			delete(m.queues, k)
		} else {
			m.queues[k] = out
		}
	}
	return removed
}

// Sweep 超时放宽：把等待过久的 ticket 与相邻桶配对（由定时器周期调用）。
//
// 先快照 key 列表再遍历：遍历过程中会删除/修改 map（配对成功后出队），
// Go 允许边遍历边删，但新增键的可见性未定义，快照是必要防御。
// 两类分支：桶内够人 → 连续配对到凑不齐；桶内不足 → 逐个尝试跨桶放宽。
func (m *Matcher) Sweep() []*Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []*Result
	now := time.Now()
	keys := make([]string, 0, len(m.queues))
	for k := range m.queues {
		keys = append(keys, k)
	}
	for _, k := range keys {
		q := m.queues[k]
		if len(q) < DefaultPlayers {
			// 单人等超时：尝试跨桶
			for _, t := range q {
				if now.Sub(t.Enqueued) > m.timeout {
					if res := m.relaxLocked(t); res != nil {
						out = append(out, res...)
					}
				}
			}
			continue
		}
		for len(m.queues[k]) >= DefaultPlayers {
			// 循环条件每次重新读 map：tryPairLocked 在分配失败时会把 pair
			// 放回队首，此时人数仍够但一直配不上——res == nil 时 break，
			// 防止同一批人反复配对烧 CPU
			res := m.tryPairLocked(k)
			if res == nil {
				break
			}
			out = append(out, res...)
		}
	}
	return out
}

// Stats 队列统计（指标/调试用）。
// waiting = 各桶人数总和（同一玩家只在一个桶）；buckets = 非空桶数——
// 桶多而人少说明池子被 elo 切碎，是放宽策略需要调参的信号。
func (m *Matcher) Stats() (waiting int, buckets int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, q := range m.queues {
		waiting += len(q)
	}
	return waiting, len(m.queues)
}

// tryPairLocked 同桶内两两配对（调用方持锁）。
//
// 取队首 DefaultPlayers 人而不是「分差最小」的两人：同桶内水平已接近，
// FIFO 还附带先到先服务的公平性，避免 O(n²) 反复比较。
// 先出队再 buildResult：失败才原样放回队首——顺序不能反，
// 否则 buildResult 期间队列视图与返回值不一致。
func (m *Matcher) tryPairLocked(key string) []*Result {
	q := m.queues[key]
	if len(q) < DefaultPlayers {
		return nil
	}
	pair := append([]*Ticket(nil), q[:DefaultPlayers]...)
	rest := q[DefaultPlayers:]
	if len(rest) == 0 {
		delete(m.queues, key)
	} else {
		m.queues[key] = rest
	}
	if res := m.buildResult(pair); res != nil {
		return res
	}
	// 分配失败（无跨服节点）：把两人放回队首，等下一轮
	// 不丢弃：跨服地址可能只是暂时没同步到，丢了要客户端重新排队；
	// 放队首保证下一轮优先再试
	m.queues[key] = append(pair, m.queues[key]...)
	return nil
}

// relaxLocked 跨桶放宽匹配：当前 ticket 与任意相邻桶的等待者配对（调用方持锁）。
//
// 候选条件：同 mode + 不是自己 + 也已等超 timeout——放宽是双向的，
// 不能把没超时的人强行配走。评分取「等待最久」而非「elo 最近」：
// 等待越久越急需解救，elo 差一点对体验的影响小于继续排队。
func (m *Matcher) relaxLocked(t *Ticket) []*Result {
	now := time.Now()
	var bestKey string
	var best *Ticket
	bestGap := m.timeout
	// 初值设成 timeout：只有等待超过 timeout 的候选才进入比较，
	// 未超时的直接排除在门外

	// 找等待最久且 elo 差距最小的候选
	for k, q := range m.queues {
		if len(q) == 0 {
			continue
		}
		for _, cand := range q {
			if cand.PlayerID == t.PlayerID && cand.NodeID == t.NodeID {
				continue
			}
			if cand.Mode != t.Mode {
				continue
			}
			gap := now.Sub(cand.Enqueued)
			if gap >= m.timeout && (best == nil || gap > bestGap) {
				best = cand
				bestKey = k
				bestGap = gap
			}
		}
	}
	if best == nil {
		return nil
	}

	// 从原队列移除双方
	// 双方可能来自不同桶，各按自己的 key 移除；顺序无关
	//（removeFromQueue 内部线性扫，先删谁都不影响结果）
	m.removeFromQueue(m.key(t.Mode, t.Elo), t)
	m.removeFromQueue(bestKey, best)

	return m.buildResult([]*Ticket{t, best})
}

// buildResult 生成匹配结果（分配 room_id 与令牌）。
//
// 跨服节点按 seq 轮询取模：把房间均匀摊到各跨服实例，避免全压在一台上。
// room_id 三段式拼装（见下）；令牌按 (room_id, player_id) HMAC 签名，
// 跨服节点用同一输入格式验签——密钥进程内随机，令牌只在本次匹配有效。
func (m *Matcher) buildResult(players []*Ticket) []*Result {
	if len(m.crossAddrs) == 0 {
		// 没有可用跨服节点：match 只负责「配人」，房间必须有所在节点，
		// 返回 nil 让调用方保留 ticket 下轮再试（tryPairLocked 会放回队首）
		return nil
	}
	// 轮询选跨服节点
	m.seq++
	crossAddr := m.crossAddrs[int(m.seq)%len(m.crossAddrs)]

	// room_id = 实例号(8b) << 40 | 秒级时间戳(32b) << 8 | 序号(8b)
	// 三段各自掩码互不污染：实例号占高位防跨实例撞号；秒级时间戳防同实例
	// 重启后复用旧号（客户端可能缓存旧 room_id 来上报）；低 8 位自增防同秒多局
	roomID := uint64(m.instanceID)<<40 |
		uint64(time.Now().Unix()&0xFFFFFFFF)<<8 |
		uint64(m.seq&0xFF)
	if roomID == 0 {
		roomID = 1
	}

	mode := players[0].Mode
	if mode == "" {
		mode = DefaultMode
	}

	out := make([]*Result, 0, len(players))
	for _, p := range players {
		var peerID int64
		var peerSnap []byte
		for _, q := range players {
			if q.PlayerID != p.PlayerID {
				peerID = q.PlayerID
				peerSnap = q.Snapshot
				break
			}
		}
		out = append(out, &Result{
			RoomID:    roomID,
			Mode:      mode,
			CrossAddr: crossAddr,
			Players: []ResultPlayer{{
				NodeID:   p.NodeID,
				PlayerID: p.PlayerID,
				Token:    m.signToken(roomID, p.PlayerID),
				PeerID:   peerID,
				PeerSnap: peerSnap,
			}},
		})
	}
	return out
}

// signToken 签发准入令牌（跨服节点校验时用同一套 HMAC 输入格式）。
// 截断到 16 字节 hex：令牌只需不可伪造，不需要全强度摘要；
// 输入格式 "center|roomID|playerID" 与跨服侧约定一致——改格式等于
// 两侧同时失效（旧令牌全部验不过），属于破坏性变更。
func (m *Matcher) signToken(roomID uint64, playerID int64) string {
	h := hmac.New(sha256.New, m.tokenKey)
	fmt.Fprintf(h, "center|%d|%d", roomID, playerID)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// key 生成队列键。
// bucket 整数除法向下取整：负 elo 归 0 桶而不是负桶——
// 扣分掉到 0 以下的玩家不该独占一个永远凑不齐的桶。
func (m *Matcher) key(mode string, elo int32) string {
	bucket := elo / EloBucket
	if elo < 0 {
		bucket = 0
	}
	return fmt.Sprintf("%s#%d", mode, bucket)
}

// removeFromQueue 从指定队列移除 ticket。
// q[:0] 原地过滤复用底层数组。删空键是必须的：Sweep 与 relaxLocked
// 依赖「桶不存在」与「桶为空」等价来减少遍历量，残留空桶会让
// 跨桶放宽扫到无意义条目、也让 Stats 的 buckets 虚高。
func (m *Matcher) removeFromQueue(key string, t *Ticket) {
	q := m.queues[key]
	out := q[:0]
	for _, x := range q {
		if x.PlayerID == t.PlayerID && x.NodeID == t.NodeID {
			continue
		}
		out = append(out, x)
	}
	if len(out) == 0 {
		delete(m.queues, key)
		return
	}
	m.queues[key] = out
}
