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
const EloBucket = 200

// Ticket 一个等待中的匹配请求。
type Ticket struct {
	NodeID   string
	PlayerID int64
	Mode     string
	Elo      int32
	Snapshot []byte
	Enqueued time.Time
}

// Result 匹配成功的结果（每位玩家一条）。
type Result struct {
	RoomID     uint64
	Mode       string
	CrossAddr  string
	Players    []ResultPlayer
}

// ResultPlayer 单个参战者的结果。
type ResultPlayer struct {
	NodeID   string
	PlayerID int64
	Token    string
	PeerID   int64
	PeerSnap []byte
}

// Matcher 匹配器。
type Matcher struct {
	instanceID uint32
	crossAddrs []string

	mu      sync.Mutex
	queues  map[string][]*Ticket // key = group|mode|bucket
	seq     uint64
	tokenKey []byte

	timeout time.Duration
}

// New 创建匹配器。
//   - instanceID：中心服实例序号（0~255），用于拼装全局唯一 room_id；
//   - crossAddrs：可分配的跨服节点地址（组内）。
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
func (m *Matcher) SetCrossAddrs(addrs []string) {
	m.mu.Lock()
	m.crossAddrs = append([]string(nil), addrs...)
	m.mu.Unlock()
}

// Enqueue 入队并尝试匹配。返回本次触发的结果（可能为空）。
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
	if time.Since(t.Enqueued) > m.timeout {
		return m.relaxLocked(t)
	}
	return nil
}

// Cancel 取消匹配（玩家下线/主动取消）。
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
func (m *Matcher) Stats() (waiting int, buckets int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, q := range m.queues {
		waiting += len(q)
	}
	return waiting, len(m.queues)
}

// tryPairLocked 同桶内两两配对（调用方持锁）。
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
	m.queues[key] = append(pair, m.queues[key]...)
	return nil
}

// relaxLocked 跨桶放宽匹配：当前 ticket 与任意相邻桶的等待者配对（调用方持锁）。
func (m *Matcher) relaxLocked(t *Ticket) []*Result {
	now := time.Now()
	var bestKey string
	var best *Ticket
	bestGap := m.timeout

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
	m.removeFromQueue(m.key(t.Mode, t.Elo), t)
	m.removeFromQueue(bestKey, best)

	return m.buildResult([]*Ticket{t, best})
}

// buildResult 生成匹配结果（分配 room_id 与令牌）。
func (m *Matcher) buildResult(players []*Ticket) []*Result {
	if len(m.crossAddrs) == 0 {
		return nil
	}
	// 轮询选跨服节点
	m.seq++
	crossAddr := m.crossAddrs[int(m.seq)%len(m.crossAddrs)]

	// room_id = 实例号(8b) << 40 | 秒级时间戳(32b) << 8 | 序号(8b)
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
func (m *Matcher) signToken(roomID uint64, playerID int64) string {
	h := hmac.New(sha256.New, m.tokenKey)
	fmt.Fprintf(h, "center|%d|%d", roomID, playerID)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// key 生成队列键。
func (m *Matcher) key(mode string, elo int32) string {
	bucket := elo / EloBucket
	if elo < 0 {
		bucket = 0
	}
	return fmt.Sprintf("%s#%d", mode, bucket)
}

// removeFromQueue 从指定队列移除 ticket。
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
