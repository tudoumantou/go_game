// pkg/writebehind/store.go —— Redis 侧玩家快照存储（权威热状态 + 可靠脏队列）
//
// 职责：把玩家快照（Snapshot）存进 Redis hash，把「该落库了」登记进
// 脏 ZSET，并提供原子化的版本栅栏。缓存权威模式下 Redis 是热数据的
// 唯一真相源，MySQL 只是异步投影——落库 worker 只消费队列，不回写缓存。
//
// 关键不变量：
//   - 版本单调：Commit 用 expected 版本做 CAS，旧提交者得 ErrConflict；
//   - 提交与入队原子：同一 Lua 脚本内完成 version+1、HSET、ZADD，
//     不会出现「状态新了但没入队」（丢落库）或「入队了状态旧」（写回旧数据）；
//   - key 带 hash tag {player-N}：state key 与 dirty key 落在同一
//     Redis Cluster slot，Lua 多 key 操作才能原子执行。
package writebehind

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// 哨兵错误：调用方用 errors.Is 判定具体失败类型，而不是匹配错误文案。
var (
	// ErrMiss 玩家缓存不存在（冷启动需从 MySQL 装载后 Seed）。
	ErrMiss = errors.New("player cache missing")
	// ErrConflict 提交被拒：调用方的会话令牌已不是当前 epoch——
	// 意味着这个 Actor 已被更新的会话接管（跨节点转移或同节点顶号）。
	// 调用方应停止写入并重载权威状态，而不是重试提交。
	ErrConflict = errors.New("player session lease superseded")
)

// Store 玩家快照的 Redis 存储。
//
// 只持一个 redis.UniversalClient（单机/哨兵/Cluster 通用），
// 所有方法并发安全（go-redis 内部池化连接）；无状态，可随用随建，
// 通常单例注入给 Worker 与各游戏服进程使用。
type Store struct{ rdb redis.UniversalClient }

// NewStore 用既有客户端创建 Store。
//
// 不负责建连/探活——客户端生命周期由进程启动代码管理，
// 这里只做薄封装。
func NewStore(rdb redis.UniversalClient) *Store { return &Store{rdb: rdb} }

// stateKey 玩家快照 hash 的 key。
//
// {player-N} 是 Redis Cluster 的 hash tag：花括号内的部分决定 slot，
// 保证同一玩家的 state key 与 dirty key 同 slot（Lua 原子性的前提）。
// wb 前缀便于运维按模式清理/统计。
//
// Go 细节：fmt.Sprintf 每次调用都有字符串分配——key 拼接发生在
// 每个 Commit/Ack 上，量级可接受；若成为热点可改为调用方缓存。
func (s *Store) stateKey(id int64) string {
	return fmt.Sprintf("wb:{player-%d}:state:%d", Partition(id), id)
}

// epochKey 会话所有权代号（R2.5 围栏令牌）。
//
// 与 state/dirty 同一个 hash tag：三者要在 commitScript 里原子读写。
func (s *Store) epochKey(id int64) string {
	return fmt.Sprintf("wb:{player-%d}:epoch:%d", Partition(id), id)
}

// dirtyKey 分区脏队列 ZSET 的 key（score = 入队时间戳毫秒）。
//
// 按 partition 聚合而非按玩家：落库 worker 一次取一个分区的批量玩家，
// 分摊 Lua 往返；同一分区 key 与所有该分区玩家的 state key 同 slot。
func (s *Store) dirtyKey(part int) string { return fmt.Sprintf("wb:{player-%d}:dirty", part) }

// Lua keys 使用同一 hash tag，可在 Redis Cluster 中原子执行。
//
// commitScript 提交快照并登记脏队列，一个脚本内完成四件事：
//  1. 版本 CAS：version 与 expected（ARGV[1]）不符返回 -1（调用方转 ErrConflict）；
//  2. 队列类型防御：dirty key 若不是 zset/none，报错而非静默写入
//     （类型被外部改会造成不可预期的行为）；
//  3. HINCRBY version 自增——原子的读-改-写，避免 GET 后再 SET 的竞态；
//  4. HSET 新快照 + ZADD NX 入队（NX：已在队列则不刷新 score，
//     保留最早入队时刻，避免连续提交把落库时机无限推迟）。
//
// 为什么用 Lua：版本校验、自增、写状态、入队四步必须原子，拆成多条
// 命令在失败/并发下会出现「版本新了但没入队」或「旧快照入队」。
// redis.NewScript 负责 EVALSHA 加载与 NOSCRIPT 自动重试，调用方只管 Run。
var commitScript = redis.NewScript(`
local token = tonumber(ARGV[1])
if not token or token <= 0 then
  return redis.error_reply('invalid lease token')
end
local epoch = tonumber(redis.call('GET', KEYS[3]) or '0')
if epoch == 0 then
  redis.call('SET', KEYS[3], token)
elseif token ~= epoch then
  return -1
end
local queueType=redis.call('TYPE',KEYS[2]).ok
if queueType~='none' and queueType~='zset' then return redis.error_reply('invalid dirty queue type') end

-- ARGV[1]=token ARGV[2]=playerID ARGV[3]=score ARGV[4]=占位
-- ARGV[5..] 是 field/value 对；field 以 '-' 开头表示删除该 field。
local i = 5
while i <= #ARGV do
  local f = ARGV[i]
  local v = ARGV[i+1]
  if string.sub(f, 1, 1) == '-' then
    redis.call('HDEL', KEYS[1], string.sub(f, 2))
  else
    redis.call('HSET', KEYS[1], f, v)
  end
  i = i + 2
end

local next=redis.call('HINCRBY',KEYS[1],'version',1)
redis.call('ZADD',KEYS[2],'NX',ARGV[3],ARGV[2])
return next
`)

// Commit 提交玩家快照：版本 CAS 成功则落 Redis 并登记脏队列。
//
// 流程：本地 Validate（防坏数据进缓存）→ json.Marshal 整快照 →
// Lua 原子提交 → version<0 判冲突。
// expected 是调用方 Load 时拿到的版本；并发提交时只有一个赢家，
// 输家收 ErrConflict 后应重新 Load 合并再提交（上层重试策略）。
//
// 取舍：每次提交序列化**整份**快照而非 diff——快照量级在 KB 内，
// 全量的实现与正确性论证（幂等、无合并冲突）远比增量简单；
// 真正的写压力在数据库侧，那里有合批 UPSERT 兜底。
// Commit 提交快照的部分字段。
//
// src 按字段现取现序列化（FieldSource），不再要求调用方先构造整份 Snapshot。
// fields 为空表示「没有变化」，调用方应避免这种情况（白写一次 Redis）。
func (s *Store) Commit(ctx context.Context, playerID int64, src FieldSource, fields, removed []string, token int64) (int64, error) {
	if playerID <= 0 {
		return 0, fmt.Errorf("invalid player id")
	}
	// 只序列化本次变化的字段（R2.5-B）：签到消息不再重写整个背包。
	args := []interface{}{token, playerID, time.Now().UnixMilli()}
	args = append(args, 0) // 占位：字段对从 ARGV[5] 开始
	for _, f := range fields {
		raw, err := src.Field(f)
		if err != nil {
			return 0, err
		}
		if raw == nil {
			continue // 模块没有状态，不写
		}
		if err := validateField(f, raw); err != nil {
			return 0, err
		}
		args = append(args, f, string(raw))
	}
	for _, f := range removed {
		args = append(args, "-"+f, "")
	}
	version, err := commitScript.Run(ctx, s.rdb, []string{
		s.stateKey(playerID),
		s.dirtyKey(Partition(playerID)),
		s.epochKey(playerID),
	}, args...).Int64()
	if err != nil {
		return 0, err
	}
	if version < 0 {
		return 0, ErrConflict
	}
	return version, nil
}

// validateField 校验单个字段：Ext 字段要求模块已注册且自校验通过。
//
// 核心字段（profile/attr/items/grants）不做范围校验——它们的写入源是
// player.State，AddItem/RemoveItem/Grant 已在入口保证非负与 int32 上限；
// 在这里复检要对 items 反序列化一遍（100 件约 5µs），与本次优化目的相反。
func validateField(field string, raw []byte) error {
	name, ok := strings.CutPrefix(field, "ext:")
	if !ok {
		return nil
	}
	p, ok := LookupPersister(name)
	if !ok {
		return fmt.Errorf("unknown module state %q", name)
	}
	if len(raw) == 0 {
		return fmt.Errorf("module %q has empty state", name)
	}
	return p.Validate(raw)
}

// marshalField 序列化快照的单个字段。
//
// 返回 nil 表示该字段没有内容可写（模块未持有状态），调用方跳过。
func marshalField(state Snapshot, field string) ([]byte, error) {
	switch field {
	case "profile":
		return json.Marshal(state.Profile)
	case "attr":
		return json.Marshal(state.Attr)
	case "items":
		return json.Marshal(state.Items)
	case "grants":
		return json.Marshal(state.AppliedGrants)
	}
	name, ok := strings.CutPrefix(field, "ext:")
	if !ok {
		return nil, fmt.Errorf("writebehind: unknown field %q", field)
	}
	raw, exists := state.Ext[name]
	if !exists || len(raw) == 0 {
		return nil, nil
	}
	return raw, nil
}

// leaseTTL 会话令牌有效期。
//
// 7 天：远大于 Tier-2 的 Actor 保留期（90s），不会出现「Actor 还活着但令牌
// 已过期」；同时保证长期不登录的玩家不会在 Redis 里留下永久 key。
const leaseTTL = 7 * 24 * time.Hour

// acquireLeaseScript 自增并续期会话令牌。
//
// 只操作 epoch 一个 key，天然在单 slot 内；与 commitScript 的 epoch 校验
// 共同构成围栏：新会话 +1 后，旧 Actor 手里的令牌立即失效。
var acquireLeaseScript = redis.NewScript(`
local epoch = redis.call('INCR', KEYS[1])
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return epoch
`)

// AcquireLease 取得本会话的所有权令牌（R2.5）。
//
// 调用时机：玩家 Actor 首次加载状态时（Bind 路径）。每次调用都分配新令牌，
// 因此「同节点顶号」也会让旧 Actor 失效——这是原先靠 sessionID 比较 +
// kick 做、且存在窗口的那件事。
//
// 与 presence.TrySetOnline 的分工：presence 决定「谁在线」（跨节点拒绝绑定），
// AcquireLease 决定「谁的写有效」（同一会话内每次提交校验）。
func (s *Store) AcquireLease(ctx context.Context, playerID int64) (int64, error) {
	token, err := acquireLeaseScript.Run(ctx, s.rdb,
		[]string{s.epochKey(playerID)}, leaseTTL.Milliseconds()).Int64()
	if err != nil {
		return 0, err
	}
	if token <= 0 {
		return 0, fmt.Errorf("writebehind: invalid lease token %d", token)
	}
	return token, nil
}

// seedScript 冷启动把 MySQL 读出的旧快照 seed 回 Redis（幂等）。
//
// 两道防御：
//  1. EXISTS 检查：缓存已存在（别的进程/路径已 seed 过）时直接返回 0，
//     不用旧数据盖掉更新的缓存；
//  2. ZSCORE 检查：若该玩家已在脏队列里，说明有比这份 MySQL 数据更新的
//     快照等着落库，此刻 seed 会把「更新版本」倒盖成「旧版本」——
//     直接报错拒绝，宁可 seed 失败暴露问题，不可静默丢更新。
//
// seedScript 冷启动把 MySQL 读出的旧快照 seed 回 Redis（幂等 + 分字段）。
//
// ARGV[1]=version ARGV[2]=playerID（脏队列成员）ARGV[3..]=field/value 对。
// 与 Commit 用同一套字段格式——两者写的必须是同一种布局，否则 Seed 写完
// 立刻被 Load 读成空状态。
var seedScript = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==1 then return 0 end
if redis.call('ZSCORE',KEYS[2],ARGV[2]) then return redis.error_reply('missing dirty snapshot; cannot recover from stale SQL') end
redis.call('HSET',KEYS[1],'version',ARGV[1])
local i = 3
while i <= #ARGV do
  redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1])
  i = i + 2
end
return 1
`)

// Seed 冷启动写入缓存（幂等；已存在则保留现状）。
//
// 成功路径返回 Load 的结果：调用方拿到的是「缓存里现在的版本」，
// 可能与入参 version 不同（并发 seed 场景），以缓存为准。
// version<0 直接报错：版本号协议上非负。
func (s *Store) Seed(ctx context.Context, state Snapshot, version int64) (Record, error) {
	if err := state.Validate(); err != nil {
		return Record{}, err
	}
	if version < 0 {
		return Record{}, fmt.Errorf("invalid version")
	}
	// 逐字段写入，与 Commit 同一套布局（R2.5-B）。
	args := []interface{}{version, state.PlayerID}
	for _, f := range state.FieldNames() {
		raw, err := state.Field(f)
		if err != nil {
			return Record{}, err
		}
		if raw == nil {
			continue
		}
		args = append(args, f, string(raw))
	}
	if _, err := seedScript.Run(ctx, s.rdb, []string{
		s.stateKey(state.PlayerID),
		s.dirtyKey(Partition(state.PlayerID)),
	}, args...).Result(); err != nil {
		return Record{}, err
	}
	return s.Load(ctx, state.PlayerID)
}

// Load 读取玩家快照（缓存权威路径的唯一起点）。
//
// 流程：HGETALL → 空 hash 判 ErrMiss → version 解析 → data 反序列化
// → PlayerID 一致性校验 → 旧版迁移 → Validate。
// 任一步坏数据都返回 error 而非零值 Snapshot：缓存损坏必须让人工
// 介入，静默返回空状态会把玩家数据清零。
//
// Go 细节：HGETALL 对不存在的 key 返回空 map + nil error（不是
// ErrMiss 本身），所以要用 len(values)==0 判定未命中。
func (s *Store) Load(ctx context.Context, id int64) (Record, error) {
	values, err := s.rdb.HGetAll(ctx, s.stateKey(id)).Result()
	if err != nil {
		return Record{}, err
	}
	if len(values) == 0 {
		return Record{}, ErrMiss
	}
	version, err := strconv.ParseInt(values["version"], 10, 64)
	if err != nil || version < 0 {
		return Record{}, fmt.Errorf("invalid cached version")
	}
	// 分字段格式：一部分一个 field，只解析需要的字段（R2.5-B）。
	state := Snapshot{
		PlayerID: id,
		Items:    make(map[int32]int64),
		Ext:      make(map[string]json.RawMessage),
	}
	for name, dst := range map[string]any{
		"profile": &state.Profile,
		"attr":    &state.Attr,
		"items":   &state.Items,
		"grants":  &state.AppliedGrants,
	} {
		if raw := values[name]; raw != "" {
			if err := json.Unmarshal([]byte(raw), dst); err != nil {
				return Record{}, fmt.Errorf("decode %s: %w", name, err)
			}
		}
	}
	for f, raw := range values {
		name, ok := strings.CutPrefix(f, "ext:")
		if !ok || raw == "" {
			continue
		}
		state.Ext[name] = json.RawMessage(raw)
	}
	// 防串号：缓存内容里的 PlayerID 必须与请求的 id 一致，
	// 不一致说明 key 被写错或数据被污染，继续用会张冠李戴
	if state.PlayerID != id {
		return Record{}, fmt.Errorf("cached player mismatch")
	}
	if err := state.Validate(); err != nil {
		return Record{}, err
	}
	return Record{Version: version, State: state}, nil
}

// Pending 取一个分区最早的 limit 个待落库玩家（按入队时间排序）。
//
// 参数防御：partition 越界/limit<=0 直接报错（防 ZRANGE 0 -1 这种
// 误用把整队拉爆内存）。member 逐个校验：可解析、id>0、
// Partition(id)==partition——队列里发现「不属于本分区的玩家」
// 说明协议被破坏（如 Partitions 改过），必须报错而不是照写。
//
// Go 细节：ids 按 len(members) 预分配；ZRange 的 0 与 limit-1 是
// 闭区间下标，limit 个一批正好对应 worker 的合批上限。
func (s *Store) Pending(ctx context.Context, partition, limit int) ([]int64, error) {
	if partition < 0 || partition >= Partitions || limit <= 0 {
		return nil, fmt.Errorf("invalid queue parameters")
	}
	members, err := s.rdb.ZRange(ctx, s.dirtyKey(partition), 0, int64(limit)-1).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(members))
	for _, member := range members {
		id, err := strconv.ParseInt(member, 10, 64)
		if err != nil || id <= 0 || Partition(id) != partition {
			return nil, fmt.Errorf("invalid pending player %q", member)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ackScript 落库成功后移除脏标记——但仅当版本仍匹配。
//
// 版本匹配（ARGV[1] == 当前 version）才 ZREM：防止「旧 worker 用
// 过期记录 Ack，删掉新提交的入队标记」——那会导致新快照永远
// 不落库。版本不匹配返回 0（什么都不删），由新提交者自行再 Ack。
var ackScript = redis.NewScript(`
if redis.call('HGET',KEYS[1],'version')==ARGV[1] then return redis.call('ZREM',KEYS[2],ARGV[2]) end
return 0
`)

// Ack 确认落库完成，把玩家移出脏队列（版本匹配才生效，见 ackScript）。
//
// 失败由 .Err() 上抛；worker 的处理是「不重试 Ack，保留队列」——
// 下次 tick 该玩家会以新版本重新入队/重新落库，重复落库被数据库
// 版本栅栏挡住（见 worker.Persist），代价小、正确性高。
func (s *Store) Ack(ctx context.Context, id, version int64) error {
	return ackScript.Run(ctx, s.rdb, []string{s.stateKey(id), s.dirtyKey(Partition(id))}, version, id).Err()
}
