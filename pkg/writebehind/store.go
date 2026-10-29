// pkg/writebehind/store.go —— Redis 侧玩家快照副本（读写 + 版本 + 租约）
//
// 职责：把玩家快照（Snapshot）存进 Redis hash，并提供版本与租约栅栏。
// 副本不是权威——权威是 Actor 内存；它服务三件事：跨进程可见（顶号判定、
// 别的节点读得到）、崩溃恢复、以及「副本落后于库」时的版本比对。
//
// 关键不变量：
//   - 版本单调：Sync 每次成功 +1，调用方据此判断副本是否落后；
//   - 租约围栏：Sync 校验 epoch，被顶号后的旧 Actor 得 ErrConflict；
//   - key 带 hash tag {player-N}：state key 与 epoch key 同 slot，
//     Lua 多 key 操作才能原子执行。
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
// 保证同一玩家的 state key 与 epoch key 同 slot——Sync 的 Lua 要同时
// 读写这两个 key，跨 slot 会直接报 CROSSSLOT。
// wb 前缀便于运维按模式清理/统计。
func (s *Store) stateKey(id int64) string {
	return fmt.Sprintf("wb:{player-%d}:state:%d", id, id)
}

// epochKey 会话所有权代号（R2.5 围栏令牌）。
//
// 与 stateKey 同一个 hash tag：Sync 的 Lua 要同时读写两者。
func (s *Store) epochKey(id int64) string {
	return fmt.Sprintf("wb:{player-%d}:epoch:%d", id, id)
}

// leaseTTL 会话令牌有效期。
//
// 7 天：远大于 Tier-2 的 Actor 保留期（90s），不会出现「Actor 还活着但令牌
// 已过期」；同时保证长期不登录的玩家不会在 Redis 里留下永久 key。
const leaseTTL = 7 * 24 * time.Hour

// acquireLeaseScript 自增并续期会话令牌。
//
// 只操作 epoch 一个 key，天然在单 slot 内；与 Sync 的 epoch 校验共同构成
// 围栏：新会话 +1 后，旧 Actor 手里的令牌立即失效。
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

// validateField 校验单个字段：Ext 字段要求模块已注册且自校验通过。
//
// 核心字段（profile/attr/items/grants）不做范围校验——它们的写入源是
// player.State，AddItem/RemoveItem/Grant 已在入口保证非负与 int32 上限；
// 在这里复检要对 items 反序列化一遍（100 件约 5µs），与本次优化目的相反。
func validateField(field string, raw []byte) error {
	name, ok := strings.CutPrefix(field, FieldExtPrefix)
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
	case FieldProfile:
		return json.Marshal(state.Profile)
	case FieldAttr:
		return json.Marshal(state.Attr)
	case FieldItems:
		return json.Marshal(state.Items)
	case FieldGrants:
		return json.Marshal(state.AppliedGrants)
	}
	name, ok := strings.CutPrefix(field, FieldExtPrefix)
	if !ok {
		return nil, fmt.Errorf("writebehind: unknown field %q", field)
	}
	raw, exists := state.Ext[name]
	if !exists || len(raw) == 0 {
		return nil, nil
	}
	return raw, nil
}

// seedScript 冷启动把 MySQL 读出的快照 seed 回 Redis（幂等 + 分字段）。
//
// ARGV[1]=version，ARGV[2..]=field/value 对。字段布局必须与 Sync 一致，
// 否则 Seed 写完立刻被 Load 读成空状态。
//
// 「已存在即返回 0」是幂等闸门：并发 seed 只有一个生效，其余读回现状。
var seedScript = redis.NewScript(`
if redis.call('EXISTS',KEYS[1])==1 then return 0 end
redis.call('HSET',KEYS[1],'version',ARGV[1])
local i = 2
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
	// 逐字段写入，与 Sync 同一套布局。
	args := []interface{}{version}
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
	return parseRecord(id, values)
}

// LoadMany 批量读快照（pipeline，一次 RTT）。
//
// 为什么需要它：跨玩家只读（好友列表、公会成员）最容易写成 N 次 HGETALL，
// 50 个好友就是 50 次往返。pipeline 把它们合并成一次。
//
// 未命中的 id 不出现在返回值里（调用方据此回源库）；单条坏数据只跳过
// 该条而不是整批失败——展示类读取不该被一个损坏的快照拖死。
func (s *Store) LoadMany(ctx context.Context, ids []int64) (map[int64]Record, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make(map[int64]*redis.MapStringStringCmd, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		cmds[id] = pipe.HGetAll(ctx, s.stateKey(id))
	}
	if len(cmds) == 0 {
		return nil, nil
	}
	// Exec 在任一命令报错时返回该错误；单条 key 不存在不算错（空 map）。
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	out := make(map[int64]Record, len(cmds))
	for id, cmd := range cmds {
		values, err := cmd.Result()
		if err != nil || len(values) == 0 {
			continue
		}
		rec, err := parseRecord(id, values)
		if err != nil {
			continue
		}
		out[id] = rec
	}
	return out, nil
}

// parseRecord 把一份 HGETALL 结果解析成 Record（Load/LoadMany 共用）。
//
// 抽出来的目的：两个入口的字段布局必须完全一致——分字段格式一旦在
// 一处漏掉新字段，就会出现「单个读得到、批量读不到」的诡异现象。
func parseRecord(id int64, values map[string]string) (Record, error) {
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
		FieldProfile: &state.Profile,
		FieldAttr:    &state.Attr,
		FieldItems:   &state.Items,
		FieldGrants:  &state.AppliedGrants,
	} {
		if raw := values[name]; raw != "" {
			if err := json.Unmarshal([]byte(raw), dst); err != nil {
				return Record{}, fmt.Errorf("decode %s: %w", name, err)
			}
		}
	}
	for f, raw := range values {
		name, ok := strings.CutPrefix(f, FieldExtPrefix)
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

// syncScript 写快照副本与版本（租约不匹配则整体拒绝）。
//
// ARGV[1]=token；ARGV[2..] 是 field/value 对，field 以 '-' 开头表示删除。
// 整个脚本原子：版本自增与字段写入不会出现「版本变了但字段没写」的中间态。
var syncScript = redis.NewScript(`
local token = tonumber(ARGV[1])
if not token or token <= 0 then
  return redis.error_reply('invalid lease token')
end
local epoch = tonumber(redis.call('GET', KEYS[2]) or '0')
if epoch == 0 then
  redis.call('SET', KEYS[2], token)
elseif token ~= epoch then
  return -1
end
local i = 2
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
return redis.call('HINCRBY', KEYS[1], 'version', 1)
`)

// Sync 把内存状态同步到 Redis 副本。
//
// 只写副本 + 版本，供跨进程读与崩溃恢复；**不承载落库**——落库由
// pkg/flush 从 Actor 内存走（副本可能落后，拿它落库等于写回旧值）。
//
// 失败语义由调用方决定：这只是「副本同步失败」，业务继续。
func (s *Store) Sync(ctx context.Context, playerID int64, src FieldSource, fields, removed []string, token int64) (int64, error) {
	if playerID <= 0 {
		return 0, fmt.Errorf("invalid player id")
	}
	args := []interface{}{token}
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
	version, err := syncScript.Run(ctx, s.rdb, []string{
		s.stateKey(playerID),
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

// Drop 删除玩家的热快照（保留租约 epoch）。
//
// 两种用途，动作相同、意图不同：
//  1. 同步落库之后（A 类数据）：库里的 player_writebehind.version 已推进，
//     缓存里的旧快照即使留着，异步通道的版本栅栏也不会采用它；但下一次
//     LoadCached 仍会读到——玩家重登会看到「充值前的余额」。删掉之后加载
//     必然 ErrMiss，走冷启动从 MySQL 读，读到的就是刚提交的数据。
//  2. 加载时发现副本落后于库：必须让下一次 Seed 能真正写入——seedScript
//     的「已存在即返回 0」不清掉就会一直读回旧副本。
//
// 保留 epochKey：租约仍归当前 Actor，顶号保护不受影响。
func (s *Store) Drop(ctx context.Context, id int64) error {
	if id <= 0 {
		return fmt.Errorf("invalid player id")
	}
	return s.rdb.Del(ctx, s.stateKey(id)).Err()
}
