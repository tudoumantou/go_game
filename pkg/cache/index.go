// pkg/cache/index.go —— 按归属者分组的实体索引（ZSET）
//
// 解决什么问题：一对多的功能（伙伴、装备、邮件、好友）登录时要「按角色
// 捞出全部 id」。没有索引只能每个功能一条 `WHERE role_id = ?`，登录变成
// N 条 SQL；用 Set 存存在性又表达不了「属于谁」。
//
// 结构（一个 ZSET 承担三件事，原本要两张表）：
//
//	key    by_owner:{table}
//	score  归属者 id（角色/玩家/公会）
//	member 实体 id
//
//	ZSCORE  member            → 判存在，O(1)（等价 SISMEMBER）
//	ZSCORE  返回 score        → 反查归属，O(1)（Hash/Set 做不到）
//	ZRANGEBYSCORE score score → 按归属者取列表，O(log N + M)
//
// 并发：ZADD/ZREM 本身原子，不存在「读列表再覆盖写」的丢失窗口——这是
// 相对「Hash + 位置编号 + 字符串列表」的关键优势。
//
// 语义：**「存在」= 在某人的名下**。孤儿行（归属者为空）ZSCORE 返回 nil，
// 判不存在；未分配状态用 score=0 占位即可。
package cache

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// ErrNotIndexed 实体不在索引里：要么不存在，要么没有归属者。
var ErrNotIndexed = errors.New("cache: entity not indexed")

// buildBatchSize 单次 ZADD 的成员数上限。
//
// 一批 500 个 Z 结构约 20KB 命令体，远低于 redis 的 proto 上限；
// 分批的意义是控制客户端内存（百万行一次性构造 []redis.Z 会吃掉上百 MB）。
const buildBatchSize = 500

// Index 归属索引。rdb 为 nil 时所有方法退化为空操作（未启用缓存）。
type Index struct {
	rdb   redis.UniversalClient
	table string
}

// Row 构建索引的一行。
type Row struct {
	ID    int64 // 实体 id → member
	Owner int64 // 归属者 id → score
}

// NewIndex 创建索引。
func NewIndex(rdb redis.UniversalClient, table string) *Index {
	return &Index{rdb: rdb, table: table}
}

// Table 索引对应的表名（日志与对账用）。
func (x *Index) Table() string { return x.table }

// Add 把实体挂到归属者名下。
//
// 已存在时 ZADD 会覆盖 score——即「转移归属」也是一次 Add，天然原子。
func (x *Index) Add(ctx context.Context, ownerID, entityID int64) error {
	if ownerID <= 0 {
		return fmt.Errorf("cache: invalid owner id %d", ownerID)
	}
	if entityID <= 0 {
		return fmt.Errorf("cache: invalid entity id %d", entityID)
	}
	if x.rdb == nil {
		return nil
	}
	return x.rdb.ZAdd(ctx, OwnerIndexKey(x.table), redis.Z{
		Score:  float64(ownerID),
		Member: entityID,
	}).Err()
}

// AddBatch 批量挂到同一归属者名下（pipeline，一次往返）。
func (x *Index) AddBatch(ctx context.Context, ownerID int64, entityIDs []int64) error {
	if ownerID <= 0 {
		return fmt.Errorf("cache: invalid owner id %d", ownerID)
	}
	if x.rdb == nil || len(entityIDs) == 0 {
		return nil
	}
	pipe := x.rdb.Pipeline()
	for _, id := range entityIDs {
		if id <= 0 {
			return fmt.Errorf("cache: invalid entity id %d", id)
		}
		pipe.ZAdd(ctx, OwnerIndexKey(x.table), redis.Z{Score: float64(ownerID), Member: id})
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Remove 摘除实体。
//
// 不需要知道它属于谁——member 就是实体 id，这是选 ZSET 的一个直接收益：
// 「删除时拿不到归属者」不再是问题。
func (x *Index) Remove(ctx context.Context, entityIDs ...int64) error {
	if x.rdb == nil || len(entityIDs) == 0 {
		return nil
	}
	members := make([]interface{}, 0, len(entityIDs))
	for _, id := range entityIDs {
		if id <= 0 {
			return fmt.Errorf("cache: invalid entity id %d", id)
		}
		members = append(members, id)
	}
	return x.rdb.ZRem(ctx, OwnerIndexKey(x.table), members...).Err()
}

// Owner 反查归属者；未索引返回 ErrNotIndexed。
func (x *Index) Owner(ctx context.Context, entityID int64) (int64, error) {
	if x.rdb == nil {
		return 0, ErrNotIndexed
	}
	score, err := x.rdb.ZScore(ctx, OwnerIndexKey(x.table), strconv.FormatInt(entityID, 10)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotIndexed
	}
	if err != nil {
		return 0, fmt.Errorf("cache: zscore %s: %w", x.table, err)
	}
	return int64(score), nil
}

// Exists 判实体是否已索引（等价 SISMEMBER，O(1)）。
func (x *Index) Exists(ctx context.Context, entityID int64) (bool, error) {
	_, err := x.Owner(ctx, entityID)
	if errors.Is(err, ErrNotIndexed) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Members 按归属者取实体 id 列表（数值升序）。
//
// Redis 对同 score 的成员按**字典序**返回（"1001" 排在 "999" 前面），
// 所以这里在 Go 侧再排一次——列表规模是该归属者的实体数（通常 < 50），
// 排序成本可忽略，换来的是结果确定。
func (x *Index) Members(ctx context.Context, ownerID int64) ([]int64, error) {
	if x.rdb == nil {
		return nil, nil
	}
	bound := strconv.FormatInt(ownerID, 10)
	members, err := x.rdb.ZRangeByScore(ctx, OwnerIndexKey(x.table), &redis.ZRangeBy{
		Min: bound,
		Max: bound,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("cache: zrangebyscore %s: %w", x.table, err)
	}
	ids := make([]int64, 0, len(members))
	for _, m := range members {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cache: bad member %q in %s: %w", m, x.table, err)
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// Count 全服已索引实体数（对账基准）。
func (x *Index) Count(ctx context.Context) (int64, error) {
	if x.rdb == nil {
		return 0, nil
	}
	n, err := x.rdb.ZCard(ctx, OwnerIndexKey(x.table)).Result()
	if err != nil {
		return 0, fmt.Errorf("cache: zcard %s: %w", x.table, err)
	}
	return n, nil
}

// CountByOwner 某归属者名下的实体数。
//
// 比 Count 更有用：对账时能直接定位「哪个角色的索引数量不对」，而不是
// 只知道总数对不上。
func (x *Index) CountByOwner(ctx context.Context, ownerID int64) (int64, error) {
	if x.rdb == nil {
		return 0, nil
	}
	bound := strconv.FormatInt(ownerID, 10)
	n, err := x.rdb.ZCount(ctx, OwnerIndexKey(x.table), bound, bound).Result()
	if err != nil {
		return 0, fmt.Errorf("cache: zcount %s: %w", x.table, err)
	}
	return n, nil
}

// Build 全量重建索引（启动期）。
//
// 分三段是为了让调用方可以分页供稿，不用把整表读进内存：
//
//	BuildBegin  → 清掉上次的半成品
//	BuildAppend → 逐批追加（可调用多次）
//	BuildCommit → RENAME 原子切换
//
// 与 IDTable.BuildOne 同一套「临时 key + RENAME」策略：构建期间业务读的
// 仍是旧索引，不会出现「索引半空 → 功能列表少一半」的中间态。
func (x *Index) Build(ctx context.Context, rows []Row) (int64, error) {
	if x.rdb == nil {
		return 0, nil
	}
	if err := x.BuildBegin(ctx); err != nil {
		return 0, err
	}
	if _, err := x.BuildAppend(ctx, rows); err != nil {
		return 0, err
	}
	return x.BuildCommit(ctx)
}

// BuildBegin 清理上一轮可能残留的半成品。
func (x *Index) BuildBegin(ctx context.Context) error {
	if x.rdb == nil {
		return nil
	}
	return x.rdb.Del(ctx, x.buildingKey()).Err()
}

// BuildAppend 追加一批到构建中的索引，返回累计条数。
func (x *Index) BuildAppend(ctx context.Context, rows []Row) (int64, error) {
	if x.rdb == nil || len(rows) == 0 {
		return 0, nil
	}
	var total int64
	for start := 0; start < len(rows); start += buildBatchSize {
		end := min(start+buildBatchSize, len(rows))
		batch := rows[start:end]
		zs := make([]redis.Z, 0, len(batch))
		for _, r := range batch {
			if r.ID <= 0 || r.Owner <= 0 {
				return total, fmt.Errorf("cache: invalid build row %+v", r)
			}
			zs = append(zs, redis.Z{Score: float64(r.Owner), Member: r.ID})
		}
		if err := x.rdb.ZAdd(ctx, x.buildingKey(), zs...).Err(); err != nil {
			return total, fmt.Errorf("cache: zadd building %s: %w", x.table, err)
		}
		total += int64(len(batch))
	}
	return total, nil
}

// BuildCommit 把构建结果原子切换到正式 key。
//
// 空表特判：一行都没有时临时 key 从未被创建，RENAME 会返回
// "ERR no such key"（与 IDTable.BuildOne 里同一处坑），直接删正式 key
// 语义等价（「都不存在」）。
func (x *Index) BuildCommit(ctx context.Context) (int64, error) {
	if x.rdb == nil {
		return 0, nil
	}
	building := x.buildingKey()
	n, err := x.rdb.ZCard(ctx, building).Result()
	if err != nil {
		return 0, fmt.Errorf("cache: zcard building %s: %w", x.table, err)
	}
	if n == 0 {
		_ = x.rdb.Del(ctx, OwnerIndexKey(x.table)).Err()
		_ = x.rdb.Del(ctx, building).Err()
		return 0, nil
	}
	if err := x.rdb.Rename(ctx, building, OwnerIndexKey(x.table)).Err(); err != nil {
		return 0, fmt.Errorf("cache: rename %s: %w", x.table, err)
	}
	return n, nil
}

// buildingKey 构建期的临时 key。
func (x *Index) buildingKey() string { return OwnerIndexKey(x.table) + ":building" }
