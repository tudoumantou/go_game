// pkg/cache/owner_index.go —— 归属索引（ZSET）的构建、展开与回源
//
// 解决什么问题：一对多的功能（邮件、公会成员、好友）登录时要「按角色捞出
// 全部实体」。没有索引只能每个功能一条 WHERE owner = ?，登录变成 N 条 SQL；
// 把「谁有哪些」建成 Redis ZSET（by_owner:{table}）后，登录时一次
// ZRANGEBYSCORE 拿到全部 id，再一次批量加载补齐数据。
//
// 三分工（与 idtable.go 对照读）：
//
//	index.go      纯 Redis 操作：Add/Remove/Owner/Exists/Members/Build
//	本文件        构建期从 MySQL 分页取材 + 读取期回落 DB + 实体行批量加载
//	idtable.go    存在性集合（Set）与实体 hash 回源（LoadEntity/Preheat）
//
// 降级哲学与 IDTable 一致：**索引是加速层，不是正确性前提**。未 ready
// （启动构建中）或 Redis 故障时，Members 回落 MySQL——表现是多打一次库，
// 而不是「功能列表为空」。后者正是这套设计里最难查的事故形态：
// 不报错、只是没数据。
package cache

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// entityBatchSize 单批 pipeline / SQL IN 的行数上限。
//
// 登录展开通常 < 100 个 id；但 Preheat 一类调用方可能给整表——
// 按批切分，控制单条命令体量与客户端内存。
const entityBatchSize = 500

// OwnerIndex 一张表的归属索引（Redis ZSET + MySQL 回源）。
//
// 与 Index 的分工：Index 是纯 Redis 结构（rdb 可为 nil）；本类型补上
// 「没有 Redis 时怎么办」——db 为 nil 时退化为「只有命中 Redis 才有数据」，
// 两者都 nil 时 LoadBatch 把所有 id 报为 missing（调用方自行处置）。
type OwnerIndex struct {
	idx      *Index
	db       *sql.DB
	table    string
	by       string // 归属者列（ZSET 的 score）
	key      string // 实体 id 列（ZSET 的 member，通常是主键）
	pageSize int64
}

// NewOwnerIndex 创建归属索引。
//
// table/by/key 三个标识符都会拼进动态 SQL，构造时不过滤、每个用法之前过
// validate（白名单见 idtable.go 的 identPattern）——配置错误要炸在
// 启动日志里，而不是变成一次注入。
func NewOwnerIndex(rdb redis.UniversalClient, db *sql.DB, table, by, key string) *OwnerIndex {
	return &OwnerIndex{
		idx:      NewIndex(rdb, table),
		db:       db,
		table:    table,
		by:       by,
		key:      key,
		pageSize: 5000,
	}
}

// SetPageSize 调整构建分页（默认 5000）。n <= 0 忽略，防止死循环。
func (x *OwnerIndex) SetPageSize(n int64) {
	if n > 0 {
		x.pageSize = n
	}
}

// Index 纯 Redis 操作面（运行时 Add/Remove/Owner/Exists/CountByOwner）。
//
// 为什么铺在 Index 上而不是 OwnerIndex 上：这些是「构建/回源之外」的
// 运行时操作，与 ready 生命周期无关——未 ready 时的 ZADD 会被随后的
// BuildOne 用库里的数据重建覆盖，而行总是先落库再 Add（构建必然取材到它），
// 不存在丢入口；ready 后的 ZADD/ZREM 直接落在正式 key 上，立即生效。
func (x *OwnerIndex) Index() *Index { return x.idx }

// Table 表名（日志/对账用）。
func (x *OwnerIndex) Table() string { return x.table }

// validate 三个标识符过白名单（动态 SQL 的前置）。
func (x *OwnerIndex) validate() error {
	if err := validateIdent(x.table); err != nil {
		return err
	}
	if err := validateIdent(x.by); err != nil {
		return err
	}
	return validateIdent(x.key)
}

// Ready 索引是否已构建完成（ready 置位 = 可以信任 ZSET）。
func (x *OwnerIndex) Ready(ctx context.Context) bool {
	if x.idx.rdb == nil {
		return false
	}
	v, err := x.idx.rdb.Get(ctx, OwnerIndexReadyKey(x.table)).Result()
	return err == nil && v == "1"
}

// Members 按归属者取实体 id 列表（数值升序）。
//
//   - ready 已置位：ZRANGEBYSCORE，O(log N + M)，不打库；
//   - 未 ready（启动构建中）：回落 MySQL；
//   - Redis 查询出错：同样回落（正确性优先，同 IDTable.Exists）。
//
// 绝不在索引答不上来时返回空列表冒充「这个人没有实体」——调用方
// （登录展开）无法区分这两种情况，会把数据缺失演变成功能缺失。
func (x *OwnerIndex) Members(ctx context.Context, ownerID int64) ([]int64, error) {
	if ownerID <= 0 {
		return nil, fmt.Errorf("cache: invalid owner id %d", ownerID)
	}
	if x.idx.rdb != nil && x.Ready(ctx) {
		ids, err := x.idx.Members(ctx, ownerID)
		if err == nil {
			return ids, nil
		}
		// Redis 异常 → 回落库（下同 IDTable.Exists 的容错取舍）
	}
	return x.dbMembers(ctx, ownerID)
}

// dbMembers 回落路径：直接按归属者查库。
func (x *OwnerIndex) dbMembers(ctx context.Context, ownerID int64) ([]int64, error) {
	if err := x.validate(); err != nil {
		return nil, err
	}
	if x.db == nil {
		return nil, fmt.Errorf("ownerindex %s: no database", x.table)
	}
	//nolint:gosec // 表名/列名经 validate 白名单校验，非用户输入
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ? ORDER BY %s",
		x.key, x.table, x.by, x.key)
	rows, err := x.db.QueryContext(ctx, q, ownerID)
	if err != nil {
		return nil, fmt.Errorf("query members %s: %w", x.table, err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]int64, 0, 16)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	// rows.Err 不能省：Next 返回 false 可能是中途出错，不查会把
	// 「读了一半」当成「这个人没有实体」。
	return out, rows.Err()
}

// BuildOne 全量构建索引（启动期）。
//
// 取材：keyset 分页 `SELECT key, by FROM table WHERE key > ? AND by > 0
// ORDER BY key LIMIT ?`——只收有归属者的行。「存在」的含义是「在某人的
// 名下」，无归属者的行（未分配/归属为空）不进索引；`by > 0` 在 SQL 侧
// 顺带把 NULL 排掉（NULL 比较结果为 NULL，同样被 WHERE 拒绝）。
//
// 落点：Index 的 :building 临时 key + RENAME 原子切换——构建期间业务读的
// 仍是旧索引，不会出现「索引半空 → 功能列表少一半」的中间态。ready 置位
// 是提交点：置位前 Members 自动回落 DB。
//
// 幂等：已 ready 直接返回现有数量（每次启动都会调，重复构建零成本）。
func (x *OwnerIndex) BuildOne(ctx context.Context) (int64, error) {
	if err := x.validate(); err != nil {
		return 0, err
	}
	if x.idx.rdb == nil || x.db == nil {
		return 0, nil
	}
	if x.Ready(ctx) {
		n, _ := x.idx.Count(ctx)
		return n, nil
	}

	// 先清掉可能残留的半成品（上次构建中途崩溃）
	if err := x.idx.BuildBegin(ctx); err != nil {
		return 0, err
	}

	var after int64
	for {
		p, err := x.queryRows(ctx, after)
		if err != nil {
			return 0, err
		}
		if p.scanned == 0 {
			break
		}
		if _, err := x.idx.BuildAppend(ctx, p.rows); err != nil {
			return 0, err
		}
		after = p.lastID
		// 不满一页 = 扫到尾了；正好满页则可能还有下一页。
		// 判据用**原始行数**而不是可入索引的行数——跳过的无归属者行
		// 同样占 LIMIT 额度，用过滤后的数量会提前终止、漏掉后面的行。
		if int64(p.scanned) < x.pageSize {
			break
		}
	}

	// BuildCommit 的返回值（ZCARD）是权威计数：它处理了空表特判
	// （一行都没有时删正式 key，语义等价于「都不存在」）。
	n, err := x.idx.BuildCommit(ctx)
	if err != nil {
		return 0, err
	}
	// ready 是提交点：置位失败时索引已就位但业务仍回落 DB，
	// 正确性不受影响，下次构建/Reconcile 会补上。
	if err := x.idx.rdb.Set(ctx, OwnerIndexReadyKey(x.table), "1", 0).Err(); err != nil {
		return n, fmt.Errorf("set ready %s: %w", x.table, err)
	}
	return n, nil
}

// page 一页取材结果。
type page struct {
	rows    []Row // 可入索引的行（已剔除无归属者）
	lastID  int64 // 本页最后一行的原始 id（翻页游标：被跳过的行也要推进）
	scanned int   // 本页原始行数（判断是否还有下一页）
}

// queryRows 构建取材：一页 (实体 id, 归属者)。
//
// 无归属者的行（owner <= 0）在这里剔除：SQL 侧的 `by > 0` 已经过滤，
// Go 侧再兜底一层——驱动/视图返回意外行时，让 BuildAppend 的校验把
// 整个构建炸掉是不值得的（漏一行有 Reconcile 兜底，构建失败没有）。
func (x *OwnerIndex) queryRows(ctx context.Context, after int64) (page, error) {
	//nolint:gosec // 标识符经 validate 白名单校验
	q := fmt.Sprintf(
		"SELECT %s, %s FROM %s WHERE %s > ? AND %s > 0 ORDER BY %s LIMIT ?",
		x.key, x.by, x.table, x.key, x.by, x.key)
	rows, err := x.db.QueryContext(ctx, q, after, x.pageSize)
	if err != nil {
		return page{}, fmt.Errorf("query index rows %s: %w", x.table, err)
	}
	defer func() { _ = rows.Close() }()

	out := page{rows: make([]Row, 0, x.pageSize)}
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Owner); err != nil {
			return page{}, err
		}
		out.scanned++
		out.lastID = r.ID
		if r.ID <= 0 || r.Owner <= 0 {
			continue
		}
		out.rows = append(out.rows, r)
	}
	if err := rows.Err(); err != nil {
		return page{}, err
	}
	return out, nil
}

// Reconcile 数量核对（维护任务，每 10 分钟一次）。
//
// ZCARD 与 `SELECT COUNT(*) WHERE by > 0` 不一致 → 清 ready 触发重建
// （重建期间 Members 回落 DB）。与 IDTable.Reconcile 同一策略：用「数量」
// 而不是全量比对——一次 COUNT(*) 就能发现大部分漂移（漏删/漏增），
// 低频任务可接受这种近似。
func (x *OwnerIndex) Reconcile(ctx context.Context) (bool, error) {
	if x.idx.rdb == nil || x.db == nil {
		return false, nil
	}
	if err := x.validate(); err != nil {
		return false, err
	}
	if !x.Ready(ctx) {
		return false, nil
	}

	cached, err := x.idx.Count(ctx)
	if err != nil {
		return false, err
	}
	//nolint:gosec // 标识符经 validate 白名单校验
	var actual int64
	if err := x.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s > 0", x.table, x.by)).Scan(&actual); err != nil {
		return false, fmt.Errorf("count %s: %w", x.table, err)
	}
	if cached == actual {
		return false, nil
	}

	if err := x.idx.rdb.Del(ctx, OwnerIndexReadyKey(x.table)).Err(); err != nil {
		return false, err
	}
	if _, err := x.BuildOne(ctx); err != nil {
		return true, err
	}
	return true, nil
}

// LoadBatch 批量加载实体行（登录展开的主路径）。
//
// 两步：
//  1. pipeline HGETALL `e:{table}:{id}`——命中的直接用，一次往返拿一批；
//  2. 未命中的合成一条 `SELECT * WHERE key IN (...)` 捞回来，HSET 回填。
//
// 返回 missing：Redis 与 MySQL 都没有的 id——索引里的残留（删行时漏清
// 索引），调用方应 Index.Remove 掉（读时自愈，缓存架构设计 §4.3）。
// 清理留在调用方决策：本方法只负责「加载」，不顺手改索引。
//
// Redis 整体故障时不报错：pipeline 失败按全部未命中处理走 MySQL——
// 与 Members 同一的降级哲学，缓存抖动不升级成登录失败。
func (x *OwnerIndex) LoadBatch(ctx context.Context, ids []int64) ([]EntityRow, []int64, error) {
	if err := x.validate(); err != nil {
		return nil, nil, err
	}
	wanted := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id > 0 {
			wanted = append(wanted, id)
		}
	}
	if len(wanted) == 0 {
		return nil, nil, nil
	}

	found := make(map[int64]map[string]string, len(wanted))
	if x.idx.rdb != nil {
		for start := 0; start < len(wanted); start += entityBatchSize {
			end := min(start+entityBatchSize, len(wanted))
			batch := wanted[start:end]
			pipe := x.idx.rdb.Pipeline()
			for _, id := range batch {
				pipe.HGetAll(ctx, EntityKey(x.table, id))
			}
			cmds, err := pipe.Exec(ctx)
			if err != nil {
				break // Redis 故障：按全部未命中处理，回落 MySQL
			}
			for i, cmd := range cmds {
				hget, ok := cmd.(*redis.MapStringStringCmd)
				if !ok {
					continue
				}
				if fields, herr := hget.Result(); herr == nil && len(fields) > 0 {
					found[batch[i]] = fields
				}
			}
		}
	}

	// 未命中 → 一次 SQL 捞回 + 回填（回填失败不影响本次返回）
	var missed []int64
	for _, id := range wanted {
		if _, ok := found[id]; !ok {
			missed = append(missed, id)
		}
	}
	if len(missed) > 0 && x.db != nil {
		for start := 0; start < len(missed); start += entityBatchSize {
			end := min(start+entityBatchSize, len(missed))
			rows, err := x.queryEntities(ctx, missed[start:end])
			if err != nil {
				return nil, nil, err
			}
			if x.idx.rdb != nil && len(rows) > 0 {
				pipe := x.idx.rdb.Pipeline()
				for _, r := range rows {
					found[r.ID] = r.Fields
					pipe.HSet(ctx, EntityKey(x.table, r.ID), stringMapToAny(r.Fields))
				}
				// 回填是加分项：失败下次再补，不让登录失败
				_, _ = pipe.Exec(ctx)
			} else {
				for _, r := range rows {
					found[r.ID] = r.Fields
				}
			}
		}
	}

	// 按入参顺序产出；两处都没有的进 missing（调用方自愈）
	rows := make([]EntityRow, 0, len(found))
	var missing []int64
	for _, id := range wanted {
		fields, ok := found[id]
		if !ok {
			missing = append(missing, id)
			continue
		}
		rows = append(rows, EntityRow{ID: id, Fields: fields})
	}
	return rows, missing, nil
}

// queryEntities 按 id 批量读实体行（动态列）。
func (x *OwnerIndex) queryEntities(ctx context.Context, ids []int64) ([]EntityRow, error) {
	if x.db == nil {
		return nil, fmt.Errorf("ownerindex %s: no database", x.table)
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	//nolint:gosec // 标识符经 validate 白名单校验，id 值走占位符绑定
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s IN (%s)",
		x.table, x.key, placeholders(len(ids)))
	rows, err := x.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query entities %s: %w", x.table, err)
	}
	defer func() { _ = rows.Close() }()
	return scanEntityRows(rows, x.key)
}

// placeholders 生成 n 个占位符（IN 子句）。
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}
