// pkg/cache/idtable.go —— Redis ID 表与回源（设计文档 12.5）
//
// 核心约定（12.5 开篇）：**所有以内存态为准的表，启动时都要在 Redis 建一份 ID 表；
// 业务查不到 ID 即等于库没有；ID 查到了但 Redis 没有数据，再去数据库加载。**
//
// 这样把「记录是否存在」这类高频判断从 MySQL 挪到 Redis O(1)，
// 避免为了一次存在性检查打一次库（如判断装备实例 uid 是否合法、邮件 id 是否属于自己）。
//
// 与 idtab.go 的区别：idtab.go 是 ID **分配**（INCR 计数器），本文件是 ID **存在性集合**。
package cache

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/redis/go-redis/v9"
)

// TableMeta 一张需要建 ID 表的表。
type TableMeta struct {
	Name string // 表名
	PK   string // 主键列名（单列，且为整型）
}

// DefaultIDTables 需要建 ID 表的表清单（12.5.1）。
//
// 不含 account / recharge_order（Tier-1，MySQL 权威，直接查询），
// 也不含 chat_record / copy_record 等流水表（无热数据，不值得维护集合）。
var DefaultIDTables = []TableMeta{
	{"player", "id"},
	{"player_attr", "player_id"},
	{"player_item", "id"},
	{"player_equip", "id"},
	{"player_skill", "id"},
	{"player_partner", "id"},
	{"player_quest", "id"},
	{"mail", "id"},
	{"friend", "id"},
	{"friend_apply", "id"},
	{"guild", "id"},
	{"guild_member", "id"},
	{"team", "id"},
	{"team_member", "id"},
}

// TableHotTables 启动时必须建好的表（ready 未置位前业务走降级路径）。
// 其余表可以后台慢慢建，不影响登录。
var TableHotTables = []string{"player", "player_equip", "mail"}

// identPattern 表名/列名白名单：动态拼 SQL 必须校验，否则等于开了 SQL 注入口子。
var identPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// IDTable ID 表管理器。
type IDTable struct {
	rdb      redis.UniversalClient
	db       *sql.DB
	pageSize int64
}

// NewIDTable 创建 ID 表管理器。
//
// rdb 为空时所有方法退化为「直接查库」——单测/无 Redis 环境仍可运行。
func NewIDTable(rdb redis.UniversalClient, db *sql.DB) *IDTable {
	return &IDTable{rdb: rdb, db: db, pageSize: 5000}
}

// SetPageSize 调整构建时的分页大小（默认 5000）。
func (t *IDTable) SetPageSize(n int64) {
	if n > 0 {
		t.pageSize = n
	}
}

// BuildAll 构建全部 ID 表。已 ready 的表会跳过（幂等，可用于每次启动）。
//
// 调用方应在后台 goroutine 执行：构建期间 ready 未置位，业务自动走「回落 DB」，
// 服务可用性不受影响（12.5.1 降级约定）。
func (t *IDTable) BuildAll(ctx context.Context, tables []TableMeta) (map[string]int64, error) {
	counts := make(map[string]int64, len(tables))
	var firstErr error
	for _, meta := range tables {
		if ctx.Err() != nil {
			return counts, ctx.Err()
		}
		n, err := t.BuildOne(ctx, meta)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		counts[meta.Name] = n
	}
	return counts, firstErr
}

// BuildOne 构建单表 ID 集合，返回记录数。
//
// 用 keyset 分页（`WHERE pk > ? ORDER BY pk LIMIT n`）而非 OFFSET：
// 大表深翻页时 OFFSET 会退化成全表扫描，keyset 每次都是索引区间扫描。
func (t *IDTable) BuildOne(ctx context.Context, meta TableMeta) (int64, error) {
	if err := validateMeta(meta); err != nil {
		return 0, err
	}
	if t.rdb == nil {
		return 0, nil
	}
	if t.Ready(ctx, meta.Name) {
		n, _ := t.Count(ctx, meta.Name)
		return n, nil
	}

	// 先清掉可能残留的半成品集合（上次构建中途崩溃）
	building := IDTabKey(meta.Name) + ":building"
	_ = t.rdb.Del(ctx, building).Err()

	var (
		lastID int64
		total  int64
	)
	for {
		ids, err := t.queryIDs(ctx, meta, lastID)
		if err != nil {
			return total, err
		}
		if len(ids) == 0 {
			break
		}
		args := make([]any, len(ids))
		for i, id := range ids {
			args[i] = id
		}
		if err := t.rdb.SAdd(ctx, building, args...).Err(); err != nil {
			return total, fmt.Errorf("sadd %s: %w", meta.Name, err)
		}
		total += int64(len(ids))
		lastID = ids[len(ids)-1]
		if int64(len(ids)) < t.pageSize {
			break
		}
	}

	// 原子切换：RENAME 覆盖旧集合，避免构建期间「集合半空 → 存在性判断误判为不存在」
	if err := t.rdb.Rename(ctx, building, IDTabKey(meta.Name)).Err(); err != nil {
		return total, fmt.Errorf("rename %s: %w", meta.Name, err)
	}
	if err := t.rdb.Set(ctx, IDTabReadyKey(meta.Name), "1", 0).Err(); err != nil {
		return total, fmt.Errorf("set ready %s: %w", meta.Name, err)
	}
	return total, nil
}

// queryIDs keyset 分页取一批主键。
func (t *IDTable) queryIDs(ctx context.Context, meta TableMeta, after int64) ([]int64, error) {
	//nolint:gosec // 表名/列名经 validateMeta 白名单校验，非用户输入
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
		meta.PK, meta.Name, meta.PK, meta.PK)
	rows, err := t.db.QueryContext(ctx, q, after, t.pageSize)
	if err != nil {
		return nil, fmt.Errorf("query ids %s: %w", meta.Name, err)
	}
	defer func() { _ = rows.Close() }()

	ids := make([]int64, 0, t.pageSize)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Ready 该表 ID 集合是否已构建完成。
func (t *IDTable) Ready(ctx context.Context, table string) bool {
	if t.rdb == nil {
		return false
	}
	v, err := t.rdb.Get(ctx, IDTabReadyKey(table)).Result()
	return err == nil && v == "1"
}

// Exists 存在性判断（12.5.2）。
//
//   - ready 已置位：查 Redis Set，O(1)，**不打库**；
//   - ready 未置位（构建中/未配置 Redis）：回落 MySQL，牺牲性能保正确性。
func (t *IDTable) Exists(ctx context.Context, table string, id int64) (bool, error) {
	if t.rdb != nil && t.Ready(ctx, table) {
		ok, err := t.rdb.SIsMember(ctx, IDTabKey(table), id).Result()
		if err == nil {
			return ok, nil
		}
		// Redis 异常 → 降级查库，不能让缓存故障变成业务故障
	}
	return t.dbExists(ctx, table, id)
}

// dbExists 直接查库（降级路径）。
func (t *IDTable) dbExists(ctx context.Context, table string, id int64) (bool, error) {
	if err := validateIdent(table); err != nil {
		return false, err
	}
	if t.db == nil {
		return false, fmt.Errorf("idtable: no database")
	}
	//nolint:gosec // 表名经白名单校验
	var one int
	err := t.db.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE id = ? LIMIT 1", table), id).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Add 登记新 ID（创建记录后调用）。
//
// 静默忽略未 ready 的表：构建流程会从库里扫到它，无需补登。
func (t *IDTable) Add(ctx context.Context, table string, id int64) error {
	if t.rdb == nil || !t.Ready(ctx, table) {
		return nil
	}
	if err := t.rdb.SAdd(ctx, IDTabKey(table), id).Err(); err != nil {
		return fmt.Errorf("idtab add %s:%d: %w", table, id, err)
	}
	return nil
}

// Remove 注销 ID（物理删除记录后调用）。
func (t *IDTable) Remove(ctx context.Context, table string, id int64) error {
	if t.rdb == nil {
		return nil
	}
	if err := t.rdb.SRem(ctx, IDTabKey(table), id).Err(); err != nil {
		return fmt.Errorf("idtab remove %s:%d: %w", table, id, err)
	}
	// 顺手清实体热数据，避免孤儿 hash（12.5.3 孤儿清理的即时部分）
	_ = t.rdb.Del(ctx, EntityKey(table, id)).Err()
	return nil
}

// Count ID 集合大小。
func (t *IDTable) Count(ctx context.Context, table string) (int64, error) {
	if t.rdb == nil {
		return 0, nil
	}
	return t.rdb.SCard(ctx, IDTabKey(table)).Result()
}

// Reconcile 数量核对（12.5.3，每 10 分钟）。
//
// 集合大小与表行数不一致时重建（临时 key + RENAME 原子切换，不阻塞查询）。
// 返回是否触发了重建。
func (t *IDTable) Reconcile(ctx context.Context, meta TableMeta) (bool, error) {
	if t.rdb == nil || t.db == nil {
		return false, nil
	}
	if err := validateMeta(meta); err != nil {
		return false, err
	}
	if !t.Ready(ctx, meta.Name) {
		return false, nil
	}

	cached, err := t.Count(ctx, meta.Name)
	if err != nil {
		return false, err
	}
	//nolint:gosec // 表名经白名单校验
	var actual int64
	if err := t.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+meta.Name).Scan(&actual); err != nil {
		return false, fmt.Errorf("count %s: %w", meta.Name, err)
	}
	if cached == actual {
		return false, nil
	}

	// 不一致：清 ready 触发重建（重建期间 Exists 回落 DB，正确性不受影响）
	if err := t.rdb.Del(ctx, IDTabReadyKey(meta.Name)).Err(); err != nil {
		return false, err
	}
	if _, err := t.BuildOne(ctx, meta); err != nil {
		return true, err
	}
	return true, nil
}

// LoadEntity 加载实体（12.5.2）：Redis hash 命中直接返回，否则回源 DB 并回填。
//
// 三类判定矩阵：
//
//	idtab 无        → 库中不存在，直接返回不存在（不打 DB）
//	idtab 有 + hash 有 → 用 Redis
//	idtab 有 + hash 无 → 回源 MySQL + HSET 回填
func (t *IDTable) LoadEntity(ctx context.Context, meta TableMeta, id int64) (map[string]string, error) {
	if err := validateMeta(meta); err != nil {
		return nil, err
	}

	key := EntityKey(meta.Name, id)
	if t.rdb != nil {
		if data, err := t.rdb.HGetAll(ctx, key).Result(); err == nil && len(data) > 0 {
			return data, nil
		}
	}

	exists, err := t.Exists(ctx, meta.Name, id)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}

	row, err := t.loadRow(ctx, meta, id)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, ErrNotFound
	}

	if t.rdb != nil {
		// 回填失败不影响本次返回（下次再补）
		_ = t.rdb.HSet(ctx, key, stringMapToAny(row)).Err()
	}
	return row, nil
}

// loadRow 按主键读一整行并转成 map（列名 → 字符串值）。
func (t *IDTable) loadRow(ctx context.Context, meta TableMeta, id int64) (map[string]string, error) {
	if t.db == nil {
		return nil, fmt.Errorf("idtable: no database")
	}
	//nolint:gosec // 表名/列名经白名单校验
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", meta.Name, meta.PK)
	rows, err := t.db.QueryContext(ctx, q, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}

	out := make(map[string]string, len(cols))
	for i, c := range cols {
		out[c] = toString(vals[i])
	}
	return out, nil
}

// CleanOrphans 清理孤儿实体 hash（12.5.3，每小时）。
//
// 用 SCAN 逐批扫描 `e:{table}:*`，idtab 里没有的删掉。
// limit 限制单次处理量，避免大实例一次扫穿 Redis。
func (t *IDTable) CleanOrphans(ctx context.Context, table string, limit int64) (int64, error) {
	if t.rdb == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = 5000
	}
	pattern := EntityKey(table, 0)
	pattern = strings.TrimSuffix(pattern, "0") + "*"

	var (
		cursor uint64
		scanned, removed int64
	)
	for scanned < limit {
		keys, next, err := t.rdb.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			return removed, err
		}
		for _, k := range keys {
			scanned++
			id := parseEntityID(k, table)
			if id == 0 {
				continue
			}
			ok, err := t.Exists(ctx, table, id)
			if err != nil {
				continue
			}
			if !ok {
				_ = t.rdb.Del(ctx, k).Err()
				removed++
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return removed, nil
}

// ErrNotFound 实体不存在。
var ErrNotFound = fmt.Errorf("entity not found")

// validateMeta 校验表名与主键列名。
func validateMeta(m TableMeta) error {
	if err := validateIdent(m.Name); err != nil {
		return err
	}
	return validateIdent(m.PK)
}

// validateIdent 标识符白名单校验（防 SQL 注入）。
func validateIdent(s string) error {
	if !identPattern.MatchString(s) {
		return fmt.Errorf("invalid identifier %q", s)
	}
	return nil
}

// parseEntityID 从 `e:{table}:{id}` 解析 id。
func parseEntityID(key, table string) int64 {
	prefix := "e:" + table + ":"
	if !strings.HasPrefix(key, prefix) {
		return 0
	}
	var id int64
	if _, err := fmt.Sscanf(key[len(prefix):], "%d", &id); err != nil {
		return 0
	}
	return id
}

// stringMapToAny map[string]string → []any（HSet 变参）。
func stringMapToAny(m map[string]string) []any {
	out := make([]any, 0, len(m)*2)
	for k, v := range m {
		out = append(out, k, v)
	}
	return out
}

// toString 把数据库列值转成字符串（回填 Redis 用）。
func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}
