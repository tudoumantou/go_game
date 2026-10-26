// pkg/cache/idtable.go —— Redis ID 表与回源（设计文档 12.5）
//
// 核心约定（12.5 开篇）：**所有以内存态为准的表，启动时都要在 Redis 建一份 ID 表；
// 业务查不到 ID 即等于库没有；ID 查到了但 Redis 没有数据，再去数据库加载。**
//
// 这样把「记录是否存在」这类高频判断从 MySQL 挪到 Redis O(1)，
// 避免为了一次存在性检查打一次库（如判断装备实例 uid 是否合法、邮件 id 是否属于自己）。
//
// 与 idtab.go 的区别：idtab.go 是 ID **分配**（INCR 计数器），本文件是 ID **存在性集合**。
//
// 架构协作：key 由 keygen（IDTabKey/IDTabReadyKey/EntityKey）统一生成；
// 启动构建由游戏服进程调度（BuildAll 放后台 goroutine），维护任务
// （Reconcile/CleanOrphans）由定时器驱动。
// 关键取舍：构建期间 ready 未置位 → Exists 自动回落 DB，构建不阻塞
// 业务、缓存故障不升级为业务故障；构建落点用临时 key + RENAME 原子
// 切换，查询端永远看不到半成品集合。
package cache

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// TableMeta 一张需要建 ID 表的表。
//
// Name/PK 都会被拼进动态 SQL，因此必须过 validateMeta 白名单校验
// （见 identPattern）——标识符无法用 SQL 参数占位，只能白名单。
type TableMeta struct {
	Name string // 表名
	PK   string // 主键列名（单列，且为整型）
}

// DefaultIDTables 需要建 ID 表的表清单（12.5.1）。
//
// 不含 account / recharge_order（Tier-1，MySQL 权威，直接查询），
// 也不含 chat_record / copy_record 等流水表（无热数据，不值得维护集合）。
// Go 知识点：包级 slice 初始化后只读——所有进程并发读安全；新增表
// 只需改这一处，元素是值拷贝，不存在共享可变状态。
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
//
// 选取逻辑：登录链路强依赖的表——player（查人）、player_equip（穿戴
// 校验）、mail（未读拉取）；它们没 ready 时每条存在性判断都在打库，
// 所以必须最先就绪。
var TableHotTables = []string{"player", "player_equip", "mail"}

// identPattern 表名/列名白名单：动态拼 SQL 必须校验，否则等于开了 SQL 注入口子。
//
// Go 知识点：MustCompile 在包初始化时编译正则，模式写错直接 panic——
// 适合静态已知模式（错误在编译期暴露，而非每次匹配时才报）。
// 约束 [a-z_][a-z0-9_]*：纯小写字母数字下划线，天然排除引号、分号、
// 注释符等一切注入载体。
var identPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// IDTable ID 表管理器。
//
// rdb/db 均可为 nil（分别退化为纯 Redis/纯 DB 模式），组合出多种部署
// 形态；pageSize 仅构建期读取，运行时只读，无需加锁。
type IDTable struct {
	rdb      redis.UniversalClient
	db       *sql.DB
	pageSize int64
}

// NewIDTable 创建 ID 表管理器。
//
// rdb 为空时所有方法退化为「直接查库」——单测/无 Redis 环境仍可运行。
// pageSize 默认 5000：一批 SAdd 参数量与单次 SQL 扫描行数的折中——
// 太大撑爆单条命令/单次查询，太小则构建轮次过多。
func NewIDTable(rdb redis.UniversalClient, db *sql.DB) *IDTable {
	return &IDTable{rdb: rdb, db: db, pageSize: 5000}
}

// SetPageSize 调整构建时的分页大小（默认 5000）。
//
// n <= 0 忽略（防御性）：防止误配 0 导致构建死循环——一页 0 行 =
// 永远翻不了页。
func (t *IDTable) SetPageSize(n int64) {
	if n > 0 {
		t.pageSize = n
	}
}

// BuildAll 构建全部 ID 表。已 ready 的表会跳过（幂等，可用于每次启动）。
//
// 调用方应在后台 goroutine 执行：构建期间 ready 未置位，业务自动走「回落 DB」，
// 服务可用性不受影响（12.5.1 降级约定）。
// 错误策略：逐表构建、记录第一错误但**不中断**——一张表失败不影响
// 其余表就绪；返回的 counts 含每表记录数，供启动日志对账。
// Go 知识点：ctx.Err() 每轮主动检查，取消后立即返回——长循环里及时
// 响应 context 取消是防 goroutine 泄漏的基本功。
func (t *IDTable) BuildAll(ctx context.Context, tables []TableMeta) (map[string]int64, error) {
	counts := make(map[string]int64, len(tables))
	var firstErr error
	for _, meta := range tables {
		if ctx.Err() != nil {
			return counts, ctx.Err()
		}
		n, err := t.BuildOne(ctx, meta)
		if err != nil {
			// 只记第一错误：后续错误大概率同源（Redis/DB 挂了），
			// 全部收集只会刷屏，继续把能建的表建完更重要。
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
//
// 构建落点在 :building 临时 key，完成后 RENAME 原子切换——查询端全程
// 看到的要么是旧完整集合、要么是新完整集合，不存在中间态。ready 置位
// 是「构建完成」的提交点：置位前 Exists 回落 DB，置位后才信任集合。
// 幂等：已 ready 直接返回现有数量（重复构建零成本）。
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
		// database/sql 扫出的是 []int64，SAdd 变参要 []any——逐元素
		// 转换；每批一批，避免一次把全表 ID 推进单条命令。
		args := make([]any, len(ids))
		for i, id := range ids {
			args[i] = id
		}
		if err := t.rdb.SAdd(ctx, building, args...).Err(); err != nil {
			return total, fmt.Errorf("sadd %s: %w", meta.Name, err)
		}
		total += int64(len(ids))
		lastID = ids[len(ids)-1]
		// 不满一页 = 扫到尾了；若正好满页则可能还有下一页，继续
		if int64(len(ids)) < t.pageSize {
			break
		}
	}

	// 原子切换：RENAME 覆盖旧集合，避免构建期间「集合半空 → 存在性判断误判为不存在」。
	//
	// 空表要特判：一行都没有时 building 从未被创建，RENAME 会返回
	// "ERR no such key"，导致空表的 ready 永远置不上（启动日志刷一屏构建失败）。
	// 取舍：空表直接删正式 key（不存在即「都不存在」），与「集合为空」
	// 语义等价。
	if total == 0 {
		_ = t.rdb.Del(ctx, IDTabKey(meta.Name)).Err()
	} else if err := t.rdb.Rename(ctx, building, IDTabKey(meta.Name)).Err(); err != nil {
		return total, fmt.Errorf("rename %s: %w", meta.Name, err)
	}
	// ready 是提交点：置位失败时上面的集合已就位但业务仍回落 DB，
	// 正确性不受影响，下次构建/Reconcile 会补上。
	if err := t.rdb.Set(ctx, IDTabReadyKey(meta.Name), "1", 0).Err(); err != nil {
		return total, fmt.Errorf("set ready %s: %w", meta.Name, err)
	}
	return total, nil
}

// queryIDs keyset 分页取一批主键。
//
// after 是上一批最后一个 ID（首轮传 0）；`pk > after` 走主键索引顺序
// 扫描，深页不退化。
// Go 知识点：defer rows.Close() 保证任何返回路径都释放连接——
// database/sql 的 Rows 不 Close 会一直占用池里一条连接，泄漏到
// MaxOpen 打满后整个服务卡死在等连接上。
func (t *IDTable) queryIDs(ctx context.Context, meta TableMeta, after int64) ([]int64, error) {
	//nolint:gosec // 表名/列名经 validateMeta 白名单校验，非用户输入
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
		meta.PK, meta.Name, meta.PK, meta.PK)
	rows, err := t.db.QueryContext(ctx, q, after, t.pageSize)
	if err != nil {
		return nil, fmt.Errorf("query ids %s: %w", meta.Name, err)
	}
	defer func() { _ = rows.Close() }()

	// 预分配 pageSize 容量：正常情况下正好一页，避免 append 扩容
	ids := make([]int64, 0, t.pageSize)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	// rows.Err() 必须查：Next 返回 false 可能是遍历结束，也可能是
	// 中途出错——不查就会把「读了一半」当「读完了」。
	return ids, rows.Err()
}

// Ready 该表 ID 集合是否已构建完成。
//
// ready key 无 TTL（不过期）：一旦构建完成就长期有效，直到 Reconcile
// 主动清除触发重建。
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
//
// 容错取舍：Redis 查询出错时不返回 error 而是继续回落 DB——存在性
// 判断的正确性优先于性能，缓存抖动不应让业务报错。
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
//
// SELECT 1 ... LIMIT 1：只取常量列、命中即停——存在性判断的网络与
// 解析开销最小。
// Go 知识点：sql.ErrNoRows 是 database/sql 的哨兵错误，「查无此行」
// 不是故障；这里 == 判定后转换成 (false, nil)，让「不存在」以正常
// 结果而非错误返回（Go 1.13+ 亦可写 errors.Is，语义相同）。
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
// 调用时序：先落库成功、再 Add——反序的话库写入失败会留下「Redis
// 有、库里无」的幽灵 ID。
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
//
// 与 Add 同样先库后缓存；SRem 之后顺手删实体 hash——否则会留下
// 「集合没有、hash 还在」的孤儿数据（12.5.3 孤儿清理的即时部分，
// 定时 CleanOrphans 兜底）。
// Del 的错误被忽略：这是锦上添花的清理，失败有定时任务兜底，不该
// 让主流程（删除记录）失败。
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
//
// SCard 是 O(1)（Redis 直接存基数），用于 Reconcile 对账与监控。
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
// 为什么用「数量」而不是全量比对集合：一次 COUNT(*) 轻量查询即可
// 发现大部分漂移（漏删/漏增），全量比对代价高一个量级；低频任务
// 可接受这种近似。
// 重建策略：先清 ready 再 BuildOne——重建期间 Exists 自动回落 DB，
// 用短时间的性能换「集合与库最终一致」。
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
//
// 顺序理由：先查 hash 再判 idtab——命中 hash 时两次 Redis 调用全省
// 掉；idtab 缺失是权威的「不存在」，此时连 DB 都不用打。
// 回填失败静默忽略：缓存是优化不是权威，下次访问再补。
// 返回值是 map（列名→字符串值），调用方按需解析具体类型。
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
//
// SELECT * + rows.Columns() 动态列：表结构演进（加列）不需要改代码。
// Go 知识点：database/sql 的 Scan 需要「指针的 slice」——先建 vals
// （装值）再建 ptrs（装 &vals[i]），是动态列扫描的惯用法；驱动返回
// 的具体类型依列类型而定（[]byte/int64/string/...），统一交 toString
// 归一。
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
// Go 知识点：SCAN 的游标是**增量式**的——cursor 从 0 开始，返回的
// next 非 0 表示还没扫完，下一轮带 next 继续；next 为 0 才是遍历完成
// （Redis 不保证一次扫描覆盖全量，这是哈希表渐进式 rehash 的设计
// 使然，所以必须循环到 0，且期间可能有 key 被重复返回）。
func (t *IDTable) CleanOrphans(ctx context.Context, table string, limit int64) (int64, error) {
	if t.rdb == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = 5000
	}
	// 复用 EntityKey 再截尾拼 *：e:{table}:0 → e:{table}:*，
	// 避免手写前缀与 keygen 不一致
	pattern := EntityKey(table, 0)
	pattern = strings.TrimSuffix(pattern, "0") + "*"

	var (
		cursor           uint64
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
//
// 哨兵错误：调用方用 errors.Is(err, cache.ErrNotFound) 与「Redis/DB
// 故障」区分开——前者是正常业务结果，后者要重试/降级。
var ErrNotFound = fmt.Errorf("entity not found")

// EntityRow 一行实体数据（动态列：列名 → 字符串值）。
//
// 与 LoadEntity 返回的 map 同一约定——表结构演进（加列）不需要改代码，
// 调用方按需解析具体类型。批量加载（OwnerIndex.LoadBatch / Preheat）
// 用它携带「哪一行」：ID 是 key 列的值。
type EntityRow struct {
	ID     int64
	Fields map[string]string
}

// Preheat 启动预热：把整表实体行灌进 Redis hash（preload_all 映射用）。
//
// 与 LoadEntity 的分工：LoadEntity 是「按需回源」（登录/访问时逐个补），
// Preheat 是「启动时全量」——服务公会/榜单这类「上线后几乎必然被全量
// 访问」的小表，省掉每个玩家登录时的回源。大表不要配 preload_all：
// 全表扫描 + 全量 HSET 会把启动拉长，内存占用线性于表大小。
//
// 分页用 keyset（pk > after），与 BuildOne 同一套理由（深翻页不退化）。
// 幂等：重复预热只是重复 HSET 同一批值；已有的行不会被清掉。
func (t *IDTable) Preheat(ctx context.Context, meta TableMeta) (int64, error) {
	if err := validateMeta(meta); err != nil {
		return 0, err
	}
	if t.rdb == nil || t.db == nil {
		return 0, nil
	}

	var (
		after int64
		total int64
	)
	for {
		//nolint:gosec // 表名/列名经 validateMeta 白名单校验
		q := fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
			meta.Name, meta.PK, meta.PK)
		rows, err := t.db.QueryContext(ctx, q, after, t.pageSize)
		if err != nil {
			return total, fmt.Errorf("preheat scan %s: %w", meta.Name, err)
		}
		entityRows, err := scanEntityRows(rows, meta.PK)
		_ = rows.Close()
		if err != nil {
			return total, err
		}
		if len(entityRows) == 0 {
			break
		}

		// 合批 HSET：一次往返写一页，行数受 pageSize 控制
		pipe := t.rdb.Pipeline()
		for _, r := range entityRows {
			pipe.HSet(ctx, EntityKey(meta.Name, r.ID), stringMapToAny(r.Fields))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return total, fmt.Errorf("preheat hset %s: %w", meta.Name, err)
		}
		total += int64(len(entityRows))
		after = entityRows[len(entityRows)-1].ID
		if int64(len(entityRows)) < t.pageSize {
			break
		}
	}
	return total, nil
}

// scanEntityRows 动态列扫描：把结果集转成 []EntityRow。
//
// keyCol 是主键列——它的值同时作为 EntityRow.ID（驱动对整型列返回
// int64，toString 归一后 ParseInt 回来）。LoadEntity 的 loadRow 是
// 单行版；这里是批量版，OwnerIndex.LoadBatch 与 Preheat 共用。
func scanEntityRows(rows *sql.Rows, keyCol string) ([]EntityRow, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([]EntityRow, 0, 8)
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := EntityRow{Fields: make(map[string]string, len(cols))}
		for i, c := range cols {
			row.Fields[c] = toString(vals[i])
		}
		id, err := strconv.ParseInt(row.Fields[keyCol], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad %s value %q", keyCol, row.Fields[keyCol])
		}
		row.ID = id
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// validateMeta 校验表名与主键列名。
//
// 两个标识符都要过：BuildOne/loadRow/Reconcile 的动态 SQL 同时拼了
// 这两个。
func validateMeta(m TableMeta) error {
	if err := validateIdent(m.Name); err != nil {
		return err
	}
	return validateIdent(m.PK)
}

// validateIdent 标识符白名单校验（防 SQL 注入）。
//
// 为什么不能参数化：SQL 参数占位符只能绑定**值**，表名列名这类标识符
// 无法参数化，动态拼接场景只能白名单校验。
func validateIdent(s string) error {
	if !identPattern.MatchString(s) {
		return fmt.Errorf("invalid identifier %q", s)
	}
	return nil
}

// parseEntityID 从 `e:{table}:{id}` 解析 id。
//
// 返回 0 表示「不是本表的实体 key 或解析失败」——调用方跳过而非报错：
// SCAN 的 pattern 可能带上同前缀的其他 key。
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
//
// HSet(key, field, value, field, value...) 需要扁平参数，这里把 map
// 展成 [k1, v1, k2, v2, ...]。
// Go 知识点：map 的 range 顺序是**随机**的——这里无所谓，Redis hash
// 本身无序，字段写入顺序不影响语义。
func stringMapToAny(m map[string]string) []any {
	out := make([]any, 0, len(m)*2)
	for k, v := range m {
		out = append(out, k, v)
	}
	return out
}

// toString 把数据库列值转成字符串（回填 Redis 用）。
//
// database/sql 默认把字节类列扫成 []byte（拷贝自驱动缓冲，可安全持有）、
// 整型扫成 int64 等；这里统一归一成 string。
// Go 知识点：类型 switch 的 case nil 匹配「接口本身为 nil」，要放在
// 最前（nil 不匹配其他具体类型 case）；[]byte → string 是值转换会
// 拷贝一份，不共享底层数组，后续改写原切片不影响已生成的字符串。
func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(x)
	case string:
		return x
	case time.Time:
		// 时间列必须显式格式化：fmt.Sprint 会产出
		// "2026-10-26 22:00:00 +0800 CST"，其中的 CST 无法被 time.Parse
		// 还原（Go 会当成零偏移的未知时区）。RFC3339 带偏移量，可无损往返。
		//
		// 触发条件：DSN 开了 parseTime=true（本项目如此），驱动把
		// DATETIME/TIMESTAMP 直接交给 time.Time。
		return x.Format(time.RFC3339)
	default:
		return fmt.Sprint(x)
	}
}
