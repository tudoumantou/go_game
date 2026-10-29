// pkg/db/db.go —— MySQL 连接池封装
//
// 职责：把 database/sql 的连接池参数化、启动探活，并提供事务模板 Tx。
// 协作关系：被各进程的存储初始化代码调用一次，产出的 *DB 再传给
// flush/writebehind/gendb 生成代码；本包不关心表结构（见 tools/gendb）。
package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Config MySQL 配置。
//
// 字段语义：
//   - DSN：go-sql-driver 连接串（user:pass@tcp(host:port)/db?params）；
//   - MaxOpen/MaxIdle：连接池上限/常驻空闲数，按设计文档第 14 章按进程类型配置；
//   - ConnMaxLife：单连接最长存活，促使驱动定期重建连接，规避 MySQL
//     wait_timeout 后「连接已被服务端关闭」的偶发报错。
//
// 零值语义：三个池参数 <=0 时保持 database/sql 默认（不限并发/默认 2 个
// 空闲/不限寿命）——生产必须显式设置，否则一个慢查询洪峰就能打爆 DB。
type Config struct {
	// DSN 数据源名称；为空时 New 直接报错（空 DSN 是配置遗漏，不是运行故障）。
	DSN string
	// MaxOpen 最大打开连接数；0 表示不限制（生产必须显式设置）。
	MaxOpen int
	// MaxIdle 最大空闲连接数；应略小于 MaxOpen，留出突发余量。
	MaxIdle int
	// ConnMaxLife 连接最长寿命；0 表示永久复用。
	ConnMaxLife time.Duration
}

// DB 包装 *sql.DB。
//
// 采用**类型嵌入**（匿名字段）而非命名字段：*sql.DB 的全部方法
// （Query/Exec/BeginTx/Ping…）自动提升为 DB 的方法，调用方无感知；
// 同时保留在外层包上挂新方法（如 Tx）的能力。嵌入的是指针，
// DB 自身按值/指针传递都安全，多个 DB 共享同一底层池。
type DB struct {
	*sql.DB
}

// New 创建 MySQL 连接池并 Ping。
//
// 流程：sql.Open 注册池（惰性，不建连）→ 应用池参数 → PingContext 探活。
// 错误语义：DSN 为空、Open 失败、Ping 失败均返回包装错误（%w 保链）；
// Ping 失败时先 Close 再返回，避免泄漏一个半初始化的池。
//
// Go 细节：
//   - sql.Open 并不连接数据库，第一次查询/Ping 时才真正建连，所以 DSN
//     语法错误可以延迟到 Ping 暴露；
//   - context.WithTimeout + defer cancel 给探活加 5s 上限；cancel 必须
//     defer，否则 context 树泄漏（关联定时器要等超时才释放）；
//   - 5s 是启动期一次性开销，宁短勿长：DB 不可用时快速失败，让守护
//     进程尽快重试或摘除节点。
func New(cfg Config) (*DB, error) {
	if cfg.DSN == "" {
		return nil, fmt.Errorf("mysql dsn is empty")
	}
	sqlDB, err := sql.Open("mysql", normalizeDSN(cfg.DSN))
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	if cfg.MaxOpen > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpen)
	}
	if cfg.MaxIdle > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdle)
	}
	if cfg.ConnMaxLife > 0 {
		sqlDB.SetConnMaxLifetime(cfg.ConnMaxLife)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		// 探活失败必须 Close：sql.Open 拿到的池即使没建过连接也占着
		// 驱动与后台连接清理 goroutine，直接返回即泄漏
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &DB{DB: sqlDB}, nil
}

// normalizeDSN 把会话时区钉死成 UTC，与驱动解析时间列时用的 loc 对齐。
//
// 背景：`parseTime=true` 时驱动按 DSN 的 `loc` 解析时间列（**默认 UTC**），
// 而 MySQL 的 `CURRENT_TIMESTAMP`/`NOW()` 用的是**服务器/会话**的
// `time_zone`。两者不一致时，所有时间戳整体偏移：
//
//	MySQL +08:00 存 22:00 → 驱动按 UTC 解析成 22:00Z
//	                      → 比真实时刻晚 8 小时（而且不报错，只是错）
//
// 靠「服务器默认是 UTC」来对齐太脆弱——运维改一次 `my.cnf` 就全错。这里
// 显式设会话时区，谁配的服务器都不影响。
//
// 为什么选 UTC 而不是 +08:00：驱动要认 `Asia/Shanghai` 必须靠 IANA 时区库，
// 而运行时镜像（distroless）没有 tzdata；引 `time/tzdata` 又要为 8 小时偏移
// 塞进 450KB。业务侧需要北京时间的地方已经有 `timeutil.BusinessZone`
// （FixedZone，不依赖 tzdata），所以存储层用 UTC 是最省的组合。
//
// 显式写了 `time_zone` 的 DSN 不动——那是调用方的显式选择。解析失败原样
// 返回，让 sql.Open/Ping 报出真实的 DSN 错误（不吞掉拼写问题）。
// defaultQueryTimeout 单条 SQL 的读写超时（驱动层兜底，见 normalizeDSN）。
//
// 5s 的取舍：正常查询在毫秒级，5s 只兜「网络黑洞 / 锁等待异常」这类卡死；
// 再短会误杀冷启动的批量读，再长则一个卡住的玩家要等太久才恢复。
const defaultQueryTimeout = 5 * time.Second

func normalizeDSN(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return dsn
	}
	if cfg.Params == nil {
		cfg.Params = make(map[string]string, 3)
	}
	if _, ok := cfg.Params["time_zone"]; !ok {
		cfg.Params["time_zone"] = "'+00:00'"
	}
	// 读写超时兜底：没有它，一条卡住的查询会永久占住玩家 Actor 线程
	// （Actor 串行，一个玩家的所有消息都排在后面）。调用方显式配了就不覆盖。
	//
	// 为什么放在 DSN 而不是逐个 handler 包 context：模块里的仓储调用有二十多处，
	// 逐个加 WithTimeout 既啰嗦又容易漏；驱动层的 readTimeout 覆盖「所有查询」，
	// 是零遗漏的兜底。handler 里更紧的 3s 仍然有效（谁先到谁生效）。
	// 用 Config 的字段而不是 Params：ParseDSN 会把 readTimeout/writeTimeout
	// 解析进这两个 time.Duration 字段，塞 Params 里反而会拼出无效参数。
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = defaultQueryTimeout
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = defaultQueryTimeout
	}
	return cfg.FormatDSN()
}

// Tx 事务辅助：fn 返回错误时回滚，否则提交。
//
// 语义：fn 内的多次执行共享一个 *sql.Tx，任一失败整体回滚保证原子性；
// fn 成功返回后提交。回滚错误用 _ 显式忽略——提交已失败时回滚多半也失败，
// 此时 fn 的原始错误更有诊断价值。
//
// Go 细节：
//   - database/sql 的 Tx 不是并发安全的，fn 内不得并发使用同一个 tx；
//   - 本模板没有 defer tx.Rollback()：若 fn panic，事务不会回滚、连接
//     不归还池，只能依赖上层 recover 或「fn 不 panic」的约定。需要
//     panic 安全的场景请在 fn 内部自行 recover（对比 worker.Persist
//     里 defer Rollback 的保险式写法）；
//   - ctx 取消会传播到 Tx 内每条语句（driver 收到即中断），停机时
//     不会留下跑不完的事务。
func (d *DB) Tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return d.TxWith(ctx, nil, fn)
}

// TxWith 带事务选项的事务模板（隔离级别 / 只读）。
//
// 补的是能力缺口而不是风格：只读一致性快照需要 ReadOnly + RepeatableRead，
// 原先这些调用点只能自己 BeginTx，于是「事务怎么起」又多了一种写法。
// 现在两个入口共用同一段收尾逻辑（错误即回滚、成功即提交）。
//
// 什么时候仍该自己写 BEGIN/COMMIT：需要**区分 COMMIT 的错误**（提交失败
// 意味着「结果未知」，与回滚后的「确定没写」是两种处置），或需要在重试
// 路径里复用同一个已构建的语句计划。本模板是黑盒，这两种需求表达不出来
// ——worldmap/store 的三处手写事务就属于此类，理由写在各自函数上。
func (d *DB) TxWith(ctx context.Context, opts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	tx, err := d.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
