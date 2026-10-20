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

	_ "github.com/go-sql-driver/mysql"
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
	sqlDB, err := sql.Open("mysql", cfg.DSN)
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
	tx, err := d.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
