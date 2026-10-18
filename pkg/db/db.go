// pkg/db/db.go —— MySQL 连接池封装
package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Config MySQL 配置。
type Config struct {
	DSN         string
	MaxOpen     int
	MaxIdle     int
	ConnMaxLife time.Duration
}

// DB 包装 *sql.DB。
type DB struct {
	*sql.DB
}

// New 创建 MySQL 连接池并 Ping。
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
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &DB{DB: sqlDB}, nil
}

// Tx 事务辅助：fn 返回错误时回滚，否则提交。
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
