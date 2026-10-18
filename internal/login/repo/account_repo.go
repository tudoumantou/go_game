// internal/login/repo/account_repo.go —— 账号仓储
package repo

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/model"
)

// AccountRepo 账号仓储。
type AccountRepo struct {
	db *db.DB
}

// NewAccountRepo 创建仓储。
func NewAccountRepo(database *db.DB) *AccountRepo {
	return &AccountRepo{db: database}
}

// FindOrCreateByDevice 按设备找账号，没有则创建游客账号。
func (r *AccountRepo) FindOrCreateByDevice(ctx context.Context, device string) (int64, error) {
	if device == "" {
		device = "anonymous"
	}

	const q = `SELECT id FROM account WHERE device = ? ORDER BY id LIMIT 1`
	var id int64
	err := r.db.QueryRowContext(ctx, q, device).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("query account by device: %w", err)
	}

	// 创建游客账号（密码占位，P7 接入渠道登录）
	suffix, err := randomHex(6)
	if err != nil {
		return 0, err
	}
	acc := &model.Account{
		Account:      "guest_" + suffix,
		PasswordHash: "",
		Salt:         "",
		Status:       0,
		Device:       device,
		LastLoginAt:  sql.NullTime{Time: time.Now(), Valid: true},
	}
	res, err := model.InsertAccount(ctx, r.db.DB, acc)
	if err != nil {
		return 0, fmt.Errorf("insert account: %w", err)
	}
	return res.LastInsertId()
}

// TouchLogin 更新最近登录时间。
func (r *AccountRepo) TouchLogin(ctx context.Context, accountID int64) error {
	const q = `UPDATE account SET last_login_at = ? WHERE id = ?`
	_, err := r.db.ExecContext(ctx, q, time.Now(), accountID)
	return err
}

func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
