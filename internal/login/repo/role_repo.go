// internal/login/repo/role_repo.go —— 账号→角色归属查询（下单鉴权用，R3.3）
package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/pkg/db"
)

// ErrNoRole 账号还没有角色。
var ErrNoRole = errors.New("account has no role")

// RoleRepo 角色归属只读查询。
//
// 登录服只做「账号是否拥有这个角色」的判断，不承担角色数据维护——
// 写入仍然只发生在游戏服。
type RoleRepo struct {
	db *db.DB
}

// NewRoleRepo 创建仓储。
func NewRoleRepo(database *db.DB) *RoleRepo {
	return &RoleRepo{db: database}
}

// FindByAccount 返回账号下最早创建的角色 ID；无角色返回 ErrNoRole。
func (r *RoleRepo) FindByAccount(ctx context.Context, accountID int64) (int64, error) {
	if accountID <= 0 {
		return 0, ErrNoRole
	}
	var playerID int64
	err := r.db.QueryRowContext(ctx,
		`SELECT id FROM player WHERE account_id = ? ORDER BY id LIMIT 1`, accountID).Scan(&playerID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNoRole
	}
	if err != nil {
		return 0, fmt.Errorf("find role by account: %w", err)
	}
	return playerID, nil
}
