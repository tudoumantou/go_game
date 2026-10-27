// internal/login/repo/user_role_repo.go —— 角色索引仓储（account_id%15 分片，R 账号中心）
//
// 架构职责：user_role 的读写与 Redis 缓存。角色真源在游戏服，本表是经 MQ
// 同步 + 回源兜底的轻量展示副本，用于选角界面列出某账号全部角色而无需
// 回源游戏服。按 account_id % 15 物理分片（user_role_0..14），同一账号的
// 全部角色落在同一分片，选角只需查一张表（见数据归属文档 §3）。
// 协作关系：游戏服创角/改名经 MQ 至少一次投递调 Upsert（幂等）；
// 登录服查某账号角色走 ListByAccount，miss 时回源游戏服 MySQL 兜底
// （设计文档 §5：禁止反向通知回写真源，但可读游戏服兜底）。
// 设计取舍：① 分片键 account_id%15 与表名零偏移对齐（0..14），路由是
// 确定性函数；② 缓存按账号缓存整张角色列表（一个账号角色数有限），
// 比逐角色缓存更省键、回源更少；③ account_id<=0 直接返回空列表，
// 未鉴权调用不穿透到 SQL；④ Redis nil 退化为直读。
package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"game_study/pkg/db"
	"game_study/pkg/model"
)

const (
	// userRoleShards 分片数（钉死 15，沿用约定，见文档 §3）。
	userRoleShards = 15
	userRolePrefix = "user_role:list:" // Redis 键前缀，后接 account_id
	userRoleTTL    = 10 * time.Minute
)

// UserRoleRepo 角色索引仓储。
type UserRoleRepo struct {
	db  *db.DB
	rdb redis.UniversalClient
}

// NewUserRoleRepo 创建仓储。
func NewUserRoleRepo(database *db.DB, rdb redis.UniversalClient) *UserRoleRepo {
	return &UserRoleRepo{db: database, rdb: rdb}
}

// shardUserRole 计算分片下标（0..14，与 DDL 物理表命名一致）。
func shardUserRole(accountID int64) int {
	return int(accountID % userRoleShards)
}

// userRoleTable 物理分片表名（运行时拼接，不在 gendb 解析范围）。
func userRoleTable(accountID int64) string {
	return fmt.Sprintf("user_role_%d", shardUserRole(accountID))
}

// userRoleKey Redis 键（按账号缓存整张角色列表）。
func userRoleKey(accountID int64) string {
	return userRolePrefix + strconv.FormatInt(accountID, 10)
}

// ListByAccount 列出某账号全部角色（选角界面用）。cache-aside，按账号缓存整列表。
// 未鉴权（accountID<=0）或账号无角色均返回空列表（非错误）。
func (r *UserRoleRepo) ListByAccount(ctx context.Context, accountID int64) ([]*model.UserRole, error) {
	if accountID <= 0 {
		return nil, nil
	}
	if r.rdb != nil {
		if raw, err := r.rdb.Get(ctx, userRoleKey(accountID)).Bytes(); err == nil {
			var out []*model.UserRole
			if json.Unmarshal(raw, &out) == nil {
				return out, nil
			}
		}
	}
	out, err := r.loadByAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if r.rdb != nil {
		if raw, mErr := json.Marshal(out); mErr == nil {
			_ = r.rdb.Set(ctx, userRoleKey(accountID), raw, userRoleTTL).Err()
		}
	}
	return out, nil
}

// loadByAccount 回源分片表（ORDER BY player_id 稳定顺序）。
func (r *UserRoleRepo) loadByAccount(ctx context.Context, accountID int64) ([]*model.UserRole, error) {
	const q = `SELECT account_id, player_id, server_id, nickname, level, created_at, updated_at
	           FROM %s WHERE account_id = ? ORDER BY player_id`
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(q, userRoleTable(accountID)), accountID)
	if err != nil {
		return nil, fmt.Errorf("list user_role: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []*model.UserRole
	for rows.Next() {
		var m model.UserRole
		if err := rows.Scan(&m.AccountId, &m.PlayerId, &m.ServerId,
			&m.Nickname, &m.Level, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &m)
	}
	return out, rows.Err()
}

// Upsert 同步一条角色索引（幂等）。游戏服创角/改名经 MQ 至少一次投递，重复安全。
// 字段级更新避免覆盖 server_id/nickname/level 之外的列；player_id 唯一键
// 保证同一角色在分片内唯一（角色全局唯一，必落在其 account_id 所在分片）。
func (r *UserRoleRepo) Upsert(ctx context.Context, m *model.UserRole) error {
	if m == nil || m.AccountId <= 0 {
		return fmt.Errorf("user_role upsert: invalid account_id")
	}
	const q = `INSERT INTO %s (account_id, player_id, server_id, nickname, level)
	           VALUES (?,?,?,?,?)
	           ON DUPLICATE KEY UPDATE server_id = VALUES(server_id),
	                                 nickname = VALUES(nickname), level = VALUES(level)`
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(q, userRoleTable(m.AccountId)),
		m.AccountId, m.PlayerId, m.ServerId, m.Nickname, m.Level)
	if err != nil {
		return fmt.Errorf("upsert user_role: %w", err)
	}
	if r.rdb != nil {
		_ = r.rdb.Del(ctx, userRoleKey(m.AccountId)).Err()
	}
	return nil
}
