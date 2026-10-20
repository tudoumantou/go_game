// internal/login/repo/role_repo.go —— 账号→角色归属查询（下单鉴权用，R3.3）
//
// 架构职责：回答「这个账号有没有角色、角色 ID 是多少」——充值下单
// 的归属校验唯一数据源。登录服只读 player 表，不写。
// 协作关系：handler.RechargeHandler.CreateOrder 在 token 鉴权后调
// FindByAccount 把 account_id 落到 player_id；service 层不参与
// （归属是纯查询，无业务规则）。
// 设计取舍：① 登录服不维护角色数据——写入只发生在游戏服，这里只做
// 只读归属判断，避免两个服务同时写 player 表；② 取「最早创建的
// 角色」而非报错列表——当前一个账号单角色是主流，多角色时取
// 首角是确定性的（ORDER BY id 稳定），调用方无需处理歧义；
// ③ account_id<=0 直接返回 ErrNoRole——未鉴权调用不应穿透到 SQL。
package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/pkg/db"
)

// ErrNoRole 账号还没有角色。
// 哨兵错误：handler 用 errors.Is 把它映射为 403（区别于 500），
// 客户端据此引导玩家「先进游戏创建角色」。
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
// ORDER BY id LIMIT 1：id 自增即创建顺序，「最早」是确定值；
// 并发创角时先提交的先返回，不会在多个角色间摇摆。
func (r *RoleRepo) FindByAccount(ctx context.Context, accountID int64) (int64, error) {
	// 防御性短路：account_id 非法时不问库——既省一次往返，
	// 也防止脏 ID 进入 SQL 层（参数化已防注入，但语义上
	// 「<=0 的账号」不可能有角色）
	if accountID <= 0 {
		return 0, ErrNoRole
	}
	var playerID int64
	err := r.db.QueryRowContext(ctx,
		`SELECT id FROM player WHERE account_id = ? ORDER BY id LIMIT 1`, accountID).Scan(&playerID)
	// ErrNoRows → 领域错误：调用方（handler）只认 ErrNoRole，
	// 不知道底层是 database/sql——仓储层的职责就是翻译驱动错误
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNoRole
	}
	if err != nil {
		return 0, fmt.Errorf("find role by account: %w", err)
	}
	return playerID, nil
}
