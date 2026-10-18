// internal/game/service/role_service.go —— 角色创建与查询（进游戏前的会话级操作）
package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// 角色服务错误（对应 4.10 错误码 2003-2005）。
var (
	ErrNoRole      = errors.New("no role")
	ErrNameUsed    = errors.New("name already used")
	ErrNameIllegal = errors.New("name illegal")
	ErrCareerBad   = errors.New("career not exist")
)

// 角色名规则：2-16 字符，中英文/数字，禁止空白与特殊符号。
var namePattern = regexp.MustCompile(`^[\p{Han}A-Za-z0-9]{2,16}$`)

// RoleService 角色服务。
type RoleService struct {
	db     *db.DB
	logger *log.Logger
}

// NewRoleService 创建服务。
func NewRoleService(database *db.DB, logger *log.Logger) *RoleService {
	return &RoleService{db: database, logger: logger}
}

// FindByAccount 查账号下角色；无角色返回 ErrNoRole。
func (s *RoleService) FindByAccount(ctx context.Context, accountID int64) (*model.Player, error) {
	const q = `SELECT id, account_id, name, level, exp, coin, scene_id, created_at, updated_at
	           FROM player WHERE account_id = ? ORDER BY id LIMIT 1`
	var p model.Player
	err := s.db.QueryRowContext(ctx, q, accountID).Scan(
		&p.Id, &p.AccountId, &p.Name, &p.Level, &p.Exp, &p.Coin, &p.SceneId, &p.CreatedAt, &p.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoRole
	}
	if err != nil {
		return nil, fmt.Errorf("query player: %w", err)
	}
	return &p, nil
}

// Create 创建角色：校验名字/职业 → 事务插入 player + player_attr。
func (s *RoleService) Create(ctx context.Context, accountID int64, name string, career int32) (int64, error) {
	name = strings.TrimSpace(name)
	if !namePattern.MatchString(name) || utf8.RuneCountInString(name) > 16 {
		return 0, ErrNameIllegal
	}
	if _, ok := gamedata.GetCareerCfg(int64(career)); !ok {
		return 0, ErrCareerBad
	}

	// 一个账号只允许一个角色（P6 简化，P7 支持多角色选择）
	if _, err := s.FindByAccount(ctx, accountID); err == nil {
		return 0, fmt.Errorf("account %d already has a role", accountID)
	} else if !errors.Is(err, ErrNoRole) {
		return 0, err
	}

	var roleID int64
	err := s.db.Tx(ctx, func(tx *sql.Tx) error {
		const insPlayer = `INSERT INTO player (account_id, name, level, exp, coin, scene_id) VALUES (?, ?, 1, 0, 0, 0)`
		res, err := tx.ExecContext(ctx, insPlayer, accountID, name)
		if err != nil {
			var me *mysqlDriver.MySQLError
			if errors.As(err, &me) && me.Number == 1062 {
				return ErrNameUsed
			}
			return fmt.Errorf("insert player: %w", err)
		}
		roleID, err = res.LastInsertId()
		if err != nil {
			return err
		}

		hp, mp, atk, def, spd := baseAttrs(career)
		const insAttr = `INSERT INTO player_attr (player_id, career, hp, mp, atk, def, spd) VALUES (?, ?, ?, ?, ?, ?, ?)`
		if _, err := tx.ExecContext(ctx, insAttr, roleID, career, hp, mp, atk, def, spd); err != nil {
			return fmt.Errorf("insert player_attr: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	s.logger.Info("role created", "role_id", roleID, "account_id", accountID, "career", career)
	return roleID, nil
}

// baseAttrs 职业初始属性（P6 内置表；P7 改读 attr_formula.xml）。
func baseAttrs(career int32) (hp, mp, atk, def, spd int32) {
	switch career {
	case 1: // 战士
		return 1000, 100, 50, 30, 10
	case 2: // 法师
		return 600, 300, 80, 15, 12
	default:
		return 800, 150, 40, 20, 10
	}
}
