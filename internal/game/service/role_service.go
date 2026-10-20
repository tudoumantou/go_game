// internal/game/service/role_service.go —— 角色创建与查询（进游戏前的会话级操作）
package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"game_study/pkg/cache"
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
	db      *db.DB
	logger  *log.Logger
	idTable *cache.IDTable // 可选：创角后登记 ID 表（12.5）
}

// NewRoleService 创建服务。
func NewRoleService(database *db.DB, logger *log.Logger) *RoleService {
	return &RoleService{db: database, logger: logger}
}

// SetIDTable 注入 ID 表（12.5：创建记录后同步登记，避免对账窗口内判断为「不存在」）。
func (s *RoleService) SetIDTable(t *cache.IDTable) { s.idTable = t }

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

// Create 创建角色：校验名字/职业 → 事务插入 player + player_attr + 初始背包。
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
	items := initialItems(s.logger) // 配置只读一次，事务内复用（热更新不会在本事务中途换表）
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

		// 初始背包与角色同事务写入：要么角色和初始道具一起出现，要么都不出现，
		// 不会留下「有角色但背包空」的半成品（客户端首屏会少展示初始道具）。
		const insItem = `INSERT INTO player_item (player_id, item_id, count) VALUES (?, ?, ?)`
		for _, it := range items {
			if _, err := tx.ExecContext(ctx, insItem, roleID, it.itemID, it.count); err != nil {
				return fmt.Errorf("insert initial item %d: %w", it.itemID, err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	// 登记 ID 表：不阻塞创角返回，失败也只是等下次对账补上
	if s.idTable != nil {
		if err := s.idTable.Add(ctx, "player", roleID); err != nil {
			s.logger.Warn("idtab add player failed", "player_id", roleID, "err", err)
		}
	}

	s.logger.Info("role created", "role_id", roleID, "account_id", accountID, "career", career, "init_items", len(items))
	return roleID, nil
}

// initItem 一行初始道具。
type initItem struct {
	itemID int32
	count  int64
}

// initialItems 读取 init_bag.xml，返回创角时要写入背包的道具。
//
// 容错策略：配置行非法（ID/数量 <= 0）、道具不存在时只告警跳过，不阻断创角——
// 创角失败的影响远大于少发一件新手道具；错误配置应在启动/发布期通过日志发现。
// 同一道具多行会合并，并按 item.xml 的堆叠上限截断，避免写出非法数量。
// 结果按 item_id 排序，保证 SQL 顺序稳定（便于测试与排查）。
func initialItems(logger *log.Logger) []initItem {
	return buildInitialItems(logger, gamedata.InitBagCfgList())
}

// buildInitialItems 与 initialItems 同逻辑，但配置行由调用方传入，便于单测覆盖
// 重复行合并、堆叠上限截断、未知道具跳过等边界。
func buildInitialItems(logger *log.Logger, rows []gamedata.InitBagCfg) []initItem {
	merged := make(map[int32]int64)
	for _, cfg := range rows {
		itemID, count := int32(cfg.Col1), cfg.Col2
		if itemID <= 0 || count <= 0 {
			logger.Warn("init bag: invalid row", "row_id", cfg.Id, "item_id", itemID, "count", count)
			continue
		}
		item, ok := gamedata.GetItemCfg(int64(itemID))
		if !ok {
			logger.Warn("init bag: item not in item.xml", "row_id", cfg.Id, "item_id", itemID)
			continue
		}
		merged[itemID] += count
		if item.Col2 > 0 && merged[itemID] > item.Col2 {
			merged[itemID] = item.Col2
		}
	}
	out := make([]initItem, 0, len(merged))
	for id, n := range merged {
		out = append(out, initItem{itemID: id, count: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].itemID < out[j].itemID })
	return out
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
