// internal/game/player/role_store.go —— 角色域的持久化实现（业务层与 SQL 的边界）
//
// 架构职责：service.RoleStore 的唯一生产实现。业务侧（internal/game/service）
// 只认「按账号查角色」「原子建角色」两个动作，本文件负责把它们翻译成
// gendb 生成的 CRUD——本包是唯一允许出现数据库驱动与表结构知识的地方。
//
// 为什么放在 player 包：角色三表（player / player_attr / player_item）与
// 玩家状态仓储（Repository）是同一份数据的两个视角——登录前按账号查、
// 进游戏后按玩家 ID 读写。放在一处，表结构知识只此一份。
//
// 依赖方向：player → service（实现其接口，Go 的依赖倒置惯例），
// service 不反向依赖 player，因此不存在循环。
package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/internal/game/service"
	"game_study/pkg/db"
	"game_study/pkg/model"
	"game_study/pkg/sqlstore"
)

// role_sync_outbox 状态机（与 DDL 注释一致）。
//
// 定义在这里而不是 model 包：model 全是生成物，手写常量会在下次 gendb
// 覆盖时消失；而这张表的使用方只有 player（写）与 mq（投递），
// player 是两者的共同上游，放在这里不会引入反向依赖。
const (
	// RoleOutboxPending 待投递。
	RoleOutboxPending int32 = 0
	// RoleOutboxPublished 已投递成功，闲置行。
	RoleOutboxPublished int32 = 1
	// RoleOutboxDead 重试耗尽，需人工介入（告警后不再自动重试，
	// 留着不删是为了让「哪些角色没同步」可查）。
	RoleOutboxDead int32 = 2
)

// RoleStore 角色持久化（实现 service.RoleStore）。
type RoleStore struct {
	*sqlstore.Base
	// serverID 本区服 ID，随创角事件写进 user_role.server_id。
	// 由配置注入而非从库读：区服身份是部署事实，不该依赖某张能改的表。
	serverID int32
}

// NewRoleStore 创建角色仓储。
func NewRoleStore(database *db.DB, serverID int32) *RoleStore {
	return &RoleStore{Base: sqlstore.New(database, "player_role"), serverID: serverID}
}

// FindByAccount 返回账号下最早创建的角色；无角色返回 service.ErrNoRole。
//
// 翻译职责在这里：sql.ErrNoRows 是驱动事实，ErrNoRole 是业务语义；
// 让 service 认识 sql 包就等于把驱动细节漏进业务层（这与本轮整改的
// 目标相反）。ORDER BY id LIMIT 1 由生成器保证（@findby:account_id）。
func (s *RoleStore) FindByAccount(ctx context.Context, accountID int64) (*model.Player, error) {
	p, err := model.FindPlayerByAccountId(ctx, s.SQL(), accountID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrNoRole
	}
	if err != nil {
		return nil, fmt.Errorf("find role by account: %w", err)
	}
	return p, nil
}

// Create 原子写 player + player_attr + player_item + role_sync_outbox，
// 返回新角色 ID。
//
// 四张表一个事务：创角是「要么全出现、要么全不出现」的复合写入，半成品
// 角色（有 player 行但没 attr/item）会让客户端首屏异常。
//
// outbox 行也必须在这个事务里——它是「角色已存在」这一事实向登录服传播的
// 唯一凭据，晚一步写就存在「角色在、索引不在」的窗口，而这个窗口没有
// 任何自愈路径（见 role_store.go 里 Outbox 段注释）。
//
// SQL 全部来自 gendb 生成的函数（InsertPlayer / InsertPlayerAttr /
// UpsertPlayerItemBatch），本文件不出现 SQL 字符串——改表结构只需重新
// 生成，编译期就能发现调用点变化。
//
// 唯一键冲突翻译：player 表只有 uk_name（id 是自增主键）与 uk_name 冲突
// 只可能是重名，因此 1062 一律翻译成 ErrNameUsed。用 db.IsDuplicate 判定
// 而不是匹配错误文本，换驱动版本也不会失效。
func (s *RoleStore) Create(ctx context.Context, d service.RoleDraft) (int64, error) {
	var roleID int64
	err := s.Tx(ctx, func(tx *sql.Tx) error {
		// 角色主行：Id 留零值，由 MySQL 自增分配（生成的 INSERT 显式列出 id 列，
		// 0 在自增列上等价于「不指定」，与 InsertAccount 的既有用法一致）
		res, err := model.InsertPlayer(ctx, tx, &model.Player{
			AccountId: d.AccountID,
			Name:      d.Name,
			Level:     1,
			Exp:       0,
			Coin:      0,
			SceneId:   0,
		})
		if err != nil {
			if db.IsDuplicate(err) {
				return service.ErrNameUsed
			}
			return fmt.Errorf("insert player: %w", err)
		}
		// LastInsertId 取自增主键：事务内已插入，必然有效
		roleID, err = res.LastInsertId()
		if err != nil {
			return fmt.Errorf("last insert id: %w", err)
		}

		if _, err := model.InsertPlayerAttr(ctx, tx, &model.PlayerAttr{
			PlayerId: roleID,
			Career:   d.Career,
			Hp:       d.Hp,
			Mp:       d.Mp,
			Atk:      d.Atk,
			Def:      d.Def,
			Spd:      d.Spd,
		}); err != nil {
			return fmt.Errorf("insert player_attr: %w", err)
		}

		// 初始背包走批量 UPSERT：draft.Items 已按 item_id 升序、去重、按堆叠
		// 上限截断（service 侧保证），这里只做模型映射。空背包直接跳过——
		// 生成函数在空切片上也会拼出非法 SQL。
		if len(d.Items) > 0 {
			rows := make([]model.PlayerItem, 0, len(d.Items))
			for _, it := range d.Items {
				rows = append(rows, model.PlayerItem{
					PlayerId: roleID,
					ItemId:   it.ItemID,
					Count:    int32(it.Count),
				})
			}
			if err := model.UpsertPlayerItemBatch(ctx, tx, rows); err != nil {
				return fmt.Errorf("insert initial items: %w", err)
			}
		}
		// 角色索引同步事件，与上面三张表同一事务。
		//
		// 为什么不在事务外、也不在这里直接发 Kafka：事务提交成功但消息没发出去，
		// 玩家有角色而选角列表里没有，且没有任何路径能自愈。写 outbox 让
		// 「有角色行」和「有待投递事件」成为同一个原子事实，投递失败退化成
		// 延迟而不是丢失。真正发消息由 mq.RoleOutboxDispatcher 负责。
		//
		// Level 与上面 InsertPlayer 的 1 必须一致：这里是选角列表的展示冗余，
		// 与主行不同步会让玩家看到「0 级角色」。
		if _, err := model.InsertRoleSyncOutbox(ctx, tx, &model.RoleSyncOutbox{
			PlayerId:  roleID,
			AccountId: d.AccountID,
			ServerId:  s.serverID,
			Nickname:  d.Name,
			Level:     1,
			Status:    RoleOutboxPending,
			CreatedAt: time.Now().UnixMilli(),
		}); err != nil {
			return fmt.Errorf("insert role_sync_outbox: %w", err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return roleID, nil
}
