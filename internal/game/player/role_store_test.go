// internal/game/player/role_store_test.go —— 角色持久化的 SQL 契约测试
//
// 测试对象从「业务逻辑」换成了「生成代码的使用方式」：SQL 由 gendb 生成，
// 这里盯的是三件事——用对了函数、参数顺序对、错误翻译对（1062→ErrNameUsed，
// ErrNoRows→ErrNoRole）。业务规则（名字/职业/初始道具）的测试在 service 包里，
// 那边不再碰 SQL。
package player

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"game_study/internal/game/service"
	"game_study/pkg/db"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// newRoleStore 起一个 sqlmock 支撑的仓储。
func newRoleStore(t *testing.T) (*RoleStore, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return NewRoleStore(&db.DB{DB: sqlDB}), mock
}

// TestRoleStoreFindByAccount 正常命中与「无行」两条路径：
// 无行必须翻译成 service.ErrNoRole（业务层不认 sql.ErrNoRows）。
func TestRoleStoreFindByAccount(t *testing.T) {
	store, mock := newRoleStore(t)
	const accountID = int64(1001)
	now := time.Now()

	mock.ExpectQuery(`SELECT id,account_id,name,level,exp,coin,scene_id,created_at,updated_at FROM player WHERE account_id = \? ORDER BY id LIMIT 1`).
		WithArgs(accountID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at"}).
			AddRow(10001, accountID, "张三", 1, 0, 0, 0, now, now))

	p, err := store.FindByAccount(context.Background(), accountID)
	require.NoError(t, err)
	require.EqualValues(t, 10001, p.Id)
	require.Equal(t, "张三", p.Name)

	mock.ExpectQuery("FROM player WHERE account_id").
		WithArgs(accountID).
		WillReturnError(sql.ErrNoRows)
	_, err = store.FindByAccount(context.Background(), accountID)
	require.ErrorIs(t, err, service.ErrNoRole)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRoleStoreCreateCommits 创角事务的完整序列：
// player（自增 ID 回填）→ player_attr → 初始道具（批量 UPSERT）→ 提交。
func TestRoleStoreCreateCommits(t *testing.T) {
	store, mock := newRoleStore(t)
	const roleID = int64(10001)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO player \(id,account_id,name,level,exp,coin,scene_id\) VALUES \(\?,\?,\?,\?,\?,\?,\?\)`).
		WithArgs(int64(0), int64(1001), "张三", int32(1), int64(0), int64(0), int32(0)).
		WillReturnResult(sqlmock.NewResult(roleID, 1))
	mock.ExpectExec(`INSERT INTO player_attr \(player_id,career,hp,mp,atk,def,spd\) VALUES \(\?,\?,\?,\?,\?,\?,\?\)`).
		WithArgs(roleID, int32(1), int32(1000), int32(100), int32(50), int32(30), int32(10)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 初始道具走批量 UPSERT：两行合成一条语句，expire_at 由 sql.NullTime 写 NULL。
	mock.ExpectExec(`INSERT INTO player_item \(player_id, item_id, count, expire_at\) VALUES \(\?, \?, \?, \?\),\(\?, \?, \?, \?\)`).
		WithArgs(
			roleID, int32(1001), int32(1000), nil,
			roleID, int32(2001), int32(1), nil,
		).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	got, err := store.Create(context.Background(), service.RoleDraft{
		AccountID: 1001,
		Name:      "张三",
		Career:    1,
		Hp:        1000, Mp: 100, Atk: 50, Def: 30, Spd: 10,
		Items: []service.InitItem{{ItemID: 1001, Count: 1000}, {ItemID: 2001, Count: 1}},
	})
	require.NoError(t, err)
	require.EqualValues(t, roleID, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRoleStoreCreateDuplicateName 重名撞 uk_name（1062）→ 回滚整笔事务，
// 翻译成 ErrNameUsed。判定走结构化错误码而不是错误文本。
func TestRoleStoreCreateDuplicateName(t *testing.T) {
	store, mock := newRoleStore(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO player \(`).
		WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry '张三' for key 'uk_name'"})
	mock.ExpectRollback()

	_, err := store.Create(context.Background(), service.RoleDraft{AccountID: 1001, Name: "张三", Career: 1})
	require.ErrorIs(t, err, service.ErrNameUsed)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRoleStoreCreateRollbackOnAttrFailure 属性写入失败同样整笔回滚：
// 不能留下「有角色行但没有属性行」的半成品（客户端首屏会异常）。
func TestRoleStoreCreateRollbackOnAttrFailure(t *testing.T) {
	store, mock := newRoleStore(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO player \(`).WillReturnResult(sqlmock.NewResult(10001, 1))
	mock.ExpectExec(`INSERT INTO player_attr \(`).WillReturnError(errors.New("disk full"))
	mock.ExpectRollback()

	_, err := store.Create(context.Background(), service.RoleDraft{AccountID: 1001, Name: "张三", Career: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "insert player_attr")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRoleStoreCreateEmptyItems 没有初始道具时不应产生 player_item 语句
// （空切片的批量 UPSERT 会拼出非法 SQL）。
func TestRoleStoreCreateEmptyItems(t *testing.T) {
	store, mock := newRoleStore(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO player \(`).WillReturnResult(sqlmock.NewResult(10001, 1))
	mock.ExpectExec(`INSERT INTO player_attr \(`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	_, err := store.Create(context.Background(), service.RoleDraft{AccountID: 1001, Name: "张三", Career: 1})
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
