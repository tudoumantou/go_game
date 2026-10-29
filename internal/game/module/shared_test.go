// internal/game/module/shared_test.go —— 共享仓储基座的降级语义
//
// 这组用例钉的是「三种不可用」各自的后果：
//   - 没有连接池（NewShared(nil)）：Ready=false，Tx 返回 ErrNoDB，不 panic；
//   - nil 接收者（模块没注入仓储）：所有方法都要能安全返回零值；
//   - 正常连接：Tx 真的开了事务（而不是静默跳过）。
//
// 第三种最容易被漏测。降级写得越顺手，越可能出现「以为在写库、其实
// 什么都没发生」的代码——所以必须有一条用例证明健康路径真的下发了 SQL。
package module

import (
	"context"
	"database/sql"
	"testing"

	"game_study/pkg/db"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// TestSharedWithoutDatabase 没有连接池：Ready=false，Tx 返回哨兵错误。
func TestSharedWithoutDatabase(t *testing.T) {
	s := NewShared(nil, "demo")
	require.False(t, s.Ready())
	require.Nil(t, s.SQL())
	require.Equal(t, "demo", s.Name())

	called := false
	err := s.Tx(context.Background(), func(*sql.Tx) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, ErrNoDB)
	require.False(t, called, "无库时不该进入事务闭包")
}

// TestSharedNilReceiver 模块没注入仓储（nil 接口/ nil 指针）：方法可调用。
//
// 这条保护的是 `m.repo` 为 nil 接口时不能直接调 Ready()——
// 各模块的 ready() 先判 nil，但基座自己也要在 nil 接收者下安全，
// 否则「少判一次」就是 panidc 而不是降级。
func TestSharedNilReceiver(t *testing.T) {
	var s *Shared
	require.False(t, s.Ready())
	require.Nil(t, s.SQL())
	require.Nil(t, s.DB())
	require.Nil(t, s.Index())
	require.Equal(t, "", s.Name())
	require.NotNil(t, s.Log(), "日志器要能用，否则降级路径的告警会再炸一次")
	require.ErrorIs(t, s.Tx(context.Background(), func(*sql.Tx) error { return nil }), ErrNoDB)

	s.AttachIndex(nil) // 不 panic
}

// TestSharedTxRunsInTransaction 健康路径：Tx 真的 BEGIN 并 COMMIT。
func TestSharedTxRunsInTransaction(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	s := NewShared(&db.DB{DB: mockDB}, "demo")
	require.True(t, s.Ready())

	require.NoError(t, s.Tx(context.Background(), func(tx *sql.Tx) error {
		require.NotNil(t, tx)
		return nil
	}))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestSharedTxRollsBackOnError 闭包返回错误时回滚，不提交。
func TestSharedTxRollsBackOnError(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	s := NewShared(&db.DB{DB: mockDB}, "demo")
	boom := sql.ErrConnDone
	require.ErrorIs(t, s.Tx(context.Background(), func(*sql.Tx) error { return boom }), boom)
	require.NoError(t, mock.ExpectationsWereMet())
}
