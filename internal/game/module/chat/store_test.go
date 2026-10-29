// internal/game/module/chat/repo_test.go —— 聊天仓储的 SQL 契约测试
//
// 只测仓储层的行为契约：幂等（1062 视为已存在）、真失败上抛、
// 以及「用的是生成函数而不是手写 SQL」这一点由语句形状保证。
package chat

import (
	"context"
	"errors"
	"testing"

	"game_study/pkg/db"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func newTestRepo(t *testing.T) (*Repo, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return NewRepo(&db.DB{DB: sqlDB}), mock
}

// TestRepoInsertIgnoresDuplicate 重投（uk_owner_msg 冲突）不报错：
// Kafka 重试与双端重复发送都靠这条幂等。
func TestRepoInsertIgnoresDuplicate(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectExec(`INSERT INTO chat_record \(id,owner_id,peer_id,msg_id,direction,channel,content\)`).
		WithArgs(int64(0), int64(1), int64(2), int64(99), int32(0), int32(1), "hi").
		WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry"})

	require.NoError(t, repo.Insert(context.Background(), 1, 2, 99, 0, 1, "hi"))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoInsertPropagatesRealError 非 1062 的失败必须上抛（不能把
// 「库挂了」当成「重复消息」静默吞掉）。
func TestRepoInsertPropagatesRealError(t *testing.T) {
	repo, mock := newTestRepo(t)
	boom := errors.New("connection reset")
	mock.ExpectExec(`INSERT INTO chat_record \(`).WillReturnError(boom)

	err := repo.Insert(context.Background(), 1, 2, 99, 0, 1, "hi")
	require.ErrorIs(t, err, boom)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoListRecentKeepsSQL 例外清单内的历史查询：limit 钳制与方向字段映射。
func TestRepoListRecentKeepsSQL(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectQuery(`FROM chat_record WHERE owner_id = \? AND peer_id = \? ORDER BY id DESC LIMIT \?`).
		WithArgs(int64(1), int64(2), 50).
		WillReturnRows(sqlmock.NewRows([]string{"msg_id", "direction", "content"}).
			AddRow(99, 1, "hey"))

	got, err := repo.ListRecent(context.Background(), 1, 2, 0) // 0 → 回落 50
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.EqualValues(t, 99, got[0].MsgID)
	require.EqualValues(t, 1, got[0].Direction)
	require.NoError(t, mock.ExpectationsWereMet())
}
