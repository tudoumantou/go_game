// internal/game/module/team/repo_test.go —— 组队仓储的事务契约测试
//
// 盯三件事：两条 INSERT 在同一事务、满员判定基于锁内计数、
// 重复加入（1062）翻译成 ErrAlreadyIn。
package team

import (
	"context"
	"testing"
	"time"

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

func teamRow(id int64, status int32) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "leader_id", "copy_id", "status", "created_at", "updated_at"}).
		AddRow(id, int64(1), int32(0), status, now, now)
}

// TestRepoCreateAtomic 建队伍 + 队长入队两条 INSERT 同事务。
func TestRepoCreateAtomic(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO team \(id,leader_id,copy_id,status\)`).
		WithArgs(int64(0), int64(1), int32(7), int32(0)).
		WillReturnResult(sqlmock.NewResult(100, 1))
	mock.ExpectExec(`INSERT INTO team_member \(id,team_id,player_id,ready\)`).
		WithArgs(int64(0), int64(100), int64(1), int32(0)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	id, err := repo.Create(context.Background(), 1, 7)
	require.NoError(t, err)
	require.EqualValues(t, 100, id)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoJoinFull 锁内人数已达上限 → ErrTeamFull，整笔回滚（不插成员行）。
func TestRepoJoinFull(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM team WHERE id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WithArgs(int64(100)).
		WillReturnRows(teamRow(100, 0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM team_member WHERE team_id = \?`).
		WithArgs(int64(100)).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(maxMembers()))
	mock.ExpectRollback()

	err := repo.Join(context.Background(), 100, 2)
	require.ErrorIs(t, err, ErrTeamFull)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoJoinDuplicate 唯一键冲突（1062）= 重复加入 → ErrAlreadyIn。
func TestRepoJoinDuplicate(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM team WHERE id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WillReturnRows(teamRow(100, 0))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM team_member WHERE team_id = \?`).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(2))
	mock.ExpectExec(`INSERT INTO team_member \(id,team_id,player_id,ready\)`).
		WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry"})
	mock.ExpectRollback()

	err := repo.Join(context.Background(), 100, 2)
	require.ErrorIs(t, err, ErrAlreadyIn)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoJoinDissolved 已解散（status=2）视同不存在。
func TestRepoJoinDissolved(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM team WHERE id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WillReturnRows(teamRow(100, 2))
	mock.ExpectRollback()

	require.ErrorIs(t, repo.Join(context.Background(), 100, 2), ErrTeamNotExist)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoLeaveLeaderDissolves 队长退出：删成员行、删全队成员行、软删队伍。
func TestRepoLeaveLeaderDissolves(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT t.id, t.leader_id FROM team t JOIN team_member m`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "leader_id"}).AddRow(100, 1))
	mock.ExpectExec(`DELETE FROM team_member WHERE team_id = \? AND player_id = \?`).
		WithArgs(int64(100), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM team_member WHERE team_id = \?`).
		WithArgs(int64(100)).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec(`UPDATE team SET status = \? WHERE id = \?`).
		WithArgs(int32(2), int64(100)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.Leave(context.Background(), 1))
	require.NoError(t, mock.ExpectationsWereMet())
}
