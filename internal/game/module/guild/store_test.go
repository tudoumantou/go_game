// internal/game/module/guild/repo_test.go —— 公会仓储的事务契约测试
//
// 重点：唯一名冲突的翻译、满员判定（锁内读数）、人数增减的读改写安全、
// 解散时的删除顺序（先成员后公会）。
package guild

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

func guildRow(id int64, level, memberCount int32) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{
		"id", "name", "leader_id", "level", "exp", "notice", "member_count", "created_at", "updated_at",
	}).AddRow(id, "g", int64(1), level, int32(0), "", memberCount, now, now)
}

// TestRepoCreateDuplicateName 重名（uk_name 冲突）→ ErrNameUsed，整笔回滚。
func TestRepoCreateDuplicateName(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO guild \(id,name,leader_id,level,exp,notice,member_count\)`).
		WithArgs(int64(0), "g", int64(1), int32(1), int32(0), "", int32(1)).
		WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry 'g'"})
	mock.ExpectRollback()

	_, err := repo.Create(context.Background(), "g", 1)
	require.ErrorIs(t, err, ErrNameUsed)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoJoinFull 锁内人数已达上限 → ErrGuildFull。
func TestRepoJoinFull(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM guild WHERE id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WithArgs(int64(100)).
		WillReturnRows(guildRow(100, 1, int32(maxMembers(1))))
	mock.ExpectRollback()

	require.ErrorIs(t, repo.Join(context.Background(), 100, 2), ErrGuildFull)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoJoinIncrementsCount 入会：插成员行 + 人数 +1（读改写，行已锁）。
func TestRepoJoinIncrementsCount(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM guild WHERE id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WithArgs(int64(100)).
		WillReturnRows(guildRow(100, 1, 3))
	mock.ExpectExec(`INSERT INTO guild_member \(id,guild_id,player_id,title,contributed\)`).
		WithArgs(int64(0), int64(100), int64(2), int32(titleMember), int32(0)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`UPDATE guild SET member_count = \? WHERE id = \?`).
		WithArgs(int32(4), int64(100)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.Join(context.Background(), 100, 2))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoQuitLeaderDissolves 会长退出：删自己成员行 → 删公会全部成员行 → 删公会行。
func TestRepoQuitLeaderDissolves(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM guild_member WHERE player_id = \?`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "guild_id", "player_id", "title", "contributed", "joined_at"}).
			AddRow(1, 100, 1, titleLeader, 0, time.Now()))
	mock.ExpectExec(`DELETE FROM guild_member WHERE player_id = \?`).
		WithArgs(int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM guild_member WHERE guild_id = \?`).
		WithArgs(int64(100)).
		WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectExec(`DELETE FROM guild WHERE id = \?`).
		WithArgs(int64(100)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.Quit(context.Background(), 1))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoQuitMemberDecrements 普通成员退出：锁公会行后人数 -1；
// 计数读改写不会丢更新（行锁），且不会减成负数。
func TestRepoQuitMemberDecrements(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM guild_member WHERE player_id = \?`).
		WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "guild_id", "player_id", "title", "contributed", "joined_at"}).
			AddRow(2, 100, 2, int32(0), 0, time.Now()))
	mock.ExpectExec(`DELETE FROM guild_member WHERE player_id = \?`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM guild WHERE id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WithArgs(int64(100)).
		WillReturnRows(guildRow(100, 1, 3))
	mock.ExpectExec(`UPDATE guild SET member_count = \? WHERE id = \?`).
		WithArgs(int32(2), int64(100)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.Quit(context.Background(), 2))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoQuitNotInGuild 没有成员行 → ErrNotInGuild，事务回滚。
func TestRepoQuitNotInGuild(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM guild_member WHERE player_id = \?`).
		WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "guild_id", "player_id", "title", "contributed", "joined_at"}))
	mock.ExpectRollback()

	require.ErrorIs(t, repo.Quit(context.Background(), 2), ErrNotInGuild)
	require.NoError(t, mock.ExpectationsWereMet())
}
