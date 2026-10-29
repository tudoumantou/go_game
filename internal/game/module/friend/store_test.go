// internal/game/module/friend/repo_test.go —— 好友仓储的事务契约测试
//
// friend 是本次整改里唯一「写路径全部在事务 + 复合键行锁里」的模块：
// 这类逻辑生成本身覆盖不了，但语句形态仍应来自生成函数。测试盯住
// 「申请的三条分支」「双向插入的幂等」「删除的幂等」。
package friend

import (
	"context"
	"testing"
	"time"

	"game_study/internal/common/errcode"
	"game_study/pkg/db"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// mysqlErr1062 复用的唯一键冲突错误（值类型，可被 errors.As 取到）。
var mysqlErr1062 = mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry"}

func newTestRepo(t *testing.T) (*Repo, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return NewRepo(&db.DB{DB: sqlDB}), mock
}

func expectPlayer(mock sqlmock.Sqlmock, id int64) {
	now := time.Now()
	mock.ExpectQuery(`FROM player WHERE id = \?`).WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at"}).
			AddRow(id, int64(1), "p", 1, 0, 0, 0, now, now))
}

// expectNoBlock 期望两次「黑名单里没有这条关系」（Apply 双向各查一次）。
//
// 空行集 → QueryRow.Scan 返回 sql.ErrNoRows，正是「没被拉黑」的信号。
func expectNoBlock(mock sqlmock.Sqlmock) {
	for i := 0; i < 2; i++ {
		mock.ExpectQuery(`FROM friend_block`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "player_id", "blocked_id", "created_at"}))
	}
}

func emptyFriendRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "player_id", "friend_id", "intimacy", "created_at"})
}

func emptyApplyRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{"id", "from_id", "to_id", "status", "created_at"})
}

// TestRepoApplyTargetNotFound 目标不存在 → 业务码 ErrFriendTargetNotFound，
// 事务正常提交（不是错误）。
func TestRepoApplyTargetNotFound(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM player WHERE id = \?`).WithArgs(int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectCommit()

	code, err := repo.Apply(context.Background(), 1, 2)
	require.NoError(t, err)
	require.EqualValues(t, errcode.ErrFriendTargetNotFound, code)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoApplyAlreadyFriends 已是好友 → ErrFriendAlready，不再往下走。
func TestRepoApplyAlreadyFriends(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	expectPlayer(mock, 2)
	expectNoBlock(mock)
	now := time.Now()
	mock.ExpectQuery(`FROM friend WHERE player_id = \? AND friend_id = \?`).
		WithArgs(int64(1), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "player_id", "friend_id", "intimacy", "created_at"}).
			AddRow(9, 1, 2, 0, now))
	mock.ExpectCommit()

	code, err := repo.Apply(context.Background(), 1, 2)
	require.NoError(t, err)
	require.EqualValues(t, errcode.ErrFriendAlready, code)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoApplyCreates 全流程：查重 → 双向申请锁查（都不存在）→
// 好友数/待处理数校验 → 插入申请。
func TestRepoApplyCreates(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	expectPlayer(mock, 2)
	expectNoBlock(mock)
	mock.ExpectQuery(`FROM friend WHERE player_id = \? AND friend_id = \?`).WillReturnRows(emptyFriendRows())
	// 对方是否申请过我
	mock.ExpectQuery(`FROM friend_apply WHERE from_id = \? AND to_id = \?`).WillReturnRows(emptyApplyRows())
	// 我是否已申请且待处理
	mock.ExpectQuery(`FROM friend_apply WHERE from_id = \? AND to_id = \?`).WillReturnRows(emptyApplyRows())
	// 好友上限：锁自己的好友行（范围锁）
	mock.ExpectQuery(`FROM friend WHERE player_id = \? ORDER BY id FOR UPDATE`).WillReturnRows(emptyFriendRows())
	// 待处理申请上限
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM friend_apply WHERE from_id = \? AND status = \?`).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO friend_apply \(id,from_id,to_id,status\)`).
		WithArgs(int64(0), int64(1), int64(2), int32(0)).
		WillReturnResult(sqlmock.NewResult(10, 1))
	mock.ExpectCommit()

	code, err := repo.Apply(context.Background(), 1, 2)
	require.NoError(t, err)
	require.EqualValues(t, errcode.OK, code)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoApplyReapplyRevivesRow 曾拒绝过的申请：插入撞唯一键 → 把同一行
// 拉回待处理并刷新申请时间（原来靠 ON DUPLICATE KEY UPDATE）。
func TestRepoApplyReapplyRevivesRow(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	expectPlayer(mock, 2)
	expectNoBlock(mock)
	mock.ExpectQuery(`FROM friend WHERE player_id = \? AND friend_id = \?`).WillReturnRows(emptyFriendRows())
	mock.ExpectQuery(`FROM friend_apply WHERE from_id = \? AND to_id = \?`).WillReturnRows(emptyApplyRows())
	// 我上次被拒（status=2）：锁查返回一行，但状态不是 0，继续走新申请
	mock.ExpectQuery(`FROM friend_apply WHERE from_id = \? AND to_id = \?`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "from_id", "to_id", "status", "created_at"}).
			AddRow(10, 1, 2, 2, time.Now()))
	mock.ExpectQuery(`FROM friend WHERE player_id = \? ORDER BY id FOR UPDATE`).WillReturnRows(emptyFriendRows())
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM friend_apply WHERE from_id = \? AND status = \?`).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(0))
	mock.ExpectExec(`INSERT INTO friend_apply \(id,from_id,to_id,status\)`).
		WillReturnError(&mysqlErr1062)
	mock.ExpectExec(`UPDATE friend_apply SET status = \?, created_at = \? WHERE from_id = \? AND to_id = \?`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	code, err := repo.Apply(context.Background(), 1, 2)
	require.NoError(t, err)
	require.EqualValues(t, errcode.OK, code)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoHandleAcceptCreatesBothSides 同意申请：双向插入（1062 幂等）+ 两条
// 申请置已同意，全部在同一事务。
func TestRepoHandleAcceptCreatesBothSides(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM friend_apply WHERE from_id = \? AND to_id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "from_id", "to_id", "status", "created_at"}).
			AddRow(10, 1, 2, 0, time.Now()))
	// becomeFriends：双方好友数校验（各一次范围锁）
	mock.ExpectQuery(`FROM friend WHERE player_id = \? ORDER BY id FOR UPDATE`).WillReturnRows(emptyFriendRows())
	mock.ExpectQuery(`FROM friend WHERE player_id = \? ORDER BY id FOR UPDATE`).WillReturnRows(emptyFriendRows())
	// 双向插入
	mock.ExpectExec(`INSERT INTO friend \(id,player_id,friend_id,intimacy\)`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO friend \(id,player_id,friend_id,intimacy\)`).WillReturnResult(sqlmock.NewResult(2, 1))
	// 两条申请置已同意
	mock.ExpectExec(`UPDATE friend_apply SET status = \? WHERE from_id = \? AND to_id = \?`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE friend_apply SET status = \? WHERE from_id = \? AND to_id = \?`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	code, err := repo.Handle(context.Background(), 2, 1, true, applyTTL)
	require.NoError(t, err)
	require.EqualValues(t, errcode.OK, code)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoHandleExpired 超过有效期：状态收敛成已拒绝并回业务码。
func TestRepoHandleExpired(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`FROM friend_apply WHERE from_id = \? AND to_id = \? ORDER BY id LIMIT 1 FOR UPDATE`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "from_id", "to_id", "status", "created_at"}).
			AddRow(10, 1, 2, 0, time.Now().Add(-2*applyTTL)))
	mock.ExpectExec(`UPDATE friend_apply SET status = \? WHERE from_id = \? AND to_id = \?`).
		WithArgs(int32(2), int64(1), int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	code, err := repo.Handle(context.Background(), 2, 1, true, applyTTL)
	require.NoError(t, err)
	require.EqualValues(t, errcode.ErrFriendApplyExpired, code)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoDeleteIdempotent 删双向两行；本来不是好友（0 行）也不报错。
func TestRepoDeleteIdempotent(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM friend WHERE player_id = \? AND friend_id = \?`).
		WithArgs(int64(1), int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM friend WHERE player_id = \? AND friend_id = \?`).
		WithArgs(int64(2), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	require.NoError(t, repo.Delete(context.Background(), 1, 2))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestBlockInsertsAndRemovesFriends 拉黑：写黑名单 + 双向删好友，同一事务。
//
// 双向删是重点：好友关系存两行（各自一份），只删一边会留下单向好友——
// 拉黑方看不到对方了，被拉黑方的列表里还有你，还能给你发申请。
func TestBlockInsertsAndRemovesFriends(t *testing.T) {
	repo, mock := newTestRepo(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO friend_block`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`DELETE FROM friend`).WithArgs(int64(7), int64(9)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM friend`).WithArgs(int64(9), int64(7)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.Block(context.Background(), 7, 9))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestBlockIgnoresDuplicate 重复拉黑按成功处理（唯一键 1062 不算错）。
func TestBlockIgnoresDuplicate(t *testing.T) {
	repo, mock := newTestRepo(t)

	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO friend_block`).WillReturnError(&mysqlErr1062)
	mock.ExpectExec(`DELETE FROM friend`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM friend`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	require.NoError(t, repo.Block(context.Background(), 7, 9), "重复拉黑不应当报错")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUnblockDeletes 取消拉黑：一条 DELETE，本来不在黑名单也成功（幂等）。
func TestUnblockDeletes(t *testing.T) {
	repo, mock := newTestRepo(t)

	mock.ExpectExec(`DELETE FROM friend_block`).WithArgs(int64(7), int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 影响 0 行也是成功

	require.NoError(t, repo.Unblock(context.Background(), 7, 9))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestApplyRejectsBlocked 被拉黑的人不能加好友：命中黑名单直接回 6810。
//
// 双向都要拦：A 拉黑 B 之后，B 加不了 A（对方拉黑我），A 也加不了 B
// （我拉黑了对方，多半是误点，但放行就等于绕过黑名单）。
func TestApplyRejectsBlocked(t *testing.T) {
	repo, mock := newTestRepo(t)

	mock.ExpectBegin()
	expectPlayer(mock, 9) // 目标存在
	mock.ExpectQuery(`FROM friend_block`).WithArgs(int64(9), int64(7)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "player_id", "blocked_id", "created_at"}).
			AddRow(1, 9, 7, time.Now()))
	mock.ExpectCommit()

	code, err := repo.Apply(context.Background(), 7, 9)
	require.NoError(t, err)
	require.EqualValues(t, int32(errcode.ErrFriendBlocked), code)
	require.NoError(t, mock.ExpectationsWereMet())
}
