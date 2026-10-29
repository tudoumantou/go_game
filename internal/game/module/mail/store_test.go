// internal/game/module/mail/repo_test.go —— 仓储层的 SQL 契约测试
//
// 业务规则用 fake repo 测（mail_test.go），这里测「仓储是否正确地用了生成函数」：
// 语句形状、参数顺序、错误翻译（1062→幂等、0 行→ErrMailNotExist）。
// 手写 SQL 时代这两层是混在一起的，改一条语句要同时改业务测试——现在分开了：
// SQL 归生成器，仓储测试只盯「用对没用对」。
package mail

import (
	"context"
	"testing"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/model"

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

// TestRepoInsertBindsNullBizID 普通投递（无 biz_id）必须写 NULL 而不是空串：
// uk_player_biz 对 NULL 不生效，这是「同一玩家多封普通邮件」不撞键的根本原因。
func TestRepoInsertBindsNullBizID(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectExec(`INSERT INTO mail \(id,player_id,template_id,title,content,channel,status,biz_id,expire_at\)`).
		WithArgs(int64(0), int64(7), int32(0), "t", "c", int32(0), int32(0), nil, nil).
		WillReturnResult(sqlmock.NewResult(100, 1))

	id, err := repo.Insert(context.Background(), &model.Mail{PlayerId: 7, Title: "t", Content: "c"})
	require.NoError(t, err)
	require.EqualValues(t, 100, id)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoDeleteRejectsOtherOwner 归属不符（0 行）→ ErrMailNotExist，且不删附件。
func TestRepoDeleteRejectsOtherOwner(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM mail WHERE id = \? AND player_id = \?`).
		WithArgs(int64(5), int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	err := repo.Delete(context.Background(), 7, 5)
	require.ErrorIs(t, err, ErrMailNotExist)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoDeleteCascadesAttachments 主邮件删成功才删附件（同一事务）。
func TestRepoDeleteCascadesAttachments(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM mail WHERE id = \? AND player_id = \?`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM mail_attachment WHERE mail_id = \?`).
		WithArgs(int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	require.NoError(t, repo.Delete(context.Background(), 7, 5))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoDeliverOfflineIdempotent 同一 biz_id 重投：1062 视为已发放，
// 不插附件、不报错、created=false。
func TestRepoDeliverOfflineIdempotent(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO mail \(`).
		WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry"})
	mock.ExpectCommit()

	id, created, err := repo.DeliverOffline(context.Background(), 7, "order-1", "t", "c",
		[]Attachment{{ItemID: 1001, Count: 1}})
	require.NoError(t, err)
	require.False(t, created)
	require.Zero(t, id)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoDeliverOfflineCreates 首次发放：插邮件 + 逐个附件，返回新 id。
func TestRepoDeliverOfflineCreates(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO mail \(`).WillReturnResult(sqlmock.NewResult(55, 1))
	mock.ExpectExec(`INSERT INTO mail_attachment \(id,mail_id,item_id,count,claimed\)`).
		WithArgs(int64(0), int64(55), int32(1001), int32(2), int32(0)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	id, created, err := repo.DeliverOffline(context.Background(), 7, "order-2", "t", "c",
		[]Attachment{{ItemID: 1001, Count: 2}})
	require.NoError(t, err)
	require.True(t, created)
	require.EqualValues(t, 55, id)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoFindAttachmentsOwnership 三种结果：别人的邮件、无附件、正常。
func TestRepoFindAttachmentsOwnership(t *testing.T) {
	repo, mock := newTestRepo(t)
	cols := []string{"id", "player_id", "template_id", "title", "content", "channel", "status", "biz_id", "expire_at", "created_at", "updated_at"}
	now := time.Now()

	// 不是自己的邮件
	mock.ExpectQuery(`FROM mail WHERE id = \?`).WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(5, 99, 0, "t", "c", 0, 0, nil, nil, now, now))
	_, err := repo.FindAttachments(context.Background(), 7, 5)
	require.ErrorIs(t, err, ErrMailNotExist)

	// 自己的邮件但没有未领附件
	mock.ExpectQuery(`FROM mail WHERE id = \?`).WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(5, 7, 0, "t", "c", 0, 0, nil, nil, now, now))
	mock.ExpectQuery(`FROM mail_attachment WHERE mail_id = \? AND claimed = \?`).
		WithArgs(int64(5), int32(0)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "mail_id", "item_id", "count", "claimed", "created_at"}))
	_, err = repo.FindAttachments(context.Background(), 7, 5)
	require.ErrorIs(t, err, ErrNoAttachment)

	// 正常两件附件
	mock.ExpectQuery(`FROM mail WHERE id = \?`).WithArgs(int64(5)).
		WillReturnRows(sqlmock.NewRows(cols).AddRow(5, 7, 0, "t", "c", 0, 0, nil, nil, now, now))
	mock.ExpectQuery(`FROM mail_attachment WHERE mail_id = \? AND claimed = \?`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "mail_id", "item_id", "count", "claimed", "created_at"}).
			AddRow(1, 5, 1001, 2, 0, now).AddRow(2, 5, 1002, 1, 0, now))
	got, err := repo.FindAttachments(context.Background(), 7, 5)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.EqualValues(t, 1001, got[0].ItemID)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoMarkTaken 两条更新同事务：附件置已领 + 邮件置已领。
func TestRepoMarkTaken(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE mail_attachment SET claimed = \? WHERE mail_id = \?`).
		WithArgs(int32(1), int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`UPDATE mail SET status = \? WHERE id = \?`).
		WithArgs(int32(2), int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.MarkTaken(context.Background(), 5))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoUnreadCount 未读数走生成的 COUNT 函数。
func TestRepoUnreadCount(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM mail WHERE player_id = \? AND status = \?`).
		WithArgs(int64(7), int32(0)).
		WillReturnRows(sqlmock.NewRows([]string{"c"}).AddRow(3))

	n, err := repo.UnreadCount(context.Background(), 7)
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRepoHasAttachmentsKeepsSQL 例外清单内的 JOIN 查询：行为保持，
// 但至少有一条测试钉住它（这类查询是唯一仍需手写 SQL 的地方）。
func TestRepoHasAttachmentsKeepsSQL(t *testing.T) {
	repo, mock := newTestRepo(t)
	mock.ExpectQuery(`SELECT DISTINCT mail_id FROM mail_attachment WHERE mail_id IN \(\?,\?\)`).
		WithArgs(int64(1), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"mail_id"}).AddRow(1))

	got, err := repo.HasAttachments(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	require.True(t, got[1])
	require.False(t, got[2])
	require.NoError(t, mock.ExpectationsWereMet())
}
