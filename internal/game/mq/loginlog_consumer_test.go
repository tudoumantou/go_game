// loginlog_consumer_test.go —— 登录流水落库（R3.2）
package mq

import (
	"context"
	"testing"

	"fmt"
	"game_study/pkg/db"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func loginLogEnvelope(t *testing.T, msg pkgmq.LoginLogMsg) *pkgmq.Envelope {
	t.Helper()
	env, err := pkgmq.NewEnvelope("login-9-1", pkgmq.TopicLoginLog, "9", msg)
	require.NoError(t, err)
	return env
}

// TestLoginLogConsumerInserts 合法事件落一行 login_log。
func TestLoginLogConsumerInserts(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	// 生成的 INSERT 显式列出 id 列（零值由自增分配），参数顺序以 schema 为准。
	mock.ExpectExec(`INSERT INTO login_log`).
		WithArgs(int64(0), int64(9), int64(0), "1.2.3.4", "device-a", int32(1), "invalid token").
		WillReturnResult(sqlmock.NewResult(1, 1))

	c := NewLoginLogConsumer(nil, &db.DB{DB: database}, log.New("test", log.WithLevel("error")))
	env := loginLogEnvelope(t, pkgmq.LoginLogMsg{
		AccountID: 9, PlayerID: 0, IP: "1.2.3.4", Device: "device-a", Result: 1, Reason: "invalid token",
	})
	err = c.handle(context.Background(), env)
	require.NoError(t, err, fmt.Sprintf("handle: %v", err))
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestLoginLogConsumerRejectsEmptyAccount 缺账号的事件不写库，交给重试/死信。
func TestLoginLogConsumerRejectsEmptyAccount(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	c := NewLoginLogConsumer(nil, &db.DB{DB: database}, log.New("test", log.WithLevel("error")))
	err = c.handle(context.Background(), loginLogEnvelope(t, pkgmq.LoginLogMsg{Result: 0}))
	require.Error(t, err, "missing account_id must be rejected")
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestLoginLogConsumerSurfacesWriteError 写库失败要上报，让消费端重试。
func TestLoginLogConsumerSurfacesWriteError(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectExec(`INSERT INTO login_log`).
		WillReturnError(context.DeadlineExceeded)

	c := NewLoginLogConsumer(nil, &db.DB{DB: database}, log.New("test", log.WithLevel("error")))
	env := loginLogEnvelope(t, pkgmq.LoginLogMsg{AccountID: 9})
	err = c.handle(context.Background(), env)
	require.Error(t, err, "write error must be surfaced")
}
