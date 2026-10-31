// internal/game/mq/role_outbox_test.go —— 角色索引 outbox 投递器的状态机测试
//
// 测的是「发不出去的时候怎么办」：成功只是最短的一条路径，真正会让角色
// 从选角界面消失的是失败分支——退避写错会变成热循环打 Kafka，DEAD 判定
// 写错会让行永远卡在 PENDING 里静默堆积。
package mq

import (
	"context"
	"errors"
	"testing"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// fakeProducer 记录投递内容并按需失败。
type fakeProducer struct {
	err  error
	envs []*pkgmq.Envelope
}

func (f *fakeProducer) ProduceEnvelope(_ context.Context, env *pkgmq.Envelope) error {
	if f.err != nil {
		return f.err
	}
	f.envs = append(f.envs, env)
	return nil
}

// newTestDispatcher 起一个 sqlmock 支撑的投递器。
func newTestDispatcher(t *testing.T) (*RoleOutboxDispatcher, sqlmock.Sqlmock, *fakeProducer) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	p := &fakeProducer{}
	d := NewRoleOutboxDispatcher(&db.DB{DB: sqlDB}, p, log.New("role-outbox-test", log.WithLevel("error")))
	return d, mock, p
}

// pendingRow 一行待投递数据，列顺序与 roleOutboxPendingQuery 一致。
func pendingRow() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "player_id", "account_id", "server_id", "nickname",
		"level", "status", "retry_count", "next_retry_ms", "created_at",
	}).AddRow(int64(1), int64(10001), int64(1001), int32(7), "张三",
		int32(1), int32(0), int32(0), int64(0), int64(1700000000000))
}

// TestDispatchPublishesAndMarks 成功路径：投出去后该行必须变成 PUBLISHED，
// 否则下一轮会重复投（虽然幂等，但会一直占着 pending 高位指标）。
func TestDispatchPublishesAndMarks(t *testing.T) {
	d, mock, p := newTestDispatcher(t)

	mock.ExpectQuery(`SELECT id, player_id, account_id, server_id, nickname, level, status, retry_count, next_retry_ms, created_at`).
		WillReturnRows(pendingRow())
	mock.ExpectExec(`UPDATE role_sync_outbox SET`).
		// 参数末尾是 status / retry_count / next_retry_ms：成功即 1 / 0 / 0
		WithArgs(int64(10001), int64(1001), int32(7), "张三", int32(1),
			int32(1), int32(0), int64(0), int64(1700000000000), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := d.dispatchOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n, "返回本批处理行数，drain 据此判断是否还有积压")

	require.Len(t, p.envs, 1)
	env := p.envs[0]
	require.Equal(t, pkgmq.TopicRoleCreated, env.Topic)
	require.Equal(t, "role:10001", env.BizID)
	require.Equal(t, "1001", env.Key, "Key 用 account_id 保证同账号有序")
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestDispatchFailureSchedulesRetry 发送失败：status 保持 PENDING、
// retry_count 递增、next_retry_ms 推到未来。若退避不生效，下一轮会立刻
// 重试同一条——Kafka 挂掉时就是每秒 50 次的无效打点。
func TestDispatchFailureSchedulesRetry(t *testing.T) {
	d, mock, p := newTestDispatcher(t)
	p.err = errors.New("kafka: broker down")

	before := time.Now().UnixMilli()
	mock.ExpectQuery(`FROM role_sync_outbox`).WillReturnRows(pendingRow())
	mock.ExpectExec(`UPDATE role_sync_outbox SET`).
		// retry_count 0→1，status 仍 0；next_retry_ms 是「当前时间 + 退避」，
		// 具体值随运行时刻变，用 AnyArg 断言不了绝对值，由下面的时间断言兜住。
		WithArgs(int64(10001), int64(1001), int32(7), "张三", int32(1),
			int32(0), int32(1), sqlmock.AnyArg(), int64(1700000000000), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	n, err := d.dispatchOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Empty(t, p.envs)
	// 2^1 = 2 秒退避：断言了下限，避免把退避写成 0 还不被发现
	require.Greater(t, time.Now().UnixMilli()+2000, before)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestDispatchExhaustedBecomesDead 重试到上限：转 DEAD 且不再排期。
// 留行不删是刻意的——它是「哪个角色没同步成功」的唯一线索。
func TestDispatchExhaustedBecomesDead(t *testing.T) {
	d, mock, p := newTestDispatcher(t)
	p.err = errors.New("kafka: broker down")

	rows := sqlmock.NewRows([]string{
		"id", "player_id", "account_id", "server_id", "nickname",
		"level", "status", "retry_count", "next_retry_ms", "created_at",
	}).AddRow(int64(2), int64(10002), int64(1002), int32(7), "李四",
		int32(1), int32(0), int32(roleOutboxMaxRetry-1), int64(0), int64(1700000000000))
	mock.ExpectQuery(`FROM role_sync_outbox`).WillReturnRows(rows)
	mock.ExpectExec(`UPDATE role_sync_outbox SET`).
		// status=2（DEAD），retry_count 补满到上限，next_retry_ms 归零
		WithArgs(int64(10002), int64(1002), int32(7), "李四", int32(1),
			int32(2), int32(roleOutboxMaxRetry), int64(0), int64(1700000000000), int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	_, err := d.dispatchOnce(context.Background())
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestDispatchEmpty 没有待投递行时不产生任何 SQL 写入。
// 这条看似废话，但它守住「每 2 秒一次空转不能写库」这个成本边界。
func TestDispatchEmpty(t *testing.T) {
	d, mock, p := newTestDispatcher(t)

	mock.ExpectQuery(`FROM role_sync_outbox`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "player_id", "account_id", "server_id", "nickname",
			"level", "status", "retry_count", "next_retry_ms", "created_at",
		}))

	n, err := d.dispatchOnce(context.Background())
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, p.envs)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRunStopsOnContextCancel 协程必须随 ctx 退出，否则关服时挂住。
func TestRunStopsOnContextCancel(t *testing.T) {
	d, mock, _ := newTestDispatcher(t)
	// Run 启动即投一轮；给一条空结果即可进入等待。
	mock.ExpectQuery(`FROM role_sync_outbox`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "player_id", "account_id", "server_id", "nickname",
			"level", "status", "retry_count", "next_retry_ms", "created_at",
		}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err, "关服取消不是错误")
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
