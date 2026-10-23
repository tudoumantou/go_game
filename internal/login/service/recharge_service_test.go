// recharge_service_test.go —— R3.3 服务端定价与回调金额校验
package service

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"game_study/internal/login/repo"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func newRechargeService(t *testing.T) (*RechargeService, sqlmock.Sqlmock) {
	t.Helper()
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { database.Close() })
	svc := NewRechargeService(repo.NewOrderRepo(&db.DB{DB: database}), NopOrderProducer{},
		log.New("test", log.WithLevel("error")))
	return svc, mock
}

// TestCreateOrderUsesServerPrice 金额只能来自配置：档位 3 → 98 元 → 9800 分。
func TestCreateOrderUsesServerPrice(t *testing.T) {
	svc, mock := newRechargeService(t)
	mock.ExpectExec(`INSERT INTO recharge_order`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	orderID, amountFen, err := svc.CreateOrder(context.Background(), 7, 3, 1)
	require.NoError(t, err, "create order")
	require.NotEqualValues(t, "", orderID, fmt.Sprintf("order=%q amount=%d, want 9800", orderID, amountFen))
	require.EqualValues(t, 9800, amountFen)
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestCreateOrderRejectsUnknownProduct 未配置的档位不写库。
func TestCreateOrderRejectsUnknownProduct(t *testing.T) {
	svc, mock := newRechargeService(t)
	_, _, err := svc.CreateOrder(context.Background(), 7, 9999, 1)
	require.Error(t, err, "unknown product must be rejected")
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestCreateOrderRejectsInvalidPlayer 非法角色不写库。
func TestCreateOrderRejectsInvalidPlayer(t *testing.T) {
	svc, _ := newRechargeService(t)
	_, _, err := svc.CreateOrder(context.Background(), 0, 1, 1)
	require.Error(t, err, "invalid player must be rejected")
}

func orderRow(orderID string, amountFen int32, status int32) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "order_id", "player_id", "product_id", "amount_fen", "channel", "status",
		"paid_at", "delivered_at", "created_at", "updated_at",
	}).AddRow(1, orderID, 7, 3, amountFen, 1, status, nil, nil, time.Now(), time.Now())
}

// TestPayCallbackRejectsAmountMismatch 回调金额与订单不一致时拒绝（防改价/部分支付）。
func TestPayCallbackRejectsAmountMismatch(t *testing.T) {
	svc, mock := newRechargeService(t)
	mock.ExpectQuery(`SELECT id, order_id, player_id, product_id, amount_fen`).
		WithArgs("O1").
		WillReturnRows(orderRow("O1", 9800, repo.OrderStatusPending))

	_, err := svc.PayCallback(context.Background(), "O1", 1)
	require.Error(t, err, "amount mismatch must be rejected")
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestPayCallbackIdempotentOnPaid 已支付订单重复回调返回成功，且不再推进/发消息。
func TestPayCallbackIdempotentOnPaid(t *testing.T) {
	svc, mock := newRechargeService(t)
	mock.ExpectQuery(`SELECT id, order_id, player_id, product_id, amount_fen`).
		WithArgs("O1").
		WillReturnRows(orderRow("O1", 9800, repo.OrderStatusPaid))

	res, err := svc.PayCallback(context.Background(), "O1", 9800)
	require.NoError(t, err, "idempotent callback")
	require.EqualValues(t, 0, res.Code)
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

// TestPayCallbackNotFound 不存在订单返回可识别错误。
func TestPayCallbackNotFound(t *testing.T) {
	svc, mock := newRechargeService(t)
	mock.ExpectQuery(`SELECT id, order_id, player_id, product_id, amount_fen`).
		WithArgs("OX").
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	_, err := svc.PayCallback(context.Background(), "OX", 100)
	require.False(t, err == nil || !IsNotFound(err), fmt.Sprintf("expected not-found, got %v", err))
}

// TestPayCallbackClosedOrderRejected 已关闭订单不能支付。
func TestPayCallbackClosedOrderRejected(t *testing.T) {
	svc, mock := newRechargeService(t)
	mock.ExpectQuery(`SELECT id, order_id, player_id, product_id, amount_fen`).
		WithArgs("O1").
		WillReturnRows(orderRow("O1", 9800, repo.OrderStatusClosed))

	if _, err := svc.PayCallback(context.Background(), "O1", 9800); err == nil {
		t.Fatal("closed order must be rejected")
	} else if errors.Is(err, repo.ErrOrderNotFound) {
		t.Fatal("wrong error type")
	}
}
