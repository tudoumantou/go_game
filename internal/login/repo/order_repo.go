// internal/login/repo/order_repo.go —— 充值订单仓储（Tier-1 严格一致）
package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/model"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// 订单状态（与 DDL 注释一致）。
const (
	OrderStatusPending   int32 = 0 // 待支付
	OrderStatusPaid      int32 = 1 // 已支付
	OrderStatusDelivered int32 = 2 // 已发货
	OrderStatusClosed    int32 = 3 // 已关闭
)

// 仓储错误。
var (
	ErrOrderExists        = errors.New("order already exists")
	ErrOrderNotFound      = errors.New("order not found")
	ErrOrderStatusConflict = errors.New("order status conflict")
)

// OrderRepo 充值订单仓储。
type OrderRepo struct {
	db *db.DB
}

// NewOrderRepo 创建仓储。
func NewOrderRepo(database *db.DB) *OrderRepo {
	return &OrderRepo{db: database}
}

// Create 创建订单；order_id 冲突返回 ErrOrderExists。
func (r *OrderRepo) Create(ctx context.Context, order *model.RechargeOrder) error {
	_, err := model.InsertRechargeOrder(ctx, r.db.DB, order)
	if err != nil {
		var me *mysqlDriver.MySQLError
		if errors.As(err, &me) && me.Number == 1062 {
			return ErrOrderExists
		}
		return fmt.Errorf("insert order: %w", err)
	}
	return nil
}

// FindByOrderID 按业务订单号查询。
func (r *OrderRepo) FindByOrderID(ctx context.Context, orderID string) (*model.RechargeOrder, error) {
	const q = `SELECT id, order_id, player_id, product_id, amount_fen, channel, status,
	                  paid_at, delivered_at, created_at, updated_at
	           FROM recharge_order WHERE order_id = ? LIMIT 1`
	var o model.RechargeOrder
	err := r.db.QueryRowContext(ctx, q, orderID).Scan(
		&o.Id, &o.OrderId, &o.PlayerId, &o.ProductId, &o.AmountFen, &o.Channel, &o.Status,
		&o.PaidAt, &o.DeliveredAt, &o.CreatedAt, &o.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrOrderNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query order: %w", err)
	}
	return &o, nil
}

// MarkPaid CAS 推进 待支付→已支付；受影响行数为 0 表示状态已变（幂等/并发）。
func (r *OrderRepo) MarkPaid(ctx context.Context, orderID string, paidAt time.Time) (bool, error) {
	const q = `UPDATE recharge_order SET status = ?, paid_at = ?
	           WHERE order_id = ? AND status = ?`
	res, err := r.db.ExecContext(ctx, q, OrderStatusPaid, paidAt, orderID, OrderStatusPending)
	if err != nil {
		return false, fmt.Errorf("mark paid: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// MarkDelivered CAS 推进 已支付→已发货。
func (r *OrderRepo) MarkDelivered(ctx context.Context, orderID string, deliveredAt time.Time) (bool, error) {
	const q = `UPDATE recharge_order SET status = ?, delivered_at = ?
	           WHERE order_id = ? AND status = ?`
	res, err := r.db.ExecContext(ctx, q, OrderStatusDelivered, deliveredAt, orderID, OrderStatusPaid)
	if err != nil {
		return false, fmt.Errorf("mark delivered: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ListPaidUndelivered 对账：已支付未发货订单（每小时补偿重投用）。
func (r *OrderRepo) ListPaidUndelivered(ctx context.Context, limit int) ([]*model.RechargeOrder, error) {
	if limit <= 0 {
		limit = 100
	}
	const q = `SELECT id, order_id, player_id, product_id, amount_fen, channel, status,
	                  paid_at, delivered_at, created_at, updated_at
	           FROM recharge_order
	           WHERE status = ?
	           ORDER BY paid_at ASC
	           LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, OrderStatusPaid, limit)
	if err != nil {
		return nil, fmt.Errorf("list paid undelivered: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*model.RechargeOrder
	for rows.Next() {
		var o model.RechargeOrder
		if err := rows.Scan(&o.Id, &o.OrderId, &o.PlayerId, &o.ProductId, &o.AmountFen,
			&o.Channel, &o.Status, &o.PaidAt, &o.DeliveredAt, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &o)
	}
	return out, rows.Err()
}

// InsertRecord 写发货流水（uk_order_id 保证幂等，重复插入忽略）。
func (r *OrderRepo) InsertRecord(ctx context.Context, order *model.RechargeOrder) error {
	const q = `INSERT INTO recharge_record (order_id, player_id, product_id, amount_fen, channel)
	           VALUES (?, ?, ?, ?, ?)
	           ON DUPLICATE KEY UPDATE order_id = order_id`
	_, err := r.db.ExecContext(ctx, q, order.OrderId, order.PlayerId, order.ProductId, order.AmountFen, order.Channel)
	if err != nil {
		return fmt.Errorf("insert recharge_record: %w", err)
	}
	return nil
}
