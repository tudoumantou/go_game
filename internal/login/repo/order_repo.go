// internal/login/repo/order_repo.go —— 充值订单仓储（Tier-1 严格一致）
//
// 架构职责：recharge_order / recharge_record 两张表的全部访问入口，
// 承载充值订单状态机的持久化。状态推进一律用「带前置条件的
// UPDATE + RowsAffected」实现 CAS（Compare-And-Swap），这是本文件
// 最重要的设计约定。
// 协作关系：service.RechargeService 是唯一调用方；MarkPaid 由支付
// 回调触发，MarkDelivered 由发货确认消费者触发，ListPaidUndelivered
// 由每小时对账任务触发。
// 设计取舍：Tier-1 严格一致——订单是钱，宁可慢不可错：
// ① 不用缓存，每次读写直连 MySQL；② 状态推进不「先查后改」，
// 而是把旧状态写进 WHERE 条件，用 RowsAffected=0 表达「状态已被
// 别人改过」——把并发冲突的裁决交给数据库单行锁，避免应用层竞态；
// ③ 发货流水用 ON DUPLICATE KEY 幂等插入，重复发货不产生重复行。
package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/model"
	"game_study/pkg/sqlstore"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// 订单状态（与 DDL 注释一致）。
// 状态机：PENDING(0) → PAID(1) → DELIVERED(2)；CLOSED(3) 是旁路
// （超时未付/人工关单）。数值与 DDL 一致——改这里必须同步改建表脚本，
// 否则应用与库对状态的理解错位。
const (
	OrderStatusPending   int32 = 0 // 待支付
	OrderStatusPaid      int32 = 1 // 已支付
	OrderStatusDelivered int32 = 2 // 已发货
	OrderStatusClosed    int32 = 3 // 已关闭
)

// 仓储错误。
// 知识点：哨兵错误（errors.New 包级变量）+ errors.Is 判断——
// 调用方用 Is 而非字符串匹配 err.Error()，错误文案可随意改而
// 不影响调用方逻辑；这是 Go 错误处理的基本契约。
var (
	ErrOrderExists         = errors.New("order already exists")
	ErrOrderNotFound       = errors.New("order not found")
	ErrOrderStatusConflict = errors.New("order status conflict")
)

// OrderRepo 充值订单仓储。
type OrderRepo struct {
	*sqlstore.Base
}

// NewOrderRepo 创建仓储。
func NewOrderRepo(database *db.DB) *OrderRepo {
	return &OrderRepo{Base: sqlstore.New(database, "order")}
}

// Create 创建订单；order_id 冲突返回 ErrOrderExists。
// order_id 由 service 生成（时间戳+随机），冲突概率极低；一旦冲突
// （时钟回拨/极端随机碰撞）按已存在处理，调用方可重试或直接视为
// 「重复下单」。
func (r *OrderRepo) Create(ctx context.Context, order *model.RechargeOrder) error {
	_, err := model.InsertRechargeOrder(ctx, r.SQL(), order)
	if err != nil {
		// 知识点：errors.As 提取驱动特定错误类型——MySQL 用错误码
		// 1062（ER_DUP_ENTRY）表达唯一键冲突，这是「用数据库约束
		// 代替应用检查」的标准姿势；跨驱动时换 As 的目标类型即可
		var me *mysqlDriver.MySQLError
		if errors.As(err, &me) && me.Number == 1062 {
			return ErrOrderExists
		}
		return fmt.Errorf("insert order: %w", err)
	}
	return nil
}

// FindByOrderID 按业务订单号查询。
// LIMIT 1 是防御性写法：order_id 有唯一索引，逻辑上最多一行，
// 显式 LIMIT 让「万一索引失效」时不返回多行 Scan 报错。
func (r *OrderRepo) FindByOrderID(ctx context.Context, orderID string) (*model.RechargeOrder, error) {
	const q = `SELECT id, order_id, player_id, product_id, amount_fen, channel, status,
	                  paid_at, delivered_at, created_at, updated_at
	           FROM recharge_order WHERE order_id = ? LIMIT 1`
	var o model.RechargeOrder
	err := r.DB().QueryRowContext(ctx, q, orderID).Scan(
		&o.Id, &o.OrderId, &o.PlayerId, &o.ProductId, &o.AmountFen, &o.Channel, &o.Status,
		&o.PaidAt, &o.DeliveredAt, &o.CreatedAt, &o.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		// 翻译为领域哨兵错误：上层（handler）用 service.IsNotFound
		// 判断并映射 5004，不该知道 sql.ErrNoRows 的存在
		return nil, ErrOrderNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("query order: %w", err)
	}
	return &o, nil
}

// MarkPaid CAS 推进 待支付→已支付；受影响行数为 0 表示状态已变（幂等/并发）。
// SQL 即状态机：WHERE status = 0 保证只有「待支付」的单会被推进。
// 并发双回调时只有一个 RowsAffected=1，另一个拿 0——用行数当
// 「抢占成功」的信号，无需 SELECT ... FOR UPDATE 显式加锁。
func (r *OrderRepo) MarkPaid(ctx context.Context, orderID string, paidAt time.Time) (bool, error) {
	const q = `UPDATE recharge_order SET status = ?, paid_at = ?
	           WHERE order_id = ? AND status = ?`
	res, err := r.DB().ExecContext(ctx, q, OrderStatusPaid, paidAt, orderID, OrderStatusPending)
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
// 与 MarkPaid 同款 CAS 模式；发货确认消息重复到达时第二个请求
// RowsAffected=0，service 按幂等成功处理——不重复写流水。
func (r *OrderRepo) MarkDelivered(ctx context.Context, orderID string, deliveredAt time.Time) (bool, error) {
	const q = `UPDATE recharge_order SET status = ?, delivered_at = ?
	           WHERE order_id = ? AND status = ?`
	res, err := r.DB().ExecContext(ctx, q, OrderStatusDelivered, deliveredAt, orderID, OrderStatusPaid)
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
// ORDER BY paid_at ASC：先补最早付的钱——玩家等待越久体验越差，
// 补偿也应按先来后到。limit<=0 兜底 100：防调用方忘传参把全表
// 拖进内存（大促时已支付未发货可能上万行）。
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
	rows, err := r.DB().QueryContext(ctx, q, OrderStatusPaid, limit)
	if err != nil {
		return nil, fmt.Errorf("list paid undelivered: %w", err)
	}
	// 知识点：rows.Close 必须 defer——未关闭的 rows 会持有数据库
	// 连接不归还，连接池耗尽后所有查询挂起；defer 保证 panic 路径
	// 也释放。循环里 Scan 到局部变量再取地址入切片：循环变量在
	// Go 1.22 前被复用，直接 &loopVar 会让所有元素指向同一个。
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
	// rows.Err() 不可省：Next 返回 false 可能是正常结束也可能是
	// 迭代出错，两者都靠 Err() 区分——漏查会把部分结果当完整结果
	return out, rows.Err()
}

// InsertRecord 写发货流水（uk_order_id 保证幂等，重复插入忽略）。
// ON DUPLICATE KEY UPDATE order_id = order_id：命中唯一键时把行
// 「更新为自己」——MySQL 特意的 no-op 惯用法，比 INSERT IGNORE
// 更明确（只忽略重复键冲突，不吞其他错误）。
func (r *OrderRepo) InsertRecord(ctx context.Context, order *model.RechargeOrder) error {
	const q = `INSERT INTO recharge_record (order_id, player_id, product_id, amount_fen, channel)
	           VALUES (?, ?, ?, ?, ?)
	           ON DUPLICATE KEY UPDATE order_id = order_id`
	_, err := r.DB().ExecContext(ctx, q, order.OrderId, order.PlayerId, order.ProductId, order.AmountFen, order.Channel)
	if err != nil {
		return fmt.Errorf("insert recharge_record: %w", err)
	}
	return nil
}
