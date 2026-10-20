package store

// delivery.go 承载回城交接单的读写（文档 9.5 / 13.2）。
//
// 交接单是「地图事务」与「玩家资产事务」之间唯一的持久桥梁：
//   - 地图侧先留单再清货（Troop→SETTLING，Cargo 保留）；
//   - 投递 worker 按 next_retry_ms 扫描 PENDING 单，至少一次投递；
//   - 玩家侧用 slg:delivery:{id} 原子去重，回执带 player_version；
//   - 只有收到确认（且 payload_hash 一致）才能把状态推进到 APPLIED。
//
// 两个提交边界之间不宣称「恰好一次」——这里所有写操作都必须是
// 幂等或带前置条件的 CAS，否则确认报文丢失重投一次就会重复发奖（INV-07）。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"game_study/internal/worldmap/domain"
)

const deliveryColumns = `delivery_id, world_id, troop_id, player_id, payload_hash, cargo,
	status, attempts, next_retry_ms, settlement_no, applied_player_version, created_at_ms`

// scanDelivery 读取一行交接单。
func scanDelivery(sc interface{ Scan(...any) error }) (*domain.Delivery, error) {
	var (
		d        domain.Delivery
		hash     []byte
		cargoRaw []byte
		status   int32
		// applied_player_version 只在地图侧做对账用，domain.Delivery 没有
		// 对应字段（那是玩家侧的版本号），扫到临时变量后丢弃。
		appliedVersion int64
	)
	if err := sc.Scan(&d.DeliveryID, &d.WorldID, &d.TroopID, &d.PlayerID, &hash, &cargoRaw,
		&status, &d.Attempts, &d.NextRetryMS, &d.SettlementNo, &appliedVersion, &d.CreatedAtMS); err != nil {
		return nil, err
	}
	if len(hash) != len(d.PayloadHash) {
		return nil, fmt.Errorf("%w: 交接单 %s payload_hash 长度=%d 期望=32",
			domain.ErrInvalidState, d.DeliveryID, len(hash))
	}
	copy(d.PayloadHash[:], hash)

	var cargo map[string]int64
	if len(cargoRaw) > 0 {
		if err := json.Unmarshal(cargoRaw, &cargo); err != nil {
			return nil, fmt.Errorf("解析交接单 cargo %s: %w", d.DeliveryID, err)
		}
	}
	decoded, err := cargoFromJSON(cargo)
	if err != nil {
		return nil, fmt.Errorf("解析交接单 cargo %s: %w", d.DeliveryID, err)
	}
	d.Cargo = decoded
	d.Status = domain.DeliveryStatus(status)
	return &d, nil
}

// LoadDelivery 按单号加载交接单。
//
// 单号是全局唯一的 UUID（不是 world 内唯一），所以这里不带 world_id——
// 这是全库唯一一处「主键即可唯一定位」的地图表，与 map_delivery 的
// DDL 保持一致（PRIMARY KEY(delivery_id)）。
func (r *Repository) LoadDelivery(ctx context.Context, deliveryID string) (*domain.Delivery, error) {
	d, err := scanDelivery(r.db.QueryRowContext(ctx,
		`SELECT `+deliveryColumns+` FROM map_delivery WHERE delivery_id = ?`, deliveryID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: delivery_id=%s", ErrDeliveryNotFound, deliveryID)
	}
	if err != nil {
		return nil, fmt.Errorf("查询交接单 %s: %w", deliveryID, err)
	}
	return d, nil
}

// ListDeliveryPending 扫描到期待投递的交接单（文档 13.2 步骤 2）。
//
// 两个设计点：
//   - 用 next_retry_ms <= now 过滤而不是「全表扫 PENDING」：投递失败会
//     按指数退避把 next_retry_ms 推远，扫描天然跳过还在退避等待的单；
//   - 按 (next_retry_ms, created_at_ms, delivery_id) 排序：前两列保证
//     「该发的先发」，最后一列保证同毫秒内的顺序稳定（分页/对账可复现）。
//
// 不带 world_id 是刻意的：交接 worker 是进程级组件，要跨世界把积压清掉；
// idx_retry(status, next_retry_ms) 正好覆盖这条查询。
func (r *Repository) ListDeliveryPending(ctx context.Context, limit int) ([]domain.Delivery, error) {
	if limit <= 0 {
		// 上限兜底：交接积压可能是上万条，一次拉全表会把内存与玩家侧
		// 配额一起打爆（文档 17.2）。
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+deliveryColumns+` FROM map_delivery
		 WHERE status = ? AND next_retry_ms <= ?
		 ORDER BY next_retry_ms ASC, created_at_ms ASC, delivery_id ASC
		 LIMIT ?`,
		int32(domain.DeliveryStatusPending), r.now(), limit)
	if err != nil {
		return nil, fmt.Errorf("查询待投递交接单: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描交接单: %w", err)
		}
		out = append(out, *d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历待投递交接单: %w", err)
	}
	return out, nil
}

// MarkDeliveryApplied 把交接单推进到 APPLIED（文档 13.2 步骤 6）。
//
// 带 status=1 前置条件是 CAS：玩家侧可能对同一单确认两次（重投），
// 第二次 RowsAffected=0。此时**不是错误**——但也不能静默当作成功，
// 因为「单子不存在」也可能导致 0 行。所以这里区分两种情况返回
// ErrDeliveryNotFound / ErrDeliveryNotPending，让调用方自己决定是否忽略。
func (r *Repository) MarkDeliveryApplied(ctx context.Context, deliveryID string, playerVersion int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE map_delivery SET status = ?, applied_player_version = ?
		 WHERE delivery_id = ? AND status = ?`,
		int32(domain.DeliveryStatusApplied), playerVersion, deliveryID, int32(domain.DeliveryStatusPending))
	if err != nil {
		return fmt.Errorf("标记交接单 APPLIED %s: %w", deliveryID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("标记交接单影响行数 %s: %w", deliveryID, err)
	}
	if n == 0 {
		if _, err := r.LoadDelivery(ctx, deliveryID); err != nil {
			return err
		}
		return fmt.Errorf("%w: delivery_id=%s", ErrDeliveryNotPending, deliveryID)
	}
	return nil
}

// MarkDeliveryRetry 记录一次失败投递与下次重试时刻（文档 13.2 步骤 2）。
//
// attempts 由调用方累加后传入（而不是 SQL 里 attempts+1）：退避计算需要
// 知道「这次是第几次」，让调用方在同一处完成读-算-写，语义更清楚。
func (r *Repository) MarkDeliveryRetry(ctx context.Context, deliveryID string, attempts int32, nextRetryMS int64) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE map_delivery SET attempts = ?, next_retry_ms = ?
		 WHERE delivery_id = ? AND status = ?`,
		attempts, nextRetryMS, deliveryID, int32(domain.DeliveryStatusPending))
	if err != nil {
		return fmt.Errorf("登记交接单重试 %s: %w", deliveryID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: delivery_id=%s", ErrDeliveryNotPending, deliveryID)
	}
	return nil
}

// QuarantineDelivery 隔离永久失败的交接单（文档 13.5）。
//
// 表里没有 reason 列：隔离原因属于运维信息，写日志 + 告警足够，
// 加列会把「临时诊断文本」变成需要迁移的 schema。这里只改 status=3，
// 让 scanner 不再捡起它。
//
// 隔离而不是删除：单子里是玩家的货物，删掉就再也无法对账；
// 隔离后人工可以按 delivery_id 在日志里找到原因并决定补发。
func (r *Repository) QuarantineDelivery(ctx context.Context, deliveryID, reason string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE map_delivery SET status = ? WHERE delivery_id = ? AND status = ?`,
		int32(domain.DeliveryStatusQuarantined), deliveryID, int32(domain.DeliveryStatusPending))
	if err != nil {
		return fmt.Errorf("隔离交接单 %s: %w", deliveryID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: delivery_id=%s", ErrDeliveryNotPending, deliveryID)
	}
	r.logger.Error("交接单已隔离，需要人工处理", "delivery_id", deliveryID, "reason", reason)
	return nil
}

// loadPendingDeliveries 读取某个世界全部未完成的交接单（恢复流程用）。
//
// 恢复时必须把它们全部挂回投递队列：单子在库里就代表「玩家的货还没入包」，
// 漏读一张就等于永久丢失一次奖励（INV-07）。
func loadPendingDeliveries(ctx context.Context, q queryer, worldID domain.WorldID) ([]domain.Delivery, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT `+deliveryColumns+` FROM map_delivery
		 WHERE world_id = ? AND status = ?
		 ORDER BY created_at_ms ASC, delivery_id ASC`,
		int64(worldID), int32(domain.DeliveryStatusPending))
	if err != nil {
		return nil, fmt.Errorf("查询世界待投递交接单 world=%d: %w", worldID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []domain.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描交接单: %w", err)
		}
		out = append(out, *d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历交接单: %w", err)
	}
	return out, nil
}
