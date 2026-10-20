package store

// collect.go 承载采集会话的读取（写入在 mysql.go 的 writeCollect 里，
// 因为采集结算必须与命令回执同事务）。
//
// 恢复语义（文档 10.1/16.1）：采集进度的唯一真源是 map_collect，
// 部队对象只持有已结算的 Cargo。重启后必须把 ACTIVE 会话全部读回内存，
// 否则「已经采了 3 小时」的会话会被当成不存在，玩家要么拿不到产量，
// 要么资源点被重复占用。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/internal/worldmap/domain"
)

const collectColumns = `collect_id, troop_id, resource_id, start_at_ms, end_at_ms,
	planned_qty, rate_milli, rule_version, status, settled_qty`

// LoadActiveSessions 加载全世界仍处于 ACTIVE 的采集会话。
//
// 按 collect_id 升序返回：恢复流程要把会话重新挂到资源点/部队的定时器上，
// 稳定的顺序让「同一世界的恢复结果」可复现，测试才能断言。
func (r *Repository) LoadActiveSessions(ctx context.Context, worldID domain.WorldID) ([]*domain.CollectSession, error) {
	return loadActiveSessions(ctx, r.db, worldID)
}

// loadActiveSessions 是 LoadActiveSessions 的事务内版本（恢复流程用）。
func loadActiveSessions(ctx context.Context, q queryer, worldID domain.WorldID) ([]*domain.CollectSession, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT `+collectColumns+` FROM map_collect
		 WHERE world_id = ? AND status = ? ORDER BY collect_id ASC`,
		int64(worldID), int32(domain.CollectStatusActive))
	if err != nil {
		return nil, fmt.Errorf("查询活动采集会话 world=%d: %w", worldID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []*domain.CollectSession
	for rows.Next() {
		sess := &domain.CollectSession{}
		var status int32
		if err := rows.Scan(&sess.ID, &sess.TroopID, &sess.ResourceID, &sess.StartAtMS, &sess.EndAtMS,
			&sess.PlannedQty, &sess.RateMilliItemsPerSec, &sess.RuleVersion, &status, &sess.SettledQty); err != nil {
			return nil, fmt.Errorf("扫描采集会话: %w", err)
		}
		sess.Status = domain.CollectStatus(status)
		if !sess.Status.Valid() {
			// 非法状态不跳过：跳过等于把资源点锁死在一个不存在的人手里。
			return nil, fmt.Errorf("%w: 采集会话 %s 状态非法 %d", domain.ErrInvalidState, sess.ID, status)
		}
		out = append(out, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历采集会话: %w", err)
	}
	return out, nil
}

// LoadCollectSession 按会话 ID 加载单条会话（对账与测试用）。
func (r *Repository) LoadCollectSession(ctx context.Context, worldID domain.WorldID, collectID string) (*domain.CollectSession, error) {
	sess := &domain.CollectSession{}
	var status int32
	err := r.db.QueryRowContext(ctx,
		`SELECT `+collectColumns+` FROM map_collect WHERE world_id = ? AND collect_id = ?`,
		int64(worldID), collectID).Scan(&sess.ID, &sess.TroopID, &sess.ResourceID, &sess.StartAtMS, &sess.EndAtMS,
		&sess.PlannedQty, &sess.RateMilliItemsPerSec, &sess.RuleVersion, &status, &sess.SettledQty)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: collect_id=%s", ErrCollectNotFound, collectID)
	}
	if err != nil {
		return nil, fmt.Errorf("查询采集会话 %s: %w", collectID, err)
	}
	sess.Status = domain.CollectStatus(status)
	return sess, nil
}
