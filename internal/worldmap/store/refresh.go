package store

// refresh.go 承载富饶块刷新进度（文档 10.5）。
//
// 刷新是「可中断、可重入」的批处理：一个 generation 可能因为重启/规则
// 变更被处理多次。进度表记录 cursor 与 finished，重启后从 cursor 继续；
// 已创建的资源点由稳定键 (block, generation, slot) 去重，因此
// 「重复处理」不会多刷出资源点——这是幂等的前提，而不是额外优化。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/internal/worldmap/domain"
)

// RefreshProgress 是一个 (world, block, generation) 的刷新进度快照。
//
// 这是 store 自己的类型而不是 domain 类型：domain 里没有刷新进度的
// 领域对象（它是纯粹的持久化协调数据），本任务也不允许改 domain。
type RefreshProgress struct {
	WorldID       domain.WorldID
	BlockID       int64
	Generation    int64
	ConfigVersion string
	Seed          int64
	Cursor        int32
	Finished      bool
	Version       uint64
}

// UpsertRefreshProgress 写入/推进刷新进度。
//
// 用 ON DUPLICATE KEY UPDATE 而不是「先查后写」：批处理可能被多个
// worker 推进（同一 generation 分片），唯一键 (world, block, generation)
// 让并发写入落到同一行上由 InnoDB 串行化，version+1 保留推进痕迹。
//
// 注意 config_version/seed 在更新分支里**不覆盖**：它们属于「这个
// generation 用哪套规则/种子」，一旦确定就不能改——改了会让同一
// generation 的前半段与后半段用不同参数，稳定键去重就失去意义。
func (r *Repository) UpsertRefreshProgress(ctx context.Context, worldID domain.WorldID, blockID, generation int64, cursor int32, finished bool) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO map_refresh
		   (world_id, rich_block_id, generation, config_version, seed, cursor, finished, version)
		 VALUES (?, ?, ?, '', 0, ?, ?, 1)
		 ON DUPLICATE KEY UPDATE cursor = VALUES(cursor), finished = VALUES(finished), version = version + 1`,
		int64(worldID), blockID, generation, cursor, boolToInt32(finished))
	if err != nil {
		return fmt.Errorf("推进刷新进度 world=%d block=%d gen=%d: %w", worldID, blockID, generation, err)
	}
	return nil
}

// LoadRefreshProgress 读取一个 (world, block, generation) 的进度。
//
// 未找到返回 (nil, false, nil) 而不是错误：刷新任务第一次跑时
// 「没有进度」是完全正常的状态，调用方据此从 cursor=0 开始。
// 找不到与查询失败必须区分，否则数据库抖动会被当成「重新刷新」。
func (r *Repository) LoadRefreshProgress(ctx context.Context, worldID domain.WorldID, blockID, generation int64) (*RefreshProgress, bool, error) {
	var (
		p        RefreshProgress
		finished int32
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT world_id, rich_block_id, generation, config_version, seed, cursor, finished, version
		 FROM map_refresh WHERE world_id = ? AND rich_block_id = ? AND generation = ?`,
		int64(worldID), blockID, generation).Scan(
		&p.WorldID, &p.BlockID, &p.Generation, &p.ConfigVersion, &p.Seed, &p.Cursor, &finished, &p.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("查询刷新进度 world=%d block=%d gen=%d: %w", worldID, blockID, generation, err)
	}
	p.Finished = finished != 0
	return &p, true, nil
}

// ListUnfinishedRefreshes 列出尚未完成的刷新批次（启动补处理用）。
//
// 按 (block, generation) 升序：先补老的 generation，避免新的 gen
// 先跑完导致「旧 gen 的资源点又出现在世界里」。
func (r *Repository) ListUnfinishedRefreshes(ctx context.Context, worldID domain.WorldID) ([]RefreshProgress, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT world_id, rich_block_id, generation, config_version, seed, cursor, finished, version
		 FROM map_refresh WHERE world_id = ? AND finished = 0
		 ORDER BY rich_block_id ASC, generation ASC`,
		int64(worldID))
	if err != nil {
		return nil, fmt.Errorf("查询未完成刷新 world=%d: %w", worldID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []RefreshProgress
	for rows.Next() {
		var (
			p        RefreshProgress
			finished int32
		)
		if err := rows.Scan(&p.WorldID, &p.BlockID, &p.Generation, &p.ConfigVersion,
			&p.Seed, &p.Cursor, &finished, &p.Version); err != nil {
			return nil, fmt.Errorf("扫描刷新进度: %w", err)
		}
		p.Finished = finished != 0
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历刷新进度: %w", err)
	}
	return out, nil
}
