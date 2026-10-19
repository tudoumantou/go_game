// pkg/flush/exec.go —— 合批写执行（事务内）
//
// batch.go 只负责拼 SQL；这里负责把「一批结构体」摊平成参数并分批执行。
// 分层原因：拼 SQL 是纯函数（可单测、无 IO），执行才是需要 tx 的部分。
package flush

import (
	"context"
	"database/sql"
)

// UpsertBatch 在事务内分批 UPSERT 一批同构行。
//
// values 把一行结构体摊平成列值，顺序必须与 cols 一致 —— 这两者都由代码生成
// （tools/gendb）产出，业务代码不需要关心表名、列名与占位符。
//
// 空输入不产生 SQL：模块本次没有数据时不应该打扰数据库。
func UpsertBatch[T any](ctx context.Context, tx *sql.Tx, table string, cols []string, rows []T, values func(T) []any) error {
	if len(rows) == 0 {
		return nil
	}
	flat := make([][]any, len(rows))
	for i, r := range rows {
		flat[i] = values(r)
	}
	for _, batch := range Chunk(flat, DefaultBatchRows, DefaultBatchBytes) {
		query, args, err := BuildBatchUpsert(table, cols, batch)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

// DeleteByPlayer 删除某玩家在某表的全部行。
//
// 适用于「快照即真相」的表（好友关系、活动进度这类整包替换的数据）：
// 先删后插语义最直白，也不会像 NOT IN 那样随保留列增长而膨胀。
// 对带稳定业务 UID 的表（如 player_item）不要用这个 —— 会丢 UID 与过期时间，
// 那种表应该按主键做差异删除。
func DeleteByPlayer(ctx context.Context, tx *sql.Tx, table, playerCol string, playerID int64) error {
	_, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+playerCol+" = ?", playerID)
	return err
}
