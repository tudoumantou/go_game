package flush

import (
	"context"
	"database/sql"
)

// FlatOps 是「玩家级单行状态表」的读写适配。
//
// 两个字段的类型与 gendb 生成的 UpsertXxxBatch / FindXxxByPK 签名逐一匹配，
// 因此生成物可以直接传函数值，业务与生成层都不需要包装 lambda：
//
//	var PlayerSettingsFlat = flush.FlatOps[PlayerSettings]{
//		Upsert: UpsertPlayerSettingsBatch,
//		Load:   FindPlayerSettingsByPK,
//	}
type FlatOps[T any] struct {
	// Upsert 在同一事务内批量写入多行（跨玩家合批，由 writebehind 切批）。
	Upsert func(ctx context.Context, tx *sql.Tx, rows []T) error
	// Load 读取单个玩家的行；没有数据时返回 sql.ErrNoRows，
	// 由 writebehind 转换为「该玩家没有这个模块的数据」(nil, nil)。
	Load func(ctx context.Context, q Querier, playerID int64) (*T, error)
}
