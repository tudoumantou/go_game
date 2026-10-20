// pkg/flush/flat.go —— 玩家级单行状态表的读写适配（FlatOps）
//
// 职责：把「一个玩家在某表里恰好一行」的常见形态抽象成一组函数字段，
// 供 pkg/writebehind 的 RegisterFlat 消费。gendb 为每张这样的表生成
// UpsertXxxBatch / FindXxxByPK，函数值与 FlatOps 字段类型逐一匹配，
// 业务侧零包装即可注册。
//
// 设计取舍：用泛型结构体 + 函数字段（策略模式）而不是为每张表生成一个
// 适配器类型——表有上百张，生成代码体积与编译时间都是成本；两行函数值
// 声明即可表达同样的契约。
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
//
// Go 细节：
//   - 泛型参数 T 是表结构体；两个字段是函数类型（一等公民，可存字段）；
//   - Upsert 收 *sql.Tx：写必须进入调用方事务，与其它模块同生共死；
//     Load 收 Querier：读既可能在事务内（冷启动全量装载）也可能在
//     事务外（单点补读），接口取两者的最小公共子集（见 querier.go）。
type FlatOps[T any] struct {
	// Upsert 在同一事务内批量写入多行（跨玩家合批，由 writebehind 切批）。
	// 契约：只写不提交，事务边界归调用方。
	Upsert func(ctx context.Context, tx *sql.Tx, rows []T) error
	// Load 读取单个玩家的行；没有数据时返回 sql.ErrNoRows，
	// 由 writebehind 转换为「该玩家没有这个模块的数据」(nil, nil)。
	// 契约：查多返回第一条（表上有 player_id 唯一约束，不可能多）。
	Load func(ctx context.Context, q Querier, playerID int64) (*T, error)
}
