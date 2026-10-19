package flush

import (
	"context"
	"database/sql"
)

// Querier 是 *sql.DB 与 *sql.Tx 的共同子集（两者都满足）。
//
// 权威定义放在 flush 而不是 pkg/model：FlatOps 这类表级适配签名需要它，
// 而 model 依赖 flush（不能反向依赖）。pkg/model 保留同名类型别名，
// 既有生成代码与业务调用点零改动。
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
