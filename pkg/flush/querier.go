// pkg/flush/querier.go —— SQL 执行抽象（*sql.DB 与 *sql.Tx 的共同子集）
//
// 把「能执行 SQL 的东西」收窄成三方法接口，读路径（FlatOps.Load 等）
// 因此不关心自己拿到的到底是直连池还是事务——同一份生成代码
// 冷启动装载（tx）与运行期补读（db）复用。
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
//
// Go 细节：
//   - Go 接口是隐式满足：*sql.DB 与 *sql.Tx 恰好都有这三个同签名方法，
//     无需声明 "implements"，编译器在传参处静态检查；
//   - 接口值语义注意「nil 陷阱」：把一个 (*sql.Tx)(nil) 存进 Querier
//     后接口本身非 nil（带类型信息），调用方法才 panic——本项目
//     Querier 均由调用方保证来自有效对象，不做运行期判空。
type Querier interface {
	// ExecContext 执行不返回行的语句（INSERT/UPDATE/DELETE）。
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	// QueryContext 执行查询；返回的 *sql.Rows 必须 Close，否则底层
	// 连接不归还池（database/sql 的经典泄漏点）。
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	// QueryRowContext 执行至多返回一行的查询（内部包装 Query）。
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
