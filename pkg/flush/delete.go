// pkg/flush/delete.go —— 墓碑删除的通用 SQL 执行器（设计文档 12.4.5）
//
// 业务侧删除记录时只登记墓碑（Flusher.MarkDelete），真正的 DELETE 由本执行器
// 在落库周期里合批完成。这样删除与更新走同一条错峰通道，避免瞬时的删除风暴。
//
// 表名必须过白名单：这里要把表名拼进 SQL，不做校验等于开了注入口子。
// 原理回顾：占位符只能保护「值」的位置，表名/列名这类标识符必须原样出现
// 在 SQL 文本里，唯一防御是白名单校验（正则限定字符集），没有「转义」
// 一说。
package flush

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// tableIdentPattern 表名白名单（小写字母/数字/下划线）。
//
// 以字母或下划线开头：避免数字开头与 SQL 关键字歧义；仅限小写是因为
// 项目约定表名全小写（MySQL 在 Linux 下大小写敏感，统一小写省去踩坑）。
// 未匹配即拒绝执行——标识符没有安全转义，只能拒绝。
var tableIdentPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// SQLDeleter 通用墓碑执行器：DELETE FROM t WHERE pk IN (...)。
//
// 字段说明：
//   - db：直接持 *sql.DB（而非 Querier），删除只在事务外执行——墓碑与
//     玩家 upsert 分属不同事务，删除失败可独立重试，不拖累写路径；
//   - pk：table → 主键列名映射，缺省 "id"（SetPK 可覆盖）。
type SQLDeleter struct {
	db *sql.DB
	pk map[string]string // table -> 主键列名（缺省 "id"）
}

// NewSQLDeleter 创建执行器。
//
// pk map 初始化为空 map：nil map 可读不可写，不 make 的话 SetPK 会 panic。
// 返回指针以复用同一实例注入 flush.Config.DeleteFn。
func NewSQLDeleter(database *sql.DB) *SQLDeleter {
	return &SQLDeleter{db: database, pk: make(map[string]string)}
}

// SetPK 指定某张表的主键列（默认 "id"）。
//
// 返回自身以支持链式配置：NewSQLDeleter(db).SetPK("t", "tid")。
// 约束：仅限启动阶段调用（运行期修改等于热改删除目标，非本组件职责）。
func (d *SQLDeleter) SetPK(table, column string) *SQLDeleter {
	d.pk[table] = column
	return d
}

// DeleteBatch 批量删除（实现 DeleteFunc）。
//
// 流程：空 ids 直接返回 nil（无 SQL）→ 表名/主键列名过白名单 →
// []int64 转 []any → 拼 SQL → ExecContext。
// ids 是同一张表的主键集合（由 flush 分片按 MarkDelete(table, id) 的
// table 分组保证），混表删除请分多次调用。
//
// Go 细节：ExecContext 的形参是 ...any，[]int64 不能直接展开传参，
// 必须逐个装箱成 []any（any 装箱有分配开销，删除路径频率低可接受）；
// ctx 取消会传播到驱动，删除语句不会在数据库里无限制跑。
func (d *SQLDeleter) DeleteBatch(ctx context.Context, table string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	if !tableIdentPattern.MatchString(table) {
		return fmt.Errorf("flush: invalid table name %q", table)
	}
	pk := d.pk[table]
	if pk == "" {
		pk = "id"
	}
	if !tableIdentPattern.MatchString(pk) {
		return fmt.Errorf("flush: invalid pk column %q", pk)
	}

	anyIDs := make([]any, len(ids))
	for i, id := range ids {
		anyIDs[i] = id
	}
	q, args, err := BuildBatchDelete(table, pk, anyIDs)
	if err != nil {
		return err
	}
	_, err = d.db.ExecContext(ctx, q, args...)
	return err
}

// 方法值签名与 DeleteFunc 一致，可直接注入 flush.Config.DeleteFn。
//
// Go 细节：这是编译期接口断言——(&SQLDeleter{}).DeleteBatch 是**方法值**
// （绑定接收者后的函数值），赋给 DeleteFunc 即完成适配，无需包装 lambda；
// 变量名用空标识符 _，只为让编译器检查类型匹配，运行期零开销。
var _ DeleteFunc = (&SQLDeleter{}).DeleteBatch

// TableName 从 "table:id" 形式解析表名（兼容旧格式），非法返回空串。
//
// 旧版墓碑标记允许带 ":id" 后缀；这里取冒号前的部分并过白名单，
// 非法返回 "" 供调用方识别「不可执行」。仅 IndexByte > 0 时截取：
// 冒号在首位（":x"）视为整体非法而非空表名。
func TableName(s string) string {
	if i := strings.IndexByte(s, ':'); i > 0 {
		s = s[:i]
	}
	if !tableIdentPattern.MatchString(s) {
		return ""
	}
	return s
}
