// pkg/flush/delete.go —— 墓碑删除的通用 SQL 执行器（设计文档 12.4.5）
//
// 业务侧删除记录时只登记墓碑（Flusher.MarkDelete），真正的 DELETE 由本执行器
// 在落库周期里合批完成。这样删除与更新走同一条错峰通道，避免瞬时的删除风暴。
//
// 表名必须过白名单：这里要把表名拼进 SQL，不做校验等于开了注入口子。
package flush

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// tableIdentPattern 表名白名单（小写字母/数字/下划线）。
var tableIdentPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// SQLDeleter 通用墓碑执行器：DELETE FROM t WHERE pk IN (...)。
type SQLDeleter struct {
	db *sql.DB
	pk map[string]string // table -> 主键列名（缺省 "id"）
}

// NewSQLDeleter 创建执行器。
func NewSQLDeleter(database *sql.DB) *SQLDeleter {
	return &SQLDeleter{db: database, pk: make(map[string]string)}
}

// SetPK 指定某张表的主键列（默认 "id"）。
func (d *SQLDeleter) SetPK(table, column string) *SQLDeleter {
	d.pk[table] = column
	return d
}

// DeleteBatch 批量删除（实现 DeleteFunc）。
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
var _ DeleteFunc = (&SQLDeleter{}).DeleteBatch

// TableName 从 "table:id" 形式解析表名（兼容旧格式），非法返回空串。
func TableName(s string) string {
	if i := strings.IndexByte(s, ':'); i > 0 {
		s = s[:i]
	}
	if !tableIdentPattern.MatchString(s) {
		return ""
	}
	return s
}
