// pkg/flush/batch.go —— 合批 SQL 构建（设计文档 12.4.4）
//
// 逐行写与合批写的差距（12.4.4 效果估算）：5K 在线、60s 周期时单分片约 3000 个脏实体，
// 逐行写是 3000 条语句；按 500 行/条合批后是 6 条。MySQL 的瓶颈在语句往返与解析，
// 不在数据量本身，所以合批是落库侧收益最大的一步。
package flush

import (
	"fmt"
	"strings"
)

// DefaultBatchRows 单条多行 UPSERT 的行数上限。
const DefaultBatchRows = 500

// DefaultBatchBytes 单条 SQL 的大小上限（4MB，对齐 max_allowed_packet 常见配置）。
const DefaultBatchBytes = 4 << 20

// BuildBatchUpsert 生成多行 UPSERT：
//
//	INSERT INTO t (c1,c2) VALUES (?,?),(?,?)
//	ON DUPLICATE KEY UPDATE c1=VALUES(c1), c2=VALUES(c2)
//
// 更新列默认跳过第一列（约定为主键或唯一键的一部分）；需要更新全部列时用
// BuildBatchUpsertWith。
//
// 返回的 SQL 与参数一一对应，可直接交给 ExecContext。
func BuildBatchUpsert(table string, cols []string, rows [][]any) (string, []any, error) {
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("batch upsert: no columns")
	}
	if len(rows) == 0 {
		return "", nil, fmt.Errorf("batch upsert: no rows")
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(") VALUES ")

	args := make([]any, 0, len(rows)*len(cols))
	placeholder := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ") + ")"
	for i, row := range rows {
		if len(row) != len(cols) {
			return "", nil, fmt.Errorf("batch upsert: row %d has %d values, want %d", i, len(row), len(cols))
		}
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(placeholder)
		args = append(args, row...)
	}

	// 更新列：去掉首列（主键/唯一键的一部分，不需要自更新）。
	// 只有一列时没有可更新字段，用自赋值 no-op —— 直接留空会生成
	// `ON DUPLICATE KEY UPDATE `，是语法错误。
	sb.WriteString(" ON DUPLICATE KEY UPDATE ")
	if len(cols) == 1 {
		sb.WriteString(cols[0])
		sb.WriteString("=")
		sb.WriteString(cols[0])
		return sb.String(), args, nil
	}
	for i, c := range cols {
		if i == 0 {
			continue
		}
		if i > 1 {
			sb.WriteString(", ")
		}
		sb.WriteString(c)
		sb.WriteString("=VALUES(")
		sb.WriteString(c)
		sb.WriteString(")")
	}
	return sb.String(), args, nil
}

// BuildBatchDelete 生成合批删除：
//
//	DELETE FROM t WHERE pk IN (?,?,?)
func BuildBatchDelete(table, pk string, ids []any) (string, []any, error) {
	if pk == "" {
		return "", nil, fmt.Errorf("batch delete: no primary key column")
	}
	if len(ids) == 0 {
		return "", nil, fmt.Errorf("batch delete: no ids")
	}

	marks := make([]string, len(ids))
	for i := range ids {
		marks[i] = "?"
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", table, pk, strings.Join(marks, ","))
	return sql, ids, nil
}

// BuildBatchDeleteWhere 带前置固定条件的合批删除：
//
//	DELETE FROM t WHERE <cond> AND pk IN (?,?,?)
//
// cond 必须是代码里的常量片段（如 "player_id = ?"），**不允许拼接用户输入**。
func BuildBatchDeleteWhere(table, cond string, condArgs []any, pk string, ids []any) (string, []any, error) {
	if cond == "" || pk == "" {
		return "", nil, fmt.Errorf("batch delete: empty condition or primary key")
	}
	if len(ids) == 0 {
		return "", nil, fmt.Errorf("batch delete: no ids")
	}

	marks := make([]string, len(ids))
	for i := range ids {
		marks[i] = "?"
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s AND %s IN (%s)",
		table, cond, pk, strings.Join(marks, ","))

	args := make([]any, 0, len(condArgs)+len(ids))
	args = append(args, condArgs...)
	args = append(args, ids...)
	return sql, args, nil
}

// Chunk 按行数与字节预算把待写行切成多批。
//
// 一次 SQL 能带多少行受两个限制：行数（避免语句过长）与字节（max_allowed_packet）。
// 只用行数会在「每行字段很多/值很长」时超包，只用字节会在「行短但极多」时语句过长。
func Chunk(rows [][]any, maxRows int, maxBytes int) [][][]any {
	if maxRows <= 0 {
		maxRows = DefaultBatchRows
	}
	if maxBytes <= 0 {
		maxBytes = DefaultBatchBytes
	}

	var (
		out     [][][]any
		current [][]any
		size    int
	)
	for _, row := range rows {
		rowSize := estimateRowBytes(row)
		// 单行就超预算：单独成批（否则永远放不进去）
		if rowSize >= maxBytes {
			if len(current) > 0 {
				out = append(out, current)
				current = nil
				size = 0
			}
			out = append(out, [][]any{row})
			continue
		}
		if len(current) >= maxRows || size+rowSize > maxBytes {
			out = append(out, current)
			current = nil
			size = 0
		}
		current = append(current, row)
		size += rowSize
	}
	if len(current) > 0 {
		out = append(out, current)
	}
	return out
}

// estimateRowBytes 估算一行占用的字节数（含占位符与逗号开销）。
func estimateRowBytes(row []any) int {
	const overhead = 4 // 占位符 + 分隔符 + 括号分摊
	n := overhead
	for _, v := range row {
		switch x := v.(type) {
		case nil:
			n += 4
		case string:
			n += len(x) + 2 // 引号
		case []byte:
			n += len(x) + 2
		case int64, int32, int, uint64, uint32:
			n += 20
		case float64, float32:
			n += 24
		case bool:
			n += 5
		default:
			n += 32
		}
	}
	return n
}
