// pkg/flush/batch.go —— 合批 SQL 构建（设计文档 12.4.4）
//
// 逐行写与合批写的差距（12.4.4 效果估算）：5K 在线、60s 周期时单分片约 3000 个脏实体，
// 逐行写是 3000 条语句；按 500 行/条合批后是 6 条。MySQL 的瓶颈在语句往返与解析，
// 不在数据量本身，所以合批是落库侧收益最大的一步。
//
// 本文件全部是纯函数：只拼 SQL 字符串与参数，不做 IO、不持有连接，
// 因此可被单测全覆盖（batch_test.go）。安全性约定：列值一律走占位符参数，
// 表名/列名等**标识符无法参数化**，只能由调用方保证来自代码常量或白名单
// （delete.go 的表名白名单即为此设）。
package flush

import (
	"fmt"
	"strings"
)

// DefaultBatchRows 单条多行 UPSERT 的行数上限。
//
// 500 行是压测与线上折中：再大则单条语句解析时间上升、行锁持有时间变长，
// 且单行失败会放大整批重试成本；再小则合批收益递减。
const DefaultBatchRows = 500

// DefaultBatchBytes 单条 SQL 的大小上限（4MB，对齐 max_allowed_packet 常见配置）。
//
// 4<<20 是位移语法写 4MiB；选 MiB 量级与 MySQL 默认 max_allowed_packet
// （常见 4MB~16MB）对齐——超限会直接拿到 "packet too large" 错误，
// 且不同环境默认值不同，保守取 4MB。
// 注意这是**语句文本 + 参数**的估算上限，不是精确值（见 estimateRowBytes）。
const DefaultBatchBytes = 4 << 20

// BuildBatchUpsert 生成多行 UPSERT：
//
//	INSERT INTO t (c1,c2) VALUES (?,?),(?,?)
//	ON DUPLICATE KEY UPDATE c1=VALUES(c1), c2=VALUES(c2)
//
// 更新列默认跳过第一列（约定为主键或唯一键的一部分）；需要更新全部列时
// 调整列序或改走通用 UPDATE 路径。
//
// 返回的 SQL 与参数一一对应，可直接交给 ExecContext。
// 错误语义：列/行为空、行列数不匹配均返回 error——拼出错的 SQL 到执行期
// 才报错，排查成本高一个量级。
//
// Go 细节：
//   - strings.Builder 是专为拼接设计的写缓冲（内部 []byte 追加），
//     比 fmt.Sprintf/+ 拼接少掉中间字符串分配；builder 内含检查指针、
//     不可复制值，始终按指针或值在单 goroutine 内顺序使用；
//   - 所有值用 ? 占位符 + args 传参（参数化查询），驱动负责转义，
//     从机制上杜绝 SQL 注入——**绝不把用户输入拼进 SQL 文本**；
//   - VALUES(c) 是 MySQL 方言（8.0.20 起官方推荐别名写法），迁 TiDB/
//     新版本时需在生成层统一替换，业务无感。
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

	// 精确预分配：每行 len(cols) 个参数，一次 make 到位，append 零扩容
	args := make([]any, 0, len(rows)*len(cols))
	// 占位符串只拼一次、N 行复用同一个 (?, ?, ...) 文本，避免每行重拼
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
//
// ids 通过参数传递（非拼接），数量决定占位符个数。
// 适用一次性删除量可控的场景；ids 极多（上万）时占位符同样受
// max_allowed_packet 约束，应改临时表 JOIN。
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
// 典型场景：按玩家维度清理（cond 固定、ids 变化）。
// 参数顺序：condArgs 在前、ids 在后，与 SQL 里条件出现顺序一致。
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

	// 一次预分配 condArgs + ids 的合并容量，两个 append 均零扩容
	args := make([]any, 0, len(condArgs)+len(ids))
	args = append(args, condArgs...)
	args = append(args, ids...)
	return sql, args, nil
}

// Chunk 按行数与字节预算把待写行切成多批。
//
// 一次 SQL 能带多少行受两个限制：行数（避免语句过长）与字节（max_allowed_packet）。
// 只用行数会在「每行字段很多/值很长」时超包，只用字节会在「行短但极多」时语句过长。
// 因此双预算取先到者：任一触发即封批。
//
// Go 细节：返回的 [][][]any 外层是新切片，内层行切片与原 rows 共享
// （只切不拷），调用方不得在分批执行期间修改 rows 内容。
// maxRows/maxBytes <=0 时回退默认值，防御零值配置把切批变成「一批装全部」。
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
		// 先封批再追加当前行：顺序反了会让本行成为超预算批的一部分
		if len(current) >= maxRows || size+rowSize > maxBytes {
			out = append(out, current)
			current = nil
			size = 0
		}
		current = append(current, row)
		size += rowSize
	}
	// 收尾：循环结束时 current 可能还攒着最后一批
	if len(current) > 0 {
		out = append(out, current)
	}
	return out
}

// estimateRowBytes 估算一行占用的字节数（含占位符与逗号开销）。
//
// 为什么是估算：精确大小需要把每个值序列化成驱动协议格式（时间/二进制各有
// 编码），代价高于估算收益；估算只需保证「不超包」这个安全属性。
// 类型 switch 覆盖常见列类型，未知类型按 32B 兜底——宁可多估不可少估。
//
// Go 细节：switch x := v.(type) 的类型 switch 在每个 case 内把 x 收窄为
// 具体类型；nil 单独处理，因为 any(nil) 不等于任何具体类型、只会落到
// nil case，混淆进 default 会按 32B 错估。
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
