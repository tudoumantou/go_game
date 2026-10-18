// tools/gendb —— DDL → Go CRUD 生成器（设计文档 16.2，参考 goctl model）
//
// 输入 sql/schema/*.sql（手写带注释 DDL），产出 pkg/model/：
//   - <table>_gen.go   结构体 + 列常量 + TableName + CRUD（全量覆盖，禁止手改）
//   - <table>_model.go 仅首次创建（自定义查询/复杂方法，见 16.4 覆盖策略）
//
// CRUD 基于 database/sql（? 占位符，MySQL 兼容）；UPSERT 使用
// INSERT ... ON DUPLICATE KEY UPDATE（落库幂等，18.2 防线②）。
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type column struct {
	Name     string // 原名 snake_case
	GoName   string // CamelCase
	GoType   string // int64/int32/string/bool/float64/time.Time
	SQLType  string // 原始 SQL 类型
	NotNull  bool
	AutoInc  bool
	AutoTime bool // DEFAULT CURRENT_TIMESTAMP：时间由 DB 生成，写路径跳过该列
	IsPK     bool
}

type table struct {
	Name    string
	Columns []column
	PKs     []string
}

func main() {
	in := flag.String("in", "sql/schema", "DDL 输入目录")
	out := flag.String("out", "pkg/model", "Go 代码输出目录")
	flag.Parse()

	entries, err := os.ReadDir(*in)
	if err != nil {
		log.Fatalf("read dir %s: %v", *in, err)
	}
	var tables []table
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		ts, err := parseSQL(filepath.Join(*in, e.Name()))
		if err != nil {
			log.Fatalf("parse %s: %v", e.Name(), err)
		}
		tables = append(tables, ts...)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })

	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *out, err)
	}
	for _, t := range tables {
		if err := genModel(*out, t); err != nil {
			log.Fatalf("gen %s: %v", t.Name, err)
		}
		fmt.Printf("  %-24s → %s_gen.go（%d 列，PK: %s）\n",
			t.Name+".sql", t.Name, len(t.Columns), strings.Join(t.PKs, "+"))
	}
	fmt.Printf("gendb 完成：%d 张表 → %s\n", len(tables), *out)
}

var createTableRe = regexp.MustCompile(`(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`+"`"+`\"]?([a-zA-Z0-9_]+)[`+"`"+`\"]?\s*\((.*?)\)\s*(?:ENGINE|COMMENT|DEFAULT|;)`)

func parseSQL(path string) ([]table, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// 去注释
	text := regexp.MustCompile("--[^\n]*").ReplaceAllString(string(raw), "")
	matches := createTableRe.FindAllStringSubmatch(text, -1)
	var tables []table
	for _, m := range matches {
		t := table{Name: m[1]}
		for _, line := range strings.Split(m[2], ",") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			up := strings.ToUpper(line)
			if strings.HasPrefix(up, "PRIMARY KEY") {
				t.PKs = pkCols(line)
				continue
			}
			if strings.HasPrefix(up, "UNIQUE") || strings.HasPrefix(up, "KEY") ||
				strings.HasPrefix(up, "INDEX") || strings.HasPrefix(up, "CONSTRAINT") ||
				strings.HasPrefix(up, "FOREIGN") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			c := column{
				Name:    strings.Trim(fields[0], "`\""),
				SQLType: strings.ToLower(fields[1]),
			}
			c.GoName = upperCamel(c.Name)
			c.GoType = goType(c.SQLType)
			c.AutoInc = strings.Contains(up, "AUTO_INCREMENT")
			c.NotNull = strings.Contains(up, "NOT NULL")
			// DEFAULT CURRENT_TIMESTAMP（created_at/updated_at）：写入时由 DB 生成，
			// 不传 Go 零值时间（time.Time{} = 0001-01-01 会超出 TIMESTAMP/DATETIME 范围，
			// MySQL 报 1292 Incorrect datetime value，集成测试实测复现）
			c.AutoTime = strings.Contains(up, "DEFAULT CURRENT_TIMESTAMP")
			t.Columns = append(t.Columns, c)
		}
		if len(t.PKs) == 0 {
			// 回退：自增列或第一列当 PK
			for _, c := range t.Columns {
				if c.AutoInc {
					t.PKs = []string{c.Name}
					break
				}
			}
			if len(t.PKs) == 0 && len(t.Columns) > 0 {
				t.PKs = []string{t.Columns[0].Name}
			}
		}
		pkSet := map[string]bool{}
		for _, p := range t.PKs {
			pkSet[p] = true
		}
		for i := range t.Columns {
			t.Columns[i].IsPK = pkSet[t.Columns[i].Name]
		}
		tables = append(tables, t)
	}
	return tables, nil
}

func pkCols(line string) []string {
	i := strings.Index(line, "(")
	j := strings.LastIndex(line, ")")
	if i < 0 || j < 0 || j <= i {
		return nil
	}
	var cols []string
	for _, p := range strings.Split(line[i+1:j], ",") {
		cols = append(cols, strings.Trim(strings.TrimSpace(p), "`\""))
	}
	return cols
}

func goType(sqlType string) string {
	base := sqlType
	if i := strings.Index(base, "("); i > 0 {
		base = base[:i]
	}
	switch {
	case strings.HasPrefix(sqlType, "tinyint(1)"):
		return "bool"
	case strings.HasPrefix(base, "bigint"):
		return "int64"
	case strings.HasPrefix(base, "int"), strings.HasPrefix(base, "smallint"):
		return "int32"
	case strings.HasPrefix(base, "tinyint(1)"):
		return "bool"
	case strings.HasPrefix(base, "tinyint"):
		return "int32"
	case strings.HasPrefix(base, "decimal"), strings.HasPrefix(base, "float"), strings.HasPrefix(base, "double"):
		return "float64"
	case strings.HasPrefix(base, "datetime"), strings.HasPrefix(base, "timestamp"), strings.HasPrefix(base, "date"):
		return "time.Time"
	default: // varchar/text/json/enum...
		return "string"
	}
}

func genModel(outDir string, t table) error {
	tn := upperCamel(t.Name) // 导出标识符统一走 CamelCase（account → Account）
	var b strings.Builder
	b.WriteString("// Code generated by tools/gendb from sql/schema. DO NOT EDIT.\n")
	b.WriteString("// " + t.Name + " 表 CRUD（覆盖策略见设计文档 16.4）\n\n")
	b.WriteString("package model\n\n")
	// import 按需生成：ctx/*sql.DB 恒定需要；time 仅当存在时间列（strings 从不使用，不引入）
	needTime := false
	for _, c := range t.Columns {
		if c.GoType == "time.Time" {
			needTime = true
		}
	}
	b.WriteString("import (\n\t\"context\"\n\t\"database/sql\"\n")
	if needTime {
		b.WriteString("\t\"time\"\n")
	}
	b.WriteString(")\n\n")

	// 结构体
	b.WriteString("// " + tn + " " + t.Name + " 表\n")
	b.WriteString("type " + tn + " struct {\n")
	for _, c := range t.Columns {
		b.WriteString("\t" + c.GoName + " " + goFieldType(c) + " `db:\"" + c.Name + "\"`\n")
	}
	b.WriteString("}\n\n")

	// 列常量
	b.WriteString("// " + tn + " 列名\n")
	b.WriteString("const (\n")
	for _, c := range t.Columns {
		b.WriteString("\tCol" + tn + upperCamel(c.Name) + " = \"" + c.Name + "\"\n")
	}
	b.WriteString(")\n\n")

	// TableName
	b.WriteString("// TableName 表名\n")
	b.WriteString("func (m *" + tn + ") TableName() string { return \"" + t.Name + "\" }\n\n")

	// Columns 列表
	b.WriteString("// " + tn + "Columns 全部列（扫描用）\n")
	b.WriteString("var " + tn + "Columns = []string{\n")
	for _, c := range t.Columns {
		b.WriteString("\tCol" + tn + upperCamel(c.Name) + ",\n")
	}
	b.WriteString("}\n\n")

	// Insert：列 = 全部列排除 DB 生成时间列（保留自增 PK：显式 Id 落库时保留，
	// Id 为零值时 MySQL 自增列特性把 0 替换为下一个自增值，行为与跳过一致）
	insCols := upsertCols(t)
	if len(insCols) > 0 {
		ph := strings.TrimSuffix(strings.Repeat("?,", len(insCols)), ",")
		names := colNames(insCols)
		b.WriteString("// Insert 插入（自增列由数据库生成）\n")
		b.WriteString("func Insert" + tn + "(ctx context.Context, db *sql.DB, m *" + tn + ") (sql.Result, error) {\n")
		b.WriteString("\treturn db.ExecContext(ctx, \"INSERT INTO " + t.Name + " (" + strings.Join(names, ",") + ") VALUES (" + ph + ")\", " + argsOf(insCols) + ")\n")
		b.WriteString("}\n\n")
	}

	// FindByPK
	b.WriteString("// Find" + tn + "ByPK 按主键查询\n")
	b.WriteString("func Find" + tn + "ByPK(ctx context.Context, db *sql.DB, " + pkParams(t) + ") (*" + tn + ", error) {\n")
	b.WriteString("\tvar m " + tn + "\n")
	b.WriteString("\tquery := \"SELECT " + strings.Join(colNames(t.Columns), ",") + " FROM " + t.Name + " WHERE " + pkWhere(t) + "\"\n")
	b.WriteString("\trow := db.QueryRowContext(ctx, query, " + pkArgNames(t) + ")\n")
	b.WriteString("\tif err := scan" + tn + "(&m, row); err != nil {\n\t\treturn nil, err\n\t}\n\treturn &m, nil\n}\n\n")

	// scan 辅助
	b.WriteString("func scan" + tn + "(m *" + tn + ", sc interface{ Scan(...any) error }) error {\n")
	b.WriteString("\treturn sc.Scan(" + scanTargets(t) + ")\n")
	b.WriteString("}\n\n")

	// UpdateByPK（更新非 PK 列）
	updCols := nonPKNonAutoCols(t)
	if len(updCols) > 0 {
		sets := make([]string, 0, len(updCols))
		for _, c := range updCols {
			sets = append(sets, c.Name+" = ?")
		}
		b.WriteString("// Update" + tn + "ByPK 按主键更新（幂等）\n")
		b.WriteString("func Update" + tn + "ByPK(ctx context.Context, db *sql.DB, m *" + tn + ") (sql.Result, error) {\n")
		b.WriteString("\treturn db.ExecContext(ctx, \"UPDATE " + t.Name + " SET " + strings.Join(sets, ", ") + " WHERE " + pkWhere(t) + "\", " + argsOf(updCols) + ", " + pkArgNamesOfM(t) + ")\n")
		b.WriteString("}\n\n")

		// UpsertByPK（INSERT ... ON DUPLICATE KEY UPDATE）
		// INSERT 列 = 全部列（含自增 PK）：显式指定 Id 时靠 PK 唯一键冲突触发 UPDATE 分支；
		// Id 为零值时 MySQL 自增列特性会把 0 替换为下一个自增值，行为不变。
		// 若 INSERT 跳过自增 PK，ON DUPLICATE 永不命中，Upsert 退化为纯 INSERT（幂等失效）。
		b.WriteString("// Upsert" + tn + "ByPK 插入或更新（落库幂等，18.2 防线②）\n")
		b.WriteString("func Upsert" + tn + "ByPK(ctx context.Context, db *sql.DB, m *" + tn + ") (sql.Result, error) {\n")
		updSet := make([]string, 0, len(updCols))
		for _, c := range updCols {
			updSet = append(updSet, c.Name+" = VALUES("+c.Name+")")
		}
		upsCols := upsertCols(t) // 全部列（含自增 PK），排除 DB 生成时间列
		phAll := strings.TrimSuffix(strings.Repeat("?,", len(upsCols)), ",")
		b.WriteString("\treturn db.ExecContext(ctx, \"INSERT INTO " + t.Name + " (" + strings.Join(colNames(upsCols), ",") + ") VALUES (" + phAll + ") ON DUPLICATE KEY UPDATE " + strings.Join(updSet, ", ") + "\", " + argsOf(upsCols) + ")\n")
		b.WriteString("}\n\n")
	}

	// DeleteByPK
	b.WriteString("// Delete" + tn + "ByPK 按主键删除\n")
	b.WriteString("func Delete" + tn + "ByPK(ctx context.Context, db *sql.DB, " + pkParams(t) + ") (sql.Result, error) {\n")
	b.WriteString("\treturn db.ExecContext(ctx, \"DELETE FROM " + t.Name + " WHERE " + pkWhere(t) + "\", " + pkArgNames(t) + ")\n")
	b.WriteString("}\n")

	path := filepath.Join(outDir, t.Name+"_gen.go")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func goFieldType(c column) string {
	if c.AutoInc {
		return "int64"
	}
	if c.GoType == "time.Time" && !c.NotNull {
		// 可空时间列（如 expire_at/paid_at）：sql.NullTime 同时根治两个问题——
		// ① 写入零值不越界（Valid=false → NULL）；② 读出 NULL 不报
		// "converting NULL to time.Time is unsupported"
		return "sql.NullTime"
	}
	return c.GoType
}

// upsertCols Upsert 的 INSERT 列：保留自增 PK（显式 Id 幂等命中唯一键），
// 排除 DB 生成时间列（created_at/updated_at 由 DEFAULT/ON UPDATE 维护）
func upsertCols(t table) []column {
	var out []column
	for _, c := range t.Columns {
		if !c.AutoTime {
			out = append(out, c)
		}
	}
	return out
}

// nonPKNonAutoCols Update 的 SET 列：排除 PK、自增列与 DB 生成时间列
// （updated_at 依赖 DDL 的 ON UPDATE CURRENT_TIMESTAMP 自动维护）
func nonPKNonAutoCols(t table) []column {
	var out []column
	for _, c := range t.Columns {
		if !c.IsPK && !c.AutoInc && !c.AutoTime {
			out = append(out, c)
		}
	}
	return out
}

func colNames(cols []column) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.Name)
	}
	return out
}

func argsOf(cols []column) string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, "m."+c.GoName)
	}
	return strings.Join(out, ", ")
}

func pkParams(t table) string {
	var out []string
	for _, p := range t.PKs {
		var goName, goType string
		for _, c := range t.Columns {
			if c.Name == p {
				goName, goType = c.GoName, c.GoType
			}
		}
		out = append(out, goName+" "+goType)
	}
	return strings.Join(out, ", ")
}

// pkArgNamesOfM 同 pkArgNames，但带 m. 前缀（用于只有 m 参数的 Update）
func pkArgNamesOfM(t table) string {
	var out []string
	for _, p := range t.PKs {
		for _, c := range t.Columns {
			if c.Name == p {
				out = append(out, "m."+c.GoName)
			}
		}
	}
	return strings.Join(out, ", ")
}

func pkArgNames(t table) string {
	var out []string
	for _, p := range t.PKs {
		for _, c := range t.Columns {
			if c.Name == p {
				out = append(out, c.GoName)
			}
		}
	}
	return strings.Join(out, ", ")
}

func pkWhere(t table) string {
	var out []string
	for _, p := range t.PKs {
		out = append(out, p+" = ?")
	}
	return strings.Join(out, " AND ")
}

func scanTargets(t table) string {
	out := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		out = append(out, "&m."+c.GoName)
	}
	return strings.Join(out, ", ")
}

func upperCamel(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' || r == ' ' })
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]) + p[1:])
	}
	return b.String()
}
