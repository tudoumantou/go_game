// tools/gendata —— XML 游戏数据 → Go 配置代码生成器（设计文档 16.1）
//
// 工程链位置：构建与运维工具链的第一环，对应 Makefile target `make gen-data`
// （= `go run ./tools/gendata -in configs/gamedata/xml -out pkg/gamedata`），
// 也是 `make gen`（proto → msgid → data → db 一键生成）的一环；产物被 CI 的
// `make gen-check` 生成到临时目录后逐字节比对，不一致即卡点失败（16.4）。
//
// 输入：configs/gamedata/xml/*.xml（策划维护的配置表，一个文件一张表）。
// XML 约定（P0 简化版，策划按此维护）：
//   - 单层结构：根元素下每行一个配置项，数据全部用【属性】表达；
//   - 每个 XML 文件 = 一张配置表，文件名即表名（item.xml → ItemCfg）；
//   - 行元素需带 id 属性（唯一键，生成 ByID 索引与 Get<Name>Cfg 查询）。
//
// 产出（全量覆盖，16.4 约定禁止手改）：
//   - pkg/gamedata/<table>_gen.go  结构体 + 列表 + ByID 索引 + 查询函数
//   - pkg/gamedata/data.go          注册表汇总（init 汇总 + go:embed 原始 XML）
//   - pkg/gamedata/embed/xml/*.xml  XML 源拷贝（go:embed 输入，16.1.3）
//
// 可选 -json-out：另导出 <table>.json（热更新输入格式与模板，见 genJSON）。
package main

import (
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// rowItem 一行配置的原始形态（属性名 → 字符串值）。
//
// Cols 与 Vals 分离是有意的：Cols 保序（决定生成结构体的字段顺序），
// Vals 按名索引（类型推断与字面量生成查值用）。XML 属性本身无序，
// 若不单独记录顺序，每次生成的字段排列可能不同，diff 噪音会淹没真实变更。
type rowItem struct {
	ID   string
	Cols []string // 有序列名（保序生成结构体）
	// Vals 列名 → XML 属性原始字符串；类型推断与字面量生成的唯一数据来源
	Vals map[string]string
}

// table 一张配置表（一个 XML 文件）：
//   - Name 生成用的 Go 标识符（item.xml → Item）；
//   - File 原始文件名，既是错误报告的定位符，也是 embed 拷贝的文件名；
//   - Rows 全部数据行，顺序即 XML 中的行序（生成物保序，便于对照排查）。
type table struct {
	Name string // Item（文件名 → CamelCase）
	File string // item.xml
	Rows []rowItem
}

// main 入口：读 XML → 校验 → 拷贝 embed → 逐表生成 → 汇总 data.go。
//
// 顺序约束：先校验再落盘（见下方 problems 检查），任何配置错误都不产出半套生成物，
// 避免「生成到一半」的中间态被后续 make gen 流程当成有效产物。
//
// 命令行：-in/-out 覆盖默认路径（make gen-check 会传临时目录，见 Makefile），
// -json-out 可选导出热更新 JSON。失败一律 log.Fatalf：日志走 stderr、
// 以非零码退出，CI 的 make gen-check 才能立即发现。
func main() {
	in := flag.String("in", "configs/gamedata/xml", "XML 输入目录")
	out := flag.String("out", "pkg/gamedata", "Go 代码输出目录")
	jsonOut := flag.String("json-out", "", "可选：把每张表导出为 <table>.json（热更新格式与模板）")
	// flag 包约定：所有参数必须在 Parse 之前注册；Parse 之后 -h 自动可用，
	// 未识别的参数直接报错退出（这就是 Makefile 只传 -in/-out 的原因）
	flag.Parse()

	entries, err := os.ReadDir(*in)
	if err != nil {
		log.Fatalf("read dir %s: %v", *in, err)
	}

	embedDir := filepath.Join(*out, "embed", "xml")

	var tables []table
	// rawXML 暂存每张表的原始字节：既要解析成行数据，也要原样拷贝进 embed 目录
	//（go:embed 的输入，见设计文档 16.1.3）
	rawXML := make(map[string][]byte, len(entries))

	// 只处理顶层 .xml：目录约定「一个文件一张表」，不递归子目录
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".xml") {
			continue
		}
		path := filepath.Join(*in, e.Name())
		rows, err := parseXML(path)
		if err != nil {
			log.Fatalf("parse %s: %v", e.Name(), err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			log.Fatalf("read %s: %v", e.Name(), err)
		}
		rawXML[e.Name()] = raw
		tables = append(tables, table{
			Name: upperCamel(strings.TrimSuffix(e.Name(), ".xml")),
			File: e.Name(),
			Rows: rows,
		})
	}
	// 按文件名排序：生成顺序稳定，重复运行的输出可 diff（gen-check 的前提）
	sort.Slice(tables, func(i, j int) bool { return tables[i].File < tables[j].File })

	// 先校验再落盘：任何一项不通过都不产出半套生成物，避免「生成到一半」的中间态。
	var problems []string
	for _, t := range tables {
		problems = append(problems, validateTable(t)...)
	}
	problems = append(problems, validateRefs(tables)...)
	if len(problems) > 0 {
		fmt.Fprintf(os.Stderr, "配置校验失败（%d 项）：\n", len(problems))
		for _, p := range problems {
			fmt.Fprintf(os.Stderr, "  ✗ %s\n", p)
		}
		os.Exit(1)
	}

	if err := os.MkdirAll(embedDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", embedDir, err)
	}
	for name, raw := range rawXML {
		if err := os.WriteFile(filepath.Join(embedDir, name), raw, 0o644); err != nil {
			log.Fatalf("copy %s: %v", name, err)
		}
	}

	// 逐表生成
	for _, t := range tables {
		if err := genTable(*out, t.Name, t.File, t.Rows); err != nil {
			log.Fatalf("gen %s: %v", t.Name, err)
		}
		fmt.Printf("  %-24s → %s_gen.go（%d 行）\n", t.File, lowerSnake(t.Name), len(t.Rows))
	}
	// 热更新格式导出（可选）：与 Cfg 结构体的 json tag 一致，可直接被 gamedata.LoadDir 读回。
	if *jsonOut != "" {
		if err := genJSON(*jsonOut, tables); err != nil {
			log.Fatalf("export json: %v", err)
		}
		fmt.Printf("已导出 %d 张表的 JSON → %s\n", len(tables), *jsonOut)
	}

	// 汇总 data.go（含内容哈希版本号）
	if err := genData(*out, tables, dirVersion(embedDir)); err != nil {
		log.Fatalf("gen data.go: %v", err)
	}
	fmt.Printf("gendata 完成：%d 张表 → %s\n", len(tables), *out)
}

// parseXML 解析单层属性型 XML：根元素忽略，其余开始的元素即一行。
//
// 用 encoding/xml 的 Token 流而不是 Unmarshal 到结构体：单层属性表的列是
// 动态的（每张表列不同），无法预定义结构体；Token 流只关心 StartElement/
// EndElement 的深度，天然适配「根 → 行」两层结构。
//
// 用深度判定而不是「元素名是否叫 root」：早期只认 <root>，其他根名（<item>、<quest>）
// 会被当成数据行，靠后续 <row> 覆盖侥幸跑通；根元素一旦带属性就会多出一行。
func parseXML(path string) ([]rowItem, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := xml.NewDecoder(f)
	var rows []rowItem
	depth := 0
	var cur *rowItem

	for {
		tok, err := dec.Token()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			// depth 自管计数器：xml.Decoder.Token 只吐 token 不带层级信息，
			// 自行 ++/-- 才能区分「根（1）/ 行（2）/ 非法嵌套（3+）」
			depth++
			switch depth {
			case 1:
				// 根元素：只作容器，属性不参与数据。
			case 2:
				cur = &rowItem{Vals: map[string]string{}}
				for _, a := range t.Attr {
					col := lowerSnake(a.Name.Local)
					if _, dup := cur.Vals[col]; !dup {
						cur.Cols = append(cur.Cols, col)
					}
					cur.Vals[col] = a.Value
					if col == "id" {
						cur.ID = a.Value
					}
				}
			default:
				return nil, fmt.Errorf("不支持嵌套元素 <%s>：配置必须是单层结构（根 → 行）", t.Name.Local)
			}
		case xml.EndElement:
			if depth == 2 && cur != nil {
				rows = append(rows, *cur)
				cur = nil
			}
			depth--
		}
	}
	return rows, nil
}

// genTable 生成 <table>_gen.go：结构体 + 表视图 + 行数据 + 查询函数。
//
// 用 strings.Builder 拼接而非 text/template：模板的空白控制（{{- -}}）会让
// 产物对微小改动也产生整片 diff，而这里的条件分支（列类型、行数）用 Go 的
// if/for 表达更直接；代价是拼接必须自己保证语法正确 —— 由 writeGoFile 的
// format.Source 做最后一道检查。
func genTable(outDir, name, file string, rows []rowItem) error {
	// 列集合（保序：以首行顺序为准，后续行补齐新列）
	var cols []string
	seen := map[string]bool{}
	if len(rows) > 0 {
		for _, c := range rows[0].Cols {
			if !seen[c] {
				seen[c] = true
				cols = append(cols, c)
			}
		}
	}

	// 列类型推断（全表一致：全 int→int64，全 bool→bool，否则 string）
	types := map[string]string{}
	for _, c := range cols {
		types[c] = inferType(rows, c)
	}

	var b strings.Builder
	// 生成物头部声明：`Code generated ... DO NOT EDIT.` 是 Go 生态约定标记，
	// gofmt/lint/IDE 见到它会把整文件当生成代码（跳过 review 与部分检查），
	// 人类也不会去手改 —— 与 16.4 的覆盖策略配套
	b.WriteString("// Code generated by tools/gendata from " + file + ". DO NOT EDIT.\n")
	b.WriteString("// 源：configs/gamedata/xml/" + file + "（全量覆盖，见设计文档 16.4）\n\n")
	b.WriteString("package gamedata\n\n")

	// 结构体
	b.WriteString("// " + name + "Cfg " + file + " 配置项\n")
	b.WriteString("type " + name + "Cfg struct {\n")
	for _, c := range cols {
		b.WriteString("\t" + upperCamel(c) + " " + types[c] + " `json:\"" + c + "\"`\n")
	}
	b.WriteString("}\n\n")

	// 表视图类型（泛型 table[T] 定义在 tables.go，手写不被覆盖）
	b.WriteString("// " + name + "Table " + file + " 的只读视图，随快照整体替换，不做原地修改\n")
	b.WriteString("type " + name + "Table = table[" + name + "Cfg]\n\n")
	// 表视图构造器：ByID 索引的键函数固定取 Id 列（行元素必须带 id 属性，见文件头 XML 约定）
	b.WriteString("func new" + name + "Table(list []" + name + "Cfg) (" + name + "Table, error) {\n")
	b.WriteString("\treturn newTable(list, func(c *" + name + "Cfg) int64 { return c.Id })\n}\n\n")

	// 行数据（私有：外部只能通过快照读，不能改）
	b.WriteString("// " + lowerSnake(name) + "Rows 内嵌快照的行数据\n")
	b.WriteString("var " + lowerSnake(name) + "Rows = []" + name + "Cfg{\n")
	for _, r := range rows {
		b.WriteString("\t{")
		parts := make([]string, 0, len(cols))
		for _, c := range cols {
			parts = append(parts, upperCamel(c)+": "+goLiteral(r.Vals[c], types[c]))
		}
		b.WriteString(strings.Join(parts, ", "))
		b.WriteString("},\n")
	}
	b.WriteString("}\n\n")

	// 兼容旧调用：读到的是「当前快照」，热更新后自动生效
	b.WriteString("// " + name + "CfgList 当前快照的全量行（等价于 Cur()." + name + ".List()）\n")
	b.WriteString("func " + name + "CfgList() []" + name + "Cfg { return Cur()." + name + ".List() }\n\n")
	b.WriteString("// Get" + name + "Cfg 按 ID 查询当前快照\n")
	b.WriteString("func Get" + name + "Cfg(id int64) (*" + name + "Cfg, bool) {\n")
	b.WriteString("\treturn Cur()." + name + ".Get(id)\n}\n")

	path := filepath.Join(outDir, lowerSnake(name)+"_gen.go")
	return writeGoFile(path, b.String())
}

// genData 生成汇总 data.go：快照结构 + go:embed 原始 XML + setter/tableNames。
//
// version 参数来自 dirVersion（embed 目录的内容哈希）：同一份 XML 永远得到
// 同一个版本号，启动日志打印它即可与线上配置核对，也是热更新后判断
// 「跑的是哪套配置」的依据。
func genData(outDir string, tables []table, version string) error {
	var b strings.Builder
	b.WriteString("// Code generated by tools/gendata. DO NOT EDIT.\n")
	b.WriteString("// 汇总：配置快照结构 + 原始 XML 内嵌（go:embed）。\n")
	b.WriteString("// 快照的通用逻辑（Cur/Replace/LoadDir/Rollback）在 tables.go，手写不覆盖。\n\n")
	b.WriteString("package gamedata\n\n")
	b.WriteString("import (\n\t\"embed\"\n\t\"encoding/json\"\n\t\"fmt\"\n)\n\n")
	b.WriteString("//go:embed embed/xml/*.xml\n")
	b.WriteString("var rawXML embed.FS\n\n")
	b.WriteString("// embeddedVersion 编译期内嵌配置的内容哈希（启动日志与线上核对用）\n")
	b.WriteString("const embeddedVersion = \"" + version + "\"\n\n")

	// 快照结构
	b.WriteString("// Tables 一份不可变配置快照：热更新整体替换，读取方永远看到自洽的一套表。\n")
	b.WriteString("type Tables struct {\n")
	b.WriteString("\tVersion string // 快照版本（内嵌哈希，或热更新后的派生版本）\n")
	for _, t := range tables {
		b.WriteString("\t" + t.Name + " " + t.Name + "Table\n")
	}
	b.WriteString("}\n\n")

	// 构造内嵌快照
	b.WriteString("// mustEmbedded 用编译期内嵌数据构造快照；主键重复属于生成期就该拦住的错误，这里直接 panic。\n")
	b.WriteString("func mustEmbedded() *Tables {\n")
	b.WriteString("\tt := &Tables{Version: embeddedVersion}\n")
	b.WriteString("\tvar err error\n")
	for _, t := range tables {
		b.WriteString("\tif t." + t.Name + ", err = new" + t.Name + "Table(" + lowerSnake(t.Name) + "Rows); err != nil {\n")
		b.WriteString("\t\tpanic(\"gamedata: \" + err.Error())\n\t}\n")
	}
	b.WriteString("\treturn t\n}\n\n")

	// 校验
	b.WriteString("// validate 热更新后的自检：任何一张表为空都视为错误输入。\n")
	b.WriteString("func (t *Tables) validate() error {\n")
	for _, t := range tables {
		b.WriteString("\tif err := t." + t.Name + ".validate(\"" + strings.TrimSuffix(t.File, ".xml") + "\"); err != nil {\n")
		b.WriteString("\t\treturn err\n\t}\n")
	}
	b.WriteString("\treturn nil\n}\n\n")

	// 按表覆盖的 setter（LoadDir/Replace 用）
	b.WriteString("// setters 表名 → 覆盖函数；表名与 XML 文件名一致（item.xml → \"item\"）。\n")
	b.WriteString("var setters = map[string]func(*Tables, []byte) error{\n")
	for _, t := range tables {
		name := strings.TrimSuffix(t.File, ".xml")
		b.WriteString("\t\"" + name + "\": func(t *Tables, raw []byte) error {\n")
		b.WriteString("\t\tvar rows []" + t.Name + "Cfg\n")
		b.WriteString("\t\tif err := json.Unmarshal(raw, &rows); err != nil {\n")
		b.WriteString("\t\t\treturn fmt.Errorf(\"decode json: %w\", err)\n\t\t}\n")
		b.WriteString("\t\ttb, err := new" + t.Name + "Table(rows)\n")
		b.WriteString("\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n")
		b.WriteString("\t\tt." + t.Name + " = tb\n")
		b.WriteString("\t\treturn nil\n\t},\n")
	}
	b.WriteString("}\n\n")

	b.WriteString("// tableNames 表名（稳定顺序，日志与 LoadedTables 用）\n")
	b.WriteString("var tableNames = []string{\n")
	for _, t := range tables {
		b.WriteString("\t\"" + strings.TrimSuffix(t.File, ".xml") + "\",\n")
	}
	b.WriteString("}\n\n")

	b.WriteString("// RawXML 读取内嵌的原始 XML（不存在返回 nil）。\n")
	b.WriteString("// 注意：它读到的是编译期内容，热更新后与运行中的快照不一致，仅用于比对与排障。\n")
	b.WriteString("func RawXML(name string) []byte {\n")
	b.WriteString("\tb, err := rawXML.ReadFile(\"embed/xml/\" + name)\n")
	b.WriteString("\tif err != nil {\n\t\treturn nil\n\t}\n\treturn b\n}\n\n")
	b.WriteString("// LoadedTables 已加载配置表名（启动自检/日志用）\n")
	b.WriteString("func LoadedTables() []string {\n\treturn append([]string(nil), tableNames...)\n}\n\n")
	b.WriteString("// Summary 配置规模摘要\n")
	b.WriteString("func Summary() string {\n")
	b.WriteString(fmt.Sprintf("\treturn fmt.Sprintf(\"gamedata: %%d tables, version %%s\", len(tableNames), Version())\n"))
	b.WriteString("}\n")
	path := filepath.Join(outDir, "data.go")
	return writeGoFile(path, b.String())
}

// writeGoFile 生成物统一走 gofmt。
//
// 不格式化会有两个后果：`make fmt` 会改生成文件，随后 `make gen-check` 必然失败；
// 而手工拼出来的代码一旦有语法错，只有到 go build 才暴露。format.Source 顺带做语法检查。
//
// Go 知识点：go/format.Source 是 gofmt 的编程接口（内部 parser + printer 往返），
// 只按规范重排不改变语义；解析失败即语法错误，因此这一行同时充当
// 生成代码的「编译期前哨」。
func writeGoFile(path, src string) error {
	out, err := format.Source([]byte(src))
	if err != nil {
		return fmt.Errorf("gofmt %s: %w", filepath.Base(path), err)
	}
	return os.WriteFile(path, out, 0o644)
}

// inferType 推断列类型：全 int64 / 全 bool / 否则 string
//
// 契约：以「该列全部行的值」为准；缺值的行跳过不参与判定（缺列由 validate.go 拦）。
// bool 优先于 int64：字面 true/false 不可能是整数，两类无歧义；默认 string 是
// 安全兜底 —— 生成物永远可编译，把类型问题暴露在运行期而不是生成期。
func inferType(rows []rowItem, col string) string {
	if len(rows) == 0 {
		return "string"
	}
	allInt, allBool := true, true
	for _, r := range rows {
		v, ok := r.Vals[col]
		if !ok {
			continue
		}
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			allInt = false
		}
		if v != "true" && v != "false" {
			allBool = false
		}
	}
	// 全表一致的列类型：全 int→int64，全 bool→bool，否则 string
	// （顺序即优先级：字面 true/false 不会同时是整数，两类不会歧义）
	switch {
	case allBool:
		return "bool"
	case allInt:
		return "int64"
	default:
		return "string"
	}
}

// goLiteral 生成 Go 字面量（int64/bool 原样，string 走 strconv.Quote 转义）。
//
// Quote 的必要性：XML 值可能含引号、反斜杠、换行，直接拼进字符串字面量
// 会产出语法错误的代码；Quote 是唯一安全的字符串字面量化入口。
func goLiteral(v, typ string) string {
	switch typ {
	case "int64":
		// 理论上全表可解析（inferType 已保证）；异常值回落 0，
		// 保证生成物永远可编译，把数据问题暴露在运行期而不是生成期
		if v == "" {
			return "0"
		}
		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			return "0"
		}
		return v
	case "bool":
		return v
	default:
		return strconv.Quote(v)
	}
}

// upperCamel snake/kebab/空格分词转 CamelCase（item_id → ItemId）。
// 列名与表名的导出标识符统一走这里，保证 XML 属性名与 Go 字段名可互相推导。
//
// 注意按字节取首字母大写（p[:1]）：列名若含非 ASCII（如中文），会截出
// 非法 UTF-8 片段，因此配置列名必须保持 ASCII（策划约定）。
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

// lowerSnake CamelCase 转 snake_case（Item → item）。
// 与 upperCamel 互逆，是「表名 ↔ 生成文件名 ↔ 包级变量名」三者换算的另一半。
// 按 rune 遍历（for range string 自动解码 UTF-8）：多字节字符不会被按字节切断，
// 与 upperCamel 的 ASCII 假设互补 —— 表名允许中文，列名不允许。
func lowerSnake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(r + 32)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// dirVersion 计算目录内全部 XML 的内容哈希（排序后拼接 sha256，取前 16 位）
//
// 用途：编译期内嵌配置的版本号（embeddedVersion），启动日志打印 + 线上核对；
// XML 任何一字节变更都会改变哈希，「改了配置忘了重新生成」会立刻现形。
// 读失败返回 "unknown" 而不报错：版本号只是诊断信息，不该阻断生成流程。
func dirVersion(dir string) string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "unknown"
	}
	h := sha256.New()
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".xml") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		raw, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			return "unknown"
		}
		h.Write([]byte(n))
		h.Write(raw)
	}
	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}

// genJSON 把表导出为 JSON 数组（热更新输入格式）。
//
// 列名与类型跟生成的 Cfg 结构体完全一致（json tag = 列名），
// 因此导出物可直接作为线上热更新的模板：改值 → 下发 → gamedata.LoadDir 读回。
//
// 用 map[string]any 而不是结构体序列化：生成器进程里没有编译期结构体，
// JSON 的 key 必须是「列名」（与 Cfg 的 json tag 一致），而不是 Go 字段名。
func genJSON(outDir string, tables []table) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	for _, t := range tables {
		rows := make([]map[string]any, 0, len(t.Rows))
		// 列集合同样以首行为准（前置校验已保证每张表非空）；缺列的行取零值，
		// 与 Cfg 结构体读回时的行为一致
		cols := t.Rows[0].Cols
		types := make(map[string]string, len(cols))
		for _, c := range cols {
			types[c] = inferType(t.Rows, c)
		}
		for _, r := range t.Rows {
			m := make(map[string]any, len(cols))
			for _, c := range cols {
				v := r.Vals[c]
				switch types[c] {
				case "int64":
					n, err := strconv.ParseInt(v, 10, 64)
					if err != nil {
						n = 0
					}
					m[c] = n
				case "bool":
					m[c] = v == "true"
				default:
					m[c] = v
				}
			}
			rows = append(rows, m)
		}
		raw, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(t.File, ".xml") + ".json"
		if err := os.WriteFile(filepath.Join(outDir, name), append(raw, '\n'), 0o644); err != nil {
			return err
		}
	}
	return nil
}
