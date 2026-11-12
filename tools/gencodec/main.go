// tools/gencodec —— 业务结构与表行之间的编解码生成器
//
// 输入：
//  1. sql/schema 下的 DDL（列名/类型/主键）；
//  2. 业务包里的结构体 + `db` tag（`// @codec table=player_signin` 标注）。
//
// 输出 `<包>/persist_gen.go`：ToRow / FromRow / RegisterStateful。
//
// 为什么生成而不是手写：手写映射加字段时容易漏一处，而且不会报错，
// 只会静默丢字段。生成器做双向校验——表有列没字段、字段没列、类型不
// 兼容，都在 make gen 阶段失败。
//
// 约定：
//   - `db:"-"` 的字段不落库（纯内存态）；
//   - JSON/TEXT 列对应复杂类型（json.Marshal）或 string（直接赋值）；
//   - 主键 player_id 由框架按玩家 ID 填入，业务结构不要声明它。
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

// ---------- schema ----------

type column struct {
	name   string
	sqlTyp string
	isPK   bool
	isJSON bool
}

var (
	reCreate = regexp.MustCompile("(?s)CREATE TABLE\\s+`?([a-zA-Z0-9_]+)`?\\s*\\((.*?)\\)\\s*ENGINE")
	reCol    = regexp.MustCompile("^\\s*`([a-zA-Z0-9_]+)`\\s+([A-Za-z]+)")
)

func loadSchema(dir string) (map[string][]column, error) {
	out := map[string][]column{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		for _, m := range reCreate.FindAllStringSubmatch(string(raw), -1) {
			table, body := m[1], m[2]
			var cols []column
			for _, line := range strings.Split(body, "\n") {
				cm := reCol.FindStringSubmatch(line)
				if cm == nil {
					continue
				}
				typ := strings.ToUpper(cm[2])
				cols = append(cols, column{
					name:   cm[1],
					sqlTyp: typ,
					isJSON: typ == "TEXT" || typ == "JSON" || typ == "BLOB" || typ == "LONGTEXT",
				})
			}
			// PRIMARY KEY 通常单独一行：命中哪个列就标哪个
			for _, line := range strings.Split(body, "\n") {
				up := strings.ToUpper(strings.TrimSpace(line))
				if !strings.HasPrefix(up, "PRIMARY KEY") {
					continue
				}
				for i := range cols {
					if strings.Contains(line, "`"+cols[i].name+"`") {
						cols[i].isPK = true
					}
				}
			}
			out[table] = cols
		}
	}
	return out, nil
}

// ---------- model ----------

type field struct {
	name string
	typ  string
	tag  string
}

type rowInfo struct {
	typeName string
	fields   []field
	flatVar  string
}

func loadModel(dir string) (map[string]rowInfo, error) {
	typeByName := map[string]rowInfo{}
	tableByType := map[string]string{}
	flatByType := map[string]string{}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_gen.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, 0)
		if err != nil {
			return nil, err
		}
		for _, d := range f.Decls {
			switch decl := d.(type) {
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						st, ok := s.Type.(*ast.StructType)
						if !ok {
							continue
						}
						ri := rowInfo{typeName: s.Name.Name}
						for _, fl := range st.Fields.List {
							if len(fl.Names) != 1 {
								continue
							}
							tag := ""
							if fl.Tag != nil {
								tag = parseTag(fl.Tag.Value, "db")
							}
							ri.fields = append(ri.fields, field{name: fl.Names[0].Name, typ: types.ExprString(fl.Type), tag: tag})
						}
						typeByName[s.Name.Name] = ri
					case *ast.ValueSpec:
						for _, n := range s.Names {
							if strings.HasSuffix(n.Name, "Flat") {
								flatByType[strings.TrimSuffix(n.Name, "Flat")] = n.Name
							}
						}
					}
				}
			case *ast.FuncDecl:
				if decl.Name.Name != "TableName" || decl.Recv == nil || len(decl.Recv.List) != 1 || decl.Body == nil {
					continue
				}
				recv := strings.TrimPrefix(types.ExprString(decl.Recv.List[0].Type), "*")
				if len(decl.Body.List) != 1 {
					continue
				}
				ret, ok := decl.Body.List[0].(*ast.ReturnStmt)
				if !ok || len(ret.Results) != 1 {
					continue
				}
				if lit, ok := ret.Results[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					tableByType[recv] = strings.Trim(lit.Value, `"`)
				}
			}
		}
	}

	out := map[string]rowInfo{}
	for typ, ri := range typeByName {
		tbl := tableByType[typ]
		if tbl == "" {
			continue
		}
		ri.flatVar = flatByType[typ]
		out[tbl] = ri
	}
	return out, nil
}

func parseTag(tagLit, key string) string {
	v, _ := reflect.StructTag(strings.Trim(tagLit, "`")).Lookup(key)
	return v
}

// ---------- 业务结构扫描 ----------

type target struct {
	dir      string
	pkg      string
	typeName string
	table    string
	validate string
	// newFn 业务空状态构造函数（如 newData）。为空时 fromRow 用 &T{}。
	//
	// 为什么需要它：&T{} 得到的 map/slice 字段是 nil，读回后业务若直接写
	// （d.Records[id] = v）会 panic；而 newData 给的是空 map / 空切片。
	// 更重要的是 toRow 侧——nil 切片 Marshal 成 null，空切片才是 []，
	// 两者落库内容不同，客户端与对账脚本都会当成两种状态。
	newFn string
	// wholeCol 非空时走「整结构一个 JSON 列」模式：toRow 直接 Marshal(d)，
	// 而不是逐字段拆列。partner 的 Stars+Field 必须整体存一列才不丢语义。
	wholeCol string
	// verifyFn fromRow 末尾的校验函数（冷启动入口）。
	//
	// 与 validate 的区别：validate 校验的是 Redis 快照（基座 Validate 里调），
	// 而 Load 走 MySQL → FromRow 这条路径不经过它。跨进程的坏数据只可能从
	// 冷启动进来，所以需要在这里再校验一次。
	verifyFn string
	fields   []field
}

// reCodecKV 解析 @codec 里的 key=value 对（table/new/whole/validate/verify）。
//
// 用通用 KV 而不是逐项可选正则：后者每加一个选项就要改一次表达式，且
// 顺序敏感（validate 必须紧跟 table）。KV 形态与顺序无关，未知 key 报错，
// 拼错 new= 不会静默退化成 &T{}。
var (
	reCodecKV  = regexp.MustCompile(`@codec\b[ \t]*(.*)`)
	reCodecOne = regexp.MustCompile(`([a-z]+)=([A-Za-z0-9_]+)`)
)

// codecKeys 是 @codec 允许出现的 key；多写一个视为笔误。
var codecKeys = map[string]bool{"table": true, "new": true, "whole": true, "validate": true, "verify": true}

// parseCodec 解析类型注释里的 @codec 注解。
//
// ok=false 表示这个类型没有标注（不是错误）；返回值带 error 的只有两种：
// 出现未知 key，或漏了 table=。这两种静默放过都会让「生成了但生成错了」
// 变成运行时问题，所以宁可在 make gen 阶段直接失败。
func parseCodec(doc string) (map[string]string, bool, error) {
	m := reCodecKV.FindStringSubmatch(doc)
	if m == nil {
		return nil, false, nil
	}
	kv := map[string]string{}
	for _, one := range reCodecOne.FindAllStringSubmatch(m[1], -1) {
		k, v := one[1], one[2]
		if !codecKeys[k] {
			return nil, false, fmt.Errorf("@codec 不认识的选项 %q（允许 table/new/whole/validate/verify）", k)
		}
		kv[k] = v
	}
	if kv["table"] == "" {
		return nil, false, fmt.Errorf("@codec 缺少 table=")
	}
	if kv["whole"] != "" && kv["new"] == "" {
		return nil, false, fmt.Errorf("@codec whole=%s 必须同时给 new=：fromRow 要先用空状态函数拿到非 nil 的 map/切片，再整体 Unmarshal", kv["whole"])
	}
	return kv, true, nil
}

func findTargets(root string) ([]target, error) {
	var out []target
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			name := f.Name()
			if f.IsDir() || !strings.HasSuffix(name, ".go") ||
				strings.HasSuffix(name, "_test.go") || strings.HasSuffix(name, "_gen.go") {
				continue
			}
			af, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
			if err != nil {
				return nil, err
			}
			for _, d := range af.Decls {
				gd, ok := d.(*ast.GenDecl)
				if !ok || gd.Tok != token.TYPE {
					continue
				}
				for _, spec := range gd.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					doc := ""
					if ts.Doc != nil {
						doc = ts.Doc.Text()
					} else if gd.Doc != nil {
						doc = gd.Doc.Text()
					}
					kv, ok, err := parseCodec(doc)
					if err != nil {
						return nil, fmt.Errorf("%s/%s %s: %w", filepath.Base(dir), name, ts.Name.Name, err)
					}
					if !ok {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					t := target{
						dir: dir, pkg: af.Name.Name, typeName: ts.Name.Name,
						table: kv["table"], validate: kv["validate"],
						newFn: kv["new"], wholeCol: kv["whole"], verifyFn: kv["verify"],
					}
					for _, fl := range st.Fields.List {
						if len(fl.Names) != 1 {
							continue
						}
						tag := ""
						if fl.Tag != nil {
							tag = parseTag(fl.Tag.Value, "db")
						}
						t.fields = append(t.fields, field{name: fl.Names[0].Name, typ: types.ExprString(fl.Type), tag: tag})
					}
					out = append(out, t)
				}
			}
		}
	}
	return out, nil
}

// ---------- 生成 ----------

type pair struct {
	biz  field
	col  column
	row  field
	json bool
}

func isComplex(typ string) bool {
	return strings.HasPrefix(typ, "[]") || strings.HasPrefix(typ, "map[") ||
		strings.HasPrefix(typ, "*") || strings.HasPrefix(typ, "struct")
}

func buildPairs(t target, cols []column, row rowInfo) ([]pair, error) {
	byCol := map[string]column{}
	for _, c := range cols {
		byCol[c.name] = c
	}
	rowByCol := map[string]field{}
	for _, f := range row.fields {
		if f.tag != "" && f.tag != "-" {
			rowByCol[f.tag] = f
		}
	}

	var pairs []pair
	used := map[string]bool{}
	for _, f := range t.fields {
		if f.tag == "-" {
			continue
		}
		if f.tag == "" {
			return nil, fmt.Errorf("%s.%s 缺少 db tag（不落库请标 db:\"-\"）", t.typeName, f.name)
		}
		c, ok := byCol[f.tag]
		if !ok {
			return nil, fmt.Errorf("%s.%s 的列 %q 在表 %s 中不存在", t.typeName, f.name, f.tag, t.table)
		}
		if c.isPK {
			return nil, fmt.Errorf("%s.%s 映射到主键 %q：主键由框架按玩家 ID 填充，业务结构不要声明", t.typeName, f.name, f.tag)
		}
		rf, ok := rowByCol[f.tag]
		if !ok {
			return nil, fmt.Errorf("表 %s 的列 %q 在 %s 中没有对应字段", t.table, f.tag, row.typeName)
		}
		p := pair{biz: f, col: c, row: rf}
		switch {
		case c.isJSON && isComplex(f.typ):
			p.json = true
		case c.isJSON && f.typ == "string":
			// 直接赋值
		case c.isJSON:
			return nil, fmt.Errorf("%s.%s 类型 %s 与列 %q(%s) 不兼容：JSON 列只接受复杂类型或 string", t.typeName, f.name, f.typ, f.tag, c.sqlTyp)
		case isComplex(f.typ):
			return nil, fmt.Errorf("%s.%s 是复杂类型 %s，但列 %q(%s) 不是 TEXT/JSON", t.typeName, f.name, f.typ, f.tag, c.sqlTyp)
		}
		pairs = append(pairs, p)
		used[f.tag] = true
	}
	for _, c := range cols {
		if c.isPK || used[c.name] {
			continue
		}
		return nil, fmt.Errorf("表 %s 的列 %q 没有对应字段（补字段，或在 DDL 注释里标明该列不参与业务）", t.table, c.name)
	}
	return pairs, nil
}

func cast(rowTyp, bizTyp, expr string) string {
	if rowTyp == bizTyp {
		return expr
	}
	return rowTyp + "(" + expr + ")"
}

func generate(t target, cols []column, row rowInfo) (string, error) {
	if row.flatVar == "" {
		return "", fmt.Errorf("表 %s 缺少 FlatOps 变量（在 DDL 上加 -- @flat:<模块名> 再 make gen）", t.table)
	}
	if t.wholeCol != "" {
		return generateWhole(t, cols, row)
	}

	pairs, err := buildPairs(t, cols, row)
	if err != nil {
		return "", err
	}
	needJSON, needFmt := false, false
	for _, p := range pairs {
		if p.json {
			needJSON, needFmt = true, true
		}
	}

	var b bytes.Buffer
	b.WriteString("// Code generated by tools/gencodec from sql/schema + db tags. DO NOT EDIT.\n\n")
	fmt.Fprintf(&b, "package %s\n\n", t.pkg)
	writeImports(&b, needJSON, needFmt)
	writeInit(&b, t, row)
	writeToRow(&b, t, row, cols, pairs)
	writeFromRow(&b, t, row, pairs)
	return finish(&b)
}

// generateWhole 生成「整个业务结构存一个 JSON 列」的编解码。
//
// 为什么需要这个模式：partner 的 Data 是 {Stars map, Field []}，两半合起来
// 才是完整语义（Field 是上阵顺序，脱离 Stars 就不可解释）。硬拆成两列会
// 让「先写 Stars 还是先写 Field」变成一致性问题；整份 Marshal 一个 TEXT
// 列，读回来是一个原子的数据结构。
//
// 代价是这一列不能做 SQL 层面的查询/局部更新——partner 没有这种需求，
// 所以值。
func generateWhole(t target, cols []column, row rowInfo) (string, error) {
	var col column
	var rf field
	okCol, okField := false, false
	for _, c := range cols {
		if c.name == t.wholeCol {
			col, okCol = c, true
		}
	}
	if !okCol {
		return "", fmt.Errorf("%s: whole=%s 在表 %s 中不存在", t.typeName, t.wholeCol, t.table)
	}
	if !col.isJSON {
		return "", fmt.Errorf("%s: whole 列 %s 必须是 TEXT/JSON 类型，当前是 %s", t.typeName, t.wholeCol, col.sqlTyp)
	}
	for _, f := range row.fields {
		if f.tag == t.wholeCol {
			rf, okField = f, true
		}
	}
	if !okField {
		return "", fmt.Errorf("表 %s 的列 %q 在行类型 %s 中没有对应字段", t.table, t.wholeCol, row.typeName)
	}
	for _, c := range cols {
		if c.isPK || c.name == t.wholeCol {
			continue
		}
		return "", fmt.Errorf("%s: whole 模式下表 %s 只能有主键与 %s 两列，多出 %q（要么去掉该列，要么改回逐字段 db tag）",
			t.typeName, t.table, t.wholeCol, c.name)
	}
	// whole 模式下业务结构不该再写 db tag：整份 Marshal 不按列对齐。
	for _, f := range t.fields {
		if f.tag != "" && f.tag != "-" {
			return "", fmt.Errorf("%s.%s 有 db tag %q，但已声明 whole=%s：整份序列化不与列一一对应，请删掉 tag",
				t.typeName, f.name, f.tag, t.wholeCol)
		}
	}

	var b bytes.Buffer
	b.WriteString("// Code generated by tools/gencodec from sql/schema + db tags. DO NOT EDIT.\n\n")
	fmt.Fprintf(&b, "package %s\n\n", t.pkg)
	writeImports(&b, true, true)
	writeInit(&b, t, row)

	fmt.Fprintf(&b, "func %sToRow(playerID int64, d *%s) (*model.%s, error) {\n", t.typeName, t.typeName, row.typeName)
	b.WriteString("\traw, err := json.Marshal(d)\n\tif err != nil {\n")
	fmt.Fprintf(&b, "\t\treturn nil, fmt.Errorf(%q, err)\n", "encode "+t.table+": %w")
	b.WriteString("\t}\n")
	fmt.Fprintf(&b, "\treturn &model.%s{\n", row.typeName)
	writePKField(&b, cols, row)
	fmt.Fprintf(&b, "\t\t%s: string(raw),\n", rf.name)
	b.WriteString("\t}, nil\n}\n\n")

	fmt.Fprintf(&b, "func %sFromRow(r *model.%s) (*%s, error) {\n", t.typeName, row.typeName, t.typeName)
	fmt.Fprintf(&b, "\td := %s()\n", t.newFn)
	fmt.Fprintf(&b, "\tif r.%s != \"\" {\n", rf.name)
	fmt.Fprintf(&b, "\t\tif err := json.Unmarshal([]byte(r.%s), d); err != nil {\n", rf.name)
	fmt.Fprintf(&b, "\t\t\treturn nil, fmt.Errorf(%q, err)\n", "decode "+t.table+": %w")
	b.WriteString("\t\t}\n\t}\n")
	writeVerify(&b, t)
	b.WriteString("\treturn d, nil\n}\n")
	return finish(&b)
}

// writeImports 写 import 块：按需带 json/fmt，固定带 model 与 writebehind。
func writeImports(b *bytes.Buffer, needJSON, needFmt bool) {
	b.WriteString("import (\n")
	if needJSON {
		b.WriteString("\t\"encoding/json\"\n")
	}
	if needFmt {
		b.WriteString("\t\"fmt\"\n")
	}
	b.WriteString("\n\t\"game_study/pkg/model\"\n\t\"game_study/pkg/writebehind\"\n)\n\n")
}

// writeInit 写 RegisterStateful 调用。
func writeInit(b *bytes.Buffer, t target, row rowInfo) {
	fmt.Fprintf(b, "func init() {\n\twritebehind.RegisterStateful(stateKey, model.%s,\n", row.flatVar)
	fmt.Fprintf(b, "\t\twritebehind.StateCodec[model.%s, %s]{\n", row.typeName, t.typeName)
	fmt.Fprintf(b, "\t\t\tToRow:   %sToRow,\n\t\t\tFromRow: %sFromRow,\n\t\t},\n", t.typeName, t.typeName)
	if t.validate != "" {
		fmt.Fprintf(b, "\t\t%s,\n", t.validate)
	}
	b.WriteString("\t)\n}\n\n")
}

// writePKField 写主键赋值（框架按玩家 ID 填）。
func writePKField(b *bytes.Buffer, cols []column, row rowInfo) {
	for _, c := range cols {
		if c.isPK && strings.EqualFold(c.name, "player_id") {
			for _, rf := range row.fields {
				if rf.tag == c.name {
					fmt.Fprintf(b, "\t\t%s: playerID,\n", rf.name)
				}
			}
		}
	}
}

func writeToRow(b *bytes.Buffer, t target, row rowInfo, cols []column, pairs []pair) {
	fmt.Fprintf(b, "func %sToRow(playerID int64, d *%s) (*model.%s, error) {\n", t.typeName, t.typeName, row.typeName)
	for _, p := range pairs {
		if !p.json {
			continue
		}
		fmt.Fprintf(b, "\t%sRaw, err := json.Marshal(d.%s)\n", p.biz.name, p.biz.name)
		b.WriteString("\tif err != nil {\n")
		fmt.Fprintf(b, "\t\treturn nil, fmt.Errorf(%q, err)\n", "encode "+t.table+"."+p.col.name+": %w")
		b.WriteString("\t}\n")
	}
	fmt.Fprintf(b, "\treturn &model.%s{\n", row.typeName)
	writePKField(b, cols, row)
	for _, p := range pairs {
		switch {
		case p.json:
			fmt.Fprintf(b, "\t\t%s: string(%sRaw),\n", p.row.name, p.biz.name)
		default:
			fmt.Fprintf(b, "\t\t%s: %s,\n", p.row.name, cast(p.row.typ, p.biz.typ, "d."+p.biz.name))
		}
	}
	b.WriteString("\t}, nil\n}\n\n")
}

// writeFromRow 写冷启动解码：逐字段从行里搬回业务结构。
func writeFromRow(b *bytes.Buffer, t target, row rowInfo, pairs []pair) {
	fmt.Fprintf(b, "func %sFromRow(r *model.%s) (*%s, error) {\n", t.typeName, row.typeName, t.typeName)
	if t.newFn != "" {
		fmt.Fprintf(b, "\td := %s()\n", t.newFn)
	} else {
		fmt.Fprintf(b, "\td := &%s{}\n", t.typeName)
	}
	for _, p := range pairs {
		switch {
		case p.json:
			fmt.Fprintf(b, "\tif r.%s != \"\" {\n", p.row.name)
			fmt.Fprintf(b, "\t\tif err := json.Unmarshal([]byte(r.%s), &d.%s); err != nil {\n", p.row.name, p.biz.name)
			fmt.Fprintf(b, "\t\t\treturn nil, fmt.Errorf(%q, err)\n", "decode "+t.table+"."+p.col.name+": %w")
			b.WriteString("\t\t}\n\t}\n")
		default:
			fmt.Fprintf(b, "\td.%s = %s\n", p.biz.name, cast(p.biz.typ, p.row.typ, "r."+p.row.name))
		}
	}
	writeVerify(b, t)
	b.WriteString("\treturn d, nil\n}\n")
}

// writeVerify 写 fromRow 末尾的业务校验。
func writeVerify(b *bytes.Buffer, t target) {
	if t.verifyFn == "" {
		return
	}
	fmt.Fprintf(b, "\tif err := %s(d); err != nil {\n\t\treturn nil, err\n\t}\n", t.verifyFn)
}

func finish(b *bytes.Buffer) (string, error) {
	src, err := format.Source(b.Bytes())
	if err != nil {
		return "", fmt.Errorf("gofmt: %w\n%s", err, b.String())
	}
	return string(src), nil
}

// ---------- main ----------

func main() {
	schemaDir := flag.String("schema", "sql/schema", "DDL 目录")
	modelDir := flag.String("model", "pkg/model", "生成的表行目录")
	modulesDir := flag.String("modules", "internal/game/module", "业务包根目录")
	check := flag.Bool("check", false, "只比对不写文件（CI 用）")
	flag.Parse()

	schema, err := loadSchema(*schemaDir)
	if err != nil {
		log.Fatalf("load schema: %v", err)
	}
	model, err := loadModel(*modelDir)
	if err != nil {
		log.Fatalf("load model: %v", err)
	}
	targets, err := findTargets(*modulesDir)
	if err != nil {
		log.Fatalf("scan modules: %v", err)
	}
	if len(targets) == 0 {
		log.Println("gencodec: 没有找到 @codec 标注")
		return
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].dir < targets[j].dir })

	bad := false
	for _, t := range targets {
		cols, ok := schema[t.table]
		if !ok {
			log.Fatalf("%s: 表 %s 不在 schema 中", t.typeName, t.table)
		}
		row, ok := model[t.table]
		if !ok {
			log.Fatalf("%s: 表 %s 没有生成的行类型（先 make gen-db）", t.typeName, t.table)
		}
		src, err := generate(t, cols, row)
		if err != nil {
			log.Fatalf("%s: %v", t.typeName, err)
		}
		out := filepath.Join(t.dir, "persist_gen.go")
		if *check {
			old, err := os.ReadFile(out)
			if err != nil || string(old) != src {
				fmt.Printf("不一致: %s（运行 make gen）\n", out)
				bad = true
			}
			continue
		}
		if err := os.WriteFile(out, []byte(src), 0o644); err != nil {
			log.Fatalf("write %s: %v", out, err)
		}
		fmt.Printf("%s.%s → %s\n", t.pkg, t.typeName, out)
	}
	if bad {
		os.Exit(1)
	}
}
