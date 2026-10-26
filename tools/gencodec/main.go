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
	fields   []field
}

var reCodec = regexp.MustCompile(`@codec\s+table=([a-zA-Z0-9_]+)(?:\s+validate=([A-Za-z0-9_]+))?`)

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
					m := reCodec.FindStringSubmatch(doc)
					if m == nil {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						continue
					}
					t := target{dir: dir, pkg: af.Name.Name, typeName: ts.Name.Name, table: m[1], validate: m[2]}
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
	pairs, err := buildPairs(t, cols, row)
	if err != nil {
		return "", err
	}
	if row.flatVar == "" {
		return "", fmt.Errorf("表 %s 缺少 FlatOps 变量（在 DDL 上加 -- @flat:<模块名> 再 make gen）", t.table)
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
	b.WriteString("import (\n")
	if needJSON {
		b.WriteString("\t\"encoding/json\"\n")
	}
	if needFmt {
		b.WriteString("\t\"fmt\"\n")
	}
	b.WriteString("\n\t\"game_study/pkg/model\"\n\t\"game_study/pkg/writebehind\"\n)\n\n")

	fmt.Fprintf(&b, "func init() {\n\twritebehind.RegisterStateful(stateKey, model.%s,\n", row.flatVar)
	fmt.Fprintf(&b, "\t\twritebehind.StateCodec[model.%s, %s]{\n", row.typeName, t.typeName)
	fmt.Fprintf(&b, "\t\t\tToRow:   %sToRow,\n\t\t\tFromRow: %sFromRow,\n\t\t},\n", t.typeName, t.typeName)
	if t.validate != "" {
		fmt.Fprintf(&b, "\t\t%s,\n", t.validate)
	}
	b.WriteString("\t)\n}\n\n")

	// ToRow
	fmt.Fprintf(&b, "func %sToRow(playerID int64, d *%s) (*model.%s, error) {\n", t.typeName, t.typeName, row.typeName)
	for _, p := range pairs {
		if !p.json {
			continue
		}
		fmt.Fprintf(&b, "\t%sRaw, err := json.Marshal(d.%s)\n", p.biz.name, p.biz.name)
		b.WriteString("\tif err != nil {\n")
		fmt.Fprintf(&b, "\t\treturn nil, fmt.Errorf(%q, err)\n", "encode "+t.table+"."+p.col.name+": %w")
		b.WriteString("\t}\n")
	}
	fmt.Fprintf(&b, "\treturn &model.%s{\n", row.typeName)
	for _, c := range cols {
		if c.isPK && strings.EqualFold(c.name, "player_id") {
			for _, rf := range row.fields {
				if rf.tag == c.name {
					fmt.Fprintf(&b, "\t\t%s: playerID,\n", rf.name)
				}
			}
		}
	}
	for _, p := range pairs {
		switch {
		case p.json:
			fmt.Fprintf(&b, "\t\t%s: string(%sRaw),\n", p.row.name, p.biz.name)
		default:
			fmt.Fprintf(&b, "\t\t%s: %s,\n", p.row.name, cast(p.row.typ, p.biz.typ, "d."+p.biz.name))
		}
	}
	b.WriteString("\t}, nil\n}\n\n")

	// FromRow
	fmt.Fprintf(&b, "func %sFromRow(r *model.%s) (*%s, error) {\n", t.typeName, row.typeName, t.typeName)
	fmt.Fprintf(&b, "\td := &%s{}\n", t.typeName)
	for _, p := range pairs {
		switch {
		case p.json:
			fmt.Fprintf(&b, "\tif r.%s != \"\" {\n", p.row.name)
			fmt.Fprintf(&b, "\t\tif err := json.Unmarshal([]byte(r.%s), &d.%s); err != nil {\n", p.row.name, p.biz.name)
			fmt.Fprintf(&b, "\t\t\treturn nil, fmt.Errorf(%q, err)\n", "decode "+t.table+"."+p.col.name+": %w")
			b.WriteString("\t\t}\n\t}\n")
		default:
			fmt.Fprintf(&b, "\td.%s = %s\n", p.biz.name, cast(p.biz.typ, p.row.typ, "r."+p.row.name))
		}
	}
	b.WriteString("\treturn d, nil\n}\n")

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
