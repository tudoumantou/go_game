package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// parseOne 把 XML 文本写到临时文件后解析，模拟真实输入。
func parseOne(t *testing.T, name, body string) ([]rowItem, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return parseXML(path)
}

func mustParse(t *testing.T, name, body string) []rowItem {
	t.Helper()
	rows, err := parseOne(t, name, body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return rows
}

// assertProblem 断言问题列表里包含指定片段。
func assertProblem(t *testing.T, problems []string, want string) {
	t.Helper()
	for _, p := range problems {
		if strings.Contains(p, want) {
			return
		}
	}
	t.Fatalf("missing problem %q in %v", want, problems)
}

func TestParseXMLIgnoresRootAttributes(t *testing.T) {
	// 根元素带属性时不能被当成数据行（旧实现只认 <root>）。
	rows := mustParse(t, "item.xml", `<item version="3">
  <row id="1" col1="a" />
  <row id="2" col1="b" />
</item>`)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2 (%+v)", len(rows), rows)
	}
	if rows[0].ID != "1" || rows[1].Vals["col1"] != "b" {
		t.Fatalf("unexpected rows: %+v", rows)
	}
}

func TestParseXMLRejectsNested(t *testing.T) {
	_, err := parseOne(t, "item.xml", `<item>
  <row id="1"><group col1="x" /></row>
</item>`)
	if err == nil || !strings.Contains(err.Error(), "嵌套") {
		t.Fatalf("nested element must be rejected, got %v", err)
	}
}

func TestValidateEmptyTable(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item></item>`)
	assertProblem(t, validateTable(table{File: "item.xml", Rows: rows}), "空表")
}

func TestValidateDuplicatePrimaryKey(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row id="1" col1="a" />
  <row id="1" col1="b" />
  <row id="1" col1="c" />
</item>`)
	problems := validateTable(table{File: "item.xml", Rows: rows})
	assertProblem(t, problems, "主键 id=1 重复")
	if len(problems) != 1 {
		t.Fatalf("duplicates should collapse into one problem, got %v", problems)
	}
}

func TestValidatePrimaryKeyShape(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row col1="a" />
  <row id="0" col1="b" />
  <row id="abc" col1="c" />
</item>`)
	problems := validateTable(table{File: "item.xml", Rows: rows})
	assertProblem(t, problems, "缺少 id")
	assertProblem(t, problems, "必须为正整数")
	assertProblem(t, problems, "不是整数")
}

func TestValidateColumnMismatch(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row id="1" col1="a" col2="1" />
  <row id="2" col1="b" />
  <row id="3" col1="c" col2="3" col3="extra" />
</item>`)
	problems := validateTable(table{File: "item.xml", Rows: rows})
	assertProblem(t, problems, `第 2 行缺少列 "col2"`)
	assertProblem(t, problems, `第 3 行多出列 "col3"`)
}

func TestValidateAllEmptyColumn(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row id="1" col1="a" col2="" />
  <row id="2" col1="b" col2="" />
</item>`)
	problems := validateTable(table{File: "item.xml", Rows: rows})
	assertProblem(t, problems, `列 "col2" 在全部行中都为空`)
}

func TestValidateNegativeCount(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row id="1" col1="a" count="-5" />
</item>`)
	assertProblem(t, validateTable(table{File: "item.xml", Rows: rows}), "count=-5 不允许为负")
}

func TestValidateRefs(t *testing.T) {
	items := table{File: "item.xml", Rows: mustParse(t, "item.xml", `<item>
  <row id="1001" col1="金币" />
</item>`)}
	rewards := table{File: "signin_reward.xml", Rows: mustParse(t, "signin_reward.xml", `<root>
  <row id="1" day="1" item_id="1001" count="100" />
  <row id="2" day="2" item_id="9999" count="1" />
</root>`)}

	problems := validateRefs([]table{items, rewards})
	assertProblem(t, problems, "item_id=9999 在 item.xml 中不存在")
	if len(problems) != 1 {
		t.Fatalf("only the missing item should fail, got %v", problems)
	}

	// 目标表不存在时按普通数值列处理，不误报。
	lonely := table{File: "other.xml", Rows: mustParse(t, "other.xml", `<other>
  <row id="1" copy_id="12345" />
</other>`)}
	if p := validateRefs([]table{lonely}); len(p) != 0 {
		t.Fatalf("unknown target table must not be validated: %v", p)
	}
}

func TestValidateRefsCommaList(t *testing.T) {
	items := table{File: "item.xml", Rows: mustParse(t, "item.xml", `<item>
  <row id="1001" col1="金币" />
  <row id="1002" col1="药水" />
</item>`)}
	rewards := table{File: "reward.xml", Rows: mustParse(t, "reward.xml", `<root>
  <row id="1" item_id="1001,1002" />
  <row id="2" item_id="1001,1003" />
</root>`)}
	problems := validateRefs([]table{items, rewards})
	if len(problems) != 1 || !strings.Contains(problems[0], "item_id=1003") {
		t.Fatalf("comma list should check each value, got %v", problems)
	}
}

func TestValidatePassesRealConfig(t *testing.T) {
	// 仓库内全部 XML 必须通过校验：校验器拦住新错误，但不得否定既有配置。
	dir := "../../configs/gamedata/xml"
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Skipf("config dir unavailable: %v", err)
	}
	var tables []table
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".xml") {
			continue
		}
		rows, err := parseXML(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatalf("parse %s: %v", e.Name(), err)
		}
		tables = append(tables, table{
			Name: upperCamel(strings.TrimSuffix(e.Name(), ".xml")),
			File: e.Name(),
			Rows: rows,
		})
	}
	if len(tables) == 0 {
		t.Fatal("no config loaded")
	}
	var problems []string
	for _, tb := range tables {
		problems = append(problems, validateTable(tb)...)
	}
	problems = append(problems, validateRefs(tables)...)
	if len(problems) > 0 {
		t.Fatalf("existing config must pass validation:\n%s", strings.Join(problems, "\n"))
	}
}
