// tools/gendata/validate_test.go —— 配置校验器（R10.2）的单元测试
//
// 覆盖三类输入：
//   - 表格级规则：空表、主键形态/重复、列一致（多列丢数据/少列零值）、整列全空、非负列；
//   - 跨表引用：`<x>_id` 命中目标表主键、逗号组合列逐值校验、目标表不存在不误报；
//   - 仓库内全部真实 XML 必须通过（校验器可以拦新错误，但不能否定既有配置）。
//
// 构造输入一律「写临时文件 → parseXML」：保证测的是真实解析路径，而不是手搓 rowItem。
package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// parseOne 把 XML 文本写到临时文件后解析，模拟真实输入。
func parseOne(t *testing.T, name, body string) ([]rowItem, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	err := os.WriteFile(path, []byte(body), 0o644)
	require.NoError(t, err)
	return parseXML(path)
}

func mustParse(t *testing.T, name, body string) []rowItem {
	t.Helper()
	rows, err := parseOne(t, name, body)
	require.NoError(t, err, "parse")
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

// TestParseXMLIgnoresRootAttributes 根元素带属性时不得被当成数据行。
//
// 回归用例：旧实现只认 <root>，<item version="3"> 会多解析出一行；
// 现在按「深度 2 才是行」判定，根元素属性一律忽略。
func TestParseXMLIgnoresRootAttributes(t *testing.T) {
	// 根元素带属性时不能被当成数据行（旧实现只认 <root>）。
	rows := mustParse(t, "item.xml", `<item version="3">
  <row id="1" col1="a" />
  <row id="2" col1="b" />
</item>`)
	require.Len(t, rows, 2)
	require.EqualValues(t, "1", rows[0].ID, fmt.Sprintf("unexpected rows: %+v", rows))
	require.EqualValues(t, "b", rows[1].Vals["col1"])
}

// TestParseXMLRejectsNested 嵌套元素必须报错（配置约定是单层结构：根 → 行）。
//
// <row> 内再嵌 <group> 属于策划笔误，静默忽略会让数据悄悄丢失，因此在解析期直接失败。
func TestParseXMLRejectsNested(t *testing.T) {
	_, err := parseOne(t, "item.xml", `<item>
  <row id="1"><group col1="x" /></row>
</item>`)
	require.Error(t, err, fmt.Sprintf("nested element must be rejected, got %v", err))
	require.Contains(t, err.Error(), "嵌套")
}

// TestValidateEmptyTable 空表要报错：没有任何 <row>，通常是文件写错或表已废弃。
func TestValidateEmptyTable(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item></item>`)
	assertProblem(t, validateTable(table{File: "item.xml", Rows: rows}), "空表")
}

// TestValidateDuplicatePrimaryKey 重复主键必须报错，且同一 id 的多行合并成一条问题。
//
// 关键断言 len(problems)==1：ByID 索引会静默覆盖，逐行报三条重复信息只会淹没真正的问题。
func TestValidateDuplicatePrimaryKey(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row id="1" col1="a" />
  <row id="1" col1="b" />
  <row id="1" col1="c" />
</item>`)
	problems := validateTable(table{File: "item.xml", Rows: rows})
	assertProblem(t, problems, "主键 id=1 重复")
	require.Len(t, problems, 1)
}

// TestValidatePrimaryKeyShape 主键三态各报一条：缺失 / 非正整数(0) / 非整数。
//
// 三种错误形态压进同一个 XML 的三行，一次覆盖，避免为每种形态复制一份表。
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

// TestValidateColumnMismatch 列不一致两个方向都要拦：多列（生成时被静默丢弃）、
// 少列（生成时为零值）—— 两者都是策划意图与产物不一致。
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

// TestValidateAllEmptyColumn 整列全空视为列名拼错：生成出来是一列零值，
// 是最容易被漏掉的一类配置错误。
func TestValidateAllEmptyColumn(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row id="1" col1="a" col2="" />
  <row id="2" col1="b" col2="" />
</item>`)
	problems := validateTable(table{File: "item.xml", Rows: rows})
	assertProblem(t, problems, `列 "col2" 在全部行中都为空`)
}

// TestValidateNegativeCount count/num/weight/rate 白名单列不允许为负（计数/权重语义）。
func TestValidateNegativeCount(t *testing.T) {
	rows := mustParse(t, "item.xml", `<item>
  <row id="1" col1="a" count="-5" />
</item>`)
	assertProblem(t, validateTable(table{File: "item.xml", Rows: rows}), "count=-5 不允许为负")
}

// TestValidateRefs 跨表引用：item_id 必须命中 item.xml 的主键；
// 目标表不存在（copy_id）时按普通数值列处理，不能误报。
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
	require.Len(t, problems, 1)

	// 目标表不存在时按普通数值列处理，不误报。
	lonely := table{File: "other.xml", Rows: mustParse(t, "other.xml", `<other>
  <row id="1" copy_id="12345" />
</other>`)}
	p := validateRefs([]table{lonely})
	require.Empty(t, p, fmt.Sprintf("unknown target table must not be validated: %v", p))
}

// TestValidateRefsCommaList 组合列 "1001,1002" 逐值校验，只报缺失的那个（1003），
// 且不把整列判为非法整数。
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
	require.Len(t, problems, 1, fmt.Sprintf("comma list should check each value, got %v", problems))
	require.Contains(t, problems[0], "item_id=1003")
}

// TestValidatePassesRealConfig 仓库内全部 XML 必须通过校验。
//
// 防「校验器进化后反过来否定既有配置」：新增规则时若老配置不过，要么改配置要么改规则，
// 不允许悄悄放宽校验。
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
		require.NoError(t, err)
		tables = append(tables, table{
			Name: upperCamel(strings.TrimSuffix(e.Name(), ".xml")),
			File: e.Name(),
			Rows: rows,
		})
	}
	require.NotEmpty(t, tables, "no config loaded")
	var problems []string
	for _, tb := range tables {
		problems = append(problems, validateTable(tb)...)
	}
	problems = append(problems, validateRefs(tables)...)
	require.Empty(t, problems, fmt.Sprintf("existing config must pass validation:\n%s", strings.Join(problems, "\n")))
}
