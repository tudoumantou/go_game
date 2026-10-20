// tools/gendata 的配置校验（R10.2）。
//
// 校验目标不是「覆盖设计清单」，而是拦住已经在配置里真实发生过的错误类别：
//  1. 结构：空表、嵌套元素、根元素带数据属性；
//  2. 主键：缺失、非正整数、重复（重复会让 ByID 索引静默覆盖，策划改半天不生效）；
//  3. 列一致：某行多出列会被「首行定列」静默丢弃，少列会生成零值——两者都要报错；
//  4. 空列：整列全空通常是列名拼错，生成出来是一列零值；
//  5. 引用：`<x>_id` 列在存在 `<x>.xml` 时必须指向真实主键；
//  6. 范围：count/num/weight/rate 这类计数列不允许为负。
//
// 浮点、二级索引等按实际使用者逐项增加，不预先堆规则。
package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// 引用列后缀 → 目标表名前缀映射由「列名去掉 _id」直接得出，
// 只有目标表存在时才校验；不存在视为普通数值列，不误报。
const refSuffix = "_id"

// 非负校验的列名白名单（语义上不允许为负的计数/权重）。
var nonNegativeCols = map[string]bool{
	"count":  true,
	"num":    true,
	"weight": true,
	"rate":   true,
}

// validateTable 单表校验，返回人类可读的问题列表（空表示通过）。
func validateTable(t table) []string {
	var errs []string
	add := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf("%s: %s", t.File, fmt.Sprintf(format, args...)))
	}

	if len(t.Rows) == 0 {
		add("空表：没有任何 <row>，通常是文件写错或表已废弃")
		return errs
	}

	// 列集合以首行为准；其余行列必须完全一致。
	head := t.Rows[0]
	headCols := colSet(head.Cols)

	seenID := make(map[int64]bool, len(t.Rows))
	type dup struct {
		id    int64
		lines []int
	}
	dups := map[int64]*dup{}

	for i, r := range t.Rows {
		line := i + 1

		// 主键：必须存在、是正整数、不重复。
		if r.ID == "" {
			add("第 %d 行缺少 id 属性", line)
		} else if id, err := strconv.ParseInt(r.ID, 10, 64); err != nil {
			add("第 %d 行 id=%q 不是整数", line, r.ID)
		} else if id <= 0 {
			add("第 %d 行 id=%d 必须为正整数", line, id)
		} else {
			if seenID[id] {
				if d := dups[id]; d != nil {
					d.lines = append(d.lines, line)
				}
			} else {
				seenID[id] = true
				dups[id] = &dup{id: id, lines: []int{line}}
			}
		}

		// 列一致：多列会丢数据，少列会生成零值，都属于策划意图与产物不一致。
		got := colSet(r.Cols)
		for _, c := range r.Cols {
			if !headCols[c] {
				add("第 %d 行多出列 %q（首行没有该列，生成时会被丢弃）", line, c)
			}
		}
		for c := range headCols {
			if !got[c] {
				add("第 %d 行缺少列 %q（首行有该列，生成时为零值）", line, c)
			}
		}

		// 非负列。
		for c, v := range r.Vals {
			if !nonNegativeCols[c] || v == "" {
				continue
			}
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				add("第 %d 行 %s=%q 不是整数", line, c, v)
				continue
			}
			if n < 0 {
				add("第 %d 行 %s=%d 不允许为负", line, c, n)
			}
		}
	}

	// 重复主键合并成一条，附带全部行号，方便一次改完。
	for _, d := range dups {
		if len(d.lines) > 1 {
			add("主键 id=%d 重复（第 %v 行），ByID 索引会静默覆盖", d.id, d.lines)
		}
	}

	// 全空列：整列都没有值，通常是列名拼错。
	for _, c := range head.Cols {
		empty := true
		for _, r := range t.Rows {
			if strings.TrimSpace(r.Vals[c]) != "" {
				empty = false
				break
			}
		}
		if empty {
			add("列 %q 在全部行中都为空，检查列名是否拼错", c)
		}
	}

	sort.Strings(errs)
	return errs
}

// validateRefs 跨表引用校验：`<x>_id` 列在存在 `<x>.xml` 时必须命中该表主键。
//
// 用「表存在」作为开关而不是维护引用清单：新增表时不必同步改校验器，
// 而命名不合约定（如 col2 存道具 ID）的表本来也无法自动判断语义，不强行猜测。
func validateRefs(tables []table) []string {
	// 表名 → 主键集合
	ids := make(map[string]map[int64]bool, len(tables))
	for _, t := range tables {
		set := make(map[int64]bool, len(t.Rows))
		for _, r := range t.Rows {
			if id, err := strconv.ParseInt(r.ID, 10, 64); err == nil {
				set[id] = true
			}
		}
		ids[strings.TrimSuffix(t.File, ".xml")] = set
	}

	var errs []string
	for _, t := range tables {
		for _, c := range t.Rows[0].Cols {
			if c == "id" || !strings.HasSuffix(c, refSuffix) {
				continue
			}
			target := strings.TrimSuffix(c, refSuffix)
			set, ok := ids[target]
			if !ok {
				continue // 目标表不存在：按普通数值列处理
			}
			for i, r := range t.Rows {
				v, ok := r.Vals[c]
				if !ok || v == "" {
					continue
				}
				// 允许 "1001,100" 这类组合列：逐个值校验。
				for _, part := range strings.Split(v, ",") {
					part = strings.TrimSpace(part)
					if part == "" {
						continue
					}
					id, err := strconv.ParseInt(part, 10, 64)
					if err != nil {
						errs = append(errs, fmt.Sprintf("%s: 第 %d 行 %s=%q 不是整数", t.File, i+1, c, part))
						continue
					}
					if !set[id] {
						errs = append(errs, fmt.Sprintf("%s: 第 %d 行 %s=%d 在 %s.xml 中不存在",
							t.File, i+1, c, id, target))
					}
				}
			}
		}
	}
	sort.Strings(errs)
	return errs
}

// colSet 列名切片 → 集合（列一致性比较用：多列/少列两个方向都靠它判断）。
func colSet(cols []string) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c] = true
	}
	return m
}
