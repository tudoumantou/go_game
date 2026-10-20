// tools/gendata 的配置校验（R10.2）。
//
// 工程链位置：gendata 生成的前置关卡 —— validateTable/validateRefs 由
// tools/gendata/main.go 在「先校验再落盘」阶段调用，对应 Makefile 的
// `make gen-data` / `make gen-check`：配置错误在这里全部拦截，不产出任何
// 生成物，CI 卡点看到的就是本文件收集的问题列表。
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
// 后缀写成常量而不是散落两处字符串：改约定（如 _ref）只动这一行。
const refSuffix = "_id"

// 非负校验的列名白名单（语义上不允许为负的计数/权重）。
// 白名单而不是全部列：reward=-1 可能是策划的特殊语义（如「不发放」），
// 只拦确定不能为负的列，避免误报把白名单越用越窄、最后没人信校验。
var nonNegativeCols = map[string]bool{
	"count":  true,
	"num":    true,
	"weight": true,
	"rate":   true,
}

// validateTable 单表校验，返回人类可读的问题列表（空表示通过）。
//
// 契约：
//   - 收集式报告而非遇错即停：一次列全所有问题，策划改一轮就能再跑，
//     比「改一个错再撞下一个错」的体验好得多；
//   - 返回值按字典序排序（末尾 sort.Strings）：多次运行输出稳定，
//     CI 日志可 diff；
//   - 只报告不修复：本包是生成器，不拥有配置的正确写法，决策权留给策划；
//   - 错误信息统一带表名前缀（add 闭包）：多张表的问题混排时能直接定位文件。
func validateTable(t table) []string {
	var errs []string
	add := func(format string, args ...any) {
		// 闭包按引用捕获 errs 与 t.File：append 重新分配切片后，
		// 闭包看到的仍是新切片，无需把 errs 传出传回
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
	// 重复主键的聚合载体：首次出现建组，后续出现只追加行号，
	// 最后统一报成一条（带全部行号，策划一次改完）
	type dup struct {
		id    int64
		lines []int
	}
	dups := map[int64]*dup{}

	for i, r := range t.Rows {
		line := i + 1

		// 主键：必须存在、是正整数、不重复。
		// if/else-if 链即「互斥判定」：一行最多命中一个分支，
		// 避免同一行因多个问题被反复报告
		if r.ID == "" {
			add("第 %d 行缺少 id 属性", line)
		} else if id, err := strconv.ParseInt(r.ID, 10, 64); err != nil {
			// ParseInt(s, 10, 64)：十进制、int64 范围。XML 属性全是字符串，
			// 整数判定集中在这里，与 gendata 的 inferType/goLiteral 口径一致
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
	// map[int64]*dup 存指针值：后续 append(d.lines) 改的是同一份结构，
	// 若存值类型 dup，对副本的修改不会写回 map
	for _, d := range dups {
		if len(d.lines) > 1 {
			add("主键 id=%d 重复（第 %v 行），ByID 索引会静默覆盖", d.id, d.lines)
		}
	}

	// 全空列：整列都没有值，通常是列名拼错。
	// TrimSpace 后再判空：策划手打的全角空格/换行不算「有值」
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

	// 输出排序：与 genmsgid/gendata 的排序约定一致 —— 生成器的一切输出
	// 都要可 diff，CI 的 gen-check 与人工 review 都依赖稳定性
	sort.Strings(errs)
	return errs
}

// validateRefs 跨表引用校验：`<x>_id` 列在存在 `<x>.xml` 时必须命中该表主键。
//
// 用「表存在」作为开关而不是维护引用清单：新增表时不必同步改校验器，
// 而命名不合约定（如 col2 存道具 ID）的表本来也无法自动判断语义，不强行猜测。
//
// 组合列支持 "1001,100" 逐个校验：掉落组/奖励组这类列是配置里的常见形态，
// 整串当整数会直接解析失败，逐个拆开才能定位到具体哪个 ID 不存在。
func validateRefs(tables []table) []string {
	// 表名 → 主键集合
	ids := make(map[string]map[int64]bool, len(tables))
	for _, t := range tables {
		set := make(map[int64]bool, len(t.Rows))
		for _, r := range t.Rows {
			// 只收成功解析的整数：非整数 id 已由 validateTable 报告，
			// 这里再报一次只会产生重复噪音
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
// Go 没有内置 set 类型，惯用法是用 map[string]bool 换取 O(1) 成员判定；
// 这里选 bool 值（而非 struct{}）图可读性 —— 集合的语义由使用方保证。
func colSet(cols []string) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c] = true
	}
	return m
}
