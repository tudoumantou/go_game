// tools/genmsgid —— 从 proto 注解生成消息号注册表（R10.1）
//
// 声明方式：在 message 上一行写 `// @msg <id>`。
//
//	// @msg 3001
//	message UseItemReq { ... }
//
// 选择注解而不是自定义 proto option 的理由：option 需要自定义 extension 与自建 protoc 插件，
// 而这里只需要「一个可被 grep、可被 review、不依赖 protoc 版本」的唯一声明点。注解由本工具
// 直接读文本，`make gen` 与 `go test` 都不依赖 buf。
//
// 校验（任一不通过即失败，不产出半套文件）：
//   - 号重复：同一号被两个 message 声明（客户端会收到对方的消息体）；
//   - message 重名：生成的 Go 常量会撞名；
//   - 号越界：不在 internal/common/constant 声明的任何号段内，或落在内部 gRPC 段；
//   - 注解悬空：`// @msg` 之后不是 message 声明。
//
// 产物：pkg/codec/msgids_gen.go（常量 + id↔名字 反查）。
package main

import (
	"bufio"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// decl 一条消息号声明。
type decl struct {
	ID     uint32
	Name   string
	File   string
	Line   int
	Packet string // 所属号段名（Bag/Battle/…），仅用于报告
}

// segment 号段（来自 internal/common/constant）。
type segment struct {
	Name     string
	Min, Max uint32
}

// main 入口：读号段（constant 包）→ 扫 proto 注解 → 校验 → 生成或 -check 比对。
//
// -check 模式不写文件：把内存里的生成结果与磁盘上的 msgids_gen.go 逐字节比对，
// 供 CI（make gen-check）发现「改了 proto 忘了跑 make gen-msgid」。
func main() {
	in := flag.String("in", "api/proto/game/v1", "proto 输入目录（递归）")
	goOut := flag.String("out", "pkg/codec/msgids_gen.go", "生成的 Go 文件")
	constDir := flag.String("constant", "internal/common/constant", "号段常量所在包目录")
	check := flag.Bool("check", false, "只校验，不写文件（CI/生成一致性检查）")
	flag.Parse()

	segments, err := parseSegments(*constDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "读取号段失败：%v\n", err)
		os.Exit(1)
	}
	if len(segments) == 0 {
		fmt.Fprintln(os.Stderr, "未从 constant 包解析到任何号段（MsgID<Group>Min/Max）")
		os.Exit(1)
	}

	decls, err := scanProtos(*in)
	if err != nil {
		fmt.Fprintf(os.Stderr, "扫描 proto 失败：%v\n", err)
		os.Exit(1)
	}
	if len(decls) == 0 {
		fmt.Fprintln(os.Stderr, "没有找到任何 // @msg 声明")
		os.Exit(1)
	}

	// 校验先行：撞号/重名/越界任一存在都不生成，避免产出半套注册表
	problems := validate(decls, segments)
	if len(problems) > 0 {
		fmt.Fprintf(os.Stderr, "消息号校验失败（%d 项）：\n", len(problems))
		for _, p := range problems {
			fmt.Fprintf(os.Stderr, "  ✗ %s\n", p)
		}
		os.Exit(1)
	}

	src := generate(decls, segments)
	if *check {
		formatted, ferr := format.Source([]byte(src))
		if ferr != nil {
			fmt.Fprintf(os.Stderr, "生成的源码有语法错误：%v\n", ferr)
			os.Exit(1)
		}
		old, err := os.ReadFile(*goOut)
		if err != nil || string(old) != string(formatted) {
			fmt.Fprintf(os.Stderr, "%s 与 proto 声明不一致，请运行 make gen-msgid\n", *goOut)
			os.Exit(1)
		}
		fmt.Printf("消息号一致：%d 条\n", len(decls))
		return
	}

	if err := os.MkdirAll(filepath.Dir(*goOut), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}
	out, err := format.Source([]byte(src))
	if err != nil {
		fmt.Fprintf(os.Stderr, "生成的源码有语法错误：%v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*goOut, out, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("genmsgid 完成：%d 条消息 → %s\n", len(decls), *goOut)
}

// scanProtos 遍历目录下全部 .proto，收集 `// @msg` 声明。
func scanProtos(dir string) ([]decl, error) {
	var decls []decl
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".proto") {
			return nil
		}
		fileDecls, err := scanFile(path)
		if err != nil {
			return err
		}
		decls = append(decls, fileDecls...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(decls, func(i, j int) bool { return decls[i].ID < decls[j].ID })
	return decls, nil
}

var (
	// reMsgAnno 匹配 `// @msg <id>` 注解行（行首行尾锚定，避免匹配到正文里的同名文本）。
	reMsgAnno = regexp.MustCompile(`^\s*//\s*@msg\s+(\d+)\s*$`)
	// reMessage 匹配 message 声明行（按文本扫描而非完整 proto 解析，不依赖 protoc/buf 版本）。
	reMessage = regexp.MustCompile(`^\s*message\s+([A-Za-z_][A-Za-z0-9_]*)\b`)
	// rePackage 匹配 proto package 行；当前仅声明未使用（package 级变量不报编译错误），
	// 留给将来按包分组的报告。
	rePackage = regexp.MustCompile(`^\s*package\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;`)
)

// scanFile 扫描单个 .proto，收集「注解 → message」绑定。
//
// 行状态机语义：见到 @msg 记 pending；空行与注释不打断邻接关系（注解与 message
// 之间常有说明注释）；pending 状态下遇到非 message 的实质行即报悬空错误。
// 所有错误带 文件:行号，直接指回 proto 声明点，不需要额外查表。
func scanFile(path string) ([]decl, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []decl
	var pendingID uint32
	var pendingLine int
	hasPending := false

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Text()

		if m := reMsgAnno.FindStringSubmatch(line); m != nil {
			if hasPending {
				return nil, fmt.Errorf("%s:%d: 上一条 @msg 未绑定到 message", path, pendingLine)
			}
			id, err := strconv.ParseUint(m[1], 10, 32)
			if err != nil || id == 0 || id > 65535 {
				return nil, fmt.Errorf("%s:%d: @msg %s 超出 1..65535", path, lineNo, m[1])
			}
			pendingID, pendingLine, hasPending = uint32(id), lineNo, true
			continue
		}

		// 空行与注释不打断「注解 → message」的邻接关系（注解与 message 之间常有说明注释）。
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "//") {
			continue
		}
		if m := reMessage.FindStringSubmatch(line); m != nil {
			if !hasPending {
				continue
			}
			out = append(out, decl{ID: pendingID, Name: m[1], File: path, Line: pendingLine})
			hasPending = false
			continue
		}
		if hasPending {
			return nil, fmt.Errorf("%s:%d: @msg 之后不是 message 声明（遇到 %q）", path, pendingLine, trimmed)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if hasPending {
		return nil, fmt.Errorf("%s:%d: @msg 后没有 message 声明", path, pendingLine)
	}
	return out, nil
}

// reSegMin/reSegMax 匹配 constant 包里的 MsgID<Group>Min/Max 常量行：
// 号段边界只有这一处来源，新增号段 = 在 constant 加一对常量，无需改本工具。
var reSegMin = regexp.MustCompile(`^\s*(MsgID([A-Za-z]+))Min\s*=\s*(\d+)`)
var reSegMax = regexp.MustCompile(`^\s*(MsgID([A-Za-z]+))Max\s*=\s*(\d+)`)

// parseSegments 从 constant 包读号段，段边界只有这一处来源。
func parseSegments(dir string) ([]segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	mins := map[string]uint32{}
	maxs := map[string]uint32{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if m := reSegMin.FindStringSubmatch(line); m != nil {
				if v, err := strconv.ParseUint(m[3], 10, 32); err == nil {
					mins[m[2]] = uint32(v)
				}
				continue
			}
			if m := reSegMax.FindStringSubmatch(line); m != nil {
				if v, err := strconv.ParseUint(m[3], 10, 32); err == nil {
					maxs[m[2]] = uint32(v)
				}
			}
		}
		_ = f.Close()
	}

	var segs []segment
	for name, min := range mins {
		max, ok := maxs[name]
		if !ok {
			return nil, fmt.Errorf("号段 MsgID%sMin 缺少对应 Max", name)
		}
		if max < min {
			return nil, fmt.Errorf("号段 %s 的 Max(%d) < Min(%d)", name, max, min)
		}
		segs = append(segs, segment{Name: name, Min: min, Max: max})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].Min < segs[j].Min })
	return segs, nil
}

// validate 做重复号、重名、越界三类检查。
func validate(decls []decl, segs []segment) []string {
	var problems []string

	byID := map[uint32]decl{}
	byName := map[string]decl{}
	for _, d := range decls {
		if prev, ok := byID[d.ID]; ok {
			problems = append(problems, fmt.Sprintf(
				"消息号 %d 重复：%s(%s:%d) 与 %s(%s:%d)",
				d.ID, prev.Name, shortPath(prev.File), prev.Line, d.Name, shortPath(d.File), d.Line))
		} else {
			byID[d.ID] = d
		}
		if prev, ok := byName[d.Name]; ok {
			problems = append(problems, fmt.Sprintf(
				"message 重名 %s：%s:%d 与 %s:%d（生成的 Go 常量会冲突）",
				d.Name, shortPath(prev.File), prev.Line, shortPath(d.File), d.Line))
		} else {
			byName[d.Name] = d
		}

		seg, ok := lookupSegment(d.ID, segs)
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf(
				"%s(%s:%d) 号 %d 不在任何号段内（见 internal/common/constant 的 MsgID*Min/Max）",
				d.Name, shortPath(d.File), d.Line, d.ID))
		case seg.Name == "Internal":
			problems = append(problems, fmt.Sprintf(
				"%s(%s:%d) 号 %d 落在内部 gRPC 段 %d-%d，客户端协议不得使用",
				d.Name, shortPath(d.File), d.Line, d.ID, seg.Min, seg.Max))
		}
	}
	return problems
}

// lookupSegment 按号查找所属号段（号段互不重叠，线性扫描足够；未命中即越界）。
func lookupSegment(id uint32, segs []segment) (segment, bool) {
	for _, s := range segs {
		if id >= s.Min && id <= s.Max {
			return s, true
		}
	}
	return segment{}, false
}

// generate 产出 Go 源码；顺序按号排序，保证生成结果稳定（可 diff、可 -check）。
func generate(decls []decl, segs []segment) string {
	// 排序放在这里而不是只放在调用方：任何入口都产出同样的文件，diff 才可信。
	sorted := make([]decl, len(decls))
	copy(sorted, decls)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	decls = sorted

	var b strings.Builder
	b.WriteString("// Code generated by tools/genmsgid from api/proto/**/*.proto. DO NOT EDIT.\n")
	b.WriteString("//\n")
	b.WriteString("// 消息号唯一声明点是 proto 里的 `// @msg <id>` 注解；改号请改 proto 后运行 make gen-msgid。\n")
	b.WriteString("// 号段边界来自 internal/common/constant 的 MsgID*Min/Max。\n\n")
	b.WriteString("package codec\n\n")

	b.WriteString("// 客户端协议消息号（与服务端模块 OnMessage 分发的常量一一对应）\n")
	b.WriteString("const (\n")
	for _, d := range decls {
		b.WriteString(fmt.Sprintf("\tMsg%-28s uint32 = %d\n", d.Name, d.ID))
	}
	b.WriteString(")\n\n")

	b.WriteString("// msgNames 消息号 → message 名（日志与排障用，避免只看到数字）\n")
	b.WriteString("var msgNames = map[uint32]string{\n")
	for _, d := range decls {
		b.WriteString(fmt.Sprintf("\t%d: %q,\n", d.ID, d.Name))
	}
	b.WriteString("}\n\n")

	b.WriteString("// msgIDs message 名 → 消息号\n")
	b.WriteString("var msgIDs = map[string]uint32{\n")
	for _, d := range decls {
		b.WriteString(fmt.Sprintf("\t%q: %d,\n", d.Name, d.ID))
	}
	b.WriteString("}\n\n")

	b.WriteString("// MsgName 返回消息号对应的 message 名。\n")
	b.WriteString("func MsgName(id uint32) (string, bool) {\n\tn, ok := msgNames[id]\n\treturn n, ok\n}\n\n")
	b.WriteString("// MsgID 返回 message 名对应的消息号。\n")
	b.WriteString("func MsgID(name string) (uint32, bool) {\n\tid, ok := msgIDs[name]\n\treturn id, ok\n}\n\n")
	b.WriteString("// MsgCount 已声明消息数（启动自检用）。\n")
	b.WriteString(fmt.Sprintf("func MsgCount() int { return %d }\n", len(decls)))
	return b.String()
}

// shortPath 报告用短路径：只保留 api/proto/ 之后的部分，错误信息更短。
func shortPath(p string) string {
	// 只保留 api/proto 之后的部分，报告更短。
	if i := strings.Index(p, "api/proto/"); i >= 0 {
		return p[i:]
	}
	return filepath.Base(p)
}
