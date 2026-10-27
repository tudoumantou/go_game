// tools/newmodule —— 生成业务模块骨架（配合 `make new-module`）
//
// 工程链位置：`make new-module NAME=mailbox MSGS=MailboxListReq:MailboxListResp`
//
// 为什么需要它：新增一个玩法原本是七步——proto 加 @msg、make gen、建目录、
// 写装配、写处理器、去 server_start.go 注册。其中「写装配」和「注册」是纯机械的，
// 且最容易漏（漏注册的模块不会报错，只是协议永远不回包）。
//
// 本工具只做「骨架 + 注册片段」，不生成 proto、不建表、不改 server_start.go：
//   - proto 要人写（字段设计是业务决策）；
//   - server_start.go 的注册位置有语义（依赖 MySQL 的模块要放进 `if s.database != nil`
//     分支），自动插入比漏写更危险，所以打印片段让人粘贴。
//
// # 两种模块形态（对应基座的两个底座）
//
// 有内存态（默认 / -table）：`module.Define[Data]` + 包级 M。
//
//	module.go   装配：stateKey + Define + 路由注册
//	types.go    状态结构：Data + newData + validate（持久化的形状）
//	handler.go  处理器：func(c *module.Ctx[Data], req *R) error，用 c.Reply 回包
//
// 有独立表（-store）：`module.Thin` + Module 结构体持仓储。
//
//	module.go   装配：Module 结构体 + New + module.Handle 注册
//	handler.go  处理器：func(m *Module) onX(ctx *module.Context, req *R, resp *P) error
//	store.go    表的访问层（驱动只在这一层出现）
//
// 两者的处理器签名不同，不是笔误：Define 底座的基座知道响应类型（c.Reply），
// Thin 底座的 Handle 提前绑定了一对 req/resp（写 resp 字段即回包）。
//
// # 持久化的两代写法
//
// 玩家级单行状态（-table）走 `@codec` 注解：types.go 里给 Data 打
// `// @codec table=player_xxx`，由 tools/gencodec 生成 persist_gen.go。
// **不再生成手写 StateCodec 的 persist.go**——手写映射猜错列会静默丢数据，
// 而注解里的列名由生成器对着 schema 校验，拼错直接失败。
//
// 独立多行表（-store）走 tools/gendb 生成的 CRUD 函数，store.go 只做
// 「调哪个生成函数」的编排，不出现 SQL 字符串。
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

func main() {
	name := flag.String("name", "", "模块名（小写，如 mailbox）")
	msgs := flag.String("msgs", "", "协议对，逗号分隔，格式 ReqType:RespType（可省略 Resp 部分）")
	table := flag.String("table", "", "玩家级单行状态表（如 player_mailbox）；要求 schema 里该表带 @flat:<name>")
	store := flag.String("store", "", "独立多行表的表名（如 mail）；生成 Thin 形态 + store.go")
	flag.Parse()

	if *name == "" {
		fail("必须指定 -name")
	}
	if !isLowerSnake(*name) {
		fail("模块名必须是 lower_snake_case（Ext 槽位键与落库标签都用它）")
	}
	for _, t := range []string{*table, *store} {
		if t != "" && !isLowerSnake(t) {
			fail(fmt.Sprintf("表名 %q 必须是 lower_snake_case", t))
		}
	}
	if *table != "" && *store != "" {
		fail("暂不支持同时指定 -table 与 -store：有内存态（Define）和有独立表（Thin）" +
			"是两种模块形态，混用需要手工组装")
	}

	pairs, err := parseMsgs(*msgs)
	if err != nil {
		fail(err.Error())
	}

	dir := filepath.Join("internal", "game", "module", *name)
	if _, err := os.Stat(dir); err == nil {
		fail(fmt.Sprintf("%s 已存在（不覆盖已有模块）", dir))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail(err.Error())
	}

	thin := *store != ""
	// 表不存在时提前说清楚：否则一路生成完，到 make gen 才因为
	// 「undefined: model.XxxFlat」失败，看不出根因在建表这一步。
	if t := *table; t != "" {
		warnIfNoTable(t)
	}
	if t := *store; t != "" {
		warnIfNoTable(t)
	}
	switch {
	case thin:
		write(filepath.Join(dir, "module.go"), thinModuleTmpl(*name, pairs))
		if len(pairs) > 0 {
			write(filepath.Join(dir, "handler.go"), thinHandlerTmpl(*name, pairs))
		}
		write(filepath.Join(dir, "store.go"), storeTmpl(*name, *store))
	default:
		write(filepath.Join(dir, "module.go"), moduleTmpl(*name, pairs))
		write(filepath.Join(dir, "types.go"), typesTmpl(*name, *table))
		if len(pairs) > 0 {
			write(filepath.Join(dir, "handler.go"), handlerTmpl(*name, pairs))
		}
	}

	fmt.Print(registerHint(*name, pairs, *table, *store))
}

// msgPair 一个协议的请求/响应类型。
type msgPair struct{ req, resp string }

// handlerName 由请求类型推出处理器名：MailboxListReq → onMailboxList。
func (p msgPair) handlerName() string { return "on" + strings.TrimSuffix(p.req, "Req") }

func parseMsgs(s string) ([]msgPair, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var out []msgPair
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		req, resp, ok := strings.Cut(item, ":")
		req = strings.TrimSpace(req)
		if !ok {
			// 省略响应时按约定推导：XxxReq → XxxResp
			if !strings.HasSuffix(req, "Req") {
				return nil, fmt.Errorf("协议 %q 缺少 :RespType，且不以 Req 结尾无法推导", item)
			}
			resp = strings.TrimSuffix(req, "Req") + "Resp"
		}
		out = append(out, msgPair{req: req, resp: strings.TrimSpace(resp)})
	}
	return out, nil
}

// header 包注释：两版共用。
func header(name, memory string) string {
	return fmt.Sprintf(`// Package %[1]s %[1]s 模块。
//
// 由 tools/newmodule 生成：完成业务逻辑后请删除本行。
//
// 架构职责：<一句话说明这个模块管什么>。
// 内存态：%[2]s
// 事件订阅：无
// 配置表：<如有>
package %[1]s
`, name, memory)
}

// moduleTmpl Define 形态的装配文件：只有常量、Define 与路由注册。
//
// 刻意不写业务：module.go 是「这个模块对外暴露了什么」的说明书，
// 打开它就该在 30 行内看完全部协议。
func moduleTmpl(name string, pairs []msgPair) string {
	var routes, imports strings.Builder
	if len(pairs) > 0 {
		imports.WriteString("\n\n\tgamev1 \"game_study/pkg/pb/game/v1\"")
		for _, p := range pairs {
			// 统一用 module.Route：它带有请求体，是绝大多数协议的形态；
			// 无参协议可删掉 req 形参改用 HandleRoute（见 repo.go 的说明）。
			fmt.Fprintf(&routes, "\tmodule.Route(M, &gamev1.%s{}, %s)\n", p.req, p.handlerName())
		}
	}

	initBlock := ""
	if len(pairs) > 0 {
		initBlock = fmt.Sprintf(`
func init() {
%s}
`, routes.String())
	}

	return header(name, "有（随玩家快照 CAS 提交）") + fmt.Sprintf(`
import (
	"game_study/internal/game/module"%[1]s
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = %[2]q

// M 模块唯一实例：日志、状态编解码、分发都由 Define 兜底，
// 不在每个模块里重复组装一遍（少一处漏配的机会）。
//
// Guard(RequireCache)：本模块有内存态，缓存权威路径没就绪时直接拒绝请求，
// 不要退化成「读库后改内存」——那会和快照提交竞争。
var M = module.Define[Data](stateKey, newData).Guard(module.RequireCache)
%[3]s`, imports.String(), name, initBlock)
}

// thinModuleTmpl Thin 形态的装配文件：Module 结构体 + New + Handle 注册。
//
// 与 Define 的区别是「数据不在内存里」：所有读写都落到独立表，
// 因此不需要 Guard(RequireCache)，也不参与快照 CAS。
func thinModuleTmpl(name string, pairs []msgPair) string {
	var handles strings.Builder
	for _, p := range pairs {
		fmt.Fprintf(&handles, "\tmodule.Handle(m.Thin, m.%s)\n", p.handlerName())
	}
	if handles.Len() > 0 {
		handles.WriteString("\n")
	}

	return header(name, "无（数据在 "+name+" 相关表里，按需查询）") + fmt.Sprintf(`
import (
	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/log"
)

// Module %[1]s 模块。
//
// 无内存态：每次请求从 store 现查现写，不参与玩家快照的 CAS 提交。
type Module struct {
	*module.Thin

	// store 表访问层；nil 表示无 DB 环境（单测），各方法需自行降级。
	store  *store
	logger *log.Logger
}

// New 创建模块。
//
// 接 *db.DB 而不是收窄成接口，是骨架从简；模块长大、单测需要 fake 时
// 应改成 store 接口（参考 mail.New 收 mailRepo 的做法）。
func New(database *db.DB, logger *log.Logger) *Module {
	m := &Module{store: newStore(database), logger: logger}
	m.Thin = module.NewThin(%[1]q, module.ThinLogger(logger))
%s	return m
}
`, name, handles.String())
}

// typesTmpl 状态结构：持久化的形状 + 新玩家初始态 + 落库前校验。
func typesTmpl(name, table string) string {
	// 走 @codec 时字段用 db tag 指定列名（gencodec 按它生成行/结构映射）；
	// 否则用 json tag（状态整体序列化进 Ext，没有列的概念）。
	codec := "\n"
	field := "`json:\"counter\"`"
	if table != "" {
		codec = fmt.Sprintf("\n// @codec table=%s\n", table)
		field = "`db:\"counter\"`"
	}

	return fmt.Sprintf(`package %[1]s

// Data 本模块的玩家私有状态。
//
// 只放需要持久化的数据；派生值（等级加成、红点）每次登录重算，
// 不落库——否则「改公式后旧数据对不上」。
//
// 字段设计：宁可拆成多个小结构，也不要把不同生命周期的数据塞进一个结构
// （比如「本周期已购次数」和「累计购买量」的清理时机完全不同）。
%[2]stype Data struct {
	// Counter 示例字段，按业务替换。
	Counter int32 %[3]s
}

// newData 新玩家的空状态。
//
// 返回非 nil：map/slice 字段在这里初始化，避免业务里写 nil map 触发 panic。
func newData() *Data { return &Data{} }

// validate 落库前校验：坏数据整批回滚，不要写进 DB。
//
// 落库是异步合批的，一条脏数据会让整批回滚并反复重试，
// 在这里挡住比在 DB 侧报错更容易定位。
func validate(*Data) error { return nil }
`, name, codec, field)
}

// handlerTmpl Define 形态的处理器：拿 Ctx[Data]，用 c.Reply 回包。
//
// 生成的空壳必须可编译：留 <TODO> 反而更慢——使用者第一件事得先修生成器。
func handlerTmpl(name string, pairs []msgPair) string {
	var b strings.Builder
	first := pairs[0]

	// 第一个处理器给完整骨架（取状态 → 判错 → 回包），其余是简版，
	// 避免整文件复制同一段样板。
	fmt.Fprintf(&b, `
// %s 处理 %s。
//
// 完整骨架：取状态 → 判错 → 回包。其余处理器是简版，照这个补。
func %s(c *module.Ctx[Data], req *gamev1.%s) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.%s{Code: int32(errcode.ErrSystem)}, err)
	}
	_ = d   // 按业务读 d 的字段
	_ = req // 按业务读 req 的字段
	return c.Reply(&gamev1.%s{})
}
`, first.handlerName(), first.req, first.handlerName(), first.req, first.resp, first.resp)

	for _, p := range pairs[1:] {
		fmt.Fprintf(&b, `
// %s 处理 %s。
func %s(c *module.Ctx[Data], req *gamev1.%s) error {
	_ = req
	return c.Reply(&gamev1.%s{})
}
`, p.handlerName(), p.req, p.handlerName(), p.req, p.resp)
	}

	return fmt.Sprintf(`package %[1]s

import (
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
)

// 写作约定（docs/模块开发约定.md）：
//	handler 只做「取状态 → 按规则判断 → 调 Repo / Grant → 回包」。
//	出现循环、事务、SQL、缓存 key 时，下沉到 types.go 或 store.go。
//	错误分支一律 return c.FailCode(resp, errcode.Xxx)，不要手写 Code 赋值。
%[2]s
`, name, strings.TrimRight(b.String(), "\n"))
}

// thinHandlerTmpl Thin 形态的处理器：签名带 resp，写完字段即回包。
func thinHandlerTmpl(name string, pairs []msgPair) string {
	var b strings.Builder
	for _, p := range pairs {
		fmt.Fprintf(&b, `
// %s 处理 %s。
//
// 回包由基座完成：本函数返回 nil 时，Handle 包装器序列化 resp 下发；
// 需要改码就 return ctx.FailCode(resp, errcode.Xxx)。
func (m *Module) %s(ctx *module.Context, req *gamev1.%s, resp *gamev1.%s) error {
	*resp = gamev1.%s{}
	_, _, _ = m, ctx, req
	return nil
}
`, p.handlerName(), p.req, p.handlerName(), p.req, p.resp, p.resp)
	}

	return fmt.Sprintf(`package %[1]s

import (
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
)

// 写作约定（docs/模块开发约定.md）：
//	handler 只做「取状态 → 按规则判断 → 调 store → 填 resp」。
//	SQL 只出现在 store.go，且必须是 tools/gendb 生成的函数。
%[2]s
`, name, strings.TrimRight(b.String(), "\n"))
}

// storeTmpl 独立表的访问层：驱动只在这一层出现。
func storeTmpl(name, table string) string {
	row := pascal(table)
	return fmt.Sprintf(`// store.go —— %[2]s 表的访问层（模块 %[1]s）。
//
// 只允许调用 tools/gendb 从 sql/schema 生成的函数（model.List%[3]sByPlayer 这类）。
// 需要生成器表达不了的只读查询（JOIN / EXISTS / 聚合）时：
//  1. 在 sql/schema/game.sql 的表前加 @findby / @listby / @lockby / @countby 注解；
//  2. 重跑 make gen-db；
//  3. 仍然写不出来才手写 SQL，并登记进 docs/功能实现状态.md 的例外清单 + 补 sqlmock。
//
// 不要在业务层（module.go / handler.go）去拿 *db.DB——那等于绕过整个约束。
package %[1]s

import (
	"game_study/pkg/db"
)

// store %[2]s 的表访问层。
type store struct {
	db *db.DB
}

func newStore(database *db.DB) *store { return &store{db: database} }

// 示例（取消注释并按表结构调整）：
//
//	func (s *store) listByPlayer(ctx context.Context, playerID int64) ([]model.%[3]s, error) {
//		return model.List%[3]sByPlayer(ctx, s.db.DB, playerID)
//	}
`, name, table, row)
}

// pascal 把 lower_snake_case 转成 PascalCase（与 gendb 的表→类型命名一致）。
func pascal(s string) string {
	var b strings.Builder
	for _, part := range strings.Split(s, "_") {
		if part == "" {
			continue
		}
		b.WriteString(upperFirst(part))
	}
	return b.String()
}

func registerHint(name string, pairs []msgPair, table, store string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "\n下一步（手工）：\n")

	step := 1
	next := func(format string, args ...any) {
		fmt.Fprintf(&b, "  %d) ", step)
		fmt.Fprintf(&b, format, args...)
		fmt.Fprintf(&b, "\n")
		step++
	}

	switch {
	case table != "":
		next("在 sql/schema/game.sql 里确认表 %s 存在、表前有 `-- @flat:%s`（主键必须是 player_id）", table, name)
		next("make gen        # 生成 model + @codec 持久化适配（缺表/列名不符会在这里报错）")
	case store != "":
		next("在 sql/schema/game.sql 里确认表 %s 存在，并按查询需要加 @findby/@listby 注解", store)
		next("make gen-db     # 生成 model 的 CRUD 函数")
	}
	if len(pairs) > 0 {
		next("在 api/proto/game/v1/ 加 message 与 // @msg 注解，然后 make gen")
	} else {
		next("proto 定好后重跑本工具（或手写 module.Route 注册）")
	}
	if store != "" {
		next("在 internal/game/server_start.go 的 initModules 里注册：s.modules.Register(%s.New(s.database, s.logger))", name)
	} else {
		next("在 internal/game/server_start.go 的 initModules 里注册：s.modules.Register(%s.M)", name)
	}
	next("go build ./... && go test ./internal/game/module/%s/", name)

	if len(pairs) == 0 {
		return b.String()
	}
	fmt.Fprintf(&b, "\n生成的处理器（handler.go）：\n")
	for _, p := range pairs {
		fmt.Fprintf(&b, "  %s\n", p.handlerName())
	}
	return b.String()
}

// warnIfNoTable 检查 schema 里是否有这张表，没有就警告（不中断）。
//
// 读不到 schema 文件时静默跳过：工具可能在仓库外被调用，
// 而「读不到」不等于「表不存在」，不该据此失败。
func warnIfNoTable(name string) {
	data, err := os.ReadFile(filepath.Join("sql", "schema", "game.sql"))
	if err != nil {
		return
	}
	if !strings.Contains(string(data), "CREATE TABLE `"+name+"`") {
		fmt.Fprintf(os.Stderr, "  警告：sql/schema/game.sql 里没有表 %s，make gen 会失败\n", name)
	}
}

func isLowerSnake(s string) bool {
	for _, r := range s {
		if !(unicode.IsLower(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return s != ""
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

func write(path, content string) {
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		fail(err.Error())
	}
	fmt.Printf("  生成 %s\n", path)
}

func fail(msg string) {
	fmt.Fprintf(os.Stderr, "newmodule: %s\n", msg)
	os.Exit(1)
}
