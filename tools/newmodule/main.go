// tools/newmodule —— 生成业务模块骨架（配合 `make new-module`）
//
// 工程链位置：`make new-module NAME=mailbox MSGS=MailboxListReq:MailboxListResp`
//
// 为什么需要它：新增一个玩法原本是七步——proto 加 @msg、make gen、建目录、
// 写 Data 结构、写装配、写处理器、去 server_start.go 注册。其中「写装配」和
// 「注册」是纯机械的，且最容易漏（漏注册的模块不会报错，只是协议永远不回包）。
//
// 本工具只做「骨架 + 注册片段」，不生成 proto、不建表、不改 server_start.go：
//   - proto 要人写（字段设计是业务决策）；
//   - server_start.go 的注册位置有语义（依赖 MySQL 的模块要放进 `if s.database != nil`
//     分支），自动插入比漏写更危险，所以打印片段让人粘贴。
//
// 生成物：
//
//	internal/game/module/<name>/<name>.go   业务文件（Data + 装配 + 处理器）
//	internal/game/module/<name>/persist.go  持久化适配（仅当指定表名）
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
	table := flag.String("table", "", "对应表名（如 player_mailbox）；留空则不生成 persist.go")
	flag.Parse()

	if *name == "" {
		fail("必须指定 -name")
	}
	if !isLowerSnake(*name) {
		fail("模块名必须是 lower_snake_case（Ext 槽位键与落库标签都用它）")
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

	write(filepath.Join(dir, *name+".go"), moduleTmpl(*name, pairs))
	fmt.Printf("  生成 %s/%s.go\n", dir, *name)

	if *table != "" {
		write(filepath.Join(dir, "persist.go"), persistTmpl(*name, *table))
		fmt.Printf("  生成 %s/persist.go\n", dir)
	}

	fmt.Print(registerHint(*name, pairs))
}

// msgPair 一个协议的请求/响应类型。
type msgPair struct{ req, resp string }

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
		if !ok {
			// 省略响应时按约定推导：XxxReq → XxxResp
			if !strings.HasSuffix(req, "Req") {
				return nil, fmt.Errorf("协议 %q 缺少 :RespType，且不以 Req 结尾无法推导", item)
			}
			resp = strings.TrimSuffix(req, "Req") + "Resp"
		}
		out = append(out, msgPair{req: strings.TrimSpace(req), resp: strings.TrimSpace(resp)})
	}
	return out, nil
}

func moduleTmpl(name string, pairs []msgPair) string {
	var (
		consts  strings.Builder
		handles strings.Builder
		onFuncs strings.Builder
	)
	for i, p := range pairs {
		base := strings.TrimSuffix(p.req, "Req") // ProbeList
		vn := lowerFirst(base)
		fmt.Fprintf(&consts, "\tmsgID%sReq  = codec.Msg%sReq\n\tmsgID%sResp = codec.Msg%sResp\n",
			base, base, base, base)
		fmt.Fprintf(&handles, "\tm.HandleR(msgID%sReq, msgID%sResp, &gamev1.%s{},\n\t\tmodule.Typed(msgID%sResp, m.on%s))\n",
			base, base, p.resp, base, upperFirst(vn))
		fmt.Fprintf(&onFuncs, `
// on%s %s 的处理器。
func (m *Module) on%s(ctx *module.Context, req *gamev1.%s, resp *gamev1.%s) error {
	_ = req
	*resp = gamev1.%s{Code: codeOK}
	return nil
}
`, upperFirst(vn), p.req, upperFirst(vn), p.req, p.resp, p.resp)
		_ = i
	}

	constBlock := ""
	if consts.Len() > 0 {
		constBlock = "const (\n" + consts.String() + ")\n"
	}

	return fmt.Sprintf(`// Package %[1]s %[1]s 模块。
//
// 由 tools/newmodule 生成：完成业务逻辑后请删除本行。
//
// 架构职责：<一句话说明这个模块管什么>。
// Ext 槽位：%[1]s（随玩家快照 CAS 提交）
// 事件订阅：无
// 配置表：<如有>
package %[1]s

import (
	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
)

const (
	stateKey = %[2]q

	// 业务码：与其它玩法错开号段，客户端按号段定位。
	codeOK     int32 = 0
	codeSystem int32 = 1 // 系统错误（缓存不可用/配置缺失）
)

%[3]s
// Data 本模块的玩家私有状态。
//
// 字段设计要点：只放需要持久化的数据；派生值（等级加成、红点）每次登录重算，
// 不要落库，否则会出现「改公式后旧数据对不上」。
type Data struct {
	// TODO: 业务字段
	Counter int32
}

// Module %[1]s 模块。
type Module struct {
	*module.Stateful[Data]
}

// New 创建模块。
func New(logger *log.Logger) *Module {
	m := &Module{}
	m.Stateful = module.NewStateful(stateKey, newData,
		module.ThinLogger(logger),
		module.ThinErrCode(codeSystem),
		module.ThinGuard(module.RequireCache),
	)
%[4]s	return m
}

// newData 新玩家的空状态。
func newData() *Data { return &Data{} }
%[5]s`, name, name, constBlock, handles.String(), onFuncs.String())
}

func persistTmpl(name, table string) string {
	// 表名 → model 类型（player_mailbox → PlayerMailbox），与 tools/gendb 的
	// 命名规则一致。推导不出来的话生成的代码会编译失败（undefined: model.Xxx），
	// 这个错误比留 <TODO> 更清晰——后者是语法错误，看不出该填什么。
	row := pascal(table)
	return fmt.Sprintf(`// internal/game/module/%[1]s/persist.go —— 持久化适配
//
// 由 tools/newmodule 生成。表结构（%[2]s）与 Data 的列映射需要手工补全：
// 猜错的映射会静默丢数据，所以这里只留骨架。
package %[1]s

import (
	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

func init() {
	writebehind.RegisterStateful(stateKey, model.%[3]sFlat,
		writebehind.StateCodec[model.%[3]s, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// toRow 业务结构 → 表行（%[2]s）。
func toRow(playerID int64, d *Data) (*model.%[3]s, error) {
	return &model.%[3]s{
		// TODO: 列映射（主键一般是 PlayerId: playerID）
	}, nil
}

// fromRow 表行 → 业务结构。
func fromRow(row *model.%[3]s) (*Data, error) {
	return &Data{
		// TODO: 列映射
	}, nil
}

// validate 落库前校验（坏数据整批回滚，不要写进 DB）。
func validate(*Data) error { return nil }
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

func registerHint(name string, pairs []msgPair) string {
	ctor := fmt.Sprintf("s.modules.Register(%s.New(s.logger))", name)
	var b strings.Builder
	fmt.Fprintf(&b, "\n下一步（手工）：\n")
	fmt.Fprintf(&b, "  1) 在 api/proto/game/v1/ 加 message 与 // @msg 注解，然后 make gen\n")
	if len(pairs) > 0 {
		fmt.Fprintf(&b, "  2) 在 internal/game/server_start.go 的 initModules 里注册：\n")
		fmt.Fprintf(&b, "       %s\n", ctor)
	} else {
		fmt.Fprintf(&b, "  2) proto 定好转后，重新跑一次本工具生成 HandleR 注册（或手写）\n")
	}
	fmt.Fprintf(&b, "  3) go build ./... && go test ./internal/game/module/%s/\n", name)
	return b.String()
}

func isLowerSnake(s string) bool {
	for _, r := range s {
		if !(unicode.IsLower(r) || unicode.IsDigit(r) || r == '_') {
			return false
		}
	}
	return s != ""
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToLower(r[0])
	return string(r)
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
}

func fail(msg string) {
	fmt.Fprintf(os.Stderr, "newmodule: %s\n", msg)
	os.Exit(1)
}
