// tools/newmodule/main_test.go —— 生成器自身：解析规则与模板正确性。
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseMsgs 协议对解析：显式响应、按约定推导、非法输入。
func TestParseMsgs(t *testing.T) {
	got, err := parseMsgs("MailboxListReq:MailboxListResp, MailboxClaimReq")
	require.NoError(t, err)
	require.Len(t, got, 2, fmt.Sprintf("len = %d", len(got)))
	require.EqualValues(t, "MailboxListReq", got[0].req, fmt.Sprintf("[0] = %+v", got[0]))
	require.EqualValues(t, "MailboxListResp", got[0].resp)
	// 省略响应时按 XxxReq → XxxResp 推导
	require.EqualValues(t, "MailboxClaimResp", got[1].resp, fmt.Sprintf("[1].resp = %q", got[1].resp))

	// 经推导的处理器名：去掉 Req 后缀加 on 前缀
	require.EqualValues(t, "onMailboxList", got[0].handlerName())
	require.EqualValues(t, "onMailboxClaim", got[1].handlerName())

	// 既不给响应又不以 Req 结尾：无法推导，必须报错而不是猜
	_, err = parseMsgs("SomethingWeird")
	require.Error(t, err, "无法推导的协议应报错")

	// 空串返回空列表（只生成骨架、不生成协议）
	got, err = parseMsgs("")
	require.NoError(t, err, fmt.Sprintf("空输入应返回 nil,nil: %v %v", got, err))
	require.Nil(t, got)
}

// TestNamingHelpers 命名规则：模块名 snake_case、表名转 PascalCase。
func TestNamingHelpers(t *testing.T) {
	cases := map[string]bool{"mailbox": true, "probe_mod": true, "a1": true, "Mailbox": false, "a-b": false, "": false}
	for in, want := range cases {
		got := isLowerSnake(in)
		require.EqualValues(t, want, got, fmt.Sprintf("isLowerSnake(%q) = %v", in, got))
	}
	// 与 tools/gendb 的表→类型规则一致
	for in, want := range map[string]string{"player_mailbox": "PlayerMailbox", "guild": "Guild", "a_b_c": "ABC"} {
		got := pascal(in)
		require.EqualValues(t, want, got, fmt.Sprintf("pascal(%q) = %q, want %q", in, got, want))
	}
}

// TestModuleTemplate 装配文件必须用现役写法（Define + module.Route）。
//
// 这条断言守的是一个真实退化：模板曾经生成 NewStateful + HandleR + Typed +
// msgID 常量，而仓库里 8 个模块早已全部迁到 Define + HandleRoute。生成的骨架
// 与现役代码不同形态，使用者第一件事就得先改写法——比手写更慢。
func TestModuleTemplate(t *testing.T) {
	out := moduleTmpl("mailbox", []msgPair{
		{req: "MailboxListReq", resp: "MailboxListResp"},
		{req: "MailboxClaimReq", resp: "MailboxClaimResp"},
	})

	for _, want := range []string{
		`const stateKey = "mailbox"`,
		"module.Define[Data](stateKey, newData)",
		"Guard(module.RequireCache)",
		"module.Route(M, &gamev1.MailboxListReq{}, onMailboxList)",
		"module.Route(M, &gamev1.MailboxClaimReq{}, onMailboxClaim)",
	} {
		require.Contains(t, out, want, fmt.Sprintf("模板缺少片段 %q\n---\n%s", want, out))
	}
	// 旧写法不能再出现（出现即说明模板又退回去了）
	for _, bad := range []string{"NewStateful", "HandleR(", "Typed(", "codec.Msg", "<TODO>"} {
		require.NotContains(t, out, bad, fmt.Sprintf("模板含过时写法 %q", bad))
	}
	requireParses(t, "module.go", out)
}

// TestModuleTemplateNoMsgs 无协议时也要能编译：不 import gamev1、不生成 init。
func TestModuleTemplateNoMsgs(t *testing.T) {
	out := moduleTmpl("mailbox", nil)
	require.NotContains(t, out, "gamev1", "无协议时不应 import gamev1（会 unused import 编译失败）")
	require.NotContains(t, out, "func init()")
	requireParses(t, "module.go", out)
}

// TestTypesTemplate 持久化形态：带表走 @codec + db tag，不带表走 json tag。
func TestTypesTemplate(t *testing.T) {
	withTable := typesTmpl("mailbox", "player_mailbox")
	require.Contains(t, withTable, "@codec table=player_mailbox")
	require.Contains(t, withTable, "`db:\"counter\"`")
	requireParses(t, "types.go", withTable)

	without := typesTmpl("mailbox", "")
	require.NotContains(t, without, "@codec")
	require.Contains(t, without, "`json:\"counter\"`")
	requireParses(t, "types.go", without)
}

// TestHandlerTemplate 处理器必须是可编译骨架，且不手写错误码赋值。
func TestHandlerTemplate(t *testing.T) {
	out := handlerTmpl("mailbox", []msgPair{
		{req: "MailboxListReq", resp: "MailboxListResp"},
		{req: "MailboxClaimReq", resp: "MailboxClaimResp"},
	})
	for _, want := range []string{
		"func onMailboxList(c *module.Ctx[Data], req *gamev1.MailboxListReq) error {",
		"func onMailboxClaim(c *module.Ctx[Data], req *gamev1.MailboxClaimReq) error {",
		"c.Repo.View(func(d *Data) error {",
		"c.Reply(resp)",
		"errcode.ErrSystem",
	} {
		require.Contains(t, out, want, fmt.Sprintf("缺少 %q\n%s", want, out))
	}
	// 新写法：错误分支只写 FailCode / Fail，不手写 Code 赋值
	require.NotContains(t, out, "Code = int32(errcode.")

	// 已删除的 API 不能出现在模板里。
	//
	// 这条守的是一个真实事故：Repository 在 24 个模块迁完后删掉了
	// Get/Save/Delete（只留 View/Mutate/Once/Touch/Reset），模板却还在
	// 生成 c.Repo.Get()——生成出来的模块第一件事是修编译错误。
	//
	// requireParses 拦不住它：那是「类型错误」不是「语法错误」。所以这里
	// 用字符串黑名单兜一层，与 TestModuleTemplate 的黑名单同款。
	for _, dead := range []string{"c.Repo.Get(", "c.Repo.Save(", "c.Repo.Delete("} {
		require.NotContains(t, out, dead,
			fmt.Sprintf("模板引用了已删除的 API %q（Repository 只有 View/Mutate/Once/Touch/Reset）", dead))
	}
	requireParses(t, "handler.go", out)
}

// TestStoreModuleTemplate 独立表形态（无内存态）的装配。
//
// 与 Define 形态的关键差别只有 D：数据不在玩家快照里，取 NoState，
// 模块自己持仓储。基座、路由函数、处理器签名全部相同——这正是
// v3.25 统一签名的目的：两种数据形态不该长出两套写法。
func TestStoreModuleTemplate(t *testing.T) {
	out := storeModuleTmpl("mailbox", []msgPair{
		{req: "MailboxListReq", resp: "MailboxListResp"},
		{req: "MailboxClaimReq", resp: "MailboxClaimResp"},
	})
	for _, want := range []string{
		"type Module struct {",
		"*module.Bound[module.NoState]",
		`module.Define[module.NoState]("mailbox")`,
		"module.Route(m.Bound, &gamev1.MailboxListReq{}, m.onMailboxList)",
		"module.Route(m.Bound, &gamev1.MailboxClaimReq{}, m.onMailboxClaim)",
		"func New(database *db.DB, logger *log.Logger) *Module",
	} {
		require.Contains(t, out, want, fmt.Sprintf("缺少 %q\n%s", want, out))
	}
	// 无内存态就不该有快照守卫；也不该再出现 Thin 直接组装（第二套处理器签名）
	require.NotContains(t, out, "RequireCache", "NoState 形态不参与快照 CAS，不该有 RequireCache")
	for _, dead := range []string{"module.NewThin", "module.Handle(", "*module.Thin"} {
		require.NotContains(t, out, dead, fmt.Sprintf("独立表形态不该再出现 %q（处理器签名已统一）", dead))
	}
	requireParses(t, "module.go", out)
}

// TestStoreHandlerTemplate 独立表形态处理器的签名必须与 Define 形态一致。
func TestStoreHandlerTemplate(t *testing.T) {
	out := storeHandlerTmpl("mailbox", []msgPair{{req: "MailboxListReq", resp: "MailboxListResp"}})
	require.Contains(t, out, "func (m *Module) onMailboxList(c *module.Ctx[module.NoState], req *gamev1.MailboxListReq) error {")
	require.Contains(t, out, "resp := &gamev1.MailboxListResp{}")
	require.Contains(t, out, "return c.Reply(resp)")
	// 旧的「写 resp 字段即回包」写法不能再出现
	require.NotContains(t, out, "*resp = gamev1.")
	requireParses(t, "handler.go", out)
}

// TestStoreTemplate 独立表骨架：驱动只在这一层出现。
func TestStoreTemplate(t *testing.T) {
	out := storeTmpl("mailbox", "mail")
	for _, want := range []string{"type store struct", "func newStore(database *db.DB)", "model.ListMailByPlayer"} {
		require.Contains(t, out, want, fmt.Sprintf("缺少 %q\n%s", want, out))
	}
	requireParses(t, "store.go", out)
}

// requireParses 断言生成物是合法 Go 语法。
//
// 只查语法不查类型：类型检查要能解析 import 的目标包，测试环境里代价太大。
// 但「语法都错」是最常见的模板事故（漏括号、格式化占位符错位），这一层足够拦下。
func requireParses(t *testing.T, name, src string) {
	t.Helper()
	fset := token.NewFileSet()
	_, err := parser.ParseFile(fset, name, src, parser.AllErrors)
	require.NoError(t, err, fmt.Sprintf("%s 不是合法 Go：\n%s", name, src))
}
