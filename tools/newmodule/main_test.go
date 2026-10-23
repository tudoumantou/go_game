// tools/newmodule/main_test.go —— 生成器自身：解析规则与模板正确性。
package main

import (
	"strings"
	"testing"
)

// TestParseMsgs 协议对解析：显式响应、按约定推导、非法输入。
func TestParseMsgs(t *testing.T) {
	got, err := parseMsgs("MailboxListReq:MailboxListResp, MailboxClaimReq")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0].req != "MailboxListReq" || got[0].resp != "MailboxListResp" {
		t.Fatalf("[0] = %+v", got[0])
	}
	// 省略响应时按 XxxReq → XxxResp 推导
	if got[1].resp != "MailboxClaimResp" {
		t.Fatalf("[1].resp = %q", got[1].resp)
	}

	// 既不给响应又不以 Req 结尾：无法推导，必须报错而不是猜
	if _, err := parseMsgs("SomethingWeird"); err == nil {
		t.Fatal("无法推导的协议应报错")
	}

	// 空串返回空列表（只生成骨架、不生成协议）
	if got, err := parseMsgs(""); err != nil || got != nil {
		t.Fatalf("空输入应返回 nil,nil: %v %v", got, err)
	}
}

// TestNamingHelpers 命名规则：模块名 snake_case、表名转 PascalCase。
func TestNamingHelpers(t *testing.T) {
	cases := map[string]bool{"mailbox": true, "probe_mod": true, "a1": true, "Mailbox": false, "a-b": false, "": false}
	for in, want := range cases {
		if got := isLowerSnake(in); got != want {
			t.Fatalf("isLowerSnake(%q) = %v", in, got)
		}
	}
	// 与 tools/gendb 的表→类型规则一致
	for in, want := range map[string]string{"player_mailbox": "PlayerMailbox", "guild": "Guild", "a_b_c": "ABC"} {
		if got := pascal(in); got != want {
			t.Fatalf("pascal(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestModuleTemplate 模板必须生成「能编译的骨架」而不是留占位符。
//
// 这条断言的价值：脚手架生成的代码若不可编译，使用者第一件事就是修生成器，
// 反而比手写更慢。
func TestModuleTemplate(t *testing.T) {
	out := moduleTmpl("mailbox", []msgPair{
		{req: "MailboxListReq", resp: "MailboxListResp"},
		{req: "MailboxClaimReq", resp: "MailboxClaimResp"},
	})

	for _, want := range []string{
		`stateKey = "mailbox"`,
		"codeSystem int32 = 1",
		"msgIDMailboxListReq  = codec.MsgMailboxListReq",
		"module.Typed(msgIDMailboxListResp, m.onMailboxList)",
		"module.Typed(msgIDMailboxClaimResp, m.onMailboxClaim)",
		"func (m *Module) onMailboxList(ctx *module.Context",
		"*module.Stateful[Data]",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("模板缺少片段 %q\n---\n%s", want, out)
		}
	}
	// 不能出现未定义的标识符或占位符
	for _, bad := range []string{"<TODO>", "msgIDMailboxListRespResp"} {
		if strings.Contains(out, bad) {
			t.Fatalf("模板含非法片段 %q", bad)
		}
	}
}

// TestPersistTemplate persist 骨架用推导出的 model 类型（可编译），而非 <TODO>。
func TestPersistTemplate(t *testing.T) {
	out := persistTmpl("mailbox", "player_mailbox")
	for _, want := range []string{"model.PlayerMailboxFlat", "model.PlayerMailbox,", "*model.PlayerMailbox"} {
		if !strings.Contains(out, want) {
			t.Fatalf("缺少 %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "<TODO>") {
		t.Fatal("persist 模板不应留 <TODO>（那不是合法 Go）")
	}
}
