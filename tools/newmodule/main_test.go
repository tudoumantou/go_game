// tools/newmodule/main_test.go —— 生成器自身：解析规则与模板正确性。
package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
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
		require.Contains(t, out, want, fmt.Sprintf("模板缺少片段 %q\n---\n%s", want, out))
	}
	// 不能出现未定义的标识符或占位符
	for _, bad := range []string{"<TODO>", "msgIDMailboxListRespResp"} {
		require.NotContains(t, out, bad, fmt.Sprintf("模板含非法片段 %q", bad))
	}
}

// TestPersistTemplate persist 骨架用推导出的 model 类型（可编译），而非 <TODO>。
func TestPersistTemplate(t *testing.T) {
	out := persistTmpl("mailbox", "player_mailbox")
	for _, want := range []string{"model.PlayerMailboxFlat", "model.PlayerMailbox,", "*model.PlayerMailbox"} {
		require.Contains(t, out, want, fmt.Sprintf("缺少 %q\n%s", want, out))
	}
	require.NotContains(t, out, "<TODO>", "persist 模板不应留 <TODO>（那不是合法 Go）")
}
