// internal/game/module/convention_test.go —— 约定检查（静态，零运行时成本）
//
// 用一个测试守住一条容易被忽略的约定：**用了 module.Typed 的 handler
// 不能再自己回包**。
//
// 为什么这条约定需要强制：Typed 在 fn 返回 nil 后会自动 reply。如果 handler
// 里又调了一次 reply，同一个请求会发出两条响应。客户端读走第一条，多出来的
// 那条留在连接里，被**下一次请求**当作自己的响应读走——表现为「每个响应都
// 晚一拍」，业务错误码（5002/7104/4005）全部被前一条残留的 code=0 顶掉。
//
// 这个 bug 单元测试抓不到（单测直接调 handler，不经过 Typed），集成测试也只能
// 间接暴露（现象是「幂等失效」，看起来像业务逻辑错）。所以放在这里做静态检查。
package module_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTypedHandlersDoNotReplyManually(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	require.NotEmpty(t, entries, "测试工作目录不对：应该在被测包目录下")

	scanned := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		files, err := filepath.Glob(filepath.Join(e.Name(), "*.go"))
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			scanned++
			checkFile(t, file)
		}
	}
	// 防止「目录扫空了所以永远通过」：必须真的扫到业务模块。
	require.Greater(t, scanned, 10, "没有扫到模块源码，检查逻辑本身失效了")
}

// checkFile 解析一个源文件：找出 module.Typed 绑定的 handler 名，
// 再检查这些 handler 的函数体里有没有回包调用。
func checkFile(t *testing.T, path string) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	// 1) 收集 Typed 绑定：handler 名 → 响应号常量名。
	//    module.Typed(MsgXxxResp, m.onXxx)
	typed := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Typed" || len(call.Args) != 2 {
			return true
		}
		m, ok := call.Args[1].(*ast.SelectorExpr)
		if !ok {
			return true
		}
		typed[m.Sel.Name] = exprString(call.Args[0])
		return true
	})
	if len(typed) == 0 {
		return
	}

	// 2) 检查这些 handler 的函数体。
	ast.Inspect(f, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil {
			return true
		}
		respID, isTyped := typed[fn.Name.Name]
		if !isTyped {
			return true
		}
		ast.Inspect(fn.Body, func(inner ast.Node) bool {
			call, ok := inner.(*ast.CallExpr)
			if !ok {
				return true
			}
			if !isResponseReply(call, respID) {
				return true
			}
			pos := fset.Position(call.Pos())
			t.Errorf("%s: handler %s 用了 module.Typed 却又回了 %s，"+
				"客户端会收到两条响应，多出来的那条会让下一次请求读错包",
				pos, fn.Name.Name, respID)
			return true
		})
		return true
	})
}

// isResponseReply 判断一次调用是不是「回本请求的响应」。
//
// 只认响应，不认推送：模块里 ctx.Reply(msgIDBagChange, ...) 这类通知是合法的，
// 判定依据是发的协议号是否等于该 handler 的响应号。
//
// 形如 m.replyTake(ctx, resp) 的薄封装无从得知协议号，但它是「只传 resp 的
// reply 辅助函数」的固定形态，按违规处理——这正是 mail/bag 出过问题的写法。
func isResponseReply(call *ast.CallExpr, respID string) bool {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		if fn.Name != "reply" {
			return false
		}
		return len(call.Args) >= 2 && exprString(call.Args[1]) == respID
	case *ast.SelectorExpr:
		switch {
		case fn.Sel.Name == "Reply":
			return len(call.Args) >= 1 && exprString(call.Args[0]) == respID
		case fn.Sel.Name == "reply":
			return len(call.Args) >= 2 && exprString(call.Args[1]) == respID
		case strings.HasPrefix(fn.Sel.Name, "reply") && len(call.Args) == 2:
			// m.replyXxx(ctx, resp)：封装回包，看不到协议号，按违规算。
			return true
		}
	}
	return false
}

// exprString 把调用表达式还原成 ctx.Reply / m.replyXxx 这样的字符串。
func exprString(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.SelectorExpr:
		if x, ok := v.X.(*ast.Ident); ok {
			return x.Name + "." + v.Sel.Name
		}
		return v.Sel.Name
	case *ast.Ident:
		return v.Name
	default:
		return ""
	}
}
