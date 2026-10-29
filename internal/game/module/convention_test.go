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

// ── 状态读写入口的唯一性 ─────────────────────────────────────────────
//
// Repository（repository.go）把「读 → 改 → 写 → 标脏」收成一个调用。
// 但抽象能不能立住，取决于有没有人绕过它——只要有一个模块还在
// Get + 手工 Save，下一个人就会照着抄。所以用静态检查把它变成 CI 卡点。
//
// 检查三类调用（都是 x.Y.Method() 形状，Y 是 Repo 或 State）：
//
//	Repo.Get / Repo.Save   已迁移模块里禁止（漏写 Save = 静默丢数据）
//	State.ClaimBiz         全仓禁止（幂等键必须与状态同一次提交）
//	State.CanHold          全仓禁止（容量预检必须与入包同一次调用）
//	State.MarkDirty        仅允许没有 Ext 槽位的模块（它们没有 Repository）

// stateCallRule 一条调用约束。
type stateCallRule struct {
	recv   string // 接收者最后一段：Repo / State
	method string // 方法名
	desc   string // 违规时打印的理由
}

var stateCallRules = []stateCallRule{
	{"Repo", "Get", "读状态用 c.Repo.View，改状态用 c.Repo.Mutate——Get+Save 的写法漏一次 Save 就是一次静默丢数据"},
	{"Repo", "Save", "改状态用 c.Repo.Mutate（changed=true 才写回），不要再手工 Save"},
	{"State", "ClaimBiz", "幂等键只走 c.Repo.Once(key, fn)：手工占键容易漏标脏，「只发奖」的请求会丢键"},
	{"State", "CanHold", "容量预检只走 module.Grant / module.PrecheckGrant：自己调容易漏掉 int32 溢出检查"},
	{"State", "MarkDirty", "有 Ext 槽位的模块用 c.Repo.Touch()，它会跳过空槽位，不为没玩过的玩家写空状态"},
}

// TestModulesUseRepository 守住「状态读写只有一个入口」。
//
// 全部模块都已完成迁移（v3.16），所以规则是全量生效、没有白名单：
// 新模块从第一天起就得走 Repository，不存在「以后再说」的存量。
func TestModulesUseRepository(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	scanned := 0
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "moduletest" {
			continue // moduletest 是测试桩，不是业务模块
		}
		mod := e.Name()
		files, err := filepath.Glob(filepath.Join(mod, "*.go"))
		require.NoError(t, err)
		// 模块形态决定 MarkDirty 是否合法：没有 Ext 槽位的模块没有 Repository
		// 可挂，只能 MarkDirty 提示一次常规 flush。形态从源码推导，不靠手工
		// 维护白名单——白名单一旦漏写，规则就会对新模块静默失效。
		hasState := moduleHasExtState(files)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue // 测试要给 State 写实现，天然会出现这些方法名
			}
			scanned++
			checkStateCalls(t, file, mod, hasState)
		}
	}
	require.Greater(t, scanned, 10, "没有扫到模块源码，检查逻辑本身失效了")
}

// moduleHasExtState 判断模块是否持有 Ext 状态：存在 Define[非 NoState] 即为是。
//
// 手写基座（chat/mail/friend/guild/team/scene）没有 Define，返回 false——
// 它们的状态在独立表或玩家快照的固定字段里，确实没有 Repository 可挂。
func moduleHasExtState(files []string) bool {
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			continue // 解析失败交给 checkStateCalls 报错
		}
		has := false
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			idx, ok := call.Fun.(*ast.IndexExpr)
			if !ok || !strings.HasSuffix(exprString(idx.X), "Define") {
				return true
			}
			if !strings.Contains(exprString(idx.Index), "NoState") {
				has = true
			}
			return true
		})
		if has {
			return true
		}
	}
	return false
}

// checkStateCalls 检查一个源文件里的状态读写调用是否走了唯一入口。
func checkStateCalls(t *testing.T, path, mod string, hasState bool) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		recv, method := callRecvAndMethod(call)
		if recv == "" {
			return true
		}
		for _, rule := range stateCallRules {
			if rule.recv != recv || rule.method != method {
				continue
			}
			if method == "MarkDirty" && !hasState {
				continue // 没有 Ext 槽位的模块只能这么写
			}
			pos := fset.Position(call.Pos())
			t.Errorf("%s: %s 模块直接调了 %s.%s —— %s", pos, mod, recv, method, rule.desc)
		}
		return true
	})
}

// callRecvAndMethod 从 x.Y.Method() 里取出 (Y, Method)。
//
// 只认「两级选择器 + 调用」这一种形状：字段名 Y 是 Repo 或 State。
// 其他形状（包级函数 module.Grant(...)、单级 a.B(...)）返回空，不参与检查。
func callRecvAndMethod(call *ast.CallExpr) (recv, method string) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", ""
	}
	owner, ok := sel.X.(*ast.SelectorExpr)
	if !ok {
		return "", ""
	}
	return owner.Sel.Name, sel.Sel.Name
}
