// internal/game/module/convention_test.go —— 约定检查（静态，零运行时成本）
//
// 处理器签名只有一种：func(*module.Ctx[D], req) error，回包由处理器自己做。
// 这里用两个静态检查守住它：
//
//  1. 所有 onXxx 处理器的第一个参数必须是 *module.Ctx[...]；
//  2. 模块里不许出现 m.Handle / m.HandleR 直接注册 ThinHandler。
//
// 为什么需要 (2)：Thin.Handle 收的是 func(*Context, msgID, body)，签名里没有
// 类型化的 req，注册处要自己写 msgID 常量、自己 Unmarshal——这正是 v3.25
// 消灭的那套形态（scene/battle 曾这么写）。编译期拦不住它：Thin 是合法 API，
// 只是业务模块不该用。所以用 AST 卡点。
//
// 为什么需要 (1)：签名统一之后，回包语义也统一了（错误分支必须回包，否则
// 客户端等到超时）。如果哪天有人加了一个 func(*Context, req, resp) 的处理器
// 并用 Thin.HandleR 挂上，签名检查会先炸。
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

// TestHandlersUseCtxSignature 处理器的第一个参数必须是 *module.Ctx[D]。
//
// 这条检查覆盖路由处理器与生命周期钩子（onLogin/onLogout/onEvent）：
// 它们都拿同一个上下文类型，也都可能改状态、回包。
func TestHandlersUseCtxSignature(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	scanned := 0
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "moduletest" {
			continue
		}
		files, err := filepath.Glob(filepath.Join(e.Name(), "*.go"))
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			scanned += checkHandlerSignature(t, file)
		}
	}
	require.Greater(t, scanned, 20, "没有扫到处理器，检查逻辑本身失效了")
}

// checkHandlerSignature 检查一个文件里的 onXxx 方法，返回检查了几个。
//
// 判据：接收者方法、名字以 on 开头、返回值恰好是一个 error。
// 这条判据会覆盖所有处理器与钩子，又不会误伤 replyEnter 这类辅助函数
// （它们返回 error，但名字不是 on 前缀）。
func checkHandlerSignature(t *testing.T, path string) int {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	n := 0
	ast.Inspect(f, func(node ast.Node) bool {
		fn, ok := node.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || fn.Body == nil {
			return true
		}
		if !strings.HasPrefix(fn.Name.Name, "on") || !returnsOnlyError(fn.Type) {
			return true
		}
		n++
		if fn.Type.Params == nil || len(fn.Type.Params.List) == 0 {
			t.Errorf("%s: %s 没有参数——处理器必须收 *module.Ctx[D]",
				fset.Position(fn.Pos()), fn.Name.Name)
			return true
		}
		if !isCtxPtr(fn.Type.Params.List[0].Type) {
			t.Errorf("%s: %s 的第一个参数不是 *module.Ctx[...]——"+
				"处理器统一用 Ctx[D]（回包自己 Reply），不要再引入 ctx+req+resp 的第二套签名",
				fset.Position(fn.Pos()), fn.Name.Name)
		}
		return true
	})
	return n
}

// returnsOnlyError 返回值是否恰好是一个 error。
func returnsOnlyError(ft *ast.FuncType) bool {
	if ft.Results == nil || len(ft.Results.List) != 1 {
		return false
	}
	id, ok := ft.Results.List[0].Type.(*ast.Ident)
	return ok && id.Name == "error"
}

// isCtxPtr 类型是否为 *module.Ctx[...]（泛型实例化后的指针）。
func isCtxPtr(e ast.Expr) bool {
	star, ok := e.(*ast.StarExpr)
	if !ok {
		return false
	}
	idx, ok := star.X.(*ast.IndexExpr)
	if !ok {
		return false
	}
	sel, ok := idx.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "module" && sel.Sel.Name == "Ctx"
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

// TestModulesDoNotRegisterThinHandler 模块不许绕过 Route/HandleRoute 直接挂原始字节处理器。
//
// Thin.Handle 收 func(*Context, msgID, body) 并要注册处自写 msgID 常量：
// 协议号写错不会报错，只会「这条协议永远收不到消息」。Route 用类型反查，
// 把这类错移到编译期/启动期。
func TestModulesDoNotRegisterThinHandler(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	scanned := 0
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "moduletest" {
			continue
		}
		files, err := filepath.Glob(filepath.Join(e.Name(), "*.go"))
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			scanned++
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, file, nil, 0)
			require.NoErrorf(t, err, "parse %s", file)
			ast.Inspect(f, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleR" {
					return true
				}
				// 只认「基座自己的方法」：m.Handle(...) / th.Handle(...) /
				// m.Thin.Handle(...)。m.repo.Handle(...) 这类仓储方法放过——
				// 判据是接收者形态，不是方法名（friend 的 repo 恰好也有 Handle）。
				switch recv := sel.X.(type) {
				case *ast.Ident: // m.Handle / th.Handle
				case *ast.SelectorExpr:
					if recv.Sel.Name != "Thin" {
						return true
					}
				default:
					return true
				}
				t.Errorf("%s: 直接用了 %s.%s 注册处理器——业务模块一律 "+
					"module.Route(b, &Req{}, onX) 或 b.HandleRoute(&Req{}, onX)，"+
					"处理器签名统一为 func(*module.Ctx[D], req) error",
					fset.Position(call.Pos()), exprString(sel.X), sel.Sel.Name)
				return true
			})
		}
	}
	require.Greater(t, scanned, 20, "没有扫到模块源码，检查逻辑本身失效了")
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
	{"State", "Ext", "Ext 槽位是基座的实现细节：读用 c.Repo.View，写用 c.Repo.Mutate，清空用 c.Repo.Reset"},
	{"State", "SetExt", "Ext 槽位是基座的实现细节：读用 c.Repo.View，写用 c.Repo.Mutate，清空用 c.Repo.Reset"},
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
// 形态从源码推导而不是维护白名单：白名单漏写一条，MarkDirty 规则对该模块
// 就静默失效了。Declare[NoState] 的模块（chat/mail/friend/guild/team/
// battle/scene/profile/attr/bag/item/rank）返回 false——它们的状态在独立表、
// Redis 或进程内存里，确实没有 Repository 可挂。
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

// ── 基座形态的唯一性 ─────────────────────────────────────────────────
//
// 业务模块只有一个基座入口：Define。D 取状态结构体就是「有 Ext 槽位」，
// 取 NoState 就是「没有」——两者是同一个 Bound，同一套接口管线。
//
// 为什么需要静态检查：NewThin 是基座**内核**，业务直接用它等于绕过
// Define 的时钟注入、链式装配与 NoState 的类型表达，然后又多出一种
// 「打开这个模块要学另一套写法」的形态。历史上正是这样长出过四种写法。
//
// 允许的例外只有基座自己的测试（thin_test.go / route_test.go，它们测的
// 就是 Thin 本身），模块子目录里一律不许出现。
func TestModulesDeclareWithDefine(t *testing.T) {
	banned := map[string]string{
		"NewThin":     "业务模块用 module.Define[D]（无 Ext 槽位就 D 取 module.NoState），不要直接 NewThin",
		"ThinLogger":  "日志器走 Define(...).WithLogger(l)",
		"ThinLogin":   "上线钩子走 Define(...).WithLogin(fn)",
		"ThinLogout":  "下线钩子走 Define(...).WithLogout(fn)",
		"ThinGuard":   "前置校验走 Define(...).Guard(fn)",
		"ThinOnEvent": "事件钩子走 Define(...).WithEvent(fn)",
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	scanned := 0
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "moduletest" {
			continue
		}
		files, err := filepath.Glob(filepath.Join(e.Name(), "*.go"))
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			scanned++
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, file, nil, 0)
			require.NoErrorf(t, err, "parse %s", file)
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				hint, bad := banned[sel.Sel.Name]
				if !bad {
					return true
				}
				t.Errorf("%s: 直接用了 module.%s —— %s",
					fset.Position(call.Pos()), sel.Sel.Name, hint)
				return true
			})
		}
	}
	require.Greater(t, scanned, 10, "没有扫到模块源码，检查逻辑本身失效了")
}

// TestModulesDoNotComputeDayIndex 禁止模块自己算「今天是第几天」。
//
// 指纹是 86400（一天的秒数）：它出现在业务代码里，就意味着这里有一份
// 本地的日序号实现。v3.21 之前有三份——商店、登录礼包、活动，
// 前两份是 (Unix+8h)/86400，活动那份漏了偏移、按 UTC 日界切，
// 表现为「限购在北京 0 点重置，活动实例要等到北京 8 点才换」。
//
// 为什么用 AST 而不是文本匹配：解释这条规则的注释里就会写
// 「不要用 now/86400」，文本匹配会把解释本身判成违规。
func TestModulesDoNotComputeDayIndex(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

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
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, file, nil, 0)
			require.NoErrorf(t, err, "parse %s", file)
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.INT || lit.Value != "86400" {
					return true
				}
				t.Errorf("%s: 自己算了日序号（86400）—— 跨天判定一律用 "+
					"timeutil.BusinessDay / BusinessDayUnix，否则北京日界与 UTC 日界会分叉",
					fset.Position(lit.Pos()))
				return true
			})
		}
	}
	require.Greater(t, scanned, 10, "没有扫到模块源码，检查逻辑本身失效了")
}

// TestModulesDefineNoStateExplicitly 无状态模块必须写 module.NoState，不许写 struct{}。
//
// 两者类型等价（NoState 是 struct{} 的别名），编译器看不出差别。但模块形态
// 推导（moduleHasExtState）读的是源码里的 "NoState" 字面量：写 struct{} 的模块
// 会被当成「有 Ext 槽位」，于是它失去 MarkDirty 的豁免——明明是 NoState 模块，
// 规则却按有状态模块判。seat 曾这么写，是全仓唯一实例。
//
// 为什么不改 moduleHasExtState 去认 struct{}：认两个字面量只是把脆弱性翻倍。
// 「无状态就写 NoState」这条约定本身是文档与代码的共同语言（repo.go 里
// 有整段解释为什么这个别名存在），统一写法比让判据兼容两种写法更划算。
func TestModulesDefineNoStateExplicitly(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	scanned := 0
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "moduletest" {
			continue
		}
		files, err := filepath.Glob(filepath.Join(e.Name(), "*.go"))
		require.NoError(t, err)
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			scanned++
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, file, nil, 0)
			require.NoErrorf(t, err, "parse %s", file)
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				idx, ok := call.Fun.(*ast.IndexExpr)
				if !ok {
					return true
				}
				if recv := exprString(idx.X); recv != "module.Define" && recv != "Define" {
					return true
				}
				if _, isStruct := idx.Index.(*ast.StructType); isStruct {
					t.Errorf("%s: 用了 Define[struct{}] —— 无状态模块写 module.NoState："+
						"两者类型等价，但形态推导靠这个字面量判断有没有 Ext 槽位",
						fset.Position(call.Pos()))
				}
				return true
			})
		}
	}
	require.Greater(t, scanned, 10, "没有扫到模块源码，检查逻辑本身失效了")
}
