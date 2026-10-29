// internal/game/module/route.go —— 统一的协议注册入口
//
// 解决什么问题：注册一条「一问一答」路由，项目里曾有三个函数——
//
//	module.Handle(m.Thin, m.onList)        // 只吃 *Thin
//	module.HandlePlain(m.Plain, m.onList)  // 只吃 *Plain
//	M.HandleRoute(&gamev1.XxxReq{}, onX)   // 只吃 *Bound
//
// 功能等价、写法不同，于是打开 bag 学一套、打开 rank 又得学一套。
// 根因是三个基座各有一个未导出的注册方法，而导出函数把它们写死了。
//
// 现在把「能注册」抽象成 registrar：模块基座实现它，注册函数只依赖它。
// 于是同一个 module.Handle 对三种基座都成立，模块写法收敛成两类：
//
//	module.Handle(base, m.onX)          // 一问一答：解包 + 自动回包
//	module.Route(b, &Req{}, m.onY)       // 自行回包（一个请求回多种响应）
//	b.HandleRoute(&Req{}, m.onZ)         // 无请求体（查询/领取这类）
//
// 为什么不把这三个也做成基座的方法：Go 的方法不能有自己的类型参数，
// 「请求/响应类型随协议而变」只能靠包级泛型函数从入参推断。
package module

import "google.golang.org/protobuf/proto"

// registrar 模块基座的注册能力。
//
// 接口未导出、方法也未导出：它只用于让本包的注册函数对多种基座通用，
// 不打算让外部实现（外部也不该有第四套基座）。
type registrar interface {
	// addRoute 登记一条已解包的路由。
	// spec.prototype 为 nil 表示不声明响应协议（错误路径不回包）。
	addRoute(reqID uint32, spec routeSpec)
}

// routeSpec 一条路由的完整声明：三种注册函数最终都归到这里。
type routeSpec struct {
	// respID 响应协议号（prototype 为 nil 时无意义）。
	respID uint32
	// prototype 零值响应原型，用于构造错误回包；nil 表示不声明。
	prototype proto.Message
	// h 已经包好解包/回包的处理器。
	h func(ctx *Context, msgID uint32, body []byte) error
}

// addRoute 实现 registrar：转调 Thin 自己的注册方法。
func (t *Thin) addRoute(reqID uint32, spec routeSpec) {
	if spec.prototype == nil {
		t.Handle(reqID, spec.h)
		return
	}
	t.HandleR(reqID, spec.respID, spec.prototype, spec.h)
}
