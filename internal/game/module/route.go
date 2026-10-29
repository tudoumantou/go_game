// internal/game/module/route.go —— 路由声明的公共结构
//
// 注册入口只有两个，都在 repo.go：
//
//	module.Route(b, &Req{}, onX)   // 带请求体：基座解包，处理器自己 Reply
//	b.HandleRoute(&Req{}, onX)     // 无请求体：语义只由身份与服务端状态决定
//
// 文件里只剩 routeSpec：两种入口与 Bound.addRoute 之间的公共载体。
//
// 历史：这里曾经是一层 registrar 抽象（`type registrar interface{ addRoute }`），
// 目的是让同一个 module.Handle 同时服务 Thin 与 Bound，消掉「Handle/HandlePlain/
// HandleRoute 三个函数各吃一种基座」。v3.25 把处理器签名统一到 Ctx[D] 之后，
// module.Handle 与它的「自动回包」语义一起删掉了，registrar 只剩一个实现，
// 抽象退化成一层间接——一并删除，只留 routeSpec。
package module

import "google.golang.org/protobuf/proto"

// routeSpec 一条路由的完整声明。
type routeSpec struct {
	// respID 响应协议号（prototype 为 nil 时无意义）。
	respID uint32
	// prototype 零值响应原型，用于 Guard 失败时构造错误回包；
	// nil 表示不声明（缓存不可用走网关通用错误）。
	prototype proto.Message
	// h 已经包好解包/回包的处理器。
	h func(ctx *Context, msgID uint32, body []byte) error
}
