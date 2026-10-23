// internal/game/module/proto.go —— 类型化的协议处理器
//
// 解决什么问题：每个 handler 都以同样三行开头、同样一行结尾——
//
//	var req gamev1.XxxReq
//	if err := proto.Unmarshal(body, &req); err != nil { return fmt.Errorf(...) }
//	resp := &gamev1.XxxResp{}
//	...
//	return reply(ctx, msgIDXxxResp, resp)
//
// 这些与业务无关，而且「忘记 Unmarshal」「回包用错响应号」都是编译期发现不了的错。
// Typed 把这段收进基座，业务只写「拿到 req、填 resp」。
package module

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtoPtr 约束「T 的指针类型且是 proto.Message」。
//
// Go 知识点：不能用 `proto.Message` 直接约束 T——proto 生成的结构体是指针
// 实现该接口，而 new(T) 只能得到结构体。所以用「约束里带 *T」的写法
// （`interface{ *T; proto.Message }`）把 T 固定成结构体类型，
// 这样 new(T) 合法且可断言为 proto.Message。
type ProtoPtr[T any] interface {
	*T
	proto.Message
}

// Typed 把「类型化业务函数」包装成 ThinHandler：自动解包请求、回包响应。
//
// 用法（配合 HandleR，响应号声明一次即可）：
//
//	m.HandleR(msgIDApplyReq, msgIDApplyResp, &gamev1.FriendApplyResp{},
//		module.Typed(msgIDApplyResp, m.apply))
//
//	func (m *Module) apply(ctx *module.Context, req *gamev1.FriendApplyReq, resp *gamev1.FriendApplyResp) error {
//		resp.Code = codeOK
//		return nil        // 返回 nil 即自动回包；非 nil 走错误通道（不回包）
//	}
//
// 与 HandleR 的分工：HandleR 声明「这个请求的响应是哪个协议」（错误回包要用），
// Typed 负责「解包 + 正常回包」。两者可独立使用。
//
// 错误语义：fn 返回 error 表示「不能给出业务响应」（解码失败之后的服务端故障），
// 此时不自动回包，由上层记日志并走错误通道——与手写 `return err` 一致。
func Typed[TReq, TResp any, PReq ProtoPtr[TReq], PResp ProtoPtr[TResp]](
	respID uint32,
	fn func(ctx *Context, req *TReq, resp *TResp) error,
) ThinHandler {
	return func(ctx *Context, msgID uint32, body []byte) error {
		// 每次 new 一个实例：模块是单例，被多个玩家 Actor 并发调用，
		// 复用实例会让两个玩家的请求内容互相串味。
		req := PReq(new(TReq))
		if len(body) > 0 {
			if err := proto.Unmarshal(body, req); err != nil {
				return fmt.Errorf("decode msg %d: %w", msgID, err)
			}
		}
		resp := new(TResp)
		if err := fn(ctx, req, resp); err != nil {
			return err
		}
		raw, err := proto.Marshal(PResp(resp))
		if err != nil {
			return fmt.Errorf("encode msg %d: %w", respID, err)
		}
		return ctx.Reply(respID, raw)
	}
}
