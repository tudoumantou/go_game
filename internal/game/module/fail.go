// internal/game/module/fail.go —— 业务失败的统一回包
//
// 解决什么问题：写一个错误分支原本是两行——设码 + `return nil`：
//
//	if req.MailId <= 0 {
//		resp.Code = int32(errcode.ErrMailNotFound)
//		return nil
//	}
//
// 全仓这类分支有 100 多处，每处都重复「转 int32、记得 return、别忘了 nil」。
// FailCode 把它收成一行，并且语义更明确：这是「业务拒绝」，不是服务端故障。
//
// 与 Context.Fail 的分工：
//   - Fail：服务端真故障，记 Warn 日志、待 error，用于「处理不了」；
//   - FailCode：可预期的业务拒绝（满包/已领取/参数非法），只置码不记日志——
//     把正常业务分支写进告警日志只会淹没真正的故障。
package module

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"game_study/internal/common/errcode"
)

// FailCode 把业务码写进响应并返回 nil（处理器直接 `return c.FailCode(...)`）。
//
// 回包本身由基座完成：Typed/Handle 包装器看到 fn 返回 nil 就序列化 resp 下发。
// 自定义签名的处理器（不走自动回包）需要自己再调一次 Reply。
func (c *Context) FailCode(msg proto.Message, code errcode.Code) error {
	setRespCode(msg, int32(code))
	return nil
}

// FailCode 在 Define 形态（Ctx[D]）里置码**并回包**。
//
// 与 Context.FailCode 的行为差异不是笔误，是两种底座的必然：
//   - Thin 形态（module.Handle）提前绑定了一对 req/resp，处理器返回 nil 时
//     包装器自动下发 resp，所以只需置码，不能再 Reply（否则回两个包）；
//   - Define 形态（module.Route）的处理器没有 resp 形参，基座不知道要回什么，
//     必须由这里显式 Reply——只置码会「静默不回包」，客户端一直等到超时。
//
// 这个区别曾经真的踩过：把 `c.Reply(&Resp{Code: X})` 机械替换成
// `c.FailCode(&Resp{}, X)` 后，商店的每个错误分支都变成不回包。
func (c *Ctx[D]) FailCode(msg proto.Message, code errcode.Code) error {
	setRespCode(msg, int32(code))
	return c.Reply(msg)
}

// setRespCode 按字段名把 code 写进任意响应消息；没有该字段时静默跳过。
//
// 为什么用反射：proto 生成类型只有 `GetCode()` 没有 `SetCode()`，而各模块的
// 响应类型互不相同。按描述符找 `code` 字段是唯一不依赖具体类型的写法；
// 错误分支是低频路径，反射开销可以忽略。
func setRespCode(m proto.Message, code int32) {
	if m == nil {
		return
	}
	rv := m.ProtoReflect()
	fd := rv.Descriptor().Fields().ByName("code")
	if fd == nil || fd.Kind() != protoreflect.Int32Kind {
		return
	}
	rv.Set(fd, protoreflect.ValueOfInt32(code))
}
