// pkg/codec/registry.go —— 消息处理器注册表（把反序列化后的消息分发给业务 Handler）
package codec

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Handler 消息处理器。
//
// 约定：ctx 携带超时与 trace，handler 内不应无限阻塞（网关按连接维度
// 有总处理时限）；返回的 error 由调用方决定「回错误包」还是「断开连接」，
// handler 本身不负责写响应。
type Handler func(ctx context.Context, msg proto.Message) error

// HandlerRegistry MsgID → Handler 映射。
//
// 并发约定与 MessageRegistry 相同：启动期注册、运行期只读、无锁。
// 一张表只应对应一个 handler（一号一义），重复注册会 panic。
type HandlerRegistry struct {
	handlers map[uint16]Handler
}

// NewHandlerRegistry 创建处理器注册表。
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[uint16]Handler)}
}

// Register 注册处理器。
//
// 重复注册直接 panic（启动期两个模块抢同一 MsgID 是配置错误）。
// 仅限启动阶段单 goroutine 调用；运行期只走 Handle。
func (r *HandlerRegistry) Register(msgID uint16, h Handler) {
	if _, ok := r.handlers[msgID]; ok {
		panic(fmt.Sprintf("handler for msgID %d already registered", msgID))
	}
	r.handlers[msgID] = h
}

// Handle 分发消息。
//
// 按 MsgID 查找 handler 并同步调用；msg 应为该 MsgID 对应的 proto 类型
// （由 MessageRegistry.Unmarshal 产出），类型不符时行为由具体 handler 决定。
// 错误语义：未注册返回 error 而**不 panic**——运行期遇到未知消息是
// 客户端行为（版本不一致/伪造包），不是进程级配置错误；
// handler 自身返回的 error 原样透出。
func (r *HandlerRegistry) Handle(ctx context.Context, msgID uint16, msg proto.Message) error {
	h, ok := r.handlers[msgID]
	if !ok {
		return fmt.Errorf("handler for msgID %d not found", msgID)
	}
	return h(ctx, msg)
}

// Has 返回是否注册了处理器。
//
// 供启动期自检（协议号有工厂但无 handler = 消息能解没人处理）与
// 运行期路由决策；只读、可并发。
func (r *HandlerRegistry) Has(msgID uint16) bool {
	_, ok := r.handlers[msgID]
	return ok
}
