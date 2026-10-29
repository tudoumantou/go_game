// pkg/codec/registry.go —— 消息处理器注册表（把反序列化后的消息分发给业务 Handler）
//
// 与 protobuf.go 的 MessageRegistry 成对：前者解决「MsgID → 消息对象」，
// 本文件解决「MsgID → 处理函数」。两者都在启动期由各业务包 init 填满，
// 运行期只读无锁；「有工厂无 handler」属于半接入状态，可用 Has 在启动
// 自检中发现（消息能解没人处理）。
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
//
// Go 细节：context 是 Go 的取消/超时传播机制，沿调用链向下传递；
// handler 内调用 Redis/MySQL/RPC 时必须把 ctx 传到底，取消信号才能
// 一路传到数据库驱动，避免网关断开后下游还在空跑。
type Handler func(ctx context.Context, msg proto.Message) error

// HandlerRegistry MsgID → Handler 映射。
//
// 并发约定与 MessageRegistry 相同：启动期注册、运行期只读、无锁。
// 一张表只应对应一个 handler（一号一义），重复注册会 panic。
type HandlerRegistry struct {
	// handlers MsgID 到处理函数的映射；启动期写满后只读。
	handlers map[uint32]Handler
}

// NewHandlerRegistry 创建处理器注册表。
//
// 返回空表；注册点通常在各业务模块的 init（或显式 Setup 函数）中，
// 保证进程开始 accept 连接前表已完备。
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[uint32]Handler)}
}

// Register 注册处理器。
//
// 重复注册直接 panic（启动期两个模块抢同一 MsgID 是配置错误）。
// 仅限启动阶段单 goroutine 调用；运行期只走 Handle。
//
// Go 细节：panic 信息带上 msgID，运维可从崩溃日志直接定位冲突双方；
// 与 MessageRegistry.Register 的 panic 语义一致（fail-fast 三件套的
// 第三处，前两处是消息工厂与 writebehind 注册表）。
func (r *HandlerRegistry) Register(msgID uint32, h Handler) {
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
//
// Go 细节：同步调用（而非 go h(...)）保证单连接内消息顺序处理，
// 也天然形成背压——慢 handler 拖慢后续消息，而不是无限堆积 goroutine。
func (r *HandlerRegistry) Handle(ctx context.Context, msgID uint32, msg proto.Message) error {
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
func (r *HandlerRegistry) Has(msgID uint32) bool {
	_, ok := r.handlers[msgID]
	return ok
}
