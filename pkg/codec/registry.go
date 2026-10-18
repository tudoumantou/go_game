// pkg/codec/registry.go —— 消息处理器注册表（把反序列化后的消息分发给业务 Handler）
package codec

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// Handler 消息处理器。
type Handler func(ctx context.Context, msg proto.Message) error

// HandlerRegistry MsgID → Handler 映射。
type HandlerRegistry struct {
	handlers map[uint16]Handler
}

// NewHandlerRegistry 创建处理器注册表。
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[uint16]Handler)}
}

// Register 注册处理器。
func (r *HandlerRegistry) Register(msgID uint16, h Handler) {
	if _, ok := r.handlers[msgID]; ok {
		panic(fmt.Sprintf("handler for msgID %d already registered", msgID))
	}
	r.handlers[msgID] = h
}

// Handle 分发消息。
func (r *HandlerRegistry) Handle(ctx context.Context, msgID uint16, msg proto.Message) error {
	h, ok := r.handlers[msgID]
	if !ok {
		return fmt.Errorf("handler for msgID %d not found", msgID)
	}
	return h(ctx, msg)
}

// Has 返回是否注册了处理器。
func (r *HandlerRegistry) Has(msgID uint16) bool {
	_, ok := r.handlers[msgID]
	return ok
}
