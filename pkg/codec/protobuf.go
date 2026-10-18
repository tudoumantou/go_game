// pkg/codec/protobuf.go —— MsgID 与 proto.Message 的注册/反序列化
package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// MessageFactory 根据 MsgID 创建空 proto.Message。
type MessageFactory func() proto.Message

// MessageRegistry MsgID → factory 映射。
type MessageRegistry struct {
	factories map[uint16]MessageFactory
}

// NewMessageRegistry 创建注册表。
func NewMessageRegistry() *MessageRegistry {
	return &MessageRegistry{factories: make(map[uint16]MessageFactory)}
}

// Register 注册一个消息工厂。
func (r *MessageRegistry) Register(msgID uint16, factory MessageFactory) {
	if _, ok := r.factories[msgID]; ok {
		panic(fmt.Sprintf("msgID %d already registered", msgID))
	}
	r.factories[msgID] = factory
}

// MustRegister 注册，失败 panic。
func (r *MessageRegistry) MustRegister(msgID uint16, factory MessageFactory) {
	r.Register(msgID, factory)
}

// Create 根据 MsgID 创建空消息。
func (r *MessageRegistry) Create(msgID uint16) (proto.Message, error) {
	f, ok := r.factories[msgID]
	if !ok {
		return nil, fmt.Errorf("msgID %d not registered", msgID)
	}
	return f(), nil
}

// Unmarshal 根据 MsgID 反序列化 body。
func (r *MessageRegistry) Unmarshal(msgID uint16, body []byte) (proto.Message, error) {
	m, err := r.Create(msgID)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(body, m); err != nil {
		return nil, fmt.Errorf("unmarshal msg %d: %w", msgID, err)
	}
	return m, nil
}

// Has 返回是否注册了该 MsgID。
func (r *MessageRegistry) Has(msgID uint16) bool {
	_, ok := r.factories[msgID]
	return ok
}

// Marshal proto 消息序列化。
func Marshal(m proto.Message) ([]byte, error) {
	return proto.Marshal(m)
}
