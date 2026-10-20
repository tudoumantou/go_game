// pkg/codec/protobuf.go —— MsgID 与 proto.Message 的注册/反序列化
package codec

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// MessageFactory 根据 MsgID 创建空 proto.Message。
//
// 约定：每次调用必须返回**全新的**空消息实例（例如 &gatev1.HandshakeReq{}），
// 不能返回包级单例——Unmarshal 会把 body 解析进这个实例，共享实例会让
// 并发/重入路径的消息内容互相串味。
type MessageFactory func() proto.Message

// MessageRegistry MsgID → factory 映射。
//
// 并发约定：Register 只允许在进程启动阶段（init / 模块加载）调用，
// 运行期只读（Create/Has 走纯 map 读），因此实现未加锁。
// **不要在运行期动态 Register/改注册表**——那需要加锁且会与在途请求竞争。
type MessageRegistry struct {
	factories map[uint16]MessageFactory
}

// NewMessageRegistry 创建注册表。
//
// 返回空表（无任何预注册消息）；调用方按模块逐个 Register，
// 通常在 net/网关的初始化代码里集中完成。
func NewMessageRegistry() *MessageRegistry {
	return &MessageRegistry{factories: make(map[uint16]MessageFactory)}
}

// Register 注册一个消息工厂。
//
// 重复注册同一 MsgID 直接 panic：这是启动期配置错误（两个模块抢同一个号），
// 应当立即炸掉而不是运行期静默覆盖——后者会让消息路由到错误的 handler，
// 且只在特定消息上偶发，极难排查。
// 调用约束：仅限启动阶段单 goroutine 调用（见 MessageRegistry 并发约定）。
func (r *MessageRegistry) Register(msgID uint16, factory MessageFactory) {
	if _, ok := r.factories[msgID]; ok {
		panic(fmt.Sprintf("msgID %d already registered", msgID))
	}
	r.factories[msgID] = factory
}

// MustRegister 注册，失败 panic。
//
// 与 Register 行为完全一致（Register 本身就 panic），保留它是为了
// 声明式/生成代码调用点的语义自描述：调用处即声明「此号必须可用」。
func (r *MessageRegistry) MustRegister(msgID uint16, factory MessageFactory) {
	r.Register(msgID, factory)
}

// Create 根据 MsgID 创建空消息。
//
// 返回的是**未反序列化**的空实例（只分配、不填内容），供调用方
// 先创建再 proto.Unmarshal；一步到位的场景用 Unmarshal。
// 错误语义：MsgID 未注册返回 error（而不是 nil,nil）——调用方必须
// 区分「没有这个消息」与「消息为空」两种情况。
func (r *MessageRegistry) Create(msgID uint16) (proto.Message, error) {
	f, ok := r.factories[msgID]
	if !ok {
		return nil, fmt.Errorf("msgID %d not registered", msgID)
	}
	return f(), nil
}

// Unmarshal 根据 MsgID 反序列化 body。
//
// body 必须是已解密的**明文**（proto marshal 产物）——外层密文的解密由
// pkg/crypto 的 SessionCipher 负责，本层不接触密文。
// 错误语义：MsgID 未注册、或 proto 解析失败（含字段损坏/版本不兼容）
// 均返回 error；错误信息带 MsgID，便于在日志里定位是哪个消息出的问题。
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
//
// 供网关在收到未知 MsgID 时做决策：Has 为 false 时回错误包或直接断开，
// 而不是走到 Create 才报错。只读、无锁、可并发调用。
func (r *MessageRegistry) Has(msgID uint16) bool {
	_, ok := r.factories[msgID]
	return ok
}

// Marshal proto 消息序列化。
//
// 纯转发 proto.Marshal；产物交给上层加密（SessionCipher.Encrypt），
// 本层不保证确定性编码（proto 不保证 map 字段顺序稳定）。
func Marshal(m proto.Message) ([]byte, error) {
	return proto.Marshal(m)
}
