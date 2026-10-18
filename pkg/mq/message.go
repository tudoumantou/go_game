// pkg/mq/message.go —— Kafka 统一消息信封
// 约定：所有业务消息共用一层信封，biz_id 为全局幂等键（order_id/mail_id/msg_id/event_id），
// 消费端先按 biz_id 做幂等去重，再解析 payload（见设计文档 13.4）。
package mq

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope 消息信封。
type Envelope struct {
	BizID     string            `json:"biz_id"`              // 全局幂等键（必填）
	Topic     string            `json:"topic"`               // 来源 topic（排查用）
	Key       string            `json:"key,omitempty"`       // 分区键（player_id/order_id）
	Payload   json.RawMessage   `json:"payload"`             // 业务载荷
	Headers   map[string]string `json:"headers,omitempty"`   // 扩展头（trace_id 等）
	CreatedAt int64             `json:"created_at"`          // 生产时间（unix 毫秒）
}

// NewEnvelope 构造信封并序列化 payload。
func NewEnvelope(bizID, topic, key string, payload any) (*Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return &Envelope{
		BizID:     bizID,
		Topic:     topic,
		Key:       key,
		Payload:   raw,
		CreatedAt: time.Now().UnixMilli(),
	}, nil
}

// Marshal 序列化信封为 Kafka value。
func (e *Envelope) Marshal() ([]byte, error) {
	if e.BizID == "" {
		return nil, fmt.Errorf("envelope biz_id is required")
	}
	return json.Marshal(e)
}

// Unmarshal 反序列化 Kafka value。
func Unmarshal(data []byte) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	if e.BizID == "" {
		return nil, fmt.Errorf("envelope biz_id is empty")
	}
	return &e, nil
}

// DecodePayload 把 payload 解析到目标结构。
func (e *Envelope) DecodePayload(v any) error {
	if len(e.Payload) == 0 {
		return fmt.Errorf("empty payload")
	}
	return json.Unmarshal(e.Payload, v)
}

// Payload 业务载荷定义（Kafka 内部契约，非对外 proto）。

// RechargeOrderMsg 充值订单（gs.recharge.order）
type RechargeOrderMsg struct {
	OrderID   string `json:"order_id"`
	PlayerID  int64  `json:"player_id"`
	ProductID int32  `json:"product_id"`
	AmountFen int32  `json:"amount_fen"`
	Channel   int32  `json:"channel"`
}

// RechargeDeliveredMsg 发货确认（gs.recharge.delivered）
type RechargeDeliveredMsg struct {
	OrderID   string `json:"order_id"`
	PlayerID  int64  `json:"player_id"`
	DeliveredAt int64 `json:"delivered_at"` // unix 毫秒
}

// MailMsg 邮件投递（gs.mail）
type MailMsg struct {
	MailID     int64  `json:"mail_id"`
	PlayerID   int64  `json:"player_id"`
	TemplateID int32  `json:"template_id"`
	Title      string `json:"title"`
	Content    string `json:"content"`
	Channel    int32  `json:"channel"`
	Items      []Item `json:"items,omitempty"` // 附件
}

// Item 道具（邮件附件/充值奖励通用）
type Item struct {
	ItemID int32 `json:"item_id"`
	Count  int64 `json:"count"`
}

// ChatCrossMsg 跨服私聊（gs.chat.cross）
type ChatCrossMsg struct {
	MsgID    int64  `json:"msg_id"`
	FromID   int64  `json:"from_id"`
	ToID     int64  `json:"to_id"`
	Channel  int32  `json:"channel"`
	Content  string `json:"content"`
	SendTime int64  `json:"send_time"`
}
