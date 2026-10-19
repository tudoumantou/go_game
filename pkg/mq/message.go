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
	BizID     string            `json:"biz_id"`            // 全局幂等键（必填）
	Topic     string            `json:"topic"`             // 来源 topic（排查用）
	Key       string            `json:"key,omitempty"`     // 分区键（player_id/order_id）
	Payload   json.RawMessage   `json:"payload"`           // 业务载荷
	Headers   map[string]string `json:"headers,omitempty"` // 扩展头（trace_id 等）
	CreatedAt int64             `json:"created_at"`        // 生产时间（unix 毫秒）
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
	OrderID     string `json:"order_id"`
	PlayerID    int64  `json:"player_id"`
	DeliveredAt int64  `json:"delivered_at"` // unix 毫秒
}

// LoginLogMsg 登录流水（gs.login.log）
//
// 每条登录尝试一条，消费端直接落 login_log 表，不做业务去重：
// 流水是事实记录，重复消费只会产生重复行，不影响玩家状态。
type LoginLogMsg struct {
	AccountID int64  `json:"account_id"`
	PlayerID  int64  `json:"player_id"` // 未创角为 0
	IP        string `json:"ip"`
	Device    string `json:"device"`
	Result    int32  `json:"result"` // 0 成功 1 失败
	Reason    string `json:"reason"` // 失败原因
	At        int64  `json:"at"`     // unix 毫秒
}

// ActivityScheduleMsg 活动实例调度（gs.activity.schedule）
//
// 中心服 leader 在进入新周期时下发；游戏服按 version 幂等应用。
// 即使一条都没收到，游戏服也能用 pkg/activity.Window 自己算出同一窗口。
type ActivityScheduleMsg struct {
	InstanceID string `json:"instance_id"`
	ActivityID int32  `json:"activity_id"`
	StartAt    int64  `json:"start_at"`
	EndAt      int64  `json:"end_at"`
	Version    int64  `json:"version"`
	Action     string `json:"action"` // start / end
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
	FromName string `json:"from_name"`
	ToID     int64  `json:"to_id"`
	Channel  int32  `json:"channel"`
	Content  string `json:"content"`
	SendTime int64  `json:"send_time"`
}

// RelayCrossMsg 中心服集群间中转（center.relay）
//
// 场景（9.3）：节点 A 连中心服#1、节点 B 连中心服#2 时，#1 无法直接向 B 推送，
// 于是把 RelayRequest 写进本 topic（Key=目标 node_id 或 group_id），
// 由各实例独立消费并只投递**自己承载的节点**，节点侧完全无感知。
//
// OriginInstance 用于去重：每个实例都用独立消费组（广播语义），
// 发消息的实例自己也会收到一份，靠该字段跳过。
type RelayCrossMsg struct {
	RelayID        string `json:"relay_id"`
	OriginInstance string `json:"origin_instance"`
	FromNode       string `json:"from_node"`
	ToNode         string `json:"to_node,omitempty"`   // 点对点目标（与 Broadcast 二选一）
	Broadcast      bool   `json:"broadcast,omitempty"` // 组播
	GroupID        string `json:"group_id,omitempty"`  // 广播时的目标分组
	Payload        []byte `json:"payload"`
	CreatedAt      int64  `json:"created_at"` // unix 毫秒
}
