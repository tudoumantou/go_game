// pkg/mq/message.go —— Kafka 统一消息信封
// 约定：所有业务消息共用一层信封，biz_id 为全局幂等键（order_id/mail_id/msg_id/event_id），
// 消费端先按 biz_id 做幂等去重，再解析 payload（见设计文档 13.4）。
//
// 架构职责：定义进程间传递的唯一 wire 格式——信封字段 + 各业务 payload 结构，
// login/center/game 三侧共用，是 Kafka 上的「内部契约」。
// 协作关系：生产侧用 NewEnvelope 构造，消费侧用 Unmarshal + DecodePayload 拆包；
// 死信路径（kafka.go）复用信封，仅追加 dlq_* 头。
// 设计取舍：
//   - Payload 用 json.RawMessage 延迟解析：信封层不关心业务 schema，
//     加新消息类型不需要改本文件的反序列化逻辑（开闭原则）；代价是
//     解析错误只能到消费端才暴露，靠 DecodePayload 的 error 上抛兜住。
//   - biz_id 必填且收发双向校验：缺 biz_id 的消息无法去重，放进系统就是
//     一颗「重复应用」地雷，所以在出口（Marshal）和入口（Unmarshal）双重拦截。
package mq

import (
	"encoding/json"
	"fmt"
	"time"
)

// Envelope 消息信封。
//
// 字段语义：Key 决定分区（Hash 均衡器下同 Key 有序），Headers 承载 trace_id 等
// 跨服务透传信息；Payload 是各业务 Msg 的 JSON 序列化结果。
//
// 并发契约：单个 Envelope 非并发安全（Headers 是普通 map），生产/消费/死信
// 改写各自发生在不同阶段，不要多 goroutine 共享一个信封。
type Envelope struct {
	BizID     string            `json:"biz_id"`            // 全局幂等键（必填）
	Topic     string            `json:"topic"`             // 来源 topic（排查用）
	Key       string            `json:"key,omitempty"`     // 分区键（player_id/order_id）
	Payload   json.RawMessage   `json:"payload"`           // 业务载荷
	Headers   map[string]string `json:"headers,omitempty"` // 扩展头（trace_id 等）
	CreatedAt int64             `json:"created_at"`        // 生产时间（unix 毫秒）
}

// NewEnvelope 构造信封并序列化 payload。
// payload 用 any：调用方传业务结构体，序列化失败（如含 chan/func）在此拦截。
func NewEnvelope(bizID, topic, key string, payload any) (*Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	// 生产端打时间戳（unix 毫秒）：消费 lag 观测与死信追溯都依赖它。
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
	// biz_id 是全局幂等键，缺失的消息无法去重，发送侧直接拒绝。
	// 在发送侧（而非信任调用方）校验：这是系统边界上的输入验证，
	// 内部代码再小心也挡不住一次手滑传空。
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
	// 与 Marshal 对称：缺 biz_id 的消息在入口处拒绝，不进入业务处理。
	// 宁可进死信也不放进业务：无 biz_id 的消息即使重复也无从判断。
	if e.BizID == "" {
		return nil, fmt.Errorf("envelope biz_id is empty")
	}
	return &e, nil
}

// DecodePayload 把 payload 解析到目标结构。
// len(Payload)==0 显式检查：json.Unmarshal 对空输入报 EOF，
// 包一层「empty payload」让排障时一眼看出是生产端没填而不是 JSON 坏。
func (e *Envelope) DecodePayload(v any) error {
	if len(e.Payload) == 0 {
		return fmt.Errorf("empty payload")
	}
	return json.Unmarshal(e.Payload, v)
}

// Payload 业务载荷定义（Kafka 内部契约，非对外 proto）。
// 演进约定：只加字段不改语义（JSON 向后兼容），删除字段前先确认
// 死信队列里的历史消息不再被补偿消费。

// RechargeOrderMsg 充值订单（gs.recharge.order）
// 由 login 服支付回调生产；game 服按 order_id 幂等发货（见 recharge_consumer.go）。
type RechargeOrderMsg struct {
	OrderID   string `json:"order_id"`
	PlayerID  int64  `json:"player_id"`
	ProductID int32  `json:"product_id"`
	AmountFen int32  `json:"amount_fen"`
	Channel   int32  `json:"channel"`
}

// RechargeDeliveredMsg 发货确认（gs.recharge.delivered）
// game 服发货成功后生产；login 服据此推进订单状态。发送失败仅告警——
// 幂等键在发货时已占位，登录服侧还有对账补偿兜底（见 recharge_consumer.go ④）。
type RechargeDeliveredMsg struct {
	OrderID     string `json:"order_id"`
	PlayerID    int64  `json:"player_id"`
	DeliveredAt int64  `json:"delivered_at"` // unix 毫秒
}

// RoleCreatedMsg 角色索引同步（gs.role.created）
//
// 由游戏服的 role_sync_outbox 投递协程生产；登录服消费后 UPSERT user_role，
// 供选角界面列出该账号在各区服的角色。Key=account_id 保证同账号有序。
//
// 幂等键是 PlayerId：一个角色只会被创建一次，重复投递只会重复写同一行
// （user_role 主键 (account_id, player_id) + uk_player），因此消费端不需要
// Deduper——DB 唯一键本身就是跨实例、跨重启的最终防线。
//
// 语义是「角色索引的当前值」而不是「增量事件」：字段可随改名/升级被覆盖，
// 消费端按最新值 UPSERT 即可，不需要按事件顺序回放。
type RoleCreatedMsg struct {
	PlayerID  int64  `json:"player_id"`
	AccountID int64  `json:"account_id"`
	ServerID  int32  `json:"server_id"`
	Nickname  string `json:"nickname"`
	Level     int32  `json:"level"`
	CreatedAt int64  `json:"created_at"` // unix 毫秒
}

// LoginLogMsg 登录流水（gs.login.log）
//
// 每条登录尝试一条，消费端直接落 login_log 表，不做业务去重：
// 流水是事实记录，重复消费只会产生重复行，不影响玩家状态。
// Result=0 成功 / 1 失败；PlayerID 在未创角时为 0（账号已过验证但无角色）。
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
// 即：该消息是「提示/加速」，不是唯一事实源——丢失不致命，重复被 version 挡掉。
type ActivityScheduleMsg struct {
	InstanceID string `json:"instance_id"`
	ActivityID int32  `json:"activity_id"`
	StartAt    int64  `json:"start_at"`
	EndAt      int64  `json:"end_at"`
	Version    int64  `json:"version"`
	Action     string `json:"action"` // start / end
}

// ActivityProgressMsg 全服活动贡献（gs.activity.progress）
//
// 上报的是玩家**当前累计进度**而不是增量：中心服按「新值 - 旧值」累加，
// 重投相同值得到 0，天然幂等，不需要额外的业务键。
// 这是「绝对值上报」模式——用冗余存储换掉一个幂等键，重投零成本。
type ActivityProgressMsg struct {
	InstanceID string `json:"instance_id"`
	ActivityID int32  `json:"activity_id"`
	PlayerID   int64  `json:"player_id"`
	Progress   int32  `json:"progress"`
}

// MailMsg 邮件投递（gs.mail）
// 消费端按 mail_id 幂等（FirstTime），投递失败 Forget 回滚标记等重投。
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
// 跨消息复用的最小结构；ItemID 对应 gamedata 的 item 表主键。
type Item struct {
	ItemID int32 `json:"item_id"`
	Count  int64 `json:"count"`
}

// ChatCrossMsg 跨服私聊（gs.chat.cross）
// 消费端按 msg_id 幂等；内容已经过发送侧敏感词过滤，此处不再校验。
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
// 这是「广播 + 自 Skip」模式：跨进程 pub/sub 下发布者必然收到回声，
// 与其费力过滤不如带来源标识让消费端自判。
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
