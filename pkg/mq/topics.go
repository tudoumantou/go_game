// pkg/mq/topics.go —— Kafka Topic 常量（设计文档 13.1 Topic 规划）
package mq

// Topic 列表（13.1）：所有 topic 集中定义，禁止业务代码硬编码字符串。
const (
	// TopicRechargeOrder 登录服支付回调 → 游戏服发货（Key=order_id）
	TopicRechargeOrder = "gs.recharge.order"
	// TopicRechargeDelivered 游戏服发货成功 → 登录服更新订单状态（Key=order_id）
	TopicRechargeDelivered = "gs.recharge.delivered"
	// TopicMail 系统邮件/补偿发放 → 游戏服（Key=player_id）
	TopicMail = "gs.mail"
	// TopicChatCross 跨服私聊投递（Key=接收方 player_id）
	TopicChatCross = "gs.chat.cross"
	// TopicPlayerEvent 玩家事件流（升级/获得道具/穿装备）
	TopicPlayerEvent = "gs.player.event"
	// TopicLoginLog 登录登出流水
	TopicLoginLog = "gs.login.log"
	// TopicFlushRetry 落库失败重试死信
	TopicFlushRetry = "gs.flush.retry"
	// TopicActivitySchedule 全局活动定时触发
	TopicActivitySchedule = "gs.activity.schedule"
	// TopicCenterRelay 中心服集群间中转（Key=目标 node_id）
	TopicCenterRelay = "center.relay"
)

// AllTopics 全部 topic（本地开发一键建 topic 用）。
func AllTopics() []string {
	return []string{
		TopicRechargeOrder,
		TopicRechargeDelivered,
		TopicMail,
		TopicChatCross,
		TopicPlayerEvent,
		TopicLoginLog,
		TopicFlushRetry,
		TopicActivitySchedule,
		TopicCenterRelay,
	}
}
