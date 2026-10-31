// pkg/mq/topics.go —— Kafka Topic 常量（设计文档 13.1 Topic 规划）
//
// 架构职责：全项目 topic 名的单一事实源；命名规范 gs.<域>.<事件>（内部），
// 跨集群中转用 center.relay（不带 gs 前缀以示区别）。
// 协作关系：生产方与消费方都 import 本包取常量，禁止出现字符串字面量——
// 拼写错误在编译期无法发现，只会在运行时表现为「消息发进黑洞」，
// 收敛成常量后改名/审计/建 topic 都有唯一清单。
// 设计取舍：AllTopics 供本地开发一键建 topic，生产环境建表走 IaC，
// 不依赖本函数——避免测试工具影响生产集群元数据。
package mq

// Topic 列表（13.1）：所有 topic 集中定义，禁止业务代码硬编码字符串。
// 分区键（Key）统一用业务主键：同键同分区 → 同玩家/同订单天然有序，
// 这是 Kafka 提供的唯一顺序保证，业务侧依赖它做「先下单后发货」等排序。
const (
	// TopicRechargeOrder 登录服支付回调 → 游戏服发货（Key=order_id）
	// 同订单的多次回调落到同一分区，保证按序消费、不重不乱。
	TopicRechargeOrder = "gs.recharge.order"
	// TopicRechargeDelivered 游戏服发货成功 → 登录服更新订单状态（Key=order_id）
	// 与 order topic 分开：确认消息可能重发，独立 topic 便于错峰消费/对账。
	TopicRechargeDelivered = "gs.recharge.delivered"
	// TopicRoleCreated 游戏服创角成功 → 登录服维护 user_role 角色索引（Key=account_id）
	// 按账号分区：同一账号在多个区服的角色同步有序，选角列表不会出现
	// 「先看到新角色、又消失」的回退。生产者是 role_sync_outbox 投递协程，
	// 不是创角事务本身——保证「有角色行必有一条待投递事件」（见该表注释）。
	TopicRoleCreated = "gs.role.created"
	// TopicMail 系统邮件/补偿发放 → 游戏服（Key=player_id）
	// 按玩家分区：同一玩家的多封邮件有序，避免「后发的先到」。
	TopicMail = "gs.mail"
	// TopicChatCross 跨服私聊投递（Key=接收方 player_id）
	TopicChatCross = "gs.chat.cross"
	// TopicPlayerEvent 玩家事件流（升级/获得道具/穿装备）
	// 只增不改的事件流，消费方（成就/任务/日志）各自独立 group 订阅。
	TopicPlayerEvent = "gs.player.event"
	// TopicLoginLog 登录登出流水
	// 共享 group 消费落库（见 loginlog_consumer.go）：事实记录，允许重复行。
	TopicLoginLog = "gs.login.log"
	// TopicFlushRetry 落库失败重试死信
	// 业务消息处理彻底失败后的归宿；补偿任务消费它并记录 dlq_* 头定位原因。
	TopicFlushRetry = "gs.flush.retry"
	// TopicActivitySchedule 全局活动定时触发
	// 由中心服 leader 单点生产，避免多 leader 重复下发（重复由 version 幂等兜住）。
	TopicActivitySchedule = "gs.activity.schedule"
	// TopicActivityProgress 全服活动贡献（游戏服 → 中心服，Key=instance_id）
	// 按实例分区保证同一活动的贡献有序累加。
	TopicActivityProgress = "gs.activity.progress"
	// TopicCenterRelay 中心服集群间中转（Key=目标 node_id）
	// 广播语义：每实例独立 group（StartOffset 必须 "last"，否则重启重放全量）。
	TopicCenterRelay = "center.relay"
)

// AllTopics 全部 topic（本地开发一键建 topic 用）。
// 返回字面量构造的新切片（天然拷贝），调用方改返回值不影响任何内部状态。
func AllTopics() []string {
	return []string{
		TopicRechargeOrder,
		TopicRechargeDelivered,
		TopicRoleCreated,
		TopicMail,
		TopicChatCross,
		TopicPlayerEvent,
		TopicLoginLog,
		TopicFlushRetry,
		TopicActivitySchedule,
		TopicActivityProgress,
		TopicCenterRelay,
	}
}
