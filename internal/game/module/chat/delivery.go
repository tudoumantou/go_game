// delivery.go —— 跨节点投递：Kafka 入站（DeliverInbound）与本地下发（deliver）。
//
// 与 handler.go 分开：这条路径由 MQ 驱动，排障看的是 topic 与离线队列，
// 而不是玩家请求的链路。
package chat

import (
	"context"

	"game_study/pkg/codec"
	pkgmq "game_study/pkg/mq"
)

// DeliverInbound 投递一条「收到」方向的跨服私聊（Kafka 消费调用）。
//
// 由 chat_cross_consumer 在「本地性判定 + msg_id 幂等」通过后调用，
// 职责只有落 in 方向历史；repo 为 nil（无 DB 环境）时静默跳过——
// 历史是尽力而为的旁路数据，不因缺库阻断 Kafka 消费。
func (m *Module) DeliverInbound(ctx context.Context, msg *pkgmq.ChatCrossMsg) error {
	if m.repo == nil {
		return nil
	}
	return m.repo.Insert(ctx, msg.ToID, msg.FromID, msg.MsgID, DirectionIn, msg.Channel, msg.Content)
}

// onSend 发送私聊（17.3：本端 out → 对方在线本服直投 / 跨服走 Kafka / 离线 pend）。

// deliver 按在线状态选择投递路径。
//
// 三路互斥：离线 → Redis 暂存；本服在线 → 落 in 历史 + Actor 推送；
// 跨服在线 → Kafka 生产（对端所在节点的广播消费者落地后处理）。
func (m *Module) deliver(toID int64, raw []byte, crossMsg *pkgmq.ChatCrossMsg) {
	if m.messenger == nil {
		return // 无组装层注入（单机模式）：只落库不投递
	}
	// 全程用 Background：OnlineNode 走 Redis 查询可能慢，且请求 ctx 在回包后
	// 即取消——投递是「受理之后」的异步动作，不能挂在请求生命周期上
	bg := context.Background()
	node, online := m.messenger.OnlineNode(bg, toID)
	switch {
	case !online:
		// 暂存失败只 Warn：接收方下次登录补投，丢一条私聊可接受
		if err := m.messenger.PendOffline(bg, toID, crossMsg); err != nil {
			m.logger.Warn("pend offline chat failed", "to", toID, "err", err)
		}
	case node == m.messenger.SelfNode():
		// 本服在线：落 in + 推送
		// 先落后推的顺序有意为之：客户端收到推送立刻拉历史时，
		// 这条 in 记录已可查，不会出现「消息到了、历史里没有」
		if m.repo != nil {
			if err := m.repo.Insert(bg, toID, crossMsg.FromID, crossMsg.MsgID, DirectionIn, channelPrivate, crossMsg.Content); err != nil {
				m.logger.Warn("save inbound chat failed", "err", err)
			}
		}
		// Push 返回 false 是掉线竞态（判定在线与推送之间掉线），静默丢弃
		m.messenger.Push(bg, toID, codec.MsgChatMessageNotify, raw)
	default:
		// 跨服：生产失败只 Warn——Kafka 侧重投由 broker/生产者重试兜底
		if err := m.messenger.ProduceCross(bg, crossMsg); err != nil {
			m.logger.Warn("produce cross chat failed", "to", toID, "err", err)
		}
	}
}

// onHistory 拉取与某好友的聊天历史。
