// internal/game/mq/mail_consumer.go —— 邮件投递消费者（gs.mail）
//
// 架构职责：把系统邮件/补偿邮件投递到玩家邮箱，是「离线可达」消息的
// 落地通道（邮件不要求玩家在线）。
// 协作关系：上游运营/补偿系统生产 gs.mail（Key=player_id 保序）；
// 本消费者经 pkg/mq.Consumer 驱动，deps 注入 Redis 去重器（Deduper）、
// 在线状态（Presence）、邮件模块（Mail）；与 mail 模块的未读推送（P6）衔接。
// 设计取舍：
//   - 幂等键用 mail_id：邮件在中心服分配全局唯一 ID，天然是幂等键；
//     Redis SETNX + TTL（24h）跨进程共享判定，重启/重投不重复发。
//   - 本地性判定前置于幂等：玩家在其它服时先 PendPush 暂存（登录时
//     drain），本服邮件才走「去重 + 落库」；顺序反了会在错误节点上
//     占用幂等标记，导致正确节点投递时被判「已处理」而丢弃。
package mq

import (
	"context"
	"fmt"

	pkgmq "game_study/pkg/mq"
)

// MailConsumer 邮件投递消费者。
// deps 是共享依赖包（见 deps.go）：Deduper/Presence/Mail/Logger/NodeID。
type MailConsumer struct {
	consumer *pkgmq.Consumer
	deps     Deps
}

// NewMailConsumer 创建消费者。
// consumer（含 topic/group 选择）由 Group 装配注入，本类型不感知 Kafka 细节。
func NewMailConsumer(consumer *pkgmq.Consumer, deps Deps) *MailConsumer {
	return &MailConsumer{consumer: consumer, deps: deps}
}

// Run 阻塞消费。
// 重试/退避/死信/commit 由 pkg/mq 统一负责，本消费者只提供 handle。
func (c *MailConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭。
// nil 判空：Kafka 未启用时 Group 不创建本消费者，零值实例 Close 需安全。
func (c *MailConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 邮件投递：
//  1. 幂等（mail_id）
//  2. 落库到「玩家所在服」的 mail 表（不要求玩家在线：邮件是离线可达的）
//  3. 玩家在线时后续由 mail 模块推送未读数（P6）
//
// 顺序说明：载荷校验 → 本地性判定 → 幂等 → 落库。本地性必须在幂等之前
// （见文件头取舍），落库失败必须 Forget 回滚标记（见下）。
func (c *MailConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.MailMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode mail: %w", err)
	}
	// PlayerID/MailID 是主键与路由依据，任一非法都说明生产者有 bug；
	// 返回 error 走死信，而不是 nil 静默丢（丢邮件 = 丢运营承诺）。
	if msg.PlayerID <= 0 || msg.MailID <= 0 {
		return fmt.Errorf("invalid mail payload")
	}

	// 本地性判定：玩家不在本服时，由 pend 暂存兜底（13.3 模式②）。
	// IsOnline 返回 (节点, 是否在线)：离线玩家 node 为空串，视为本服处理
	// （邮件本就离线可达，由本服 mail 表承载）。
	node, online := c.deps.Presence.IsOnline(ctx, msg.PlayerID)
	if online && c.deps.NodeID != "" && node != c.deps.NodeID {
		// 玩家在其它服：暂存，登录 drain
		// PendPush 失败仅 Warn 不返回 error：此路径已是兜底（正确节点
		// 会因收不到投递而由对账/重投补偿），返回 error 只会让本条消息
		// 重试到死信，不如留下告警让人工介入更快。
		if err := c.deps.Presence.PendPush(ctx, "mail", msg.PlayerID, env.Payload, 200, 0); err != nil {
			c.deps.Logger.Warn("pend mail failed", "player_id", msg.PlayerID, "err", err)
		}
		return nil
	}

	// 幂等：SETNX idem:gs.mail:{mail_id}，TTL 24h。跨进程共享判定，
	// 重投/重启后同一封邮件只有一个实例能拿到「首次」。
	// err 上抛（而非当已处理）：Redis 故障时无法判定是否处理过，
	// 此时推进 offset 等于放弃去重，宁可重试/停住。
	first, err := c.deps.Deduper.FirstTime(ctx, pkgmq.TopicMail, fmt.Sprintf("%d", msg.MailID), pkgmq.DefaultIdemTTL)
	if err != nil {
		return err
	}
	if !first {
		// 已处理：直接 return nil，pkg/mq 随后提交 offset——
		// 「重复消息被幂等挡掉」也是成功路径，不构成错误。
		return nil
	}

	if c.deps.Mail == nil {
		return fmt.Errorf("mail module not initialized")
	}
	mailID, err := c.deps.Mail.Deliver(ctx, &msg)
	if err != nil {
		// 关键：投递失败必须 Forget 删掉刚写的幂等标记。不删的话重投会被
		// FirstTime 判「已处理」直接跳过，邮件永久丢失；删掉后重投可重新
		// 判定首次并再次投递——这是「至少一次」语义的配套补偿。
		// Deliver 内部还有 mail 表唯一键兜底，Forget 后重投不会双发。
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicMail, fmt.Sprintf("%d", msg.MailID))
		return fmt.Errorf("deliver mail: %w", err)
	}
	// db_id 是 mail 表自增主键：与 mail_id（业务键）分开记录，
	// 排障时能把「消息里的 ID」和「库里的行」对上。
	c.deps.Logger.Info("mail delivered", "mail_id", msg.MailID, "db_id", mailID, "player_id", msg.PlayerID)
	return nil
}
