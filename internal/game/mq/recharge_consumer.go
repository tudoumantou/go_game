// internal/game/mq/recharge_consumer.go —— 充值发货消费者（gs.recharge.order）
//
// 架构职责：支付回调的最终落地——把「已付款订单」变成「玩家到账」，
// 并向 login 服回发确认推进订单状态。是资金链路的最后一环。
// 协作关系：login 服支付回调生产 gs.recharge.order（Key=order_id 保序）；
// 本消费者经 pkg/mq.Consumer 驱动，依赖 Presence（在线判定）、Deduper
// （Redis 幂等）、玩家 Actor 系统（加道具）、Mail（离线发放）、
// Producer（回发 delivered 确认）。
// 设计取舍：
//   - 玩家不在线不丢单：在线走 Actor 即时发放；离线/跨服走 deliverOffline
//     写持久邮件（订单号作 mail.biz_id 唯一键）。充值消息宁可留痕不可丢，
//     与「邮件离线可达」复用同一套 Tier-1 兜底（R2.3）。
//   - 确认消息失败只告警：发货成功（奖励到账/邮件入库）后 delivered
//     发送失败不影响本次交付，login 服侧有对账补偿重投；若此时返回
//     error，重投会被幂等标记挡成「已处理」，确认永远补发不了——
//     告警 + 对账优于报错重试。
package mq

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/game/module/mail"
	"game_study/internal/game/player"
	pkgmq "game_study/pkg/mq"
)

// RechargeConsumer 充值发货消费者。
// deps 见 deps.go；Producer 允许为 nil（关发货确认的降级装配），
// 此时确认步骤静默跳过，订单状态推进依赖 login 服对账。
type RechargeConsumer struct {
	consumer *pkgmq.Consumer
	deps     Deps
}

// NewRechargeConsumer 创建消费者。
// consumer（topic/group）由 Group 装配注入，本类型只写业务逻辑。
func NewRechargeConsumer(consumer *pkgmq.Consumer, deps Deps) *RechargeConsumer {
	return &RechargeConsumer{consumer: consumer, deps: deps}
}

// Run 阻塞消费。
// 重试/退避/死信/commit 由 pkg/mq 统一负责。
func (c *RechargeConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭。
// nil 判空：Kafka 未启用时零值实例 Close 需安全。
func (c *RechargeConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 发货流程（13.3 模式①）：
//  1. 本地性判定：玩家必须在本服在线，否则丢弃（由登录服对账补偿重投）
//  2. 幂等：SETNX idem:gs.recharge.order:{order_id}
//  3. 应用奖励（给玩家 Actor 加道具）
//  4. 发 gs.recharge.delivered（登录服更新订单状态）
//
// 实际实现与文档的差异：步骤 1 不走「丢弃」，玩家不在线时改走
// deliverOffline 持久邮件发放（见文件头取舍）——充值不可因玩家离线丢失。
func (c *RechargeConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.RechargeOrderMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode recharge order: %w", err)
	}
	// OrderID 是幂等键与对账凭据，PlayerID 是发放对象——缺一即非法消息，
	// 返回 error 进死信留痕，绝不静默跳过（跳过=吞钱投诉）。
	if msg.OrderID == "" || msg.PlayerID <= 0 {
		return fmt.Errorf("invalid recharge order payload")
	}

	// ① 本地性判定：不在本服时改走离线发放，保证充值不因玩家不在线而丢
	// 三态语义：完全离线（!online）与「在线但在别的节点」都走离线邮件——
	// 邮件落库在玩家归属服，上线即可领取，不必等玩家回到本节点。
	node, online := c.deps.Presence.IsOnline(ctx, msg.PlayerID)
	if !online || (c.deps.NodeID != "" && node != c.deps.NodeID) {
		return c.deliverOffline(ctx, msg)
	}

	// ② 幂等
	// order_id 全局唯一（login 服支付单号），天然幂等键；SETNX + 24h TTL
	// 跨进程共享判定。Redis 故障上抛重试——无法判定时不能假定未处理。
	first, err := c.deps.Deduper.FirstTime(ctx, pkgmq.TopicRechargeOrder, msg.OrderID, pkgmq.DefaultIdemTTL)
	if err != nil {
		return err
	}
	if !first {
		// 已处理（发货成功过）：直接确认完成。重复投递是 at-least-once
		// 的正常现象，幂等挡掉后 offset 照常推进。
		return nil // 已处理
	}

	// ③ 应用奖励
	// 每个失败分支后都 Forget：标记已占位但业务没成，不删的话重投会被
	// 判「已处理」而永不补发（资金事故）；删掉让重投重新走完整流程。
	items, err := rewardItems(msg)
	if err != nil {
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicRechargeOrder, msg.OrderID)
		return err
	}
	if err := c.grant(ctx, msg.PlayerID, msg.OrderID, items); err != nil {
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicRechargeOrder, msg.OrderID)
		return err
	}

	// ④ 发货确认
	// 失败仅 Warn（见文件头取舍）：奖励已到账，确认可补；返回 error 会让
	// 重投被幂等挡掉，确认反而永远发不出去。
	if c.deps.Producer != nil {
		delivered := pkgmq.RechargeDeliveredMsg{
			OrderID:     msg.OrderID,
			PlayerID:    msg.PlayerID,
			DeliveredAt: time.Now().UnixMilli(),
		}
		out, err := pkgmq.NewEnvelope(msg.OrderID, pkgmq.TopicRechargeDelivered, msg.OrderID, delivered)
		if err == nil {
			if err := c.deps.Producer.ProduceEnvelope(ctx, out); err != nil {
				c.deps.Logger.Warn("produce delivered failed", "order_id", msg.OrderID, "err", err)
			}
		}
	}

	c.deps.Logger.Info("recharge delivered", "order_id", msg.OrderID, "player_id", msg.PlayerID)
	return nil
}

// deliverOffline 玩家离线（或不在本服）时的持久发放：写一封 biz_id 幂等的系统邮件。
//
// 这是 R2.3 的 Tier-1 兜底：订单号作为持久业务键落在 mail.biz_id 唯一键上，
// 重投、重启、跨服都不重复发；玩家上线后在邮箱里领取，领取本身走快照业务键。
// 写库成功即视为已交付，继续发 delivered 确认，避免订单卡在「已支付」。
// 与在线路径的差异：幂等由 DB 唯一键保证（而非 Redis）——离线发放可能
// 由任意实例执行，DB 唯一键是跨实例跨重启的最终防线。
func (c *RechargeConsumer) deliverOffline(ctx context.Context, msg pkgmq.RechargeOrderMsg) error {
	items, err := rewardItems(msg)
	if err != nil {
		return err
	}
	if c.deps.Mail == nil {
		return fmt.Errorf("no mail module for offline recharge delivery")
	}
	// 预分配切片容量：附件数=奖励档位数，make(..., 0, len(items)) 避免
	// append 增长中的多次重新分配（小优化，路径上可读性优先）。
	atts := make([]mail.Attachment, 0, len(items))
	for _, it := range items {
		// int64→int32 显式窄化：道具数量受 recharge_product 档位上限约束
		// （演示规则下 ≤ 订单金额，远小于 int32 上限）；显式转换把
		// 「我确认不会溢出」写成代码，而不是隐式截断。
		atts = append(atts, mail.Attachment{ItemID: it.ItemID, Count: int32(it.Count)})
	}
	// msg.OrderID 作为 biz_id：落 mail.biz_id 唯一键，重复调用返回
	// created=false（已存在）——重投幂等在 DB 层闭环。
	created, err := c.deps.Mail.DeliverOffline(ctx, msg.PlayerID, msg.OrderID,
		"充值到账", fmt.Sprintf("订单 %s 的充值奖励已发放，请领取附件。", msg.OrderID), atts)
	if err != nil {
		return err
	}
	if created {
		c.deps.Logger.Info("recharge delivered offline", "order_id", msg.OrderID, "player_id", msg.PlayerID)
	}
	// 已入库（或此前已入库）都视为交付完成，推进订单状态。
	// 不区分 created：重复投递时订单状态幂等推进，login 侧对账无差异。
	c.confirmDelivered(ctx, msg)
	return nil
}

// confirmDelivered 发送发货确认（登录服据此推进订单状态）。
// 与 handle ④ 内联逻辑相同，抽出来供在线/离线两条路径复用——
// 确认动作必须只有一处实现，否则两条路径容易改漏一处。
func (c *RechargeConsumer) confirmDelivered(ctx context.Context, msg pkgmq.RechargeOrderMsg) {
	if c.deps.Producer == nil {
		return
	}
	delivered := pkgmq.RechargeDeliveredMsg{
		OrderID:     msg.OrderID,
		PlayerID:    msg.PlayerID,
		DeliveredAt: time.Now().UnixMilli(),
	}
	out, err := pkgmq.NewEnvelope(msg.OrderID, pkgmq.TopicRechargeDelivered, msg.OrderID, delivered)
	if err != nil {
		return
	}
	if err := c.deps.Producer.ProduceEnvelope(ctx, out); err != nil {
		c.deps.Logger.Warn("produce delivered failed", "order_id", msg.OrderID, "err", err)
	}
}

// grant 给玩家 Actor 发奖励并等待结果。
//
// Actor 模型要点：不直接读写玩家状态，而是向玩家 Actor 投递
// GrantItemsMsg——所有状态变更在 Actor 的单线程循环里顺序执行，
// 天然无锁；本函数只负责「投递 + 等回执」。
// done channel 容量 1：Actor 回写结果不会阻塞（即使本函数已超时放弃），
// 防止慢消费者拖垮 Actor 信箱；time.After 与 ctx.Done 三个 case
// 保证：Actor 卡死有 3s 上限、关服能立即中断，不会无限等。
func (c *RechargeConsumer) grant(ctx context.Context, playerID int64, orderID string, items []player.GrantItem) error {
	pid, ok := c.deps.Players.Get(playerID)
	if !ok {
		// Actor 不在本地表：与「在线判定」竞态（刚好下线）或路由表延迟，
		// 返回 error 走重试/死信，由对账补偿最终落离线邮件路径。
		return fmt.Errorf("player %d actor not found", playerID)
	}
	done := make(chan error, 1)
	// BizID 透传给 Actor：玩家侧道具流水可追溯到订单，防重幂等的
	// 第二道闸（Actor 内按 biz_id 去重）。
	//
	// Sync 置真：充值属于 A 类数据，回执必须在 MySQL 提交之后发出。
	// 异步通道等的是一个落库窗口，Redis 掉电会连窗口一起丢——订单已经
	// 标记 delivered 但货没到账，只能靠人工补偿。
	msg := &player.GrantItemsMsg{
		Source: "recharge", BizID: orderID, Items: items, Sync: true, Done: done,
	}
	if err := c.deps.Sys.Send(pid, msg); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-time.After(8 * time.Second):
		// 8s 上限：同步路径含一次 MySQL 事务（自身 5s 超时），必须留出
		// 余量，否则回执还在路上就被判超时，消费者会重复投递（虽然幂等
		// 兜得住，但会放大日志与重试）。Actor 正常处理是毫秒级。
		return fmt.Errorf("grant timeout for player %d", playerID)
	case <-ctx.Done():
		// 关服中断：立即让路，不等剩余超时；消息不提交，重启重放，
		// 幂等标记兜住重复发放。
		return ctx.Err()
	}
}

// rewardItems 计算充值奖励（P5：1 元 = 10 钻石的演示规则，P6 接 recharge_product.xml）。
// 演示期硬编码档位：AmountFen（分）→ 钻石 1:1；生产必须改为查
// gamedata 的 RechargeProduct 表（按 product_id 取档位），否则无法支撑
// 多档位/折扣/活动加成。校验 amount_fen>0：0 元订单是脏数据，
// 但也要小心——真实场景的 0 元单应由配置表白名单放行，而非这里拒绝。
func rewardItems(msg pkgmq.RechargeOrderMsg) ([]player.GrantItem, error) {
	if msg.AmountFen <= 0 {
		return nil, fmt.Errorf("amount_fen must be > 0")
	}
	// 演示道具 ID 1001 = 钻石；1 分 = 1 钻石（P6 按档位表配置）
	return []player.GrantItem{{ItemID: 1001, Count: int64(msg.AmountFen)}}, nil
}
