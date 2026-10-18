// internal/game/mq/recharge_consumer.go —— 充值发货消费者（gs.recharge.order）
package mq

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/game/player"
	pkgmq "game_study/pkg/mq"
)

// RechargeConsumer 充值发货消费者。
type RechargeConsumer struct {
	consumer *pkgmq.Consumer
	deps     Deps
}

// NewRechargeConsumer 创建消费者。
func NewRechargeConsumer(consumer *pkgmq.Consumer, deps Deps) *RechargeConsumer {
	return &RechargeConsumer{consumer: consumer, deps: deps}
}

// Run 阻塞消费。
func (c *RechargeConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭。
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
func (c *RechargeConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.RechargeOrderMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode recharge order: %w", err)
	}
	if msg.OrderID == "" || msg.PlayerID <= 0 {
		return fmt.Errorf("invalid recharge order payload")
	}

	// ① 本地性判定（不命中本服时不设幂等键，避免误导真正持有节点）
	node, online := c.deps.Presence.IsOnline(ctx, msg.PlayerID)
	if !online || (c.deps.NodeID != "" && node != c.deps.NodeID) {
		c.deps.Logger.Debug("recharge skip (player not on this node)",
			"order_id", msg.OrderID, "player_id", msg.PlayerID, "node", node)
		return nil
	}

	// ② 幂等
	first, err := c.deps.Deduper.FirstTime(ctx, pkgmq.TopicRechargeOrder, msg.OrderID, pkgmq.DefaultIdemTTL)
	if err != nil {
		return err
	}
	if !first {
		return nil // 已处理
	}

	// ③ 应用奖励
	items, err := rewardItems(msg)
	if err != nil {
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicRechargeOrder, msg.OrderID)
		return err
	}
	if err := c.grant(ctx, msg.PlayerID, items); err != nil {
		pkgmq.Forget(ctx, c.deps.Deduper, pkgmq.TopicRechargeOrder, msg.OrderID)
		return err
	}

	// ④ 发货确认
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

// grant 给玩家 Actor 发奖励并等待结果。
func (c *RechargeConsumer) grant(ctx context.Context, playerID int64, items []player.GrantItem) error {
	pid, ok := c.deps.Players.Get(playerID)
	if !ok {
		return fmt.Errorf("player %d actor not found", playerID)
	}
	done := make(chan error, 1)
	msg := &player.GrantItemsMsg{Source: "recharge", Items: items, Done: done}
	if err := c.deps.Sys.Send(pid, msg); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		return fmt.Errorf("grant timeout for player %d", playerID)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// rewardItems 计算充值奖励（P5：1 元 = 10 钻石的演示规则，P6 接 recharge_product.xml）。
func rewardItems(msg pkgmq.RechargeOrderMsg) ([]player.GrantItem, error) {
	if msg.AmountFen <= 0 {
		return nil, fmt.Errorf("amount_fen must be > 0")
	}
	// 演示道具 ID 1001 = 钻石；1 分 = 1 钻石（P6 按档位表配置）
	return []player.GrantItem{{ItemID: 1001, Count: int64(msg.AmountFen)}}, nil
}
