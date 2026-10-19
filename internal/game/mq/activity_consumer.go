package mq

import (
	"context"
	"fmt"

	activitymod "game_study/internal/game/module/activity"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// ActivityConsumer 消费 gs.activity.schedule，把活动实例写入进程级注册表（R7.2）。
//
// 幂等：按 version（= start_at）比较，旧版本或相同版本直接丢弃——
// 乱序与重复投递都不会把活动窗口改回去。
type ActivityConsumer struct {
	consumer *pkgmq.Consumer
	reg      *activitymod.Registry
	logger   *log.Logger
}

// NewActivityConsumer 创建消费者。
func NewActivityConsumer(c *pkgmq.Consumer, reg *activitymod.Registry, logger *log.Logger) *ActivityConsumer {
	return &ActivityConsumer{consumer: c, reg: reg, logger: logger}
}

func (c *ActivityConsumer) Name() string { return "activity.schedule" }

func (c *ActivityConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

func (c *ActivityConsumer) Close() error { return c.consumer.Close() }

func (c *ActivityConsumer) handle(_ context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.ActivityScheduleMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode activity schedule: %w", err)
	}
	if msg.ActivityID <= 0 || msg.EndAt <= msg.StartAt {
		return fmt.Errorf("invalid activity schedule: %+v", msg)
	}

	applied := c.reg.Apply(activitymod.Window{
		InstanceID: msg.InstanceID,
		ActivityID: msg.ActivityID,
		StartAt:    msg.StartAt,
		EndAt:      msg.EndAt,
		Version:    msg.Version,
	})
	if !applied {
		c.logger.Debug("activity schedule ignored (stale)", "instance_id", msg.InstanceID, "version", msg.Version)
		return nil
	}
	c.logger.Info("activity schedule applied",
		"instance_id", msg.InstanceID, "start", msg.StartAt, "end", msg.EndAt)
	return nil
}
