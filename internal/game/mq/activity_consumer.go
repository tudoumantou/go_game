// internal/game/mq/activity_consumer.go —— 活动实例调度消费者（gs.activity.schedule，R7.2）
//
// 架构职责：消费中心服/运营下发的活动窗口（开服/关服时间），写入进程级
// 注册表 activitymod.Registry，供活动模块校验「现在是否开放」。
//
// 协作关系：注册表由 group.go 在创建消费者时注入（进程单例，所有模块
// 共享读）；本消费者是注册表唯一的写入口。
//
// 设计取舍：
//   - 为什么幂等按 version 比较而不是去重表：version 就是 start_at，
//     「只接受更新版本」天然同时解决重复投递与乱序投递——旧版本到达时
//     直接丢弃，注册表永远收敛到最新窗口；
//   - 为什么 Apply 失败不返回错误：被丢弃的是「过期事实」而非「处理
//     失败」，返回错误会触发 Kafka 重投，白白放大流量。
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

// Name 消费者标识（日志与关闭顺序排查用）。
func (c *ActivityConsumer) Name() string { return "activity.schedule" }

// Run 阻塞消费，直到 ctx 取消或底层 consumer 出错。
func (c *ActivityConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭底层 consumer。
func (c *ActivityConsumer) Close() error { return c.consumer.Close() }

// handle 单条调度消息：解码 → 基本校验 → 版本化应用。
//
// 校验（endAt>startAt、activityID>0）失败算消息有毒：返回错误让 Kafka
// 重试到上限后进死信，而不是把非法窗口写进注册表。
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
