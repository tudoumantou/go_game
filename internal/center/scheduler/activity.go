// Package scheduler 全服调度（R7.1/R7.2）。
//
// 只有中心服 leader 执行：按配置算出当前活动实例，落库并下发。
// 「只有日志里当选 leader」不算实现——真正的判定是：即使游戏服一条消息都没收到，
// 它也能用同一套周期算法算出正确的活动窗口（见 pkg/activity）。
package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/activity"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
)

// Service 活动实例调度。
type Service struct {
	db       *db.DB
	producer *pkgmq.Producer
	logger   *log.Logger
	now      func() time.Time
}

// New 创建调度器；producer 为空时只落库不下发（降级为「游戏服自己算」）。
func New(database *db.DB, producer *pkgmq.Producer, logger *log.Logger) *Service {
	return &Service{db: database, producer: producer, logger: logger, now: time.Now}
}

// Run 由 leader 生命周期驱动，30s 一次：活动边界最长一天，秒级精度没有意义。
func (s *Service) Run(ctx context.Context, isLeader func() bool) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if isLeader == nil || !isLeader() {
				continue
			}
			if err := s.Tick(ctx); err != nil {
				s.logger.Warn("activity schedule failed", "err", err)
			}
		}
	}
}

// Tick 同步全部活动实例。
func (s *Service) Tick(ctx context.Context) error {
	now := s.now().Unix()
	var firstErr error
	for _, cfg := range gamedata.ActivityPeriodCfgList() {
		if err := s.syncOne(ctx, int32(cfg.Id), int32(cfg.Col1), now); err != nil {
			s.logger.Warn("activity sync failed", "activity_id", cfg.Id, "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// syncOne 计算当前周期；与库里已有实例不同则落库并下发。
//
// 同一周期重复执行不会重复下发：instance_id 是主键，已存在且 period 相同就跳过。
func (s *Service) syncOne(ctx context.Context, activityID int32, durationDays int32, now int64) error {
	start, end, period := activity.Window(durationDays, now)
	instanceID := fmt.Sprintf("%d-%d", activityID, period)

	row, err := model.FindActivityInstanceByPK(ctx, s.db.DB, instanceID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if row != nil {
		return nil // 本期已同步过
	}

	ins := &model.ActivityInstance{
		InstanceId: instanceID,
		ActivityId: activityID,
		Period:     period,
		StartAt:    start,
		EndAt:      end,
		Version:    start, // 版本 = 开始时间，单调递增
		UpdatedAt:  now,
	}
	if _, err := model.UpsertActivityInstanceByPK(ctx, s.db.DB, ins); err != nil {
		return err
	}
	s.publish(ctx, ins, "start")
	s.logger.Info("activity instance scheduled",
		"instance_id", instanceID, "start", start, "end", end)
	return nil
}

// publish 下发调度消息；失败只记日志——游戏服会用同一算法自己算，不会因此漏活动。
func (s *Service) publish(ctx context.Context, ins *model.ActivityInstance, action string) {
	if s.producer == nil {
		return
	}
	msg := pkgmq.ActivityScheduleMsg{
		InstanceID: ins.InstanceId,
		ActivityID: ins.ActivityId,
		StartAt:    ins.StartAt,
		EndAt:      ins.EndAt,
		Version:    ins.Version,
		Action:     action,
	}
	env, err := pkgmq.NewEnvelope(ins.InstanceId, pkgmq.TopicActivitySchedule, ins.InstanceId, msg)
	if err != nil {
		s.logger.Warn("build schedule envelope failed", "instance_id", ins.InstanceId, "err", err)
		return
	}
	if err := s.producer.ProduceEnvelope(ctx, env); err != nil {
		s.logger.Warn("publish schedule failed", "instance_id", ins.InstanceId, "err", err)
	}
}
