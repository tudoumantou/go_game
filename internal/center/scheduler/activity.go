// Package scheduler 全服调度（R7.1/R7.2）。
//
// 只有中心服 leader 执行：按配置算出当前活动实例，落库并下发。
// 「只有日志里当选 leader」不算实现——真正的判定是：即使游戏服一条消息都没收到，
// 它也能用同一套周期算法算出正确的活动窗口（见 pkg/activity）。
//
// 协作关系：调度消息经 Kafka gs.activity.schedule 下发游戏服；下发失败
// 不影响正确性——游戏服本地算法可自愈，Kafka 只是「提前知情」的优化。
// 实例状态落 activity_instance 表：leader 换人后新 leader 从库里看到
// 「本期已同步过」，不会重复下发。
package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/internal/center/leader"
	"game_study/pkg/activity"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
	"game_study/pkg/periodic"
	"game_study/pkg/sqlstore"
)

// Service 活动实例调度。
//
// 依赖极简：db 存实例状态（幂等依据），producer 发调度消息（可空降级），
// now 可注入让测试控制「当前处于哪个周期」——周期算法的边界
// （跨期瞬间、刚上线）全靠它覆盖。
type Service struct {
	*sqlstore.Base
	producer *pkgmq.Producer
	logger   *log.Logger
	now      func() time.Time
}

// New 创建调度器；producer 为空时只落库不下发（降级为「游戏服自己算」）。
// 降级而不是失败：调度下发是优化项，没有 Kafka 的中心服仍能把
// 活动状态落库，游戏服侧算法独立算窗口，功能不残缺。
func New(database *db.DB, producer *pkgmq.Producer, logger *log.Logger) *Service {
	return &Service{Base: sqlstore.New(database, "scheduler"), producer: producer, logger: logger, now: time.Now}
}

// Run 由 leader 生命周期驱动。
//
// 节拍取 leader.TickInterval（活动边界最长一天，秒级精度没有意义）。
// 循环骨架（含 ticker 必须 Stop、ctx 与 tick 同一条 select）见 pkg/periodic
// ——此前这条注释在三个包里各写一遍，写法也各差一点。
func (s *Service) Run(ctx context.Context, isLeader func() bool) {
	periodic.Run(ctx, periodic.Options{
		Name:     "activity",
		Interval: leader.TickInterval,
		Logger:   s.logger,
		Gate:     periodic.Guard(isLeader),
		Fn:       s.Tick,
	})
}

// Tick 同步全部活动实例。
// 单个活动失败不阻断其余：firstErr 只留第一个错误供上层告警，
// 循环继续——一个活动的配置错误不该让全服调度停摆。
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
//
// 「先落后发」的顺序：消息可能丢，库里的状态不会——重启后的新 leader
// 看到本期已有记录便不再补发，游戏服靠本地算法覆盖，两端一致。
func (s *Service) syncOne(ctx context.Context, activityID int32, durationDays int32, now int64) error {
	start, end, period := activity.Window(durationDays, now)
	instanceID := fmt.Sprintf("%d-%d", activityID, period)

	row, err := model.FindActivityInstanceByPK(ctx, s.SQL(), instanceID)
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
	if _, err := model.UpsertActivityInstanceByPK(ctx, s.SQL(), ins); err != nil {
		return err
	}
	s.publish(ctx, ins, "start")
	s.logger.Info("activity instance scheduled",
		"instance_id", instanceID, "start", start, "end", end)
	return nil
}

// publish 下发调度消息；失败只记日志——游戏服会用同一算法自己算，不会因此漏活动。
//
// envelope 的 key 用 instance_id：同一活动的调度消息落同一 Kafka 分区，
// 游戏服侧按序处理（start 必须先于后续期），乱序会开出过期活动。
// action 参数预留多动作（start/end），当前只发 start。
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
