// Package milestone 全服活动里程碑（R7.3 / R7.4）。
//
// 两条独立路径：
//   - 消费 gs.activity.progress：把各玩家「当前累计进度」按差值累加成全服进度；
//   - leader 定时判定：达到阈值就给贡献玩家生成发奖任务，再逐条发邮件并标记完成。
//
// 为什么上报绝对值而不是增量：重投相同值得到差值 0，天然幂等；
// 增量上报需要业务键，而「玩家在某个活动里的进度」本身就是唯一状态。
package milestone

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"game_study/pkg/activity"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"

	"github.com/redis/go-redis/v9"
)

const (
	statusPending int32 = 0
	statusDone    int32 = 1

	// batchSize 单轮发奖条数：控制单次事务时长，失败后只需重试这一批。
	batchSize = 100
)

// 累加脚本：只有进度变大才更新，返回本次增量。
//
//	KEYS[1] = activity:progress:{instance}（Hash: player_id → 进度）
//	KEYS[2] = activity:total:{instance}（全服累计）
//	ARGV[1] = player_id, ARGV[2] = 当前进度
var incrScript = redis.NewScript(`
local key, totalKey = KEYS[1], KEYS[2]
local pid, val = ARGV[1], tonumber(ARGV[2])
local old = tonumber(redis.call('HGET', key, pid) or '0')
if val <= old then return 0 end
redis.call('HSET', key, pid, val)
redis.call('INCRBY', totalKey, val - old)
return val - old
`)

// Service 全服里程碑。
type Service struct {
	db       *db.DB
	rdb      redis.UniversalClient
	producer *pkgmq.Producer
	consumer *pkgmq.Consumer
	logger   *log.Logger
	now      func() time.Time
}

// New 创建服务。
func New(database *db.DB, rdb redis.UniversalClient, producer *pkgmq.Producer, consumer *pkgmq.Consumer, logger *log.Logger) *Service {
	return &Service{db: database, rdb: rdb, producer: producer, consumer: consumer, logger: logger, now: time.Now}
}

// RunConsumer 阻塞消费贡献上报，直到 ctx 取消。
func (s *Service) RunConsumer(ctx context.Context) error {
	if s.consumer == nil {
		return nil
	}
	return s.consumer.Run(ctx, s.handleProgress)
}

func (s *Service) handleProgress(_ context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.ActivityProgressMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode activity progress: %w", err)
	}
	if msg.InstanceID == "" || msg.PlayerID <= 0 || msg.Progress <= 0 {
		return fmt.Errorf("invalid activity progress: %+v", msg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := incrScript.Run(ctx, s.rdb,
		[]string{progressKey(msg.InstanceID), totalKey(msg.InstanceID)},
		msg.PlayerID, msg.Progress).Int64(); err != nil {
		return fmt.Errorf("incr activity progress: %w", err)
	}
	return nil
}

// Run 由 leader 驱动：判定里程碑 + 发奖。
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
				s.logger.Warn("milestone tick failed", "err", err)
			}
		}
	}
}

// Tick 一轮：先按当前进度生成任务，再发待发任务。
// 生成与发送分开：发奖变慢不会阻塞新里程碑的达成判定。
func (s *Service) Tick(ctx context.Context) error {
	if err := s.createTasks(ctx); err != nil {
		return err
	}
	return s.grantPending(ctx)
}

// createTasks 遍历配置，达阈值就给贡献玩家建任务。
//
// 活动结束后不再生成新任务（迟到的贡献只更新进度，不补发）：
// 这是明确规则——否则活动下线后仍可能因为一条迟到上报触发全服发奖。
func (s *Service) createTasks(ctx context.Context) error {
	now := s.now().Unix()
	for _, cfg := range gamedata.ActivityMilestoneCfgList() {
		inst, ok := s.currentInstance(int32(cfg.Col1), now)
		if !ok {
			continue // 活动没有周期配置：不参与里程碑
		}
		if now >= inst.end {
			continue // 已结束：不再产生新任务
		}

		total, err := s.rdb.Get(ctx, totalKey(inst.id)).Int64()
		if err != nil || total < cfg.Col2 {
			continue
		}

		// 同一实例同一里程碑只展开一次：SETNX 成功者负责建任务。
		marker := fmt.Sprintf("activity:milestone:%s:%d", inst.id, cfg.Id)
		ok, err = s.rdb.SetNX(ctx, marker, 1, 30*24*time.Hour).Result()
		if err != nil || !ok {
			continue
		}

		players, err := s.rdb.HKeys(ctx, progressKey(inst.id)).Result()
		if err != nil || len(players) == 0 {
			continue
		}
		created := 0
		for _, pidStr := range players {
			var pid int64
			if _, err := fmt.Sscanf(pidStr, "%d", &pid); err != nil {
				continue
			}
			task := &model.ActivityGrantTask{
				Id:          fmt.Sprintf("%s:%d:%d", inst.id, cfg.Id, pid),
				InstanceId:  inst.id,
				MilestoneId: int32(cfg.Id),
				PlayerId:    pid,
				Status:      statusPending,
				UpdatedAt:   now,
			}
			// INSERT IGNORE：重复展开时保持已有状态（已完成不会被重置为待发）。
			if _, err := s.db.DB.ExecContext(ctx, `
				INSERT IGNORE INTO activity_grant_task (id, instance_id, milestone_id, player_id, status, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)`,
				task.Id, task.InstanceId, task.MilestoneId, task.PlayerId, task.Status, task.UpdatedAt); err != nil {
				s.logger.Warn("create grant task failed", "id", task.Id, "err", err)
				continue
			}
			created++
		}
		s.logger.Info("milestone reached",
			"instance_id", inst.id, "milestone_id", cfg.Id, "total", total, "tasks", created)
	}
	return nil
}

// grantPending 分批发奖：取一批 → 逐条发邮件 → 标记完成。
// 中断后下一轮从剩余 pending 继续；mail_id 稳定，重发不会重复到账。
func (s *Service) grantPending(ctx context.Context) error {
	rows, err := s.db.DB.QueryContext(ctx, `
		SELECT id, instance_id, milestone_id, player_id
		FROM activity_grant_task WHERE status = ? ORDER BY id LIMIT ?`, statusPending, batchSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	type task struct {
		id, instanceID string
		milestoneID    int32
		playerID       int64
	}
	var batch []task
	for rows.Next() {
		var t task
		if err := rows.Scan(&t.id, &t.instanceID, &t.milestoneID, &t.playerID); err != nil {
			return err
		}
		batch = append(batch, t)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, t := range batch {
		itemID, count := milestoneReward(t.milestoneID)
		if itemID > 0 && s.producer != nil {
			msg := pkgmq.MailMsg{
				MailID:   stableID("milestone:" + t.id),
				PlayerID: t.playerID,
				Title:    "全服里程碑奖励",
				Content:  "全服目标达成，感谢参与",
				Channel:  1,
				Items:    []pkgmq.Item{{ItemID: itemID, Count: count}},
			}
			env, err := pkgmq.NewEnvelope("milestone:"+t.id, pkgmq.TopicMail,
				fmt.Sprintf("%d", t.playerID), msg)
			if err == nil {
				if err := s.producer.ProduceEnvelope(ctx, env); err != nil {
					// 发送失败保留 pending：下一轮重试，mail_id 相同不会重复到账。
					s.logger.Warn("send milestone reward failed", "task", t.id, "err", err)
					continue
				}
			}
		}
		if _, err := s.db.DB.ExecContext(ctx,
			`UPDATE activity_grant_task SET status = ?, updated_at = ? WHERE id = ?`,
			statusDone, s.now().Unix(), t.id); err != nil {
			return err
		}
	}
	return nil
}

// instance 当前活动实例（由周期算法算出，不依赖调度消息）。
type instance struct {
	id    string
	start int64
	end   int64
}

func (s *Service) currentInstance(activityID int32, now int64) (instance, bool) {
	cfg, ok := gamedata.GetActivityPeriodCfg(int64(activityID))
	if !ok {
		return instance{}, false
	}
	start, end, period := activity.Window(int32(cfg.Col1), now)
	return instance{
		id:    fmt.Sprintf("%d-%d", activityID, period),
		start: start,
		end:   end,
	}, true
}

func milestoneReward(milestoneID int32) (int32, int64) {
	cfg, ok := gamedata.GetActivityMilestoneCfg(int64(milestoneID))
	if !ok {
		return 0, 0
	}
	return int32(cfg.Col3), cfg.Col4
}

func progressKey(instanceID string) string { return "activity:progress:" + instanceID }
func totalKey(instanceID string) string    { return "activity:total:" + instanceID }

// stableID 由任务 ID 派生稳定 mail_id（与归档奖励同一思路）。
func stableID(key string) int64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return int64(h & 0x7fffffffffffffff)
}

var _ = sql.ErrNoRows

// Close 关闭贡献消费者。
func (s *Service) Close() error {
	if s.consumer == nil {
		return nil
	}
	return s.consumer.Close()
}
