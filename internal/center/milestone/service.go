// Package milestone 全服活动里程碑（R7.3 / R7.4）。
//
// 两条独立路径：
//   - 消费 gs.activity.progress：把各玩家「当前累计进度」按差值累加成全服进度；
//   - leader 定时判定：达到阈值就给贡献玩家生成发奖任务，再逐条发邮件并标记完成。
//
// 为什么上报绝对值而不是增量：重投相同值得到差值 0，天然幂等；
// 增量上报需要业务键，而「玩家在某个活动里的进度」本身就是唯一状态。
//
// 协作关系：消费路径（RunConsumer）与判定路径（Run）跑在不同 goroutine——
// 上报量大但每次只是 Redis 累加，判定低频但要建任务发奖，分开后互不阻塞。
// 活动实例（instance）由周期算法算出，与 scheduler 包同源同算法，
// 两侧不需要任何通信就对「当前是第几期」达成一致。
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
	"game_study/pkg/sqlstore"

	"github.com/redis/go-redis/v9"
)

const (
	statusPending int32 = 0
	statusDone    int32 = 1

	// batchSize 单轮发奖条数：控制单次事务时长，失败后只需重试这一批。
	// 100 是经验值：一封邮件对应一条 UPDATE，再大单轮耗时不可控，再小则轮次太频。
	batchSize = 100
)

// 累加脚本：只有进度变大才更新，返回本次增量。
//
//	KEYS[1] = activity:progress:{instance}（Hash: player_id → 进度）
//	KEYS[2] = activity:total:{instance}（全服累计）
//	ARGV[1] = player_id, ARGV[2] = 当前进度
//
// 用 Lua 而不是 HGET+HSET+INCRBY 三次往返：三条命令合并为一次原子执行，
// 中间不会插入其它玩家的上报，「读旧值 → 判断 → 写新值」不存在竞态。
// 进度没变大返回 0：调用方无需关心是否重复上报，天然幂等（包注释的前提）。
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
//
// 四个依赖各自可空、空即降级：无 consumer 不接收上报（判定自然无数据），
// 无 producer 只建任务不发奖，无 rdb 判定路径不工作，db 存任务断点。
// now 可注入：测试据此把活动推到「已结束/未开始」等时间边界。
type Service struct {
	*sqlstore.Base
	rdb      redis.UniversalClient
	producer *pkgmq.Producer
	consumer *pkgmq.Consumer
	logger   *log.Logger
	now      func() time.Time
}

// New 创建服务。
func New(database *db.DB, rdb redis.UniversalClient, producer *pkgmq.Producer, consumer *pkgmq.Consumer, logger *log.Logger) *Service {
	return &Service{Base: sqlstore.New(database, "milestone"), rdb: rdb, producer: producer, consumer: consumer, logger: logger, now: time.Now}
}

// RunConsumer 阻塞消费贡献上报，直到 ctx 取消。
// consumer 为 nil 时直接返回 nil：Kafka 未配置的部署（纯中心服模式）
// 不需要这条路，返回 nil 而不是错误——没配是无害的，报错会让上层重启。
func (s *Service) RunConsumer(ctx context.Context) error {
	if s.consumer == nil {
		return nil
	}
	return s.consumer.Run(ctx, s.handleProgress)
}

func (s *Service) handleProgress(_ context.Context, env *pkgmq.Envelope) error {
	// 三步：解码 → 校验字段 → Lua 累加。校验失败返回 error 让消费框架记录，
	// 但框架不应重投——非法消息重投一万次也还是非法，跳过即可
	var msg pkgmq.ActivityProgressMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode activity progress: %w", err)
	}
	if msg.InstanceID == "" || msg.PlayerID <= 0 || msg.Progress <= 0 {
		return fmt.Errorf("invalid activity progress: %+v", msg)
	}
	// 独立 3s 超时：Redis 慢查询不该拖住整个消费循环
	//（同一分区后续消息都在排队，%w 保留链供上层判定）
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
// 30s 一拍与 scheduler/archive 对齐：leader 类任务统一节拍，
// 运维排查时按同一节奏看日志即可。非 leader 期间空转不判错。
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
// 生成与发送分开：发奖变慢（Kafka 抖动）不会阻塞新里程碑的达成判定——
// 阈值已到却因发奖慢而延迟建任务，玩家会觉得「达标了但奖励不出现」。
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
//
// SETNX 抢展开权：同一里程碑可能被多个 leader 候选同时判定到，
// 只有抢到 marker 的实例负责建任务，其余跳过——避免重复展开。
func (s *Service) createTasks(ctx context.Context) error {
	now := s.now().Unix()
	for _, cfg := range gamedata.ActivityMilestoneCfgList() {
		inst, ok := s.currentInstance(int32(cfg.Col1), now)
		if !ok {
			continue // 活动没有周期配置：不参与里程碑
		}
		// 已结束：不再产生新任务（迟到的贡献仍会更新进度，只是不触发奖励）
		if now >= inst.end {
			continue
		}

		total, err := s.rdb.Get(ctx, totalKey(inst.id)).Int64()
		// err 与不足阈值都跳过：Redis 瞬时故障时宁可本轮不判定（下轮再来），
		// 也不按错误数据发奖
		if err != nil || total < cfg.Col2 {
			continue
		}

		// 同一实例同一里程碑只展开一次：SETNX 成功者负责建任务。
		// 抢不到的实例直接跳过——多中心服部署下保证只有一方展开，
		// 任务表不会出现并发重复插入；30 天 TTL 覆盖一个赛季周期
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
			// 与 UPSERT 的区别：任务行一旦存在就保留原 status——已发奖（done）
			// 的任务不能被一次重跑打回待发，那会重复发奖
			if _, err := s.SQL().ExecContext(ctx, `
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
//
// 顺序「先发后标记」：标记意味着承诺已送达；反过来先标记后发送，
// 进程在两者之间崩溃就会丢奖励。发送失败保持 pending 重试。
func (s *Service) grantPending(ctx context.Context) error {
	rows, err := s.SQL().QueryContext(ctx, `
		SELECT id, instance_id, milestone_id, player_id
		FROM activity_grant_task WHERE status = ? ORDER BY id LIMIT ?`, statusPending, batchSize)
	// ORDER BY id + LIMIT：按稳定顺序取批，中断后下一轮从断点续，
	// 不会漏也不会跳；id 前缀含 instance，天然按活动聚集
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
					// continue 跳过标记：这条任务下轮会重新取到
					s.logger.Warn("send milestone reward failed", "task", t.id, "err", err)
					continue
				}
			}
		}
		if _, err := s.SQL().ExecContext(ctx,
			`UPDATE activity_grant_task SET status = ?, updated_at = ? WHERE id = ?`,
			statusDone, s.now().Unix(), t.id); err != nil {
			// 逐条 UPDATE 而非批量：单条失败即中断本批，已成功的保持 done，
			// 未完成的继续 pending——断点精度到条，重试代价最小
			return err
		}
	}
	return nil
}

// instance 当前活动实例（由周期算法算出，不依赖调度消息）。
//
// 用算法而非消息同步：调度消息可能丢/延迟，周期算法是确定函数，
// 任何进程任何时间算出来都一致（R7.1「游戏服自己也会算」同源）。
type instance struct {
	id    string
	start int64
	end   int64
}

// currentInstance 由活动配置 + 当前时间算出所处周期（与 scheduler 同算法）。
// 没有周期配置的活动返回 false：不参与里程碑（一次性活动没有「全服累计」容器）。
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

// milestoneReward 从配置取奖励（道具 + 数量）。
// 配置缺失返回 (0, 0)，调用方据此跳过发送——没有奖励配置的里程碑
// 只记录不发奖，好过发一封空邮件。
func milestoneReward(milestoneID int32) (int32, int64) {
	cfg, ok := gamedata.GetActivityMilestoneCfg(int64(milestoneID))
	if !ok {
		return 0, 0
	}
	return int32(cfg.Col3), cfg.Col4
}

// progressKey/totalKey 按实例隔离：换期（新 period）后 key 不同，
// 上一期的进度与累计自动成为历史数据，不需要显式清理。
func progressKey(instanceID string) string { return "activity:progress:" + instanceID }
func totalKey(instanceID string) string    { return "activity:total:" + instanceID }

// stableID 由任务 ID 派生稳定 mail_id（与归档奖励同一思路）。
//
// 手写 FNV-1a 而非引入 hash/fnv：常数即标准 offset basis/prime，
// 与归档侧算法一致——同一输入在两侧得到同一 mail_id，便于对账。
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
// 只关 consumer：producer/db 的生命周期由 center 统一管理，
// 这里重复 Close 会和上层抢资源（多数实现的第二次 Close 直接报错）。
func (s *Service) Close() error {
	if s.consumer == nil {
		return nil
	}
	return s.consumer.Close()
}
