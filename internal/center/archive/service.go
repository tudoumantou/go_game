// Package archive 赛季归档与统计落库（R8.3 / R8.4）。
//
// 只有中心服 leader 执行：赛季切换、榜单归档、结算奖励、统计落库。
// 换主或重启后从 MySQL 恢复断点：
//   - 赛季起点在 center_season（换主不会重新计时）；
//   - 每个 (season, rank_type) 的归档进度在 rank_archive_task；
//   - 归档本身按 rank_history 的唯一键 UPSERT，重跑不会产生重复行。
package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"game_study/internal/center/rank"
	"game_study/internal/center/stats"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
	internalv2 "game_study/pkg/pb/internalv2"
)

// rankEntry 归档回调的条目类型（rank.Service 用内部协议结构）。
type rankEntry = internalv2.RankEntry

// 归档任务状态。
const (
	StatusPending int32 = 0
	StatusRunning int32 = 1
	StatusDone    int32 = 2
)

// 单行表主键。
const seasonRowID int32 = 1

// rankTypeIDs 榜单类型 → 库里的数字类型（rank_history.rank_type 是 TINYINT）。
// 新增榜单时在这里登记，避免用「字符串哈希」当类型。
var rankTypeIDs = map[string]int32{
	"arena_score": 1,
}

// Config 归档与落库配置。
type Config struct {
	SeasonDays   int      // 赛季长度（天），<=0 用 30
	RankTypes    []string // 需要归档的榜单，空用 arena_score
	TopN         int64    // 归档前 N 名，<=0 用 1000
	RewardTop    int      // 前 N 名发奖，<=0 不发
	RewardItemID int32    // 奖励道具
	RewardCount  int64    // 每人数量
	GroupID      string   // 分组（rank_history/feature_stats 的分组维度）
	StatsEnabled bool     // 是否落 feature_stats
}

// Service 赛季归档与统计落库。
type Service struct {
	db       *db.DB
	rank     *rank.Service
	stats    *stats.Service
	producer *pkgmq.Producer
	cfg      Config
	logger   *log.Logger

	now func() time.Time // 测试注入
}

// New 创建服务；db/rank 必填，其余可空（对应能力自动跳过）。
func New(database *db.DB, rankSvc *rank.Service, statsSvc *stats.Service,
	producer *pkgmq.Producer, cfg Config, logger *log.Logger) *Service {
	if cfg.SeasonDays <= 0 {
		cfg.SeasonDays = 30
	}
	if len(cfg.RankTypes) == 0 {
		cfg.RankTypes = []string{"arena_score"}
	}
	if cfg.TopN <= 0 {
		cfg.TopN = 1000
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "group-01"
	}
	return &Service{
		db: database, rank: rankSvc, stats: statsSvc,
		producer: producer, cfg: cfg, logger: logger,
		now: time.Now,
	}
}

// Run 由 leader 生命周期驱动：非 leader 期间空转，成为 leader 后自动接上。
// 间隔取 30s：赛季切换与统计落库都不需要秒级精度，过频只会增加无谓查询。
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
				s.logger.Warn("archive tick failed", "err", err)
			}
		}
	}
}

// Tick 一次调度：赛季到期就归档切换，然后落统计。
// 任一步失败都不影响另一步——统计落库失败不该阻止赛季归档。
func (s *Service) Tick(ctx context.Context) error {
	var firstErr error

	if err := s.rotateIfDue(ctx); err != nil {
		s.logger.Error("season rotate failed", "err", err)
		firstErr = err
	}
	if s.cfg.StatsEnabled && s.stats != nil {
		if err := s.flushStats(ctx); err != nil {
			s.logger.Warn("stats flush failed", "err", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// currentSeason 读当前赛季；首次运行时落一行默认值。
func (s *Service) currentSeason(ctx context.Context) (string, int64, error) {
	row, err := model.FindCenterSeasonByPK(ctx, s.db.DB, seasonRowID)
	if err == nil {
		return row.Season, row.StartedAt, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", 0, err
	}
	// 首次启动：沿用 rank 服务的默认赛季，起点记为现在。
	season := s.rank.Season()
	now := s.now().Unix()
	if err := s.saveSeason(ctx, season, now); err != nil {
		return "", 0, err
	}
	return season, now, nil
}

func (s *Service) saveSeason(ctx context.Context, season string, startedAt int64) error {
	_, err := model.UpsertCenterSeasonByPK(ctx, s.db.DB, &model.CenterSeason{
		Id:        seasonRowID,
		Season:    season,
		StartedAt: startedAt,
		UpdatedAt: s.now().Unix(),
	})
	return err
}

// rotateIfDue 赛季到期则归档旧榜并切到新赛季。
func (s *Service) rotateIfDue(ctx context.Context) error {
	season, startedAt, err := s.currentSeason(ctx)
	if err != nil {
		return err
	}
	days := int64(s.cfg.SeasonDays) * 24 * 3600
	if s.now().Unix() < startedAt+days {
		return nil
	}

	for _, rankType := range s.cfg.RankTypes {
		if err := s.archiveOne(ctx, season, rankType); err != nil {
			// 任一榜单归档失败就停在这里：不切赛季，下一轮继续重试。
			// 切了赛季会让失败的榜单永远失去归档机会。
			return fmt.Errorf("archive %s/%s: %w", season, rankType, err)
		}
	}

	next := nextSeason(season)
	s.rank.SetSeason(next)
	if err := s.saveSeason(ctx, next, s.now().Unix()); err != nil {
		return fmt.Errorf("save new season %s: %w", next, err)
	}
	s.logger.Info("season rotated", "from", season, "to", next)
	return nil
}

// archiveOne 归档单个榜单：写 task 断点 → 取旧榜 → 批量 UPSERT → 发奖 → 标记完成。
func (s *Service) archiveOne(ctx context.Context, season, rankType string) error {
	taskID := season + ":" + rankType
	task, err := model.FindRankArchiveTaskByPK(ctx, s.db.DB, taskID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if task != nil && task.Status == StatusDone {
		return nil // 已确认归档完成
	}

	// 先落 running 再动手：进程在归档中途退出时，下一轮会重跑（UPSERT 幂等）。
	if err := s.saveTask(ctx, taskID, season, rankType, StatusRunning, 0); err != nil {
		return err
	}

	written := 0
	err = s.rank.Archive(ctx, rankType, season, s.cfg.TopN, func(entries []*rankEntry) error {
		rows := make([]model.RankHistory, 0, len(entries))
		now := s.now()
		for _, e := range entries {
			rows = append(rows, model.RankHistory{
				GroupId:    0, // 全局榜：group_id 保留 0，分组榜接入时填真实 group
				RankType:   rankTypeIDs[rankType],
				Season:     seasonNumber(season),
				PlayerId:   e.PlayerId,
				Score:      e.Score,
				RankNo:     int32(e.Rank),
				SnapshotAt: now,
			})
		}
		if err := s.db.Tx(ctx, func(tx *sql.Tx) error {
			return model.UpsertRankHistoryBatch(ctx, tx, rows)
		}); err != nil {
			return err
		}
		written = len(rows)
		s.reward(ctx, season, rankType, entries)
		return nil
	})
	if err != nil {
		return err
	}

	return s.saveTask(ctx, taskID, season, rankType, StatusDone, written)
}

func (s *Service) saveTask(ctx context.Context, id, season, rankType string, status int32, rows int) error {
	_, err := model.UpsertRankArchiveTaskByPK(ctx, s.db.DB, &model.RankArchiveTask{
		Id:        id,
		Season:    season,
		RankType:  rankType,
		Status:    status,
		Rows:      int32(rows),
		UpdatedAt: s.now().Unix(),
	})
	return err
}

// reward 赛季结算奖励：给前 RewardTop 名各发一封邮件。
//
// 邮件走 Kafka 的 gs.mail，mail_id 由 (season, rank_type, player_id) 派生：
// 重跑归档时同一个 mail_id 会被消费端幂等去重，不会重复发。
func (s *Service) reward(ctx context.Context, season, rankType string, entries []*rankEntry) {
	if s.producer == nil || s.cfg.RewardTop <= 0 || s.cfg.RewardItemID <= 0 || s.cfg.RewardCount <= 0 {
		return
	}
	topic := pkgmq.TopicMail
	for _, e := range entries {
		if int(e.Rank) > s.cfg.RewardTop {
			break
		}
		mailID := stableID(fmt.Sprintf("rank_reward:%s:%s:%d", season, rankType, e.PlayerId))
		msg := pkgmq.MailMsg{
			MailID:     mailID,
			PlayerID:   e.PlayerId,
			TemplateID: 0,
			Title:      "赛季结算奖励",
			Content:    fmt.Sprintf("%s 第 %d 名奖励", season, e.Rank),
			Channel:    1,
			Items:      []pkgmq.Item{{ItemID: s.cfg.RewardItemID, Count: s.cfg.RewardCount}},
		}
		env, err := pkgmq.NewEnvelope(fmt.Sprintf("rank_reward:%s:%s:%d", season, rankType, e.PlayerId),
			topic, fmt.Sprintf("%d", e.PlayerId), msg)
		if err != nil {
			s.logger.Warn("build reward envelope failed", "player_id", e.PlayerId, "err", err)
			continue
		}
		if err := s.producer.ProduceEnvelope(ctx, env); err != nil {
			// 发奖失败不回滚归档：历史已落库，奖励下一轮归档重跑时会补发（mail_id 相同不重复）。
			s.logger.Warn("send rank reward failed", "player_id", e.PlayerId, "err", err)
		}
	}
}

// flushStats 把各窗口的统计写入 feature_stats（R8.4）。
//
// window_start 对齐到窗口边界，value 取该窗口当前聚合值：
// 同一窗口内多次写入是 UPSERT 覆盖，不会把 5 分钟一次的采样累加成天文数字。
func (s *Service) flushStats(ctx context.Context) error {
	now := s.now()
	for _, feature := range s.stats.Features() {
		reports := s.stats.Query(s.cfg.GroupID, feature, 0, now.Unix())
		if len(reports) == 0 {
			continue
		}
		for _, rep := range reports {
			for window, value := range rep.Metrics {
				start, end := windowRange(now, window)
				// 唯一键是 (group_id, feature, window_start)，不是自增主键，
				// 所以用 ON DUPLICATE KEY 覆盖同一窗口，而不是插入新行。
				if _, err := s.db.DB.ExecContext(ctx, `
					INSERT INTO feature_stats (group_id, feature, window_start, window_end, value, updated_at)
					VALUES (?, ?, ?, ?, ?, NOW())
					ON DUPLICATE KEY UPDATE window_end = VALUES(window_end), value = VALUES(value)`,
					0, feature, start, end, value); err != nil {
					return fmt.Errorf("upsert %s/%s/%s: %w", feature, window, start, err)
				}
			}
		}
	}
	return nil
}

// windowRange 把「现在」对齐到统计窗口：1m/5m/1h 对齐到整分/整 5 分/整点，1d 对齐到当天零点。
func windowRange(now time.Time, window string) (time.Time, time.Time) {
	switch window {
	case stats.Window1m:
		start := now.Truncate(time.Minute)
		return start, start.Add(time.Minute)
	case stats.Window5m:
		start := now.Truncate(5 * time.Minute)
		return start, start.Add(5 * time.Minute)
	case stats.Window1h:
		start := now.Truncate(time.Hour)
		return start, start.Add(time.Hour)
	case stats.Window1d:
		y, m, d := now.Date()
		start := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
		return start, start.Add(24 * time.Hour)
	default:
		start := now.Truncate(time.Minute)
		return start, start.Add(time.Minute)
	}
}

// nextSeason s1 → s2；无法解析时追加时间戳，保证赛季名一定变化。
func nextSeason(cur string) string {
	if len(cur) > 1 && cur[0] == 's' {
		var n int
		if _, err := fmt.Sscanf(cur[1:], "%d", &n); err == nil {
			return fmt.Sprintf("s%d", n+1)
		}
	}
	return fmt.Sprintf("%s-%d", cur, time.Now().Unix())
}

// seasonNumber s1 → 1；解析失败返回 0（不阻断归档）。
func seasonNumber(season string) int32 {
	var n int32
	if _, err := fmt.Sscanf(season, "s%d", &n); err == nil {
		return n
	}
	return 0
}

// stableID 由业务键派生稳定的 int64（邮件 mail_id 需要数值）。
func stableID(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}
