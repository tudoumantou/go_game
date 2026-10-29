// Package archive 赛季归档与统计落库（R8.3 / R8.4）。
//
// 只有中心服 leader 执行：赛季切换、榜单归档、结算奖励、统计落库。
// 换主或重启后从 MySQL 恢复断点：
//   - 赛季起点在 center_season（换主不会重新计时）；
//   - 每个 (season, rank_type) 的归档进度在 rank_archive_task；
//   - 归档本身按 rank_history 的唯一键 UPSERT，重跑不会产生重复行。
//
// 架构位置：archive 是 rank/stats 的下游消费者——读 Redis ZSet 快照、
// 把结果写进 MySQL，自身不持有排行数据；「先全部归档成功再切赛季」
// 的顺序保证任何一步失败都停在本期，下轮从断点续跑。
package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"game_study/internal/center/leader"
	"game_study/internal/center/rank"
	"game_study/internal/center/stats"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
	internalv2 "game_study/pkg/pb/internalv2"
	"game_study/pkg/periodic"
	"game_study/pkg/sqlstore"
)

// rankEntry 归档回调的条目类型（rank.Service 用内部协议结构）。
// 类型别名（type X = Y）而非新定义：与 internalv2.RankEntry 是同一类型，
// 归档回调与 rank 包之间零转换成本，proto 加字段时两侧自动同步。
type rankEntry = internalv2.RankEntry

// 归档任务状态机：pending(0) → running(1) → done(2)。
// 状态落库的意义大于内存：进程在 running 态崩溃后，重启靠它决定
// 「重跑本任务」而不是当成新任务重新展开。
const (
	StatusPending int32 = 0
	StatusRunning int32 = 1
	StatusDone    int32 = 2
)

// 单行表主键。
// center_season 是全服唯一的单行表，固定主键 1 让 UPSERT 退化为
// 「按主键覆盖」，免去先 SELECT 再决定 INSERT/UPDATE 的竞态窗口。
const seasonRowID int32 = 1

// rankTypeIDs 榜单类型 → 库里的数字类型（rank_history.rank_type 是 TINYINT）。
// 新增榜单时在这里登记，避免用「字符串哈希」当类型。
// 显式小表而非字符串哈希：库里的值人眼可读、SQL 可直接按数字筛；
// 哈希值会随代码演进漂移，历史行事后再也对不上号。
var rankTypeIDs = map[string]int32{
	"arena_score": 1,
}

// Config 归档与落库配置（center 启动时由 cfg.Rank 映射而来）。
type Config struct {
	SeasonDays   int      // 赛季长度（天），<=0 用 30：赛季越短奖励发放越频繁，运营可调
	RankTypes    []string // 需要归档的榜单，空用 arena_score：新增榜单在此登记（见 rankTypeIDs）
	TopN         int64    // 归档前 N 名，<=0 用 1000：只存头部，长尾占库不划算
	RewardTop    int      // 前 N 名发奖，<=0 不发：0 是「关闭发奖」的显式开关
	RewardItemID int32    // 奖励道具
	RewardCount  int64    // 每人数量
	GroupID      string   // 分组（rank_history/feature_stats 的分组维度）
	StatsEnabled bool     // 是否落 feature_stats（依赖 statsSvc 非 nil，由调用方置位）
}

// Service 赛季归档与统计落库。
//
// 字段分三组：
//   - 数据源：db（MySQL，断点与结果）、rank（Redis 榜单快照）、stats（窗口聚合）；
//   - 出口：producer（Kafka 发奖邮件），nil 时归档照常、只跳过发奖；
//   - 横切：cfg（配置）、logger、now（时钟注入——测试据此模拟「赛季已到期」）。
type Service struct {
	*sqlstore.Base
	rank     *rank.Service
	stats    *stats.Service
	producer *pkgmq.Producer
	cfg      Config
	logger   *log.Logger

	now func() time.Time // 测试注入
}

// New 创建服务；db/rank 必填，其余可空（对应能力自动跳过）。
// 配置兜底在此一次做完：SeasonDays/RankTypes/TopN/GroupID 的非法值统一
// 归一化，Tick 及下游函数里就不必到处判空。
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
		Base: sqlstore.New(database, "archive"), rank: rankSvc, stats: statsSvc,
		producer: producer, cfg: cfg, logger: logger,
		now: time.Now,
	}
}

// Run 由 leader 生命周期驱动：非 leader 期间空转，成为 leader 后自动接上。
//
// 循环骨架交给 pkg/periodic：ticker 释放、ctx 双监听、isLeader 缺省即禁止
// 这三件事各只在一个地方实现。这里只声明本任务的四个决定——节拍取
// leader.TickInterval（三个有主任务共用一个值）、leader 才能跑、失败只告警
// 不退出（归档可重试，退出循环等于放弃本实例的有主职责）。
func (s *Service) Run(ctx context.Context, isLeader func() bool) {
	periodic.Run(ctx, periodic.Options{
		Name:     "archive",
		Interval: leader.TickInterval,
		Logger:   s.logger,
		Gate:     periodic.Guard(isLeader),
		Fn:       s.Tick,
	})
}

// Tick 一次调度：赛季到期就归档切换，然后落统计。
// 任一步失败都不影响另一步——统计落库失败不该阻止赛季归档。
// 两者独立还有一层考虑：归档失败要重试（firstErr 向上传播触发告警），
// 统计失败可容忍（30s 后下一轮自然会再来）。
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
//
// sql.ErrNoRows 必须用 errors.Is 判断：驱动包装错误时 == 会漏判
// （errors.Is 能穿透多层 %w）。首次启动的起点记「现在」而不是 rank
// 服务的创建时间——归档只关心「从哪刻起算一期」。
func (s *Service) currentSeason(ctx context.Context) (string, int64, error) {
	row, err := model.FindCenterSeasonByPK(ctx, s.SQL(), seasonRowID)
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

// saveSeason 写入赛季起点（单行表 UPSERT）。
// UpdatedAt 每次刷新：排查时能看出这条全局状态最后被谁、何时改动。
func (s *Service) saveSeason(ctx context.Context, season string, startedAt int64) error {
	_, err := model.UpsertCenterSeasonByPK(ctx, s.SQL(), &model.CenterSeason{
		Id:        seasonRowID,
		Season:    season,
		StartedAt: startedAt,
		UpdatedAt: s.now().Unix(),
	})
	return err
}

// rotateIfDue 赛季到期则归档旧榜并切到新赛季。
//
// 到期判定用 Unix 秒数差：不受时区与夏令时影响。顺序是刻意为之——
// 先把本期所有榜单归档完，最后才切赛季（见循环内注释）。
func (s *Service) rotateIfDue(ctx context.Context) error {
	season, startedAt, err := s.currentSeason(ctx)
	if err != nil {
		return err
	}
	days := int64(s.cfg.SeasonDays) * 24 * 3600
	// 未到期直接返回：绝大多数 tick 走这条，归档整体是低频路径
	if s.now().Unix() < startedAt+days {
		return nil
	}

	for _, rankType := range s.cfg.RankTypes {
		if err := s.archiveOne(ctx, season, rankType); err != nil {
			// 任一榜单归档失败就停在这里：不切赛季，下一轮继续重试。
			// 切了赛季会让失败的榜单永远失去归档机会——旧榜 key 已无引用，
			// 未归档数据随赛季切换永久丢失。%w 保留错误链供上层 errors.Is 判定。
			return fmt.Errorf("archive %s/%s: %w", season, rankType, err)
		}
	}

	next := nextSeason(season)
	// 先改内存态再落库：落库失败时下轮重走本函数，内存态已被覆盖也无妨（幂等）
	s.rank.SetSeason(next)
	if err := s.saveSeason(ctx, next, s.now().Unix()); err != nil {
		return fmt.Errorf("save new season %s: %w", next, err)
	}
	s.logger.Info("season rotated", "from", season, "to", next)
	return nil
}

// archiveOne 归档单个榜单：写 task 断点 → 取旧榜 → 批量 UPSERT → 发奖 → 标记完成。
//
// 断点机制让「归档一半进程被杀」可恢复：状态机 pending → running → done，
// running 态重启后重跑（rank_history 按唯一键 UPSERT，不产生重复行），
// done 态直接跳过——一个赛季只会完整结算一次。
func (s *Service) archiveOne(ctx context.Context, season, rankType string) error {
	taskID := season + ":" + rankType
	task, err := model.FindRankArchiveTaskByPK(ctx, s.SQL(), taskID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	// done 是终态：重复调用（leader 重试/人工触发）直接返回，保证只结算一次
	if task != nil && task.Status == StatusDone {
		return nil // 已确认归档完成
	}

	// 先落 running 再动手：进程在归档中途退出时，下一轮会重跑（UPSERT 幂等）。
	// 显式写状态而不是靠「有没有 done」推断——排查时能区分「没跑过」与「跑挂了」
	if err := s.saveTask(ctx, taskID, season, rankType, StatusRunning, 0); err != nil {
		return err
	}

	written := 0
	// Archive 按名次升序回调整个榜单；TopN 之外的尾部条目不进历史（只存头部）
	err = s.rank.Archive(ctx, rankType, season, s.cfg.TopN, func(entries []*rankEntry) error {
		// 预分配 len(entries) 容量：回调可能分批到达，一次分配免去 append 反复扩容
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
		// 一批一个事务：要么整批入库要么整体回滚，不留半批历史；
		// 回调返回错误即中断归档（rank.Archive 向上传播），由 leader 下轮重试
		if err := s.Tx(ctx, func(tx *sql.Tx) error {
			return model.UpsertRankHistoryBatch(ctx, tx, rows)
		}); err != nil {
			return err
		}
		written = len(rows)
		// 发奖放在入库之后：历史已落库才发奖；失败下轮补发（mail_id 稳定不重复）
		s.reward(ctx, season, rankType, entries)
		return nil
	})
	if err != nil {
		return err
	}

	return s.saveTask(ctx, taskID, season, rankType, StatusDone, written)
}

// saveTask 更新归档断点（UPSERT 单行）。
// rows 记录实际写入行数：done 时非 0 说明本期确实产出了历史，运维可核对。
func (s *Service) saveTask(ctx context.Context, id, season, rankType string, status int32, rows int) error {
	_, err := model.UpsertRankArchiveTaskByPK(ctx, s.SQL(), &model.RankArchiveTask{
		Id:           id,
		Season:       season,
		RankType:     rankType,
		Status:       status,
		ArchivedRows: int32(rows),
		UpdatedAt:    s.now().Unix(),
	})
	return err
}

// reward 赛季结算奖励：给前 RewardTop 名各发一封邮件。
//
// 邮件走 Kafka 的 gs.mail，mail_id 由 (season, rank_type, player_id) 派生：
// 重跑归档时同一个 mail_id 会被消费端幂等去重，不会重复发。
//
// 前置条件（producer/RewardTop/道具/数量）任一不满足即整体跳过：
// 奖励配置缺项时静默跳过，好过发一封空奖励邮件。entries 已按名次升序，
// 因此遇到第一个超限名次即可 break，无需扫完整个榜单。
func (s *Service) reward(ctx context.Context, season, rankType string, entries []*rankEntry) {
	if s.producer == nil || s.cfg.RewardTop <= 0 || s.cfg.RewardItemID <= 0 || s.cfg.RewardCount <= 0 {
		return
	}
	topic := pkgmq.TopicMail
	for _, e := range entries {
		// entries 按名次升序：第一个超限的之后全部超限，break 提前收尾
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
			// 单封失败只告警不中断：一名玩家的邮件故障不该阻塞其余人的奖励。
			s.logger.Warn("send rank reward failed", "player_id", e.PlayerId, "err", err)
		}
	}
}

// flushStats 把各窗口的统计写入 feature_stats（R8.4）。
//
// window_start 对齐到窗口边界，value 取该窗口当前聚合值：
// 同一窗口内多次写入是 UPSERT 覆盖，不会把 5 分钟一次的采样累加成天文数字。
//
// 读的是 Redis 多实例合并值（MergedMetrics）而不是本地 Query：
// 多中心服部署下各实例只写自己那份快照，只有求和才是全服真实值。
func (s *Service) flushStats(ctx context.Context) error {
	now := s.now()
	for _, feature := range s.stats.Features() {
		// 读 Redis 上的多实例合并值：各实例只写自己那份，这里求和。
		// 用本地 Query 会把其它实例的贡献漏掉（多中心服部署下落库值偏小）。
		metrics := s.stats.MergedMetrics(ctx, s.cfg.GroupID, feature)
		// 没有任何实例上报过该 feature：连空行也不写，保持表干净
		if len(metrics) == 0 {
			continue
		}
		{
			for window, value := range metrics {
				start, end := windowRange(now, window)
				// 唯一键是 (group_id, feature, window_start)，不是自增主键，
				// 所以用 ON DUPLICATE KEY 覆盖同一窗口，而不是插入新行。
				// 补跑/重跑同一窗口必须是覆盖语义，否则每次落库多一行，
				// GM 查询把多行累加成错误值。
				if _, err := s.SQL().ExecContext(ctx, `
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
//
// 1m/5m/1h 用 time.Truncate（按 Unix 零点截断，不受时区影响）；1d 必须
// 特殊处理——Truncate(24h) 在跨夏令时的时区会切出偏移的「零点」，
// 用 Date() 取本地零点更稳。default 兜 1m：未知窗口宁可按最小粒度对齐，
// 也不产出错位的 window_start（错位的起点会让 UPSERT 覆盖写串行）。
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
//
// 「一定变化」是硬约束：赛季名不变会让 archiveOne 的 taskID
// （season:rankType）撞上上一期已 done 的任务，本期归档被直接跳过。
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
//
// rank_history.season 是数字列，存不了字符串赛季名；解析失败退 0
// 只是丢一个筛选维度，好过让整个归档失败。
func seasonNumber(season string) int32 {
	var n int32
	if _, err := fmt.Sscanf(season, "s%d", &n); err == nil {
		return n
	}
	return 0
}

// stableID 由业务键派生稳定的 int64（邮件 mail_id 需要数值）。
//
// FNV-1a 64 位后掩掉符号位：mail_id 下游按非负处理；
// 「同键同值」是消费端幂等去重的全部依据，换算法等于历史邮件全部可重发。
func stableID(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}
