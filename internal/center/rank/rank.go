// Package rank 全服排行榜（设计文档 9.4）
//
// 存储：Redis ZSet，member = `{playerId}:{serverId}`，score = 分数。
//
//	rank:global:{season}:{rankType}         全服总榜
//	rank:group:{groupID}:{season}:{rankType} 分组榜
//
// 查询：ZREVRANGE（Top-N）/ ZREVRANK（我的名次）/ ZRANGEBYSCORE（周边），均 O(logN)。
//
// 赛季：`season` 作为 key 的一部分天然隔离；切换赛季时把旧 ZSet 归档到 MySQL
// `rank_history` 再删 key（归档由 leader 执行，见 9.6）。
package rank

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/redis/go-redis/v9"
)

// 榜单作用域。
const (
	ScopeGlobal = "global"
	// ScopeGroupPrefix 分组榜 key 前缀（实际 key 为 rank:group:{id}:{season}:{type}）
	ScopeGroupPrefix = "group"
)

// Service 排行榜服务。
type Service struct {
	rdb    redis.UniversalClient
	logger *log.Logger
	// currentSeason 当前赛季（切换由 leader 触发）
	currentSeason string
}

// New 创建排行榜服务。
func New(rdb redis.UniversalClient, season string, logger *log.Logger) *Service {
	if season == "" {
		season = "s1"
	}
	return &Service{rdb: rdb, logger: logger, currentSeason: season}
}

// Season 当前赛季。
func (s *Service) Season() string { return s.currentSeason }

// SetSeason 切换赛季（leader 归档后调用）。
func (s *Service) SetSeason(season string) {
	if season != "" {
		s.currentSeason = season
	}
}

// Write 累加一条排行分数（实现 center/rpc.RankWriter）。
//
// 语义：`RankReport.score` 是**增量**（delta），用 ZINCRBY 累加。
// 这与「竞技场胜负加减分」一致；若要上报绝对值（如公会战力），
// 需新增 `absolute` 标志位区分，P14 暂不引入。
//
// 幂等性：上报走 Report 流且节点侧保证不重复发送；重复发送会导致重复累加，
// 因此游戏服侧必须只在结算成功后上报一次。
func (s *Service) Write(rep *internalv2.RankReport) {
	if rep == nil || rep.PlayerId == 0 {
		return
	}
	type_ := rep.RankType
	if type_ == "" {
		return
	}
	season := rep.Season
	if season == "" {
		season = s.currentSeason
	}
	scope := rep.Scope
	if scope == "" {
		scope = ScopeGlobal
	}

	key := Key(scope, season, type_)
	member := Member(rep.PlayerId, rep.NodeId)

	ctx := context.Background()
	if err := s.rdb.ZIncrBy(ctx, key, float64(rep.Score), member).Err(); err != nil {
		s.logger.Warn("rank zincrby failed", "key", key, "player", rep.PlayerId, "err", err)
	}
}

// TopN 取前 N 名。
func (s *Service) TopN(rankType, season string, n int32) []*internalv2.RankEntry {
	if n <= 0 {
		n = 100
	}
	if season == "" {
		season = s.currentSeason
	}
	key := Key(ScopeGlobal, season, rankType)

	ctx := context.Background()
	rows, err := s.rdb.ZRevRangeWithScores(ctx, key, 0, int64(n-1)).Result()
	if err != nil {
		s.logger.Warn("rank zrevrange failed", "key", key, "err", err)
		return nil
	}

	out := make([]*internalv2.RankEntry, 0, len(rows))
	for i, z := range rows {
		playerID, _ := parseMember(fmt.Sprint(z.Member))
		out = append(out, &internalv2.RankEntry{
			PlayerId: playerID,
			// Name 留空：中心服不持有玩家数据（9.1），展示名由客户端按 playerId 补齐
			Score: int64(z.Score),
			Rank:  int64(i + 1),
		})
	}
	return out
}

// Around 取指定玩家周边排名（含自己），按名次升序返回。
func (s *Service) Around(rankType, season string, playerID int64, n int32) []*internalv2.RankEntry {
	if n <= 0 {
		n = 20
	}
	if season == "" {
		season = s.currentSeason
	}
	key := Key(ScopeGlobal, season, rankType)
	ctx := context.Background()

	// 先找该玩家的 member（serverId 未知，用 SCAN 匹配前缀代价高；
	// 这里约定同一玩家只在一个服有排行数据，用 ZSCAN 匹配 player_id 前缀）
	member, ok := s.findMember(ctx, key, playerID)
	if !ok {
		return nil
	}

	rank, err := s.rdb.ZRevRank(ctx, key, member).Result()
	if err != nil {
		s.logger.Debug("rank zrevrank failed", "key", key, "member", member, "err", err)
		return nil
	}

	half := int64(n) / 2
	start := rank - half
	if start < 0 {
		start = 0
	}
	stop := start + int64(n) - 1

	rows, err := s.rdb.ZRevRangeWithScores(ctx, key, start, stop).Result()
	if err != nil {
		return nil
	}

	out := make([]*internalv2.RankEntry, 0, len(rows))
	for i, z := range rows {
		pid, _ := parseMember(fmt.Sprint(z.Member))
		out = append(out, &internalv2.RankEntry{
			PlayerId: pid,
			Score:    int64(z.Score),
			Rank:     start + int64(i) + 1,
		})
	}
	return out
}

// RankOf 查询某玩家的名次与分数（0 表示未上榜）。
func (s *Service) RankOf(rankType, season string, playerID int64) (rankNo int64, score int64) {
	if season == "" {
		season = s.currentSeason
	}
	key := Key(ScopeGlobal, season, rankType)
	ctx := context.Background()

	member, ok := s.findMember(ctx, key, playerID)
	if !ok {
		return 0, 0
	}
	r, err := s.rdb.ZRevRank(ctx, key, member).Result()
	if err != nil {
		return 0, 0
	}
	sc, err := s.rdb.ZScore(ctx, key, member).Result()
	if err != nil {
		return 0, 0
	}
	return r + 1, int64(sc)
}

// Archive 归档指定赛季的榜单到给定回调（由 leader 在赛季切换时调用）。
//
// 回调收到的是「按名次升序」的完整榜单，调用方负责写 MySQL。
func (s *Service) Archive(ctx context.Context, rankType, season string, topN int64, fn func(entries []*internalv2.RankEntry) error) error {
	if season == "" {
		return fmt.Errorf("season is required for archive")
	}
	if topN <= 0 {
		topN = 1000
	}
	key := Key(ScopeGlobal, season, rankType)

	rows, err := s.rdb.ZRevRangeWithScores(ctx, key, 0, topN-1).Result()
	if err != nil {
		return fmt.Errorf("zrevrange %s: %w", key, err)
	}
	if len(rows) == 0 {
		return nil
	}

	entries := make([]*internalv2.RankEntry, 0, len(rows))
	for i, z := range rows {
		pid, _ := parseMember(fmt.Sprint(z.Member))
		entries = append(entries, &internalv2.RankEntry{
			PlayerId: pid,
			Score:    int64(z.Score),
			Rank:     int64(i + 1),
		})
	}
	return fn(entries)
}

// Reset 清空指定赛季的榜单（归档完成后调用）。
func (s *Service) Reset(ctx context.Context, rankType, season string) error {
	if season == "" {
		return fmt.Errorf("season is required")
	}
	return s.rdb.Del(ctx, Key(ScopeGlobal, season, rankType)).Err()
}

// findMember 在 ZSet 里找某玩家的 member（member 形如 "12345:game-01"）。
//
// 用 ZSCAN 前缀匹配而不是全量 ZRANGE —— 大榜单下后者代价过高。
func (s *Service) findMember(ctx context.Context, key string, playerID int64) (string, bool) {
	prefix := strconv.FormatInt(playerID, 10) + ":"
	var cursor uint64
	for {
		keys, next, err := s.rdb.ZScan(ctx, key, cursor, prefix+"*", 100).Result()
		if err != nil {
			return "", false
		}
		// ZScan 返回 [member, score, member, score, ...]
		for i := 0; i+1 < len(keys); i += 2 {
			if strings.HasPrefix(keys[i], prefix) {
				return keys[i], true
			}
		}
		cursor = next
		if cursor == 0 {
			return "", false
		}
	}
}

// Key 构造 ZSet key。
func Key(scope, season, rankType string) string {
	if scope == ScopeGlobal || scope == "" {
		return fmt.Sprintf("rank:global:%s:%s", season, rankType)
	}
	return fmt.Sprintf("rank:%s:%s:%s", scope, season, rankType)
}

// Member 构造 ZSet member。
func Member(playerID int64, serverID string) string {
	if serverID == "" {
		serverID = "unknown"
	}
	return fmt.Sprintf("%d:%s", playerID, serverID)
}

// parseMember 解析 member。
func parseMember(member string) (int64, string) {
	parts := strings.SplitN(member, ":", 2)
	if len(parts) == 0 {
		return 0, ""
	}
	pid, _ := strconv.ParseInt(parts[0], 10, 64)
	if len(parts) == 1 {
		return pid, ""
	}
	return pid, parts[1]
}

// SortEntries 按名次升序排序（归档/展示统一用）。
func SortEntries(list []*internalv2.RankEntry) {
	sort.SliceStable(list, func(i, j int) bool { return list[i].Rank < list[j].Rank })
}
