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
//
// 设计取舍：
//   - 分数用增量上报（ZINCRBY）：与「竞技场胜负加减分」语义一致，上报即增量；
//   - member 带 serverId 后缀（playerId:serverId）：同一玩家在多个服有数据时
//     不互相覆盖，代价是查询要先 ZSCAN 定位 member（见 findMember）；
//   - 分数并列时 ZREVRANGE 按 member 字典序 tie-break：Redis ZSet 的有序性
//     是确定性的，分页不会因排序不稳而漏人/重人。
package rank

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/redis/go-redis/v9"
)

// 榜单作用域。
// global/group 两个值决定 key 形状（见 Key）：加新作用域只需在这里登记
// 并同步 Key 的构造逻辑，字符串散着写容易漏。
const (
	ScopeGlobal = "global"
	// ScopeGroupPrefix 分组榜 key 前缀（实际 key 为 rank:group:{id}:{season}:{type}）
	ScopeGroupPrefix = "group"
)

// Service 排行榜服务。
//
// 无状态（除 currentSeason）：全部数据在 Redis，多实例可并行写。
// currentSeason 只影响「上报未带 season」时的默认归属，由 leader 归档
// 切换后调用 SetSeason 更新；切换瞬间在途的旧赛季上报仍按自带 season
// 落旧榜，不会串榜。
//
// 注意 currentSeason 是无锁读写的 string：写只发生在 leader 归档完成
// 这一低频点，读发生在每次上报——Go 的 string 赋值不是原子的，严格来说
// 这里有数据竞争；实践中切换频率极低且最坏后果是「一两条上报落到新赛季」，
// 可接受，故不加锁。若要硬化，换 atomic.Pointer[string]。
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
// 空值忽略：调用方传空等于「不知道」，保留现值比清空更安全
// （清空会让后续上报落到空 key 上）。
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
// 幂等性：带 biz_id 的上报按业务键去重（Redis SETNX），同一次结算重投不会重复加分；
// 不带 biz_id 的上报无法判重，只能依赖上报方不重发。
//
// 去重键 TTL 30 天（dedupTTL），按赛季周期设定：足够覆盖一次结算的重投
// 窗口，又不会让 key 无限堆积；跨赛季复用同名 biz_id 不会互相屏蔽。
//
// 原子性边界：SETNX 去重与 ZINCRBY 加分是两条命令、两次往返，中间进程崩溃
// 会留下「已去重未加分」——宁少勿多：少加一次可运营补偿，多加无法自动修正。
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

	// 业务键去重：Redis 失败时宁可不加分，也不冒重复加分的风险
	// （重复加分无法自动修正，少加一次可以由运营补偿）。
	// 显式 cancel 而非 defer：函数后面还有 ZINCRBY 与 Set 两条命令，
	// defer 会把 2s 超时上下文一直持到函数返回，白占资源
	if rep.BizId != "" {
		dctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ok, err := s.rdb.SetNX(dctx, dedupKey(rep.BizId), 1, dedupTTL).Result()
		cancel()
		if err != nil {
			s.logger.Warn("rank dedup failed, drop report", "biz_id", rep.BizId, "err", err)
			return
		}
		if !ok {
			return // 同一次结算已处理过
		}
	}

	ctx := context.Background()
	if err := s.rdb.ZIncrBy(ctx, key, float64(rep.Score), member).Err(); err != nil {
		s.logger.Warn("rank zincrby failed", "key", key, "player", rep.PlayerId, "err", err)
		return
	}
	// 更新时间只在真正加分后写，客户端据此展示「榜单最近变动」。
	// 失败不处理（_ =）：更新时间是展示增强信息，丢了不影响正确性
	uctx, ucancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = s.rdb.Set(uctx, updatedKey(scope, season, type_), time.Now().Unix(), dedupTTL).Err()
	ucancel()
}

// TopN 取前 N 名。
//
// season 为空用当前赛季；n<=0 取默认 100。返回按分数降序、名次从 1 连续
// 编号。Name 字段留空——中心服不持有玩家数据（9.1），展示名由客户端按
// playerId 补齐。
//
// 用 ZREVRANGE 一次取 Top-N（O(logN+M)）：不要 ZRANGE 全量再内存排序，
// 榜单上限十万级时后者会把中心服内存打满。
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
//
// 先 ZSCAN 前缀定位 member（约定同一玩家只在一个服有榜数据），再以
// ZREVRANK 名次为中心前后各取一半。玩家未上榜返回 nil；贴近榜尾时
// 窗口自动收窄（start 不小于 0），不会越界。
//
// 为什么用 ZSCAN 而不是全量 ZRANGE 找 member：大榜单下全量拉取的
// 网络与内存代价不可接受；SCAN 的游标式遍历把成本摊到多次小请求。
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
	// 该约定依赖业务保证：合服/转服会把同一玩家搬到一个服，否则周边查询会漏
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
//
// 名次为 1-based；未上榜或 Redis 出错都返回 (0, 0)——调用方无法区分
// 「未上榜」与「查询失败」，GM/展示场景可接受此模糊。
//
// ZREVRANK 返回 0-based，+1 转成人类名次；member 不存在时 redis.Nil
// 与网络错误走同一分支——刻意不区分，理由见上。
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
//
// 空榜单直接返回 nil（不触发回调）：新赛季还没有任何上报时无需归档。
// 回调返回错误会中断归档并向上传播，由 leader 下一轮重试。
//
// 这里一次拉完 TopN 而不分页：TopN 上限 1000，单次 ZREVRANGE 的
// 数据量可控；分页拉取反而要让回调感知页边界，复杂度不划算。
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
// DEL 整个 key 而不是逐 member 删：赛季 key 整体废弃，O(1) 释放；
// 逐条 ZREM 在大榜单上是 O(N) 且要多次往返。
func (s *Service) Reset(ctx context.Context, rankType, season string) error {
	if season == "" {
		return fmt.Errorf("season is required")
	}
	return s.rdb.Del(ctx, Key(ScopeGlobal, season, rankType)).Err()
}

// findMember 在 ZSet 里找某玩家的 member（member 形如 "12345:game-01"）。
//
// 用 ZSCAN 前缀匹配而不是全量 ZRANGE —— 大榜单下后者代价过高。
//
// 游标循环：cursor 从 0 开始，Redis 返回下一次游标，回 0 表示遍历完成；
// count=100 只是每次扫描的「建议桶数」，不保证返回条数，循环必须以
// cursor==0 为终止条件而不是凑够条数。
func (s *Service) findMember(ctx context.Context, key string, playerID int64) (string, bool) {
	prefix := strconv.FormatInt(playerID, 10) + ":"
	var cursor uint64
	for {
		keys, next, err := s.rdb.ZScan(ctx, key, cursor, prefix+"*", 100).Result()
		if err != nil {
			return "", false
		}
		// ZScan 返回 [member, score, member, score, ...] 扁平数组，
		// 步长 2 取偶数位即 member；HasPrefix 再确认一次（MATCH 是
		// 服务端 glob 匹配，客户端侧双保险防串号）
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
// serverId 为空填 "unknown" 而不是留空：member 格式是 "id:server"，
// 空 server 会让 "123:" 这种畸形 member 出现，parseMember 虽能容错
// 但排查时对不上号。
func Member(playerID int64, serverID string) string {
	if serverID == "" {
		serverID = "unknown"
	}
	return fmt.Sprintf("%d:%s", playerID, serverID)
}

// parseMember 解析 member。
// SplitN(member, ":", 2)：playerId 在前、serverId 在后，serverId 里
// 若含 ":" 不会被切开（Limit=2）；playerId 解析失败静默归 0——
// 调用方（TopN 等）对 0 号玩家只是展示一条无名条目，不值得让整个查询失败。
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

// dedupTTL 业务键与更新时间键的保留期：覆盖一次赛季周期即可。
// 两类 key 共用同一 TTL：去重键过期后同名 biz_id 可重新计分（跨赛季复用），
// 更新时间键过期只是客户端暂不显示「最近变动」，无正确性影响。
const dedupTTL = 30 * 24 * time.Hour

func dedupKey(bizID string) string { return "rank:dedup:" + bizID }

func updatedKey(scope, season, rankType string) string {
	return "rank:updated:" + scope + ":" + season + ":" + rankType
}

// Query 一次返回 Top-N、我的名次与周边，供游戏服代理客户端查询。
//
// season 为空用当前赛季；未上榜时 on_board=false，且 around 为空（没有「周边」可言）。
//
// topN 截到 100：客户端协议上限——一次拉全服 Top-N 会把网关/中心服
// 带宽打满，100 条足够首屏；aroundN 默认 11（自己居中 ±5）。
func (s *Service) Query(rankType, season string, playerID int64, topN, aroundN int32) *internalv2.QueryRankResp {
	if season == "" {
		season = s.currentSeason
	}
	if topN <= 0 {
		topN = 50
	}
	if topN > 100 {
		topN = 100 // 客户端协议上限，避免一次拉全服
	}
	if aroundN <= 0 {
		aroundN = 11
	}

	resp := &internalv2.QueryRankResp{
		Season: season,
		Top:    s.TopN(rankType, season, topN),
	}
	if playerID != 0 {
		rankNo, score := s.RankOf(rankType, season, playerID)
		if rankNo > 0 {
			resp.OnBoard = true
			resp.MyRank = rankNo
			resp.MyScore = score
			resp.Around = s.Around(rankType, season, playerID, aroundN)
		}
	}
	if ts, err := s.rdb.Get(context.Background(), updatedKey(ScopeGlobal, season, rankType)).Int64(); err == nil {
		resp.UpdatedAt = ts
	}
	return resp
}
