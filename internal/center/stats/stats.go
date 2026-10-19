// Package stats 分组功能统计（设计文档 9.5）
//
// 聚合维度：group_id × feature × 时间窗（1m / 5m / 1h / 1d）。
//
// 实现要点：
//   - 内存里按窗口开环形桶，写入 O(1)；
//   - 每 5 分钟快照到 Redis Hash（多实例共享，供 GM 查询）；
//   - 查询时把窗口内各桶累加返回，不落 MySQL（MySQL 归档由 P15 的定时任务做）。
//
// 为什么不用 Redis 直接做计数器：每个 rank/feature 上报都走 Redis 会增加中心服的
// 网络往返；内存聚合后批量快照，写入成本从 O(N 条上报) 降到 O(1 次快照)。
package stats

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/redis/go-redis/v9"
)

// 时间窗定义。
const (
	Window1m = "1m"
	Window5m = "5m"
	Window1h = "1h"
	Window1d = "1d"
)

// windowSize 各窗口的桶宽度与桶数量。
//
//	1m  → 60 个 1s 桶
//	5m  → 60 个 5s 桶
//	1h  → 60 个 1m 桶
//	1d  → 24 个 1h 桶
var windowSpec = map[string]struct {
	bucket time.Duration
	count  int
}{
	Window1m: {time.Second, 60},
	Window5m: {5 * time.Second, 60},
	Window1h: {time.Minute, 60},
	Window1d: {time.Hour, 24},
}

// Series 一个 (group, feature) 的时间序列（多窗口）。
type Series struct {
	mu      sync.Mutex
	windows map[string]*ring
	// lastTS 最近一次上报时间（查询时判断是否过期）
	lastTS int64
}

// ring 环形桶。
type ring struct {
	bucketSize time.Duration
	buckets    []int64
	// slotTS 每个槽位对应的时间片起点（用于判断是否过期需清零）
	slotTS []int64
	head   int
}

func newRing(spec struct {
	bucket time.Duration
	count  int
}) *ring {
	return &ring{
		bucketSize: spec.bucket,
		buckets:    make([]int64, spec.count),
		slotTS:     make([]int64, spec.count),
	}
}

// add 累加到当前时间片；跨片时先把该槽位清零。
func (r *ring) add(now time.Time, delta int64) {
	slot := now.Unix() / int64(r.bucketSize.Seconds())
	idx := int(slot % int64(len(r.buckets)))
	if r.slotTS[idx] != slot {
		r.slotTS[idx] = slot
		r.buckets[idx] = 0
	}
	r.buckets[idx] += delta
	r.head = idx
}

// sumWindow 汇总 [since, now] 内的累计值。
func (r *ring) sumWindow(now time.Time, dur time.Duration) int64 {
	cur := now.Unix() / int64(r.bucketSize.Seconds())
	slots := int64(dur / r.bucketSize)
	var total int64
	for i := int64(0); i < slots; i++ {
		slot := cur - i
		idx := int(slot % int64(len(r.buckets)))
		if r.slotTS[idx] == slot {
			total += r.buckets[idx]
		}
	}
	return total
}

// Service 统计服务。
type Service struct {
	rdb    redis.UniversalClient
	logger *log.Logger

	mu     sync.RWMutex
	series map[string]*Series // key = group|feature

	// features 已上报过的 feature 列表（供 GM 查询时枚举）
	features map[string]struct{}

	// instanceID 本实例标识：快照按实例分字段写，多实例求和而不是互相覆盖（R8.4）。
	instanceID string
}

// SetInstanceID 设置实例标识（center 启动时注入）。为空时退化为 "default"，
// 单实例部署下行为不变；多实例必须各用不同 ID，否则仍会覆盖。
func (s *Service) SetInstanceID(id string) {
	if id != "" {
		s.instanceID = id
	}
}

// New 创建统计服务。
func New(rdb redis.UniversalClient, logger *log.Logger) *Service {
	return &Service{
		rdb:      rdb,
		logger:   logger,
		series:   make(map[string]*Series),
		features: make(map[string]struct{}),
	}
}

// Write 累计一条功能上报（实现 center/rpc.StatsWriter）。
func (s *Service) Write(rep *internalv2.FeatureReport) {
	if rep == nil || rep.Feature == "" {
		return
	}
	groupID := rep.GroupId
	if groupID == "" {
		groupID = "group-01"
	}
	key := groupID + "|" + rep.Feature

	ts := rep.Ts
	if ts <= 0 {
		ts = time.Now().Unix()
	}
	now := time.Unix(ts, 0)

	s.mu.Lock()
	se, ok := s.series[key]
	if !ok {
		se = &Series{windows: make(map[string]*ring, len(windowSpec))}
		for name, spec := range windowSpec {
			se.windows[name] = newRing(spec)
		}
		s.series[key] = se
	}
	s.features[rep.Feature] = struct{}{}
	s.mu.Unlock()

	se.mu.Lock()
	defer se.mu.Unlock()
	// 每个 metric 独立累加（metrics 里可能既有计数也有求和）
	for _, v := range rep.Metrics {
		for name := range windowSpec {
			se.windows[name].add(now, v)
		}
	}
	se.lastTS = ts
}

// Query 查询统计（实现 center/rpc.StatsQuerier）。
//
// 返回每个 (group, feature) 在 [from, to] 内的累计值，按 window=1h 聚合。
func (s *Service) Query(groupID, feature string, from, to int64) []*internalv2.FeatureReport {
	now := time.Now()
	if to > 0 {
		now = time.Unix(to, 0)
	}

	s.mu.RLock()
	keys := make([]string, 0, len(s.series))
	for k := range s.series {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	sort.Strings(keys)

	var out []*internalv2.FeatureReport
	for _, k := range keys {
		gid, feat, ok := splitKey(k)
		if !ok {
			continue
		}
		if groupID != "" && gid != groupID {
			continue
		}
		if feature != "" && feat != feature {
			continue
		}

		s.mu.RLock()
		se := s.series[k]
		s.mu.RUnlock()
		if se == nil {
			continue
		}

		se.mu.Lock()
		// 用 1h 窗口作为「近期累计」的代表值；更长区间需 P15 的 MySQL 归档
		val := se.windows[Window1h].sumWindow(now, time.Hour)
		last := se.lastTS
		se.mu.Unlock()

		out = append(out, &internalv2.FeatureReport{
			GroupId: gid,
			Feature: feat,
			Metrics: map[string]int64{"total": val},
			Ts:      last,
		})
	}
	return out
}

// Snapshot 把内存统计写入 Redis（供多实例共享 / GM 跨实例查询）。
//
// key 形如 stats:{group}:{feature}，field 为窗口名，value 为当前窗口累计。
func (s *Service) Snapshot(ctx context.Context) error {
	now := time.Now()

	s.mu.RLock()
	entries := make([]struct {
		key string
		se  *Series
	}, 0, len(s.series))
	for k, se := range s.series {
		entries = append(entries, struct {
			key string
			se  *Series
		}{k, se})
	}
	s.mu.RUnlock()

	pipe := s.rdb.Pipeline()
	for _, e := range entries {
		gid, feat, ok := splitKey(e.key)
		if !ok {
			continue
		}
		redisKey := fmt.Sprintf("stats:%s:%s", gid, feat)

		e.se.mu.Lock()
		vals := make(map[string]int64, len(windowSpec))
		for name, spec := range windowSpec {
			vals[name] = e.se.windows[name].sumWindow(now, spec.bucket*time.Duration(spec.count))
		}
		e.se.mu.Unlock()

		// field = 实例 ID，value = 该实例各窗口的 JSON。
		// 早期实现是 HSet(窗口名 → 值)：多个中心服实例写同一 field 会互相覆盖，
		// 后写者把先写者的量抹掉，落库值偏小且无法察觉。
		raw, err := json.Marshal(vals)
		if err != nil {
			continue
		}
		pipe.HSet(ctx, redisKey, s.instanceOf(), string(raw))
	}
	if len(entries) == 0 {
		return nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("stats snapshot: %w", err)
	}
	return nil
}

// Series 数量（指标用）。
func (s *Service) SeriesCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.series)
}

// Features 已上报的 feature 列表。
func (s *Service) Features() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.features))
	for f := range s.features {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func splitKey(k string) (groupID, feature string, ok bool) {
	for i := 0; i < len(k); i++ {
		if k[i] == '|' {
			return k[:i], k[i+1:], true
		}
	}
	return "", "", false
}

// instanceOf 实例标识；未设置时用 default（单实例部署无需关心）。
func (s *Service) instanceOf() string {
	if s.instanceID == "" {
		return "default"
	}
	return s.instanceID
}

// MergedMetrics 读取某 (group, feature) 在 Redis 上的多实例合并值。
//
// 每个实例只写自己那份，这里逐个相加——这是 R8.4 要求的「合并而不是覆盖」。
// 某个实例的 JSON 解析失败只跳过该实例并记日志：宁可少算一份，也不要整体失败
// 导致所有实例的数据都落不了库。
func (s *Service) MergedMetrics(ctx context.Context, groupID, feature string) map[string]int64 {
	key := fmt.Sprintf("stats:%s:%s", groupID, feature)
	fields, err := s.rdb.HGetAll(ctx, key).Result()
	if err != nil || len(fields) == 0 {
		return nil
	}

	out := make(map[string]int64, len(windowSpec))
	for instance, raw := range fields {
		var vals map[string]int64
		if err := json.Unmarshal([]byte(raw), &vals); err != nil {
			s.logger.Warn("stats snapshot decode failed", "instance", instance, "feature", feature, "err", err)
			continue
		}
		for window, v := range vals {
			out[window] += v
		}
	}
	return out
}

// InstanceCount 某 (group, feature) 的已上报实例数（诊断多实例合并是否生效）。
func (s *Service) InstanceCount(ctx context.Context, groupID, feature string) int64 {
	n, err := s.rdb.HLen(ctx, fmt.Sprintf("stats:%s:%s", groupID, feature)).Result()
	if err != nil {
		return 0
	}
	return n
}
