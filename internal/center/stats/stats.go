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
		vals := make(map[string]interface{}, len(windowSpec))
		for name, spec := range windowSpec {
			vals[name] = e.se.windows[name].sumWindow(now, spec.bucket*time.Duration(spec.count))
		}
		e.se.mu.Unlock()

		pipe.HSet(ctx, redisKey, vals)
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
