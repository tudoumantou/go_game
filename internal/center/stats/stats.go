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
//
// 多实例协作（R8.4）：各实例快照写 Redis Hash 的**不同 field**（field=实例ID），
// 读取时（MergedMetrics）逐 field 求和——合并而不是覆盖。早期实现用窗口名
// 做 field，多实例互相抹掉对方的数据，落库值偏小且无法察觉。
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
//
// 统一「约 60 桶」的形状：内存占用可预测（每 series 约 200 个 int64），
// 桶数少则粒度粗、多则浪费——60 桶 × 4 窗口是精度与内存的平衡点。
// 匿名结构体做 spec 值：轻量、无需再命名一个类型。
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
//
// 自带一把 Mutex 而不是共用 Service 的大锁：不同 (group, feature) 的
// 上报彼此无关，按 series 加锁让并发上报真正并行——锁粒度从
// 「整个服务」细到「一条序列」，QPS 上限提高一个量级。
type Series struct {
	mu      sync.Mutex
	windows map[string]*ring
	// lastTS 最近一次上报时间（查询时判断是否过期）
	lastTS int64
}

// ring 环形桶。
//
// 定长数组 + 取模寻址：O(1) 写入、O(1) 内存，永不增长。
// slotTS 是「槽位卫生」的关键：数组复用导致旧槽里可能躺着上个周期的
// 数据，靠「槽位记录的时间片 ≠ 当前时间片」判定过期并清零——
// 没有它，本周期的查询会把上周期的残值加进来。
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
// slot = 秒数 / 桶宽，idx = slot % count：同一物理槽位每 count 个
// 时间片轮回一次；slotTS 不匹配说明这是新一轮的第一笔，先清零再累加。
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
// 从当前片往回数 slots 个：slotTS 对不上的槽直接跳过——
// 环形数组里「位置有值」不等于「时间属于窗口内」，这是环形缓冲的
// 经典陷阱，靠每槽带时间戳解决。
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
//
// 两级锁：s.mu（RWMutex）保护 series/features 两张 map 的结构
// （增删 key），每个 Series 自带 mu 保护自己的环形桶。读路径
// （Query/Features）只碰 s.mu，写路径（Write）短暂持 s.mu 建 series
// 后即释放，再持 series 级锁累加——不会出现「一次上报堵住所有查询」。
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
//
// 幂等且忽略空值：启动时序上可能被调用多次（center.Start 里一次），
// 传空是调用方没拿到 hostname 的情况，保留现值更安全。
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
//
// ts 为 0 时用本地时钟：上报方（游戏服）可能不填时间戳，此时以
// 「到达时间」近似——统计是聚合语义，秒级误差可接受。
// group 空兜底 group-01：与 center 的默认分组一致，避免空 key。
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
	// 注意写的是全部 4 个窗口：一次上报同时喂 1m/5m/1h/1d，
	// 查询时按需取窗口——用写入时的固定成本换查询时的零计算
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
//
// to 为 0 时用「现在」：调用方（GM 工具）常常只传 from；to 是历史
// 回溯点（审计用）。先快照 key 列表再逐个处理：避免持锁遍历 map
// 的同时被 Write 修改（Go map 遍历中增删未定义）。
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
		// 二次检查：快照与加锁之间该 series 理论上不会消失（没有删除路径），
		// nil 检查是防御未来的删除逻辑
		if se == nil {
			continue
		}

		se.mu.Lock()
		// 用 1h 窗口作为「近期累计」的代表值；更长区间需 P15 的 MySQL 归档
		// sumWindow 会跳过过期槽位：窗口内无数据时返回 0，不是脏值
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
//
// 用 Pipeline 批量 HSet：每个 series 一条命令，N 个 series 一次往返
// （pipe.Exec）——快照每 5 分钟一次但 series 可能上百，逐条发会把
// 中心服与 Redis 间的连接占满。注意实际 field 是实例 ID（见下面注释）。
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
		redisKey := statsKey(gid, feat)

		e.se.mu.Lock()
		vals := make(map[string]int64, len(windowSpec))
		for name, spec := range windowSpec {
			vals[name] = e.se.windows[name].sumWindow(now, spec.bucket*time.Duration(spec.count))
		}
		e.se.mu.Unlock()

		// field = 实例 ID，value = 该实例各窗口的 JSON。
		// 早期实现是 HSet(窗口名 → 值)：多个中心服实例写同一 field 会互相覆盖，
		// 后写者把先写者的量抹掉，落库值偏小且无法察觉。
		// JSON 而非多 field：四个窗口一次序列化，读取端一次解析，
		// 且将来加窗口不用改 Redis 数据结构
		raw, err := json.Marshal(vals)
		if err != nil {
			continue
		}
		pipe.HSet(ctx, redisKey, s.instanceOf(), string(raw))
	}
	// 空 series 提前返回：pipe.Exec 空管道会直接报错，
	// 且没数据时不该产生一次 Redis 往返
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

// statsKey Redis 聚合哈希的键：stats:{group}:{feature}。
//
// 单独成函数不是为了少几个字符：这个 key 在本文件出现三处（读窗口、合并、
// 数实例），而 splitKey 只解析「实例标识」不碰它。三处各拼一遍时，任何一处
// 改了分隔符或字段顺序，另外两处不会报错——只会在 GM 面板上表现为「合并
// 读到 0 条」或「实例数恒为 0」。反向解析不存在（没有 splitStatsKey），
// 所以这里就是唯一权威。
func statsKey(groupID, feature string) string {
	return fmt.Sprintf("stats:%s:%s", groupID, feature)
}

func splitKey(k string) (groupID, feature string, ok bool) {
	// 按第一个 '|' 切分：group 或 feature 里含 '|' 时归属于前段，
	// 约定 group/feature 命名不带 '|'，切分无歧义
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
//
// HGetAll 一次取回全部实例的 field：合并是读多写少路径（归档时一次），
// 一次往返优于逐 field HGet。
func (s *Service) MergedMetrics(ctx context.Context, groupID, feature string) map[string]int64 {
	key := statsKey(groupID, feature)
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
// HLen 取 field 数 = 活着的实例数：新实例上线/旧实例停写都会体现在这，
// 是「合并到底合了几个」的唯一直接证据。Redis 出错返回 0——
// 诊断接口，调用方（GM）看到 0 会去查 Redis，不会误判成「没数据」。
func (s *Service) InstanceCount(ctx context.Context, groupID, feature string) int64 {
	n, err := s.rdb.HLen(ctx, statsKey(groupID, feature)).Result()
	if err != nil {
		return 0
	}
	return n
}
