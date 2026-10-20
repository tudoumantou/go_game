// tools/loadtest/stats.go —— 压测结果统计
//
// 工程链位置：loadtest 三个压测模式（conn/login/biz）共用的结果记录与汇报
// 组件；scripts/bench.sh 汇总 reports/ 下的 JSON 就是本文件 WriteJSON 的产物。
//
// 只做必要的事：记录每次请求的耗时，结束时算分位。不引入 HDR 直方图库 ——
// 压测规模（十万级样本）用排序算精确分位完全够用，且没有桶边界误差。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// Recorder 并发安全的耗时记录器。
//
// 用法约定：NewRecorder 构造即开始计时 → 各 goroutine 并发 Add/AddError →
// （可选）Stop 冻结计时窗口 → Summarize 出报告。三个压测模式共用，
// 因此它只依赖标准库、不依赖任何协议细节。
//
// 同步策略：一把 sync.Mutex 保护全部字段。压测量级下锁竞争可忽略（临界区
// 只有 append/自增）；换 atomic 反而要把 latencies 与 errSample 拆成两种
// 同步原语，更易错 —— 正确性优先，热点不在这里。
type Recorder struct {
	mu        sync.Mutex
	latencies []time.Duration
	errors    int64
	errSample []string // 前几条错误信息（诊断用，避免压测只报数字不报原因）
	started   time.Time
	stopped   time.Time
}

// NewRecorder 创建记录器并开始计时。
// started 在构造时打点：调用方不需要（也不应）手动 Start ——
// 「计时窗口 = 建 Recorder 到 Stop/Summarize」是唯一口径，避免各模式
// 各自实现出偏差。
func NewRecorder() *Recorder {
	return &Recorder{started: time.Now()}
}

// Add 记录一次成功请求。
// 只存耗时不存时间戳：分位统计只需要样本值；若将来要做「随时间退化」的
// 趋势图，再改成存 (t, d) 二元组，接口保持不变。
func (r *Recorder) Add(d time.Duration) {
	r.mu.Lock()
	r.latencies = append(r.latencies, d)
	r.mu.Unlock()
}

// AddError 记录一次失败请求（可选带错误原因，前 5 条会被保留）。
//
// 变参 err ...error 让「只计数」与「带原因」两种调用共存（runConn 只计数，
// login/biz 带 err）；失败样本截断到 5 条是刻意的 —— 压测失败常是同因
// 海量重复，全量保留只会把报告撑爆。
func (r *Recorder) AddError(err ...error) {
	r.mu.Lock()
	r.errors++
	if len(r.errSample) < 5 && len(err) > 0 && err[0] != nil {
		r.errSample = append(r.errSample, err[0].Error())
	}
	r.mu.Unlock()
}

// Stop 冻结计时（连接保持类压测：建连完成后请求就结束了，
// 后续 hold 阶段不应计入吞吐）。
func (r *Recorder) Stop() {
	r.mu.Lock()
	r.stopped = time.Now()
	r.mu.Unlock()
}

// Count 已完成请求数。
// 口径注意：只数成功样本（latencies 长度），不含失败；
// 成功率要调用方自己用 Count/(Count+Errors) 算，这里不代劳。
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.latencies)
}

// Errors 失败数。
func (r *Recorder) Errors() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errors
}

// Summary 统计结果。
// json tag 全小写 + err_samples omitempty：bench.sh 的汇总脚本按 key 取值，
// 无失败样本时省略字段，报告更干净。Elapsed/TPS/延迟统一用毫秒与秒，
// 与 23.4 的验收指标（TPS、P99 ms）单位一致，不用换算。
type Summary struct {
	Name    string  `json:"name"`
	Count   int     `json:"count"`
	Errors  int64   `json:"errors"`
	Elapsed float64 `json:"elapsed_sec"`
	TPS     float64 `json:"tps"`
	AvgMS   float64 `json:"avg_ms"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
	// ErrSamples 前几条错误原文（最多 5 条）
	ErrSamples []string `json:"err_samples,omitempty"`
}

// Summarize 计算分位数。
//
// 分位定义：P99 = 第 99% 分位（排序后下标 ceil(0.99*n)-1），
// 与 Prometheus histogram_quantile 的口径接近，便于和线上指标对照。
//
// 计时口径：Stop 过则用 Stop 时刻（连接保持类压测只把建连阶段算进吞吐），
// 否则用「Summarize 被调用的时刻」。TPS = 成功数 / 窗口秒数 —— 失败既不进
// 分子也不进分母，吞吐反映的是「有效完成速率」。
func (r *Recorder) Summarize(name string) Summary {
	r.mu.Lock()
	defer r.mu.Unlock()

	end := time.Now()
	if !r.stopped.IsZero() {
		end = r.stopped
	}
	elapsed := end.Sub(r.started).Seconds()
	s := Summary{
		Name:       name,
		Count:      len(r.latencies),
		Errors:     r.errors,
		Elapsed:    elapsed,
		ErrSamples: append([]string(nil), r.errSample...),
	}
	if elapsed > 0 {
		s.TPS = float64(len(r.latencies)) / elapsed
	}
	if len(r.latencies) == 0 {
		return s
	}

	sorted := make([]time.Duration, len(r.latencies))
	copy(sorted, r.latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

	s.AvgMS = ms(total / time.Duration(len(sorted)))
	s.P50MS = ms(percentile(sorted, 0.50))
	s.P95MS = ms(percentile(sorted, 0.95))
	s.P99MS = ms(percentile(sorted, 0.99))
	s.MaxMS = ms(sorted[len(sorted)-1])
	return s
}

// percentile 取分位值（p ∈ (0,1]）。
// 下标公式是 ceil(p*n)-1 的等价写法：int(float64(n)*p)-1 后夹紧到 [0, n-1]。
// 夹紧是必须的：浮点误差可能让 0.99*n 算出 n.0000001，下标冲出末尾；
// p=1.0 时 int(n*1.0)-1 = n-1 刚好不越界，但依赖这一点不如显式防御。
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted))*p) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// Print 控制台输出。
// 延迟保留两位小数（ms）：亚毫秒链路看整数毫秒会全是 0.00，微秒级细节
// 看 -json 产物。失败样例最多打 5 条，与 Recorder 的截断一致。
func (s Summary) Print() {
	fmt.Printf("\n=== %s ===\n", s.Name)
	fmt.Printf("请求数 %d，失败 %d，耗时 %.1fs，吞吐 %.0f/s\n",
		s.Count, s.Errors, s.Elapsed, s.TPS)
	if s.Count > 0 {
		fmt.Printf("延迟  avg %.2fms  p50 %.2fms  p95 %.2fms  p99 %.2fms  max %.2fms\n",
			s.AvgMS, s.P50MS, s.P95MS, s.P99MS, s.MaxMS)
	}
	for i, e := range s.ErrSamples {
		if i == 0 {
			fmt.Println("失败样例：")
		}
		fmt.Printf("  - %s\n", e)
	}
}

// WriteJSON 落盘（供 bench.sh 汇总成报告）。
// path 为空是合法的 no-op：调用方（main 的 login/biz 分支）不必先判空再调，
// 「可选输出」内聚在实现里；0o644 是报告文件（与压测数据同级，非敏感）。
func (s Summary) WriteJSON(path string) error {
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
