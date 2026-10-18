// tools/loadtest/stats.go —— 压测结果统计
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
type Recorder struct {
	mu        sync.Mutex
	latencies []time.Duration
	errors    int64
	errSample []string // 前几条错误信息（诊断用，避免压测只报数字不报原因）
	started   time.Time
	stopped   time.Time
}

// NewRecorder 创建记录器并开始计时。
func NewRecorder() *Recorder {
	return &Recorder{started: time.Now()}
}

// Add 记录一次成功请求。
func (r *Recorder) Add(d time.Duration) {
	r.mu.Lock()
	r.latencies = append(r.latencies, d)
	r.mu.Unlock()
}

// AddError 记录一次失败请求（可选带错误原因，前 5 条会被保留）。
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
