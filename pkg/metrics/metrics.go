// Package metrics 指标采集与暴露（设计文档 20.3 可观测性）。
//
// 设计要点：
//   - 统一注册到自定义 Registry（不用默认全局 registry，避免测试相互污染）；
//   - 服务名作为常量标签 `service`，同一个 Grafana 面板可按服务聚合；
//   - 指标命名遵循 Prometheus 最佳实践：<namespace>_<subsystem>_<name>_<unit>；
//   - 全部指标在包初始化时注册一次，业务侧只调用 Set/Inc/Observe，不做动态注册
//     （动态注册会在高基数标签下泄漏内存）。
package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace 指标前缀（gs = game server）。
const Namespace = "gs"

// registry 全局注册表（本包唯一）。
var registry = prometheus.NewRegistry()

var registerOnce sync.Once

// serviceLabels 当前进程的服务名（由 Init 注入）。
var (
	serviceMu   sync.RWMutex
	serviceName = "unknown"
	enabled     atomic.Bool
)

// Init 初始化指标（注入服务名 + 注册进程/Go 运行时指标）。
// 幂等：多次调用只生效第一次。
//
// 关键顺序：enabled 必须在**全部指标注册完成后**才置 true。
// 否则存在竞态窗口 —— 其他 goroutine 看到 enabled=true 提前调用
// ConnInc() 等函数时，指标指针仍为 nil，会触发 SIGSEGV。
func Init(service string) {
	serviceMu.Lock()
	serviceName = service
	serviceMu.Unlock()

	registerOnce.Do(func() {
		registry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
		registerConn()
		registerMessage()
		registerActor()
		registerFlush()
		registerChannel()
		registerCross()
		registerLogin()
		registerCenter()
		registerKafka()
		enabled.Store(true)
	})
}

// Service 返回当前服务名（供其他包拼标签）。
func Service() string {
	serviceMu.RLock()
	defer serviceMu.RUnlock()
	return serviceName
}

// Enabled 指标是否已初始化。
// 各组件在热路径（Actor 每轮循环、每条消息）先判断此开关，
// 避免未启用指标的场景（单测、CLI 工具）产生无谓开销。
func Enabled() bool { return enabled.Load() }

// Registry 返回指标注册表（测试用）。
func Registry() *prometheus.Registry { return registry }

// ============================ 连接与消息 ============================

var (
	connGauge   *prometheus.GaugeVec
	connTotal   *prometheus.CounterVec
	msgCounter  *prometheus.CounterVec
	msgInflight *prometheus.GaugeVec
	msgLatency  *prometheus.HistogramVec
	msgSize     *prometheus.HistogramVec
)

func registerConn() {
	connGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "gateway", Name: "connections",
		Help: "当前活跃连接数",
	}, []string{"service"})
	connTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "gateway", Name: "connections_total",
		Help: "累计接受连接数",
	}, []string{"service", "result"})
	registry.MustRegister(connGauge, connTotal)
}

func registerMessage() {
	msgCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "message", Name: "total",
		Help: "消息处理总数（按方向与结果）",
	}, []string{"service", "direction", "result"})
	msgInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "message", Name: "inflight",
		Help: "正在处理的消息数",
	}, []string{"service"})
	msgLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "message", Name: "duration_seconds",
		Help:    "消息端到端处理耗时（20.1 目标 P99 < 50ms）",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	}, []string{"service", "direction"})
	msgSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "message", Name: "size_bytes",
		Help:    "消息体大小（单包上限 64KB）",
		Buckets: []float64{64, 256, 1024, 4096, 16384, 65536},
	}, []string{"service", "direction"})
	registry.MustRegister(msgCounter, msgInflight, msgLatency, msgSize)
}

// ConnInc 连接建立。
func ConnInc() {
	if !enabled.Load() {
		return
	}
	connGauge.WithLabelValues(Service()).Inc()
	connTotal.WithLabelValues(Service(), "accepted").Inc()
}

// ConnDec 连接关闭。
func ConnDec() {
	if !enabled.Load() {
		return
	}
	connGauge.WithLabelValues(Service()).Dec()
}

// ConnRejected 拒绝连接（超 max_conns / 握手失败等）。
func ConnRejected(reason string) {
	if !enabled.Load() {
		return
	}
	connTotal.WithLabelValues(Service(), "rejected_"+reason).Inc()
}

// MsgInc 消息计数（direction: in/out；result: ok/error）。
func MsgInc(direction, result string) {
	if !enabled.Load() {
		return
	}
	msgCounter.WithLabelValues(Service(), direction, result).Inc()
}

// MsgObserve 消息处理耗时与大小。
func MsgObserve(direction string, d time.Duration, size int) {
	if !enabled.Load() {
		return
	}
	msgLatency.WithLabelValues(Service(), direction).Observe(d.Seconds())
	if size > 0 {
		msgSize.WithLabelValues(Service(), direction).Observe(float64(size))
	}
}

// MsgInflightInc 进入处理。
func MsgInflightInc() {
	if !enabled.Load() {
		return
	}
	msgInflight.WithLabelValues(Service()).Inc()
}

// MsgInflightDec 处理完成。
func MsgInflightDec() {
	if !enabled.Load() {
		return
	}
	msgInflight.WithLabelValues(Service()).Dec()
}

// ============================ Actor ============================

var (
	actorMailbox    *prometheus.GaugeVec
	actorProcessed  *prometheus.CounterVec
	actorLatency    *prometheus.HistogramVec
	actorPanic      *prometheus.CounterVec
	actorDropped    *prometheus.CounterVec
	actorCountGauge prometheus.Gauge
)

func registerActor() {
	actorMailbox = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "mailbox_depth",
		Help: "Actor 信箱当前积压消息数（有界 4096，满则丢最旧）",
	}, []string{"service", "shard"})
	actorProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "processed_total",
		Help: "Actor 已处理消息总数",
	}, []string{"service", "result"})
	actorLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "handle_duration_seconds",
		Help:    "单条消息 Actor 内处理耗时",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
	}, []string{"service"})
	actorPanic = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "panic_total",
		Help: "Actor panic 次数（已 recover）",
	}, []string{"service"})
	actorCountGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "count",
		Help: "存活 Actor 数",
	})
	actorDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "dropped_total",
		Help: "信箱满时丢弃的最旧消息数（背压代价，>0 说明 Actor 处理不过来）",
	}, []string{"service"})
	registry.MustRegister(actorMailbox, actorProcessed, actorLatency, actorPanic, actorDropped, actorCountGauge)
}

// ActorMailbox 设置某分片信箱深度（只在分片循环里低频调用，避免锁竞争）。
func ActorMailbox(shard int, depth int) {
	if !enabled.Load() {
		return
	}
	actorMailbox.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Set(float64(depth))
}

// ActorProcessed 处理计数。
func ActorProcessed(ok bool) {
	if !enabled.Load() {
		return
	}
	result := "ok"
	if !ok {
		result = "error"
	}
	actorProcessed.WithLabelValues(Service(), result).Inc()
}

// ActorObserve 处理耗时。
func ActorObserve(d time.Duration) {
	if !enabled.Load() {
		return
	}
	actorLatency.WithLabelValues(Service()).Observe(d.Seconds())
}

// ActorPanicInc panic 计数。
func ActorPanicInc() {
	if !enabled.Load() {
		return
	}
	actorPanic.WithLabelValues(Service()).Inc()
}

// ActorDroppedAdd 信箱丢弃消息计数（背压）。
func ActorDroppedAdd(n int64) {
	if !enabled.Load() {
		return
	}
	if n > 0 {
		actorDropped.WithLabelValues(Service()).Add(float64(n))
	}
}

// ActorCountAdd 存活 Actor 数变化。
func ActorCountAdd(delta float64) {
	if !enabled.Load() {
		return
	}
	actorCountGauge.Add(delta)
}

// ============================ Flusher ============================

var (
	flushDirty    *prometheus.GaugeVec
	flushDuration *prometheus.HistogramVec
	flushRows     *prometheus.CounterVec
	flushFail     *prometheus.CounterVec
	flushLag      *prometheus.GaugeVec
)

func registerFlush() {
	flushDirty = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "flush", Name: "dirty_entities",
		Help: "待落库脏实体数（按分片）",
	}, []string{"service", "shard"})
	flushDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "flush", Name: "duration_seconds",
		Help:    "单分片单轮落库耗时",
		Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5},
	}, []string{"service"})
	flushRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "flush", Name: "rows_total",
		Help: "累计落库实体数",
	}, []string{"service", "result"})
	flushFail = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "flush", Name: "failures_total",
		Help: "落库失败次数（失败重回 dirty）",
	}, []string{"service", "shard"})
	flushLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "flush", Name: "lag_seconds",
		Help: "落库延迟：距上次成功落库的秒数（20.3 告警项）",
	}, []string{"service", "shard"})
	registry.MustRegister(flushDirty, flushDuration, flushRows, flushFail, flushLag)
}

// FlushDirty 设置分片脏实体数。
func FlushDirty(shard, n int) {
	if !enabled.Load() {
		return
	}
	flushDirty.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Set(float64(n))
}

// FlushObserve 单轮落库耗时。
func FlushObserve(d time.Duration) {
	if !enabled.Load() {
		return
	}
	flushDuration.WithLabelValues(Service()).Observe(d.Seconds())
}

// FlushRows 落库行数（ok/fail）。
func FlushRows(ok bool, n int) {
	if !enabled.Load() {
		return
	}
	if !ok {
		return
	}
	flushRows.WithLabelValues(Service(), "ok").Add(float64(n))
}

// FlushFail 落库失败。
func FlushFail(shard int) {
	if !enabled.Load() {
		return
	}
	flushFail.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Inc()
	// 失败时延迟从这一刻起算
	flushLag.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Set(0)
}

// FlushLag 设置落库延迟（秒）。
func FlushLag(shard int, seconds float64) {
	if !enabled.Load() {
		return
	}
	flushLag.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Set(seconds)
}

// ============================ Channel 池 ============================

var (
	channelStreams *prometheus.GaugeVec
	channelPool    *prometheus.GaugeVec
	channelErr     *prometheus.CounterVec
)

func registerChannel() {
	channelStreams = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "channel", Name: "streams",
		Help: "当前活跃 stream 数（单 Channel 上限 2000）",
	}, []string{"service"})
	channelPool = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "channel", Name: "pool_size",
		Help: "Channel 池内连接数",
	}, []string{"service"})
	channelErr = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "channel", Name: "errors_total",
		Help: "Channel 建流/发送失败次数",
	}, []string{"service", "op"})
	registry.MustRegister(channelStreams, channelPool, channelErr)
}

// ChannelStreams 活跃 stream 数。
func ChannelStreams(n int) {
	if !enabled.Load() {
		return
	}
	channelStreams.WithLabelValues(Service()).Set(float64(n))
}

// ChannelPoolSize 池大小。
func ChannelPoolSize(n int) {
	if !enabled.Load() {
		return
	}
	channelPool.WithLabelValues(Service()).Set(float64(n))
}

// ChannelError 建流/发送失败。
func ChannelError(op string) {
	if !enabled.Load() {
		return
	}
	channelErr.WithLabelValues(Service(), op).Inc()
}

// ============================ 跨服房间 ============================

var (
	crossRooms   *prometheus.GaugeVec
	crossSettled *prometheus.CounterVec
)

func registerCross() {
	crossRooms = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "cross", Name: "rooms",
		Help: "跨服房间数（按状态）",
	}, []string{"service", "state"})
	crossSettled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "cross", Name: "settled_total",
		Help: "已结算房间数（按原因：all_reported / timeout / closed）",
	}, []string{"service", "reason"})
	registry.MustRegister(crossRooms, crossSettled)
}

// CrossRooms 设置某状态房间数。
func CrossRooms(state string, n int) {
	if !enabled.Load() {
		return
	}
	crossRooms.WithLabelValues(Service(), state).Set(float64(n))
}

// CrossSettled 结算计数。
func CrossSettled(reason string) {
	if !enabled.Load() {
		return
	}
	crossSettled.WithLabelValues(Service(), reason).Inc()
}

// ============================ 登录 ============================

var loginPreload *prometheus.HistogramVec

func registerLogin() {
	loginPreload = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "login", Name: "preload_duration_seconds",
		Help:    "登录预加载耗时（20.1 目标 P99 < 2s）",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"service", "result"})
	registry.MustRegister(loginPreload)
}

// LoginPreloadObserve 记录一次预加载耗时。
func LoginPreloadObserve(d time.Duration, ok bool) {
	if !enabled.Load() {
		return
	}
	result := "ok"
	if !ok {
		result = "error"
	}
	loginPreload.WithLabelValues(Service(), result).Observe(d.Seconds())
}

// ============================ 中心服 ============================

var (
	centerNodes        *prometheus.GaugeVec
	centerMatchQueue   prometheus.Gauge
	centerRelayPending prometheus.Gauge
	centerStatsSeries  prometheus.Gauge
	centerIsLeader     prometheus.Gauge
)

func registerCenter() {
	centerNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "center", Name: "nodes",
		Help: "已注册节点数（按类型）",
	}, []string{"service", "type"})
	centerMatchQueue = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "center", Name: "match_queue",
		Help: "等待匹配的玩家数",
	})
	centerRelayPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "center", Name: "relay_pending",
		Help: "待重试的中转消息数",
	})
	centerStatsSeries = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "center", Name: "stats_series",
		Help: "内存聚合中的统计序列数（group × feature）",
	})
	centerIsLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "center", Name: "is_leader",
		Help: "本实例是否为 etcd 选主 leader（1/0）",
	})
	registry.MustRegister(centerNodes, centerMatchQueue, centerRelayPending,
		centerStatsSeries, centerIsLeader)
}

// CenterNodes 设置某类型节点数。
func CenterNodes(nodeType string, n int) {
	if !enabled.Load() {
		return
	}
	centerNodes.WithLabelValues(Service(), nodeType).Set(float64(n))
}

// CenterMatchQueue 设置匹配队列长度。
func CenterMatchQueue(n int) {
	if !enabled.Load() {
		return
	}
	centerMatchQueue.Set(float64(n))
}

// CenterRelayPending 设置待重试中转数。
func CenterRelayPending(n int) {
	if !enabled.Load() {
		return
	}
	centerRelayPending.Set(float64(n))
}

// CenterStatsSeries 设置统计序列数。
func CenterStatsSeries(n int) {
	if !enabled.Load() {
		return
	}
	centerStatsSeries.Set(float64(n))
}

// CenterIsLeader 设置选主状态。
func CenterIsLeader(v bool) {
	if !enabled.Load() {
		return
	}
	f := 0.0
	if v {
		f = 1
	}
	centerIsLeader.Set(f)
}

// ============================ Kafka ============================

var (
	kafkaLag      *prometheus.GaugeVec
	kafkaConsumed *prometheus.CounterVec
	kafkaProduced *prometheus.CounterVec
)

func registerKafka() {
	kafkaLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "kafka", Name: "consumer_lag",
		Help: "消费 lag（20.3 告警项）",
	}, []string{"service", "topic"})
	kafkaConsumed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "kafka", Name: "consumed_total",
		Help: "消费消息数",
	}, []string{"service", "topic", "result"})
	kafkaProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "kafka", Name: "produced_total",
		Help: "生产消息数",
	}, []string{"service", "topic", "result"})
	registry.MustRegister(kafkaLag, kafkaConsumed, kafkaProduced)
}

// KafkaLag 设置 topic 消费 lag。
func KafkaLag(topic string, lag int64) {
	if !enabled.Load() {
		return
	}
	kafkaLag.WithLabelValues(Service(), topic).Set(float64(lag))
}

// KafkaConsumed 消费计数（result: ok/retry/dead）。
func KafkaConsumed(topic, result string) {
	if !enabled.Load() {
		return
	}
	kafkaConsumed.WithLabelValues(Service(), topic, result).Inc()
}

// KafkaProduced 生产计数。
func KafkaProduced(topic string, ok bool) {
	if !enabled.Load() {
		return
	}
	result := "ok"
	if !ok {
		result = "error"
	}
	kafkaProduced.WithLabelValues(Service(), topic, result).Inc()
}

// ============================ HTTP 端点 ============================

// Server 指标 HTTP 服务（/metrics + /healthz + /readyz）。
type Server struct {
	srv      *http.Server
	listener net.Listener
}

// HealthFunc 健康检查函数（返回 nil 表示健康）。
type HealthFunc func(ctx context.Context) error

// NewServer 创建指标服务。
//   - addr 为空时不启动（返回 nil, nil）；
//   - health 返回 nil 表示 ready，非 nil 则 /readyz 返回 503。
func NewServer(addr string, health HealthFunc) (*Server, error) {
	if addr == "" {
		return nil, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	// /healthz 存活探针：进程活着即 200（不查依赖，避免中间件抖动导致重启风暴）
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// /readyz 就绪探针：依赖可用才 200（etcd 注册、DB、Redis 等）
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if health == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := health(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: " + err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	return &Server{
		listener: ln,
		srv:      &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
	}, nil
}

// Start 启动（非阻塞）。
func (s *Server) Start() {
	if s == nil {
		return
	}
	go func() {
		if err := s.srv.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// 指标服务失败不拖垮主服务，仅记录
			_ = err
		}
	}()
}

// Addr 返回实际监听地址（addr 传 :0 时用于测试）。
func (s *Server) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop 优雅关闭。
func (s *Server) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}
