// Package metrics 指标采集与暴露（设计文档 20.3 可观测性）。
//
// 设计要点：
//   - 统一注册到自定义 Registry（不用默认全局 registry，避免测试相互污染）；
//   - 服务名作为常量标签 `service`，同一个 Grafana 面板可按服务聚合；
//   - 指标命名遵循 Prometheus 最佳实践：<namespace>_<subsystem>_<name>_<unit>；
//   - 全部指标在包初始化时注册一次，业务侧只调用 Set/Inc/Observe，不做动态注册
//     （动态注册会在高基数标签下泄漏内存）。
//
// 架构位置：所有进程（gate/game/center/cross/login/flusher）共享本包；
// 存储链路的消费者是 flush 与 writebehind——它们只调用 FlushObserve/
// FlushDirty/FlushTombstone 等emit 函数，不接触 prometheus 类型，
// 指标定义的增改因此不影响业务代码。
//
// 使用模式（本文件所有 emit 函数的统一形状）：
//
//	if metrics.Enabled() {                       // 热路径先查开关，未启用零开销
//	    metrics.FlushObserve(d)                  // 函数内部 WithLabelValues(Service())
//	}
//
// 开关语义：Enabled() 为 false 时所有 emit 函数立即返回——单测/CLI
// 不初始化指标时不会 panic（指标指针为 nil），也没有无谓的 label 查找。
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
//
// 出现在每个指标名最前（gs_gateway_connections_total），Grafana 里按
// 前缀过滤即可只看本项目的指标，避免与 node_exporter 等混淆。
const Namespace = "gs"

// registry 全局注册表（本包唯一）。
//
// 用 NewRegistry 而非 prometheus 默认注册表：默认表是进程级全局单例，
// 单测多次 Init/重复注册会 panic，自定义表把注册行为关进包内可控。
// 一个进程一个 registry，Register() 暴露给 /metrics handler 使用。
var registry = prometheus.NewRegistry()

// registerOnce 保证指标只注册一次。
//
// Init 可能被多处调用（各组件启动时），Once 把「注册 + enabled=true」
// 收敛为一次性动作；注意 Once.Do 内若 panic，Go 认为该 Once 已执行
// 完毕（后续 Do 直接返回不重试）——注册代码必须自身不 panic，
// MustRegister 的重复注册错误在 Once 保护下不会触发。
var registerOnce sync.Once

// serviceLabels 当前进程的服务名（由 Init 注入）。
//
// serviceName 用 RWMutex 保护：string 不能原子读写，且「一次写入 +
// 极多次读取」正是读写锁的最佳场景；enabled 用 atomic.Bool：
// 单字读可原子，Enabled() 在每条消息的热路径上调用，无锁读更快。
// 两种原语的选择依据是「值的大小与访问模式」，不是偏好。
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
//
// Go 细节：atomic.Bool 的 Store/Load 在 Go 内存模型下建立
// happens-before 关系——enabled.Store(true) 之前的所有写
// （各 register* 函数对包级指针的赋值）对随后 Load 到 true 的
// goroutine 可见，这就是「先注册后开开关」无需额外锁的原因。
func Init(service string) {
	// 先记服务名（RLock 读取方立刻能看到新值），再做一次性注册
	serviceMu.Lock()
	serviceName = service
	serviceMu.Unlock()

	registerOnce.Do(func() {
		// Go 运行时 + 进程级指标：GC、goroutine 数、CPU、内存、fd 等，
		// 排查落库延迟类问题时的第一手上下文
		registry.MustRegister(
			collectors.NewGoCollector(),
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		)
		registerConn()
		registerMessage()
		registerActor()
		registerReplica()
		registerFlush()
		registerChannel()
		registerCross()
		registerLogin()
		registerIDTab()
		registerGameData()
		registerOwnerIndex()
		registerRedisBreaker()
		registerCenter()
		registerKafka()
		// 开关最后置 true：见函数注释的竞态窗口说明
		enabled.Store(true)
	})
}

// Service 返回当前服务名（供其他包拼标签）。
//
// 未 Init 时返回 "unknown"（零值兜底），保证 emit 函数在未初始化
// 场景也不会因为空 service 标签而难排查。
func Service() string {
	serviceMu.RLock()
	defer serviceMu.RUnlock()
	return serviceName
}

// Enabled 指标是否已初始化。
// 各组件在热路径（Actor 每轮循环、每条消息）先判断此开关，
// 避免未启用指标的场景（单测、CLI 工具）产生无谓开销。
//
// 单次原子读约几 ns，远低于一次 WithLabelValues 的 map 查找 +
// 互斥锁开销——这正是热路径前置开关的理由。
func Enabled() bool { return enabled.Load() }

// Registry 返回指标注册表（测试用）。
//
// 测试用它 Gather 断言指标值（如 flusher_test 验证 dirty 计数）；
// 业务代码不应直接接触，统一走 emit 函数。
func Registry() *prometheus.Registry { return registry }

// ============================ 连接与消息 ============================

// 连接/消息指标的 label 语义：service 恒定（本进程名），
// direction/result 是低基数枚举——label 基数直接决定 Prometheus 的
// 时间序列数量与内存，绝不放 player_id 之类的高基数值进 label。
var (
	connGauge   *prometheus.GaugeVec
	connTotal   *prometheus.CounterVec
	msgCounter  *prometheus.CounterVec
	msgInflight *prometheus.GaugeVec
	msgLatency  *prometheus.HistogramVec
	msgSize     *prometheus.HistogramVec
)

// registerConn 注册网关连接指标（Gauge 当前值 + Counter 累计）。
//
// 选型依据：当前连接数可增可减 → Gauge；累计接受数只增不减 → Counter。
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

// registerMessage 注册消息计数/在途/延迟/大小指标。
//
// Histogram 的桶按「目标 SLO 附近密、两端疏」的原则选：latency 桶
// 覆盖 1ms~2.5s（20.1 目标 P99<50ms，50ms 附近有 25/50 两个桶），
// size 桶对齐协议层 64B~64KB 的常见包型。
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
//
// Gauge +1、Counter +1 成对出现：回答「现在多少连接」与「累计接过
// 多少连接」两个问题（连接数下降时前者降、后者不降）。
func ConnInc() {
	if !enabled.Load() {
		return
	}
	connGauge.WithLabelValues(Service()).Inc()
	connTotal.WithLabelValues(Service(), "accepted").Inc()
}

// ConnDec 连接关闭。
//
// 与 ConnInc 严格配对调用（网关连接生命周期两端），否则 Gauge 会漂移
// 成负数——漂移的连接数指标比没有指标更误导。
func ConnDec() {
	if !enabled.Load() {
		return
	}
	connGauge.WithLabelValues(Service()).Dec()
}

// ConnRejected 拒绝连接（超 max_conns / 握手失败等）。
//
// result 标签是 rejected_<reason> 拼接：reason 是封闭枚举
// （代码内固定几种），不是用户输入——高基数的 reason 会引爆序列数。
func ConnRejected(reason string) {
	if !enabled.Load() {
		return
	}
	connTotal.WithLabelValues(Service(), "rejected_"+reason).Inc()
}

// MsgInc 消息计数（direction: in/out；result: ok/error）。
//
// direction/result 都是调用方传入的封闭枚举；按结果拆分是告警的
// 输入（error 率突增 = 协议或业务异常）。
func MsgInc(direction, result string) {
	if !enabled.Load() {
		return
	}
	msgCounter.WithLabelValues(Service(), direction, result).Inc()
}

// MsgObserve 消息处理耗时与大小。
//
// 一条消息只 Observe 一次（发送前或接收后，由调用方选定点）；
// size<=0 时跳过大小观测（部分消息无 body，如纯 ACK）。
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
//
// 与 MsgInflightDec 配对；两者之差即「正在处理的消息数」，
// 持续增长说明 handler 集体变慢（背压信号）。
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
	actorRejected   *prometheus.CounterVec
	actorCountGauge prometheus.Gauge
)

// registerActor 注册 Actor 模型指标（信箱深度/处理/延迟/panic/拒绝/存活数）。
//
// mailbox_depth 与 rejected_total 是一对背压观测点：信箱满时拒绝
// **新**消息、保留旧消息——深度上涨 + 拒绝计数上涨 = 消费者跟不上，
// 是扩容或降级的触发信号。
func registerActor() {
	actorMailbox = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "mailbox_depth",
		Help: "Actor 信箱当前积压消息数（有界；满时拒绝新消息，不丢弃旧消息）",
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
	actorRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "actor", Name: "rejected_total",
		Help: "信箱满时被拒绝的新消息数（背压；调用方收到 ErrMailboxFull 并自行重试/降级）",
	}, []string{"service"})
	registry.MustRegister(actorMailbox, actorProcessed, actorLatency, actorPanic, actorRejected, actorCountGauge)
}

// ActorMailbox 设置某分片信箱深度（只在分片循环里低频调用，避免锁竞争）。
//
// Set 而非 Inc：深度是「当前值」，每轮循环重设即可，不需要增减配对；
// 低频调用是因为 WithLabelValues 内部有 map+锁，热路径逐消息调用
// 会与业务争锁。
func ActorMailbox(shard int, depth int) {
	if !enabled.Load() {
		return
	}
	actorMailbox.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Set(float64(depth))
}

// ActorProcessed 处理计数。
//
// ok=false 记 error：与 MsgInc 的 result 拆分同理，错误率是告警输入；
// panic 不计入 error（单独走 ActorPanicInc，避免掩盖崩溃）。
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
//
// 与 MsgObserve 的区别：这里只测 Actor 内处理段，不含网关排队与
// 网络往返——两者对比可定位延迟出在队列还是处理。
func ActorObserve(d time.Duration) {
	if !enabled.Load() {
		return
	}
	actorLatency.WithLabelValues(Service()).Observe(d.Seconds())
}

// ActorPanicInc panic 计数。
//
// Actor 循环 recover 后调用：panic 被吞掉但必须留痕，否则
// 「偶发丢消息」类故障永远查不到根因。
func ActorPanicInc() {
	if !enabled.Load() {
		return
	}
	actorPanic.WithLabelValues(Service()).Inc()
}

// ActorRejectedAdd 信箱背压拒绝计数（满队列拒绝新消息，旧消息保留）。
//
// 用 Add(n) 而非 Inc()：一次拒绝可能批量上报（分片循环攒了一轮
// 的拒绝数），Add 支持一次性加 N。
func ActorRejectedAdd(n int64) {
	if !enabled.Load() {
		return
	}
	if n > 0 {
		actorRejected.WithLabelValues(Service()).Add(float64(n))
	}
}

// ActorCountAdd 存活 Actor 数变化。
//
// delta 可正可负（上线/下线），是进程级单 Gauge（无 label）——
// 按分片拆没有意义，Actor 总数才是容量信号。
func ActorCountAdd(delta float64) {
	if !enabled.Load() {
		return
	}
	actorCountGauge.Add(delta)
}

// ============================ Replica（Redis 副本异步回写） ============================

// replica 子系统指标：供 pkg/writebehind.Replica 使用。
//
// 三个指标对应三种故障形态：
//   - pending 持续高位：Redis 写不动或连接打满，副本越落越远；
//   - lag 上涨：待刷玩家的等待时间，超 2s 说明跨节点读到的已明显落后；
//   - dropped 非零：队列打满/重试用尽/租约被接管，副本已经丢数据。
var (
	replicaPending *prometheus.GaugeVec
	replicaLag     *prometheus.GaugeVec
	replicaRows    *prometheus.CounterVec
	replicaFail    *prometheus.CounterVec
	replicaDropped *prometheus.CounterVec
)

func registerReplica() {
	replicaPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "replica", Name: "pending_players",
		Help: "待回写副本的玩家数（合并后）",
	}, []string{"service"})
	replicaLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "replica", Name: "lag_seconds",
		Help: "单轮内最旧一条记录的等待秒数（副本新鲜度）",
	}, []string{"service"})
	replicaRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "replica", Name: "rows_total",
		Help: "累计回写成功次数",
	}, []string{"service"})
	replicaFail = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "replica", Name: "failures_total",
		Help: "回写失败次数（记录留在队列重试）",
	}, []string{"service"})
	replicaDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "replica", Name: "dropped_total",
		Help: "丢弃的回写记录（reason: queue_full/max_attempts/lease_superseded）",
	}, []string{"service", "reason"})
	registry.MustRegister(replicaPending, replicaLag, replicaRows, replicaFail, replicaDropped)
}

// ReplicaPending 设置待回写玩家数。
func ReplicaPending(n int) {
	if !enabled.Load() {
		return
	}
	replicaPending.WithLabelValues(Service()).Set(float64(n))
}

// ReplicaLag 设置副本新鲜度（秒）。
//
// 告警建议：lag > 2s 持续 1 分钟 → 副本已落后到影响跨节点读；
// lag > 10s 或 pending > 10000 → Redis 基本不可用，按故障处理。
func ReplicaLag(seconds float64) {
	if !enabled.Load() {
		return
	}
	replicaLag.WithLabelValues(Service()).Set(seconds)
}

// ReplicaRows 回写成功条数。
func ReplicaRows(n int) {
	if !enabled.Load() || n <= 0 {
		return
	}
	replicaRows.WithLabelValues(Service()).Add(float64(n))
}

// ReplicaFail 回写失败次数。
func ReplicaFail() {
	if !enabled.Load() {
		return
	}
	replicaFail.WithLabelValues(Service()).Inc()
}

// ReplicaDropped 丢弃记录（副本丢数据，但内存权威不受影响）。
func ReplicaDropped(reason string) {
	if !enabled.Load() {
		return
	}
	replicaDropped.WithLabelValues(Service(), reason).Inc()
}

// ============================ Flusher ============================

// flush 子系统指标：供 pkg/flush 分片落库与 20.3 告警项使用。
// lag_seconds 是核心告警源（落库延迟超过 2 个周期即触发）。
var (
	flushDirty    *prometheus.GaugeVec
	flushDuration *prometheus.HistogramVec
	flushRows     *prometheus.CounterVec
	flushFail     *prometheus.CounterVec
	flushLag      *prometheus.GaugeVec
	flushTomb     *prometheus.CounterVec
)

// registerFlush 注册落库指标（按 shard 标签拆分各分片）。
//
// duration 桶覆盖 5ms~5s：单分片单轮落库正常在百 ms 级，超过 1s
// 说明数据库侧已吃力（或 batch 过大），是需要扩容/降配的信号。
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
	flushTomb = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "flush", Name: "tombstone_total",
		Help: "物理删除（墓碑）行数（按结果：ok/fail/dropped）",
	}, []string{"service", "shard", "result"})
	registry.MustRegister(flushDirty, flushDuration, flushRows, flushFail, flushLag, flushTomb)
}

// FlushTombstone 墓碑删除计数。
//
// result 三态：ok（删除成功）/ fail（失败回填重试）/ dropped
// （未注入 DeleteFn 被丢弃）——dropped 持续上涨说明删除链路没接，
// 数据只删内存不删库，属于必须修通的配置问题。
func FlushTombstone(shard int, result string, n int) {
	if !enabled.Load() {
		return
	}
	flushTomb.WithLabelValues(Service(), fmt.Sprintf("%d", shard), result).Add(float64(n))
}

// FlushDirty 设置分片脏实体数。
//
// 每轮 flush 后重设（当前值语义）；持续高位不降 = 落库吞吐跟不上
// 写入速度，是 20.3 的容量告警输入。
func FlushDirty(shard, n int) {
	if !enabled.Load() {
		return
	}
	flushDirty.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Set(float64(n))
}

// FlushObserve 单轮落库耗时。
//
// 无 shard 标签：耗时分布按进程聚合更有意义（分片级噪音大），
// 需要分片下钻时看 duration 的分位数 + dirty 指标交叉分析。
func FlushObserve(d time.Duration) {
	if !enabled.Load() {
		return
	}
	flushDuration.WithLabelValues(Service()).Observe(d.Seconds())
}

// FlushRows 落库行数（ok/fail）。
//
// 只累计 ok 分支：失败行数由 FlushFail 按轮次计数，两个指标口径
// 不同（行 vs 轮），混在一个 Counter 里会算错速率。
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
//
// 同时把 lag 归零重算：失败时刻即「新起算点」，否则 lag 会在
// 持续失败中越拖越大、失去「距上次成功」的语义。
func FlushFail(shard int) {
	if !enabled.Load() {
		return
	}
	flushFail.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Inc()
	// 失败时延迟从这一刻起算
	flushLag.WithLabelValues(Service(), fmt.Sprintf("%d", shard)).Set(0)
}

// FlushLag 设置落库延迟（秒）。
//
// 调用方（flusher.runShard）传入 now-lastSuccess；本函数只负责
// 落值。告警规则：lag > 2×interval 即触发（20.3）。
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

// registerChannel 注册 Channel 池指标（gateway ↔ backend 的长连接池）。
//
// streams 是「当前活跃流」（有界资源，上限 2000），pool_size 是
// 「池内物理连接数」——两个数接近上限说明需要扩容池，错误计数
// 上涨说明 backend 侧不健康。
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
//
// 由池按周期重设（当前值语义）；接近 2000 上限时应告警——
// 再涨会被动拒绝新流，表现为玩家连不上。
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
//
// op 是封闭枚举（dial/send/recv 等），低基数；按 op 拆分可定位
// 失败发生在建链还是收发阶段。
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

// registerCross 注册跨服房间指标（center/cross 进程）。
//
// rooms 按 state 标签拆分（等待/进行中/结算中），settled 按 reason
// 拆分（all_reported/timeout/closed）——timeout 占比是跨服链路
// 健康度的直接信号（timeout 多说明有节点不上报）。
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
//
// reason=timeout 的持续上涨意味着跨服节点失联或变慢，需要结合
// 各 game 服的 flush/kafka 指标联合排查。
func CrossSettled(reason string) {
	if !enabled.Load() {
		return
	}
	crossSettled.WithLabelValues(Service(), reason).Inc()
}

// ============================ 登录 ============================

var loginPreload *prometheus.HistogramVec

// registerLogin 注册登录预加载耗时（login/game 进程共用）。
//
// 桶覆盖 1ms~5s：20.1 目标 P99 < 2s，2s 处有桶边界；按 result
// 拆分（ok/error）避免失败重试把 P99 抬失真。
func registerLogin() {
	loginPreload = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "login", Name: "preload_duration_seconds",
		Help:    "登录预加载耗时（20.1 目标 P99 < 2s）",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"service", "result"})
	registry.MustRegister(loginPreload)
}

// LoginPreloadObserve 记录一次预加载耗时。
//
// ok=false 也要 Observe：失败路径的耗时分布是区分「慢失败」
// （等超时）与「快失败」（参数错）的依据。
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

// ============================ ID 表 ============================

var (
	idtabSize      *prometheus.GaugeVec
	idtabReady     *prometheus.GaugeVec
	idtabReconcile *prometheus.CounterVec
	idtabBuildDur  *prometheus.HistogramVec
)

// registerIDTab 注册 ID 表（存在性判断内存集）指标。
//
// ready=0 表示该表还在构建，存在性判断回落 MySQL——是「缓存未热」
// 的明确信号；reconcile_mismatch 是 20.3 告警项：对账不一致说明
// 缓存与库已漂移，需要触发重建。
func registerIDTab() {
	idtabSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "idtab", Name: "size",
		Help: "ID 表集合大小（条数）",
	}, []string{"service", "table"})
	idtabReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "idtab", Name: "ready",
		Help: "ID 表构建完成标记（1/0）。0 表示存在性判断回落 MySQL",
	}, []string{"service", "table"})
	idtabReconcile = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "idtab", Name: "reconcile_mismatch_total",
		Help: "对账发现数量不一致的次数（20.3 告警项）",
	}, []string{"service", "table"})
	idtabBuildDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "idtab", Name: "build_duration_seconds",
		Help:    "ID 表构建耗时",
		Buckets: []float64{0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
	}, []string{"service", "table"})
	registry.MustRegister(idtabSize, idtabReady, idtabReconcile, idtabBuildDur)
}

// IDTabState 上报某张表 ID 集合的状态。
//
// size 与 ready 一起 Set（一个调用两指标），保证两者在时间点上
// 一致——分两次 Set 会让抓取窗口看到「size 已更新但 ready 仍 0」
// 的矛盾态。
func IDTabState(table string, size int64, ready bool) {
	if !enabled.Load() {
		return
	}
	idtabSize.WithLabelValues(Service(), table).Set(float64(size))
	r := 0.0
	if ready {
		r = 1
	}
	idtabReady.WithLabelValues(Service(), table).Set(r)
}

// IDTabReconcileMismatch 对账不一致计数。
func IDTabReconcileMismatch(table string) {
	if !enabled.Load() {
		return
	}
	idtabReconcile.WithLabelValues(Service(), table).Inc()
}

// IDTabBuildObserve 记录单表构建耗时。
//
// 构建在启动/重建时发生（分钟级也可能），桶到 60s；
// 耗时突增 = 表数据量异常增长或数据库变慢。
func IDTabBuildObserve(table string, d time.Duration) {
	if !enabled.Load() {
		return
	}
	idtabBuildDur.WithLabelValues(Service(), table).Observe(d.Seconds())
}

// ============================ 配置表热更 ============================

var (
	gamedataReload    *prometheus.CounterVec
	gamedataReloadDur *prometheus.HistogramVec
)

// registerGameData 注册热更指标（R16）。
//
// result 只有 ok/fail 两个值：热更是低频运维动作，按表名/目录分标签只会
// 制造时间序列而不提供新信息——具体哪张表失败看日志里的 err。
func registerGameData() {
	gamedataReload = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "gamedata", Name: "reload_total",
		Help: "配置表热更次数（result=ok/fail；fail 时当前快照保持不变）",
	}, []string{"service", "result"})
	gamedataReloadDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "gamedata", Name: "reload_duration_seconds",
		Help:    "热更加载耗时（读目录 + 33 张表解析 + 建索引）",
		Buckets: []float64{0.005, 0.01, 0.05, 0.1, 0.5, 1, 2},
	}, []string{"service"})
	registry.MustRegister(gamedataReload, gamedataReloadDur)
}

// GameDataReload 记录一次热更结果。
//
// took <= 0 时不观测耗时：失败路径（目录不可读、JSON 解析失败）的耗时
// 没有比较意义，混进直方图会污染「热更变慢」这个告警信号。
func GameDataReload(result string, took time.Duration) {
	if !enabled.Load() {
		return
	}
	gamedataReload.WithLabelValues(Service(), result).Inc()
	if took > 0 {
		gamedataReloadDur.WithLabelValues(Service()).Observe(took.Seconds())
	}
}

// ============================ 归属索引 ============================

var (
	ownerIndexSize      *prometheus.GaugeVec
	ownerIndexReady     *prometheus.GaugeVec
	ownerIndexReconcile *prometheus.CounterVec
	ownerIndexBuildDur  *prometheus.HistogramVec
)

// registerOwnerIndex 注册归属索引（by_owner ZSET）指标。
//
// 与 idtab 指标分开：同一张表可能两个结构都建（存在性集合 + 归属索引），
// 合成一个指标族会让 size 标签互相覆盖。ready=0 表示索引还在构建，
// 登录展开回落 MySQL——「缓存未热」的明确信号；reconcile_mismatch 是
// 对账告警项：索引与库已漂移。
func registerOwnerIndex() {
	ownerIndexSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "owner_index", Name: "size",
		Help: "归属索引成员数（ZSET 全量）",
	}, []string{"service", "table"})
	ownerIndexReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "owner_index", Name: "ready",
		Help: "归属索引构建完成标记（1/0）。0 表示登录展开回落 MySQL",
	}, []string{"service", "table"})
	ownerIndexReconcile = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "owner_index", Name: "reconcile_mismatch_total",
		Help: "对账发现数量不一致的次数（20.3 告警项）",
	}, []string{"service", "table"})
	ownerIndexBuildDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: "owner_index", Name: "build_duration_seconds",
		Help:    "归属索引构建耗时",
		Buckets: []float64{0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
	}, []string{"service", "table"})
	registry.MustRegister(ownerIndexSize, ownerIndexReady, ownerIndexReconcile, ownerIndexBuildDur)
}

// OwnerIndexState 上报某张表归属索引的状态（size 与 ready 一个调用两指标）。
func OwnerIndexState(table string, size int64, ready bool) {
	if !enabled.Load() {
		return
	}
	ownerIndexSize.WithLabelValues(Service(), table).Set(float64(size))
	r := 0.0
	if ready {
		r = 1
	}
	ownerIndexReady.WithLabelValues(Service(), table).Set(r)
}

// OwnerIndexReconcileMismatch 对账不一致计数。
func OwnerIndexReconcileMismatch(table string) {
	if !enabled.Load() {
		return
	}
	ownerIndexReconcile.WithLabelValues(Service(), table).Inc()
}

// OwnerIndexBuildObserve 记录单表索引构建耗时。
func OwnerIndexBuildObserve(table string, d time.Duration) {
	if !enabled.Load() {
		return
	}
	ownerIndexBuildDur.WithLabelValues(Service(), table).Observe(d.Seconds())
}

// ============================ Redis 熔断 ============================

var (
	redisBreakerState    *prometheus.GaugeVec
	redisBreakerOpened   *prometheus.CounterVec
	redisBreakerRejected *prometheus.CounterVec
)

// registerRedisBreaker 注册熔断指标。
//
// state 是 0/1/2（closed/open/half_open），配一条 `state > 0` 的告警即可
// 覆盖「Redis 不可用」；opened_total 与 rejected_total 的比值能看出
// 熔断是否在频繁抖动（抖动说明阈值偏低或故障是间歇性的）。
func registerRedisBreaker() {
	redisBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace, Subsystem: "redis_breaker", Name: "state",
		Help: "熔断状态：0=closed 1=open 2=half_open",
	}, []string{"service"})
	redisBreakerOpened = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "redis_breaker", Name: "opened_total",
		Help: "熔断打开次数（频繁增长说明故障在抖动）",
	}, []string{"service"})
	redisBreakerRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: "redis_breaker", Name: "rejected_total",
		Help: "被熔断快速拒绝的命令数（这些请求没有发往 Redis）",
	}, []string{"service"})
	registry.MustRegister(redisBreakerState, redisBreakerOpened, redisBreakerRejected)
}

// RedisBreakerState 上报熔断状态（0=closed 1=open 2=half_open）。
func RedisBreakerState(state int32) {
	if !enabled.Load() {
		return
	}
	redisBreakerState.WithLabelValues(Service()).Set(float64(state))
}

// RedisBreakerOpened 熔断打开一次。
func RedisBreakerOpened() {
	if !enabled.Load() {
		return
	}
	redisBreakerOpened.WithLabelValues(Service()).Inc()
}

// RedisBreakerRejected 记一条被熔断拒绝的命令。
func RedisBreakerRejected() {
	if !enabled.Load() {
		return
	}
	redisBreakerRejected.WithLabelValues(Service()).Inc()
}

// ============================ 中心服 ============================

var (
	centerNodes        *prometheus.GaugeVec
	centerMatchQueue   prometheus.Gauge
	centerRelayPending prometheus.Gauge
	centerStatsSeries  prometheus.Gauge
	centerIsLeader     prometheus.Gauge
)

// registerCenter 注册中心服指标（节点/匹配队列/中转/统计序列/选主）。
//
// is_leader 是单 Gauge（选主状态进程级唯一，不需要 label）；
// relay_pending 持续上涨说明中转消息发不出去（backend 不可达），
// 是跨服链路的先行故障信号。
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
//
// 序列数 = group × feature 的基数，持续上涨预示内存压力——
// 应在采集端对 group 基数做上限（高基数同样是指标/内存杀手）。
func CenterStatsSeries(n int) {
	if !enabled.Load() {
		return
	}
	centerStatsSeries.Set(float64(n))
}

// CenterIsLeader 设置选主状态。
//
// 0/1 而非 true/false：Gauge 只吃 float64，布尔转 0/1 是
// Prometheus 的惯例（方便直接当开关查询：== 1）。
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

// registerKafka 注册 MQ 指标（pkg/mq 的消费者/生产者共用）。
//
// consumer_lag 是 20.3 告警项：lag 持续上涨 = 消费速度跟不上生产，
// 最终表现为业务延迟（如落库排队）；按 topic 拆分定位哪个主题堵。
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
//
// 由消费者周期采样上报（非每条消息）——lag 本身是 broker 侧累计值，
// 重设即可，不需要增减配对。
func KafkaLag(topic string, lag int64) {
	if !enabled.Load() {
		return
	}
	kafkaLag.WithLabelValues(Service(), topic).Set(float64(lag))
}

// KafkaConsumed 消费计数（result: ok/retry/dead）。
//
// retry/dead 占比是消息质量信号：dead 上涨说明幂等表或业务
// 校验持续拒绝，需要人工介入（死信不再自动重投）。
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
//
// 三端点分工（k8s 探针语义）：
//   - /metrics：Prometheus 抓取；
//   - /healthz：存活探针——进程活着即 200，**不查依赖**，避免中间件
//     抖动引发重启风暴（重启解决不了 DB/Redis 故障，只会放大）；
//   - /readyz：就绪探针——依赖（etcd 注册、DB、Redis）可用才 200，
//     不可用时返回 503 让负载均衡摘流，比「活着但服务不了」更诚实。
//
// nil 接收者宽容：所有方法都容忍 s == nil（addr 为空即不启动），
// 调用方无需到处判空——这是 Go 的常见防御式 API 姿态。
type Server struct {
	// srv 底层 http.Server（Handler 为上面的 mux）。
	srv *http.Server
	// listener 启动时 bind 的监听器；持有它是为了 Addr() 能回报
	// :0 场景下的实际端口，也为了让 Stop 精确关闭这一个 server。
	listener net.Listener
}

// HealthFunc 健康检查函数（返回 nil 表示健康）。
//
// 由进程注入（查 DB/Redis/etcd）；被 /readyz 以 2s 超时调用——
// 超时同样按不健康处理（慢依赖也是不就绪）。
type HealthFunc func(ctx context.Context) error

// NewServer 创建指标服务。
//   - addr 为空时不启动（返回 nil, nil）；
//   - health 返回 nil 表示 ready，非 nil 则 /readyz 返回 503。
//
// 流程：先 Listen（bind 失败尽早暴露，比 Serve 时才报错好）→
// 组 mux 注册三端点 → 组装 Server。
//
// Go 细节：
//   - 返回 (*Server)(nil), nil 的「nil, nil」组合配合下面的 nil 接收者
//     方法，让「不启用指标服务」的配置路径零分支；
//   - ReadHeaderTimeout 防 Slowloris 类攻击（只发半截头不发的
//     慢客户端会占用连接）；
//   - promhttp.ContinueOnError：单个指标采集出错只跳过该指标，
//     不让整个 /metrics 失败——一个坏指标不该拖垮全部可观测性。
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
	// profiling 端点（PPROF_ENABLE=1 才注册，见 pprof.go）
	registerPprof(mux)
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
//
// Serve 放进独立 goroutine；ErrServerClosed 是 Shutdown 的正常
// 返回，用 errors.Is 判定后忽略——把「主动关闭」与「真的服务
// 失败」区分开。其余错误只记录（_ = err 占位，由调用方需要时
// 扩展为日志），不拖垮主服务：指标端点挂不影响游戏逻辑。
//
// Go 细节：nil 接收者检查让「未启动的 Server」调用 Start 成为
// 安全的 no-op（NewServer 返回 nil 时调用方常常忘了判空）。
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
//
// :0 让内核自动选端口，测试用本方法拿真实端口拼 URL；
// nil 安全检查同 Start。
func (s *Server) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop 优雅关闭。
//
// Shutdown 等待在途请求处理完再关（区别于暴力 Close）：
// 进行中的 /metrics 抓取不会被切断。ctx 控制等待上限——
// 超时未完成则放弃等待返回 error（停机不能无限等一个
// 卡住的抓取连接）。nil 安全同 Start。
func (s *Server) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}
