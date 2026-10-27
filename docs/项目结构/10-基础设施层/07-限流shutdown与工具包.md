# 07-限流 shutdown 与工具包

一句话定位：本文覆盖游戏进程的「外围保障层」——`pkg/ratelimit` 用自研令牌桶在网关/聊天/登录三处频控，`pkg/shutdown` 用四阶段编排保证停机不丢数据，`pkg/log`/`metrics`/`mq`/`pool`/`flush`/`crypto`/`db`/`activity` 与 `cmd/flusher` 各司其职，共同构成六个进程共用的地基。

## 1. 概述

本篇的十个包有两个共同点：**不含任何业务知识**（不认识玩家、道具、战斗），**被全部进程依赖**（gateway/game/center/cross/login/flusher）。它们是前面几篇文档反复出现的「幕后角色」：Actor 停机时调 `shutdown.Manager`（见 [`01-Actor模型.md`](01-Actor模型.md)），落库 worker 的报错过滤用 `isShutdownCancel`（见 [`06-缓存cache与writebehind.md`](06-缓存cache与writebehind.md)），限流三处配置来自 `LimitConfig`（见 [`03-配置系统与热更新.md`](03-配置系统与热更新.md)）。

全文分三部分：两道自研组件（限流、停机）讲透设计；八个工具包按「定位 + 关键取舍 + 用法」速览，不重复源码注释已说清的部分。

## 2. 涉及文件

| 路径 | 职责 |
|---|---|
| `pkg/ratelimit/ratelimit.go` | 令牌桶：`Bucket`（单桶）与 `Keyed`（按 key 分桶 + 惰性清理），时钟可注入 |
| `pkg/shutdown/shutdown.go` | 优雅停机编排：四阶段序列、单步超时、错误聚合 |
| `pkg/log/log.go`、`doc.go` | zap 封装：结构化日志、service 字段固化、函数选项 |
| `pkg/metrics/metrics.go` | Prometheus 指标：自定义 Registry、emit 函数、`/metrics`+`/healthz`+`/readyz` |
| `pkg/mq/doc.go`、`kafka.go`、`idempotent.go`、`topics.go`、`message.go` | Kafka 生产者/消费者、信封与 biz_id 幂等、topic 常量 |
| `pkg/pool/buffer.go` | `sync.Pool` 字节缓冲/writer 复用（网关收发热路径） |
| `pkg/flush/flusher.go`、`batch.go`、`delete.go`、`exec.go`、`querier.go`、`flat.go` | 进程内分片落库框架（非缓存玩家通道）+ 合批 SQL 构建 |
| `pkg/crypto/rsa.go`、`handshake.go`、`aes.go`、`session_cipher.go` | RSA-OAEP 握手、AES-256-GCM 会话加密、Seq 防重放 |
| `pkg/db/db.go` | MySQL 连接池封装 + `Tx` 事务模板 |
| `pkg/model/*_gen.go`（60 个生成文件） | gendb 生成的表结构体与 CRUD/批量读写（另见 tools/gendb） |
| `pkg/activity/window.go` | 活动周期计算（固定基准等分，无时区依赖） |
| `cmd/flusher/main.go` | 独立 Tier-2 落库进程（复用游戏服配置） |

## 3. 核心设计

### 3.1 三道限流防线

限流分散在三个进程、三个层级，参数全部来自配置的 `LimitConfig`（三值语义：**0=默认开启、负数=关闭、正数=配置值**，见 [`03-配置系统与热更新.md`](03-配置系统与热更新.md) 第 4.2 节）：

| 层 | 装配点 | 默认参数 | 限流 key | 拦什么 |
|---|---|---|---|---|
| 网关包频率 | `internal/gateway/server.go:191` | 20 包/s（突发 40），idle 5min | 连接 ID（`router.go:148`） | 心跳之外的任何协议刷包；越早拦越省 CPU |
| 聊天频控 | `chat.go:221` 默认，`server_start.go:248` 用 `ChatInterval` 覆盖 | 1 条/s（突发 3），idle 10min | 玩家 ID（`chat.go:272`） | 聊天刷屏 |
| 登录防爆破 | `login/server.go:158`（默认值在 `auth.go:73`） | 20 次/min（突发 10），idle 10min | 客户端 IP（`auth.go:109`） | 密码爆破 |

三处都是 `rate<=0` 走 `SetPacketLimiter(nil)`/`SetChatLimiter(nil)` 的关闭路径——用配置关限流时调用点不用改，这正是 `NewKeyed` 把「不限流」做进实现而非靠调用方判空的原因。`rate<=0` 时 `Allow` 恒 true，关闭路径零分叉。

### 3.2 停机四阶段

`shutdown.Manager` 按显式 Phase 序列执行（`pkg/shutdown/shutdown.go:124`）：**DRAIN（摘注册拒新）→ DRAINING（停入口 + 排空在途）→ FLUSH（刷盘）→ STOP（停组件）**。顺序即正确性：先拒新后排空，否则在途永远收敛不到零；先刷盘后停组件，否则排空期/刷盘期的写入直接失败。

游戏服（`internal/game/server.go:219`）是注册步骤最多的进程，一张表看全貌：

| 阶段 | 步骤 | 动作 |
|---|---|---|
| DRAIN | `unregister` | 摘 etcd 注册 + 通知在线会话重连 + 等 `register_drain`（默认 3s）让网关完成摘流 |
| DRAINING | `stop-rpc` / `stop-consumers` / `drain-actors` | 停 gRPC、停 Kafka 消费者、`actorSys.Drain` 等信箱排空（状态写进 Redis） |
| FLUSH | `flush-all` | `wbWorker.FlushAll` 排空 Redis 脏队列 → `flusher.FlushAll` 非缓存玩家落库 → `flusher.Stop` |
| STOP | 七个步骤 | 停 cross/center 客户端、停 Actor 系统、关 Kafka producer、关 etcd、关 DB/Redis、停 metrics |

反过来（先 FLUSH 再停入口）会留一个窗口：FlushAll 扫完后仍有请求进来标脏，进程退出时这批数据谁都没落库——`f5a265d` 修的就是这个。

## 4. 关键代码解析

### 4.1 `pkg/ratelimit`：为什么不用 `x/time/rate`

文件头注释（`pkg/ratelimit/ratelimit.go:1`）给了答案：只需要「令牌桶 + 按 key 兜一张表」这一点能力，引入依赖不划算；而且需要注入时钟（测试要推进时间，不能真睡），自己写更直接。

```go
// pkg/ratelimit/ratelimit.go:126（节选）
func (k *Keyed) Allow(key string) bool {
	if k.rate <= 0 {
		return true
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	now := k.now()
	k.maybeSweep(now)

	b, ok := k.buckets[key]
	if !ok {
		b = &keyBucket{tokens: k.burst, last: now} // 新 key 初始满桶：允许一次突发
		k.buckets[key] = b
	} else if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * k.rate
		if b.tokens > k.burst {
			b.tokens = k.burst
		}
		b.last = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
```

四个设计点：

- **初始满桶**：新连接/新玩家第一次访问就带 `burst` 个令牌——登录后客户端连发几个请求是正常形态，不能被误伤；
- **按 key 分桶 + 惰性清理**：`maybeSweep` 每 `idle/2` 顺手扫一次 map，删掉超过 `idle` 未访问的桶。不起后台 goroutine 的理由写在注释里：限流器在热路径上，起 goroutine 要么多一个生命周期要管，要么停机时漏；清扫本身只是一趟 map 遍历；
- **`SetClock` 注入时钟**：默认 `time.Now`，测试传可推进的假时钟，精确控制补充速率与清理时机，不用真睡；
- **`Len()` 观测口**：运维/测试看当前跟踪的 key 数（测试用它断言清理生效）。

`Bucket` 与 `Keyed` 的区别只是「一张表」：前者单桶（全局总闸），后者按 key 分桶。两者构造都把 `burst<1` 兜底为 1——容量 0 的桶永远拒请求，几乎一定是配置错误。

### 4.2 `pkg/shutdown`：显式序列、单步超时、错误聚合

```go
// pkg/shutdown/shutdown.go:124（节选）
for _, phase := range []Phase{PhaseDrain, PhaseDraining, PhaseFlush, PhaseStop} {
	for _, st := range steps {
		if st.Phase != phase {
			continue
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("shutdown budget exhausted before %s: %w", st.Name, err))
			return errors.Join(errs...)
		}
		stepCtx, cancel := context.WithTimeout(ctx, timeout)
		err := st.Fn(stepCtx)
		cancel() // 立即 cancel 而非 defer：循环里 defer 会累积到 Run 返回
		if err != nil {
			errs = append(errs, fmt.Errorf("%s (%s): %w", st.Name, phase.String(), err))
		}
	}
}
return errors.Join(errs...)
```

五个关键决策：

1. **显式 Phase 序列而非 iota 排序**：新增 Phase 时忘记加进序列会直接漏执行，编译期不报错；显式列表让每次新增都改一处、可 review；
2. **单步失败不中断后续**：跳过清理只会让进程更难收场；但失败必须进返回值，由 main 转非零退出码——「保存失败」不该被日志里一行 Warn 掩盖；
3. **单步 20s 默认超时**：覆盖一次 Kafka/DB flush 批次的富余量，又不让单步卡死吞掉总预算（总预算由 ctx 控制，如 K8s 给的 30s，单步超时是二级防线）；
4. **预算耗尽如实报告**：`ctx` 到期后剩余步骤不再执行，记一条「budget exhausted before X」——K8s 只给有限关服时间，被放弃的步骤必须能在退出日志里看到；
5. **`cancel()` 立即调用**：循环体里 `defer cancel` 会把每个 stepCtx 背后的 timer 拖到 Run 返回，十次循环累积十个未释放 context（go vet lostcancel 检查项）。

`Step.Fn` 收到的是已包上单步超时的 ctx，**Fn 必须把 ctx 传给所有阻塞调用**（DB/Kafka/网络），否则超时形同虚设——这是注册方（各进程 main）的契约。

### 4.3 工具包速览

**pkg/log** —— 全进程日志唯一出口，禁止直接 import zap。`New(service)` 通过 `zap.Fields` 把 service 固化进每条日志，多进程日志汇到同一采集系统后可按 service 分流。选 zap 而非 slog 的理由：热路径（网关每秒数万包）下 slog 的反射与 `interface{}` 分配开销不可忽略。`Fatal` 只允许出现在启动路径——`os.Exit` 不执行 defer，运行期用它会绕过 shutdown 编排。

**pkg/metrics** —— 自定义 Registry（不用默认全局表，避免测试相互污染）+ `gs` 命名空间 + service 常量标签。两个要点：`Init` 里 `enabled.Store(true)` 必须放在**全部指标注册完成后**，否则别的 goroutine 可能看到开关已开但指标指针仍为 nil，直接 SIGSEGV；emit 函数统一形状是 `if metrics.Enabled() { ... }`，单次原子读几 ns，未初始化场景零开销。HTTP 三端点分工明确：`/healthz` 活着即 200（不查依赖，避免中间件抖动引发重启风暴），`/readyz` 依赖可用才 200（不可用返回 503 让负载均衡摘流）。

**pkg/mq** —— Kafka 唯一出入口，两个可靠性决策值得单记：生产者**同步写**（`Async=false`，`WriteMessages` 返回即已 ack，「发送成功」可直接当事实，本项目每秒几百~几千条的量级不值得用异步的丢消息风险换吞吐）；消费者**手动 commit**（`CommitInterval=0`），提交规则是「业务成功」或「失败消息已可靠写死信」才推进 offset——死信写不进去时绝不能提交，那等于既没处理也没留下痕迹，消息凭空消失，宁可让分区停住等重启重放。死信用独立的 `context.Background()` + 5s 超时：已经把消息取出来的这一条要尽力留下痕迹，不该被调用方取消连带取消。幂等由 biz_id 承担（`idem:{topic}:{biz_id}`，Redis SETNX，TTL 默认 24h），实现是 `pkg/cache.RedisDeduper`，接口定义在 mq 包（依赖倒置，消费方注入，单测可换 `NoopDeduper`）。

**pkg/pool** —— `sync.Pool` 复用 `bytes.Buffer` 与 `sizedWriter`。`Acquire` 后立即 Reset（池中对象可能残留上次内容，脏 buffer 直接写会把上个连接的字节拼进这个包）；`AcquireWriter(hint)` 的 hint 只是首次/扩容参考下限，池里的 writer 反复复用已长大的 buffer，容量随历史峰值自适应。契约：Acquire/Release 严格配对，Release 后不得再持有 `Bytes()` 结果。

**pkg/flush** —— 非缓存玩家的进程内落库通道（缓存玩家走 writebehind，两条通道互补）。`playerID` 经 crc32 哈希到 10 个分片（不用取模：发号器分配的 ID 带低位规律，取模会把相邻 ID 挤到同片），各分片在 `nextTick` 对齐的错峰时刻触发——对齐「interval 边界 + 分片偏移」而非「启动后每 interval 一次」，滚动发布时相位不会被打乱。`FlushAll` 停机排空带两道保险：ctx 到期即停；一轮之后脏数据没减少（持久化持续失败）即停并报错，避免「失败→重回 dirty→再失败」死循环。合批 SQL（`BuildBatchUpsert`）值全部走占位符，标识符只能由调用方保证是代码常量。

**pkg/crypto** —— 网关安全链四段：`rsa.go`（非对称原语与 PEM 解析，握手期一次性）→ `handshake.go`（ServerHello/ClientKeyExchange/Finished 状态机）→ `aes.go`（AES-256-GCM，每连接一个密钥）→ `session_cipher.go`（帧级 Seq + 64 位滑动窗口防重放）。会话密钥经 HKDF-SHA256 派生而非直接用 pre-master（域分隔 + 拉伸）。选 GCM 而非 CBC+HMAC：一次计算同时完成加密与认证，标准库有 AES-NI 硬件加速。防重放窗口实现是「一个 uint64 位图」：bit i 表示 `recvSeq-i` 是否已见，`1<<diff` 定位、`&` 查询、`|=` 置位，几条 CPU 指令完成全部判断——设计文档写的 1024 位窗口，实现取 64，注释里明确记录了差异。

**pkg/db** —— `sql.Open` 注册池（惰性不建连）→ 应用池参数 → `PingContext` 探活；Ping 失败先 Close 再返回，避免泄漏半初始化的池。`ConnMaxLife` 促使驱动定期重建连接，规避 MySQL `wait_timeout` 后「连接已被服务端关闭」的偶发报错。`Tx` 模板：fn 失败回滚、成功提交；注意它**没有** `defer tx.Rollback()`，fn panic 时事务不会回滚——对比 `worker.Persist` 里 `defer func(){ _ = tx.Rollback() }()` 的保险式写法，两种风格都在代码里，按调用方是否可能 panic 选择。

**pkg/model** —— gendb 从 `sql/schema` 生成的 60 个文件：表结构体 + CRUD + 批量 UPSERT + `XxxFlat` 变量（`flush.FlatOps[T]` 函数对）。业务代码零 SQL 字符串；`player_writebehind` 是框架表，不生成 Flat 标注，天然排除在模块注册之外（见 [`06-缓存cache与writebehind.md`](06-缓存cache与writebehind.md) 第 4.9 节）。

**pkg/activity** —— 活动周期从固定基准（2020-01-01 00:00 +08:00）起按持续天数等分。不需要存「活动什么时候开的」：给定任意时刻，中心服和每个游戏服都能独立算出同一个周期，消息丢了也不会算错。`FixedZone` 而非 `LoadLocation`：容器镜像常缺 tzdata，固定 +8 让计算结果在任何机器上确定；`Window` 的 `[start, end)` 左闭右开避免边界时刻同时属于两个周期（重复发奖）。

**cmd/flusher** —— 独立 Tier-2 落库进程。三个设计决策：复用游戏服配置（不读独立配置，保证两侧对批量大小、间隔的认知一致）；blank import `internal/game/module/register` 触发各模块 `init` 注册 Persister，自己不导入任何业务模块（防止业务演化拖着落库进程一起改）；`signal.NotifyContext` 取消后停止取新任务，已取出未落库的任务保留在 Redis，重启后续跑，不丢数据。进程退出会被进程管理器拉起，所以退出码非零即可，不需要内部重试到底。

## 5. 演进记录

| 提交 | 改了什么 | 为什么 |
|---|---|---|
| `eed6721` P0 | 工程骨架：目录结构 + 生成体系 + CI 五步卡点 | log/metrics/crypto/db 等包的首批落地 |
| `a0ed305` P1-P4 | 接入加密链路、Actor、持久化；pkg/crypto 四件套、pkg/pool、pkg/db、pkg/mq 初版 | 最小可运行链路的安全与存储地基 |
| `143c3b8` P14 | Redis 排行榜/分组统计/etcd 选主；pkg/metrics 补 center/idtab 指标族 | 全服玩法需要可观测性配套 |
| `e978d52` R1.2 | pkg/flush 保存回执与关服排空（`FlushAll` + 脏量不降即停的保险） | 停机只落「前 500 个玩家」、其余静默丢失的问题 |
| `f5a265d` R1.3/R1.4 | shutdown 关闭顺序落实为「停入口→排空在途→确认落库」；Kafka 死信失败不再提交 offset | 先 FlushAll 再停入口的窗口期数据没人落库；死信写失败还提交 offset 等于消息凭空消失 |
| `1051ef9` | 新增 `pkg/ratelimit`（Keyed 令牌桶）并接入网关包频率/聊天频控/登录防爆破 | 此前三道频控缺位：刷包打满网关 CPU、聊天可被脚本刷屏、登录接口可被爆破 |
| `c7933eb` | 引入 errgroup/lo/automaxprocs，修复预加载死锁 | automaxprocs 按 cgroup 配额设 GOMAXPROCS，K8s 限核时避免按宿主机核数开 P 导致 GC/调度抖动 |
| `497e603` | 修 shutdown 两个时间竞争导致的 flaky 测试 | 测试依赖真实时间边界，偶发失败削弱 CI 信号 |
| `6665059` | 修正 10 个「撒谎的 doc.go」（注释声称的能力与实现不符） | 文档失信比没有文档更糟 |
| `0e7d1f0`/`a40f144` | 全量补充详细中文注释与 Go 语言知识点 | 见 [`01-Actor模型.md`](01-Actor模型.md) 演进记录的同一轮 |
| `091ef50`/`ec6258d`/`a5f369f` | testify 三连：批量替换错误断言、机械转换 677 处、收尾 | if-断言从 1808 处降到 215 处（−88%），测试代码净减约 2000 行 |

演进主线：**停机语义一路收紧**（先有 FlushAll → 再有阶段顺序 → 再有不丢 offset 的死信规则），**频控从缺位到三道防线**，**可观测性从进程指标到分链路指标族**。

## 6. 测试情况

- **pkg/ratelimit**（`ratelimit_test.go`，6 项，全部用可推进假时钟）：突发后按速率补充（容量 3 打 3 个放行、第 4 个拒、过 500ms 补 1 个、过 10s 补满且不超容量）；`rate<=0` 关闭路径恒放行；**Keyed 按 key 隔离**（一个 key 打空不影响另一个）；**空闲 key 被清扫**（推进超过 idle 后 `Len()` 归零，证明惰性清理生效）；活跃 key 不被误清；并发 Allow（多 goroutine 抢同一 key，总数不超过 burst——锁正确性）。
- **pkg/shutdown**（`shutdown_test.go`，4 项）：按 Phase 顺序执行（阶段内按注册顺序）；**单步失败不中断且错误聚合**（两个失败步骤的错误都出现在 `errors.Join` 结果里）；**预算耗尽提前返回**（ctx 已取消时后续步骤记「budget exhausted」）；单步超时生效（慢步骤被 20s/配置超时切断）。
- **pkg/metrics**（3 项）：`Init` 注册后各指标可 Gather；HTTP 三端点行为（`/healthz` 200、`/readyz` 健康/不健康两种）；**未 Init 时 emit 函数安全**（子进程测试验证 nil 指标指针不 panic——`noinit_test.go` 用子进程跑，因为 Init 是进程级一次性动作）。
- **pkg/mq**（14 项）：信封 roundtrip 与缺 biz_id 拒绝、空 payload、`IdemKey` 格式、`NoopDeduper` 恒真、topic 唯一性；消费者七项覆盖全部提交规则——成功提交、重试后死信并提交、**死信写失败保留 offset**、畸形载荷进死信、无死信 writer 时保留 offset、commit 失败上浮、取消不提交。
- **pkg/flush**（`batch_test` 12 项 + `exec_test` 3 项 + `flusher_test` 10 项）：合批 UPSERT/删除的 SQL 与参数形态、行列不匹配报错、单列自赋值 no-op、切批（超 `DefaultBatchRows` 自动分多条）；`nextTick` 对齐 interval 边界且跟随 interval 缩放（测试环境 2s 周期不会等 60s）、墓碑随 upsert 同周期合批、删除失败保留墓碑、未注入 `DeleteFn` 时墓碑丢弃（只告警）、删除先于 upsert、`ShardIndex` 稳定性、`FlushAll` 排空全部批次、持久化失败保留脏数据。
- **pkg/crypto**（2 个文件）：加解密 roundtrip 与篡改检测、握手并发安全（`handshake_concurrency_test.go`）。
- 集成层：`TestP6GameplayFlow` 全流程回归（真实 MySQL/Redis + 网关 + 机器人）覆盖限流与停机路径；P15 演练（`331b5e5`）含故障注入下的关服排空验证，另见 [`../60-测试体系/01-测试总览与分层.md`](../60-测试体系/01-测试总览与分层.md)。

## 7. 小结

这一层的包有个共同气质：**把「迟早要出的事」提前设计好**。限流把「客户端失控」挡在网关外（三道防线、配置可关、测试不睡真觉）；停机把「进程总要重启」变成四阶段编排（顺序即正确性、失败必上浮、预算耗尽如实报告）；MQ 把「Kafka 至少一次」的死角用「手动 commit + 死信成功才提交 + biz_id 幂等」三件套补齐；落库框架把「写库压力」用分片错峰与合批摊平。它们单独看都不复杂，合在一起才是「分布式游戏服务端」与「能跑的 demo」之间的差别。

对使用者的三个提醒：注册 shutdown 步骤时，跨阶段依赖用 Phase 表达、阶段内依赖靠注册顺序，别指望注册顺序穿越阶段；新指标一律走 emit 函数 + `Enabled()` 前置开关，不要直接碰 prometheus 类型；新 topic 一律用 `pkg/mq` 常量，字符串字面量拼错只会在运行时表现为「消息发进黑洞」。
