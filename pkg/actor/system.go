// pkg/actor/system.go —— ActorSystem：分片事件循环与生命周期
//
// 架构职责：actor 包的调度核心——把 Actor 编入分片、为每个分片跑一个
// goroutine 事件循环、对外提供 Send/Spawn/Terminate/Drain 等 API，
// 并驱动定时器 goroutine。
// 协作关系：actor.go（cell 装配）、mailbox.go（信箱 push/pop/close）、
// context.go（每条消息的处理上下文）、pkg/metrics（深度/拒绝/panic 指标）。
// 关键取舍：
//   - 每分片单 goroutine + per-Actor budget 轮转：片内串行免锁，片间并行；
//   - wake 信号（cap 1）+ 100ms ticker：低延迟与兜底兼得；
//   - fnv 哈希而非 maphash：跨进程/重启稳定，本地缓存不因重启失效。
package actor

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/metrics"
)

// Logger Actor 系统日志接口（避免 pkg/actor 依赖具体日志实现）。
//
// 目前用于 panic 恢复与定时消息投递失败的可观测输出；
// keysAndValues 为交替的 key/value 对（slog 风格），由实现方解析。
// 依赖倒置：接口由上层（进程装配层）注入具体实现，actor 包本身不
// import 任何日志库——保持内核可单测、可复用。
type Logger interface {
	Error(msg string, keysAndValues ...any)
}

// System Actor 系统。
//
// 并发模型：actorID 经 fnv32a 哈希取模落到某个 shard，每个 shard 一个
// goroutine，串行处理名下所有 Actor 的消息——Actor 内部无锁，分片间并行。
// 与缓存的契约：一次 Actor 消息处理 = 一份快照 = 一次 CAS 提交，
// 见 docs/R2.1-缓存原子变更契约.md。
//
// 生命周期：NewSystem → Start → （业务运行）→ Stop；优雅关闭顺序为
// 停外部入口 → Drain 等在途消息 → 保存 → Stop（见 Drain 注释）。
//
// Go 知识点（System 字段的同步选择）：
//   - stopCh 用 close 而非 send 广播停止：close 会让所有分片 select 的
//     <-stopCh 同时返回零值，天然「一对多」信号；重复 close 会 panic，
//     故用 sync.Once 包住（Once.Do 保证全局只执行一次）；
//   - wg/timerWg 两个 WaitGroup 分别等分片与定时器 goroutine，粒度分开
//     便于 Stop 里依次 Wait；经典约束「Add 必须发生在 Wait 可能返回
//     之前」；
//   - shards slice 创建后只读（元素指针指向的 shard 内部另有锁），
//     可被所有 goroutine 无锁并发访问。
type System struct {
	// shardCount 分片数，配置要求 2 的幂（config.Validate 强校验）；
	// 寻址在 shardFor：fnv32a(id) % shardCount。
	shardCount  int
	mailboxSize int
	shards      []*shard
	logger      Logger
	wg          sync.WaitGroup
	stopOnce    sync.Once
	stopCh      chan struct{}

	// 定时器：timerLoop 每 100ms 线性扫描到期的 entry 并投递。
	// timerMu 保护 timers 切片——ScheduleOnce（业务 goroutine）与
	// fireTimers（定时器 goroutine）跨 goroutine 读写同一切片。
	timerMu sync.Mutex
	timers  []*timerEntry
	timerWg sync.WaitGroup
}

// Config ActorSystem 配置。
//
// ShardCount/MailboxSize <= 0 时分别回退默认 1024；Logger 可选，
// 不提供则 panic 时走 debug.PrintStack（输出到 stderr）。
// 按值传入（非指针）：配置是短生命周期小对象，值语义避免调用方之后
// 修改外部 Config 影响已创建的 System。
type Config struct {
	ShardCount  int
	MailboxSize int
	Logger      Logger // 可选：panic 恢复时记录堆栈
}

// NewSystem 创建 ActorSystem（不启动任何 goroutine，需显式 Start）。
//
// 分片在创建时全部就绪，Spawn 之前即可投递——消息先入信箱排队，
// 等 Start 后由分片循环消费。
// 默认 1024 分片：足够把热点玩家打散；空闲分片阻塞在 wake 上，
// 成本主要是内存而非 CPU。
// Go 知识点：shards 用 make 预分配后按下标填充，创建后不再 append——
// 「只读 slice」可被所有 goroutine 无锁并发访问。
func NewSystem(cfg Config) *System {
	// 默认 1024 分片：足够把热点玩家打散；空闲分片阻塞在 wake 上，
	// 成本主要是内存而非 CPU。
	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 1024
	}
	if cfg.MailboxSize <= 0 {
		cfg.MailboxSize = 1024
	}
	s := &System{
		shardCount:  cfg.ShardCount,
		mailboxSize: cfg.MailboxSize,
		shards:      make([]*shard, cfg.ShardCount),
		logger:      cfg.Logger,
		stopCh:      make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i] = newShard(i, s)
	}
	return s
}

// Start 启动所有分片 goroutine。
//
// wg.Add 必须在 go 之前完成：否则 Start 尚未返回时 Stop 若已跑完
// Wait，后启动的 goroutine 将漏等（经典 WaitGroup 竞态）。
// ctx 取消与 Stop 任一发生都会让循环退出。
// Go 知识点：context 取消沿 goroutine 调用树传播——Start 的 ctx 由
// 进程层持有，进程级超时/信号可一次性熄灭全部分片 goroutine，这是
// 防 goroutine 泄漏的主通道。
func (s *System) Start(ctx context.Context) {
	for _, sh := range s.shards {
		s.wg.Add(1)
		go sh.run(ctx, &s.wg)
	}
	s.timerWg.Add(1)
	go s.timerLoop(ctx)
}

// Stop 停止所有分片。
//
// stopOnce 保证 stopCh 只 close 一次（重复 close 会 panic）；之后等
// 所有分片与定时器 goroutine 退出。Stop 不等待在途消息处理完——
// 需要保证处理完应在 Stop 之前 Drain。
func (s *System) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
	s.timerWg.Wait()
}

// Drain 等待所有分片把信箱里的消息处理完（含已取出、正在执行的），ctx 到期返回错误。
//
// 关闭顺序里必须排在保存之前：在途消息的最后一次 Redis 提交如果没等到，
// 后续 FlushAll 读到的就是旧快照，玩家最后一段进度会丢。
//
// Drain 不阻止新消息投递，调用方应先停掉外部入口（gRPC、Kafka 消费者），
// 否则一边排空一边进新消息，只能等到超时。
// Go 知识点：返回的错误用 %w 包装 ctx.Err()，调用方可用
// errors.Is(err, context.DeadlineExceeded/Canceled) 区分超时与取消。
func (s *System) Drain(ctx context.Context) error {
	// 10ms 轮询：Drain 是关闭路径上的低频操作，该粒度足够；
	// 更短只是白烧 CPU，更长则拖长关闭总时长。
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.idle() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("actor drain: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// idle 所有分片都空闲才算 Drain 完成。
//
// 任一 shard 有积压或在途消息即返回 false——「全部空闲」是 AND 语义，
// 一个忙分片就能拖住整个 Drain。
func (s *System) idle() bool {
	for _, sh := range s.shards {
		if !sh.idle() {
			return false
		}
	}
	return true
}

// Spawn 创建 Actor。
//
// 只注册不启动：Actor 生命周期与信箱同在，Terminate 时移除；
// id 重复返回错误（分片内 ID 唯一，否则两条消息流会挤同一个信箱）。
// 可在 Start 之前调用（消息先排队，等人来消费）。
// 寻址与注册同用 shardFor(id)：保证「消息路由到的分片」与「Actor
// 注册的分片」永远一致，不会出现消息落错片。
func (s *System) Spawn(id string, actor Actor) (*PID, error) {
	sh := s.shardFor(id)
	pid := &PID{ID: id, Shard: sh.index}
	cell := newActorCell(pid, actor, s.mailboxSize, s)
	if err := sh.add(cell); err != nil {
		return nil, err
	}
	if metrics.Enabled() {
		metrics.ActorCountAdd(1)
	}
	return pid, nil
}

// Send 向 PID 发送消息。
//
// 匿名发送（无 sender）的快捷方式，语义同 SendWithSender(target, msg, nil)；
// 接收方 Respond 会因 sender 为 nil 而静默跳过。
func (s *System) Send(target *PID, msg any) error {
	return s.SendWithSender(target, msg, nil)
}

// SendWithSender 带发送者发送消息。
//
// sender 为 nil 表示匿名发送（定时器/系统消息），接收方 Respond 会跳过；
// target 为 nil 属于调用方 bug，返回错误而不 panic。
// Go 知识点：按 target.ID 重新计算分片而不是信 PID.Shard——寻址逻辑
// 单点收敛（改哈希算法只改一处），也防调用方伪造 Shard 字段；
// deliver 内的错误已用 %w 包装 ErrActorGone，errors.Is 可判。
func (s *System) SendWithSender(target *PID, msg any, sender *PID) error {
	if target == nil {
		return fmt.Errorf("target pid is nil")
	}
	sh := s.shardFor(target.ID)
	return sh.deliver(target.ID, envelope{sender: sender, message: msg})
}

// ScheduleOnce 调度一次性延迟消息。
//
// 入队即返回（O(1) 追加），到期扫描在 timerLoop；计时精度为 100ms
// tick，且重启即丢失——需要持久化的定时任务应落库或用其他调度。
// target 为 nil 时静默忽略（与 Send 报错不同：定时任务允许配空目标）。
// Go 知识点：timers 是跨 goroutine 共享的 slice，append 与 fireTimers
// 的原地过滤都必须持 timerMu——slice 非并发安全，两个 goroutine 同时
// append 可能丢元素或踩坏底层数组。
func (s *System) ScheduleOnce(d time.Duration, target *PID, msg any) {
	if target == nil {
		return
	}
	s.timerMu.Lock()
	s.timers = append(s.timers, &timerEntry{fireAt: time.Now().Add(d), target: target, msg: msg})
	s.timerMu.Unlock()
}

// shardFor 计算 id 归属的分片。
//
// 选 fnv 而非 maphash：maphash 的种子随进程变化，同一条 actorID 在
// 不同进程会落到不同分片；fnv 是固定算法，跨进程/跨重启结果一致
// （重启后同一玩家仍落同一分片，本地缓存不失效）。
// fnv 是纯函数（无进程级种子）——同输入在任何进程、任何次运行结果一致。
func (s *System) shardFor(id string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return s.shards[int(h.Sum32())%s.shardCount]
}

// Terminate 停止并移除 Actor。
//
// 幂等：移除不存在的 Actor 静默返回。关闭信箱后新 push 全部返回
// ErrActorGone；已入队的消息仍会被分片排空（从已关闭 channel 读安全）。
// Terminate 不等待在途消息——需要等待请先 Drain。
// pid 为 nil 静默忽略（关闭路径上允许批量传 nil）。
// 顺序保证：shard.remove 内先 close 信箱再摘 map（同一把写锁），
// 不存在「已摘除但信箱仍可投递」的窗口。
func (s *System) Terminate(pid *PID) {
	if pid == nil {
		return
	}
	sh := s.shardFor(pid.ID)
	sh.remove(pid.ID)
}

type shard struct {
	index int
	cells map[string]*actorCell
	mu    sync.RWMutex
	sys   *System

	// wake 唤醒信号（容量 1，非阻塞写）。
	//
	// 分片空闲时如果只靠 100ms ticker 轮询，消息到达后最坏要等 100ms 才被处理 ——
	// 实测业务 P99 因此被钉在 ~150ms，直接违反「P99 < 50ms」的验收目标。
	// 改为「有消息立即唤醒 + ticker 兜底」，延迟降到微秒级。
	// Go 知识点：cap 1 + struct{} 是 Go 惯用的「合并信号」——多个 Actor
	// 同时到达只保留一个信号（分片循环反正会排空全部信箱，不需要精确
	// 计数）；struct{} 零大小，信号 channel 不占负载。
	wake chan struct{}

	// inflight 已从信箱取出、尚未处理完的消息数。
	//
	// 关闭时需要区分「信箱空」和「真的处理完」：一条消息被 pop 之后信箱计数已归零，
	// 但 handler 可能还在写 Redis。只看 len(mbox) 会让 Drain 提前返回，丢掉最后一条业务。
	// atomic.Int64：分片循环写、idle() 读，单变量计数用原子操作即可，
	// 无需互斥锁（Go 1.19+ 的类型化 atomic 自带 64 位对齐保证）。
	inflight atomic.Int64

	// lastRejected 上次上报时的累计拒绝数（只由分片循环 goroutine 读写，无需加锁）
	lastRejected int64
}

// newShard 创建分片。wake 容量 1：多个 Actor 同时到达只保留一个唤醒
// 信号——分片循环会排空所有信箱，不需要精确计数；极端情况下信号被吞
// 时由 100ms ticker 兜底，不会丢消息。
func newShard(index int, sys *System) *shard {
	return &shard{
		wake:  make(chan struct{}, 1),
		index: index,
		cells: make(map[string]*actorCell),
		sys:   sys,
	}
}

// add 注册 cell 到分片；ID 已存在返回错误。
//
// 调用方（Spawn）已按 shardFor 选好分片，此处只做唯一性校验。
// cells map 由 mu 保护：add/remove 持写锁，deliver/idle/allCells 持读锁。
// Go 知识点：map 非并发安全——两个 goroutine 同时写会直接 fatal
// （concurrent map writes，不可 recover），一读一写也可能读到撕裂
// 状态，所以所有访问点都必须持锁。
func (sh *shard) add(cell *actorCell) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.cells[cell.pid.ID]; ok {
		return fmt.Errorf("actor %s already exists", cell.pid.ID)
	}
	sh.cells[cell.pid.ID] = cell
	return nil
}

// deliver 把消息投递到 id 对应的信箱并唤醒分片。
//
// 先取 cell 再 push：cell 取出后可能被并发 Terminate（close 信箱），
// 此时 push 返回 ErrActorGone——两种竞争结果都不会 panic。
// push 成功才 notify：失败（满/已终止）时信箱状态未变，无需唤醒。
// Go 知识点：fmt.Errorf("%w: %s", ErrActorGone, id) 用 %w 保留错误链，
// 调用方 errors.Is 判类型、%s 附带 id 供排查——错误既要机器可判
// 又要人可读。
func (sh *shard) deliver(id string, env envelope) error {
	sh.mu.RLock()
	cell, ok := sh.cells[id]
	sh.mu.RUnlock()
	if !ok {
		// 包一层 ErrActorGone：调用方可用 errors.Is 判定，同时带上 id 便于排查
		return fmt.Errorf("%w: %s", ErrActorGone, id)
	}
	if err := cell.mbox.push(env); err != nil {
		return err
	}
	sh.notify()
	return nil
}

// notify 唤醒分片循环（非阻塞：已有待处理信号时直接返回）。
//
// select+default 保证发送方永不被 wake 阻塞：分片循环可能正忙，
// 此时信号合并（丢弃多余唤醒）是安全的——循环每轮都会排空所有信箱。
func (sh *shard) notify() {
	select {
	case sh.wake <- struct{}{}:
	default:
	}
}

// remove 关闭信箱并摘除 cell；不存在则静默返回（幂等）。
//
// close 与 delete 在同一把写锁内：deliver 要么在锁外已取到 cell
// （push 时被 closed 挡住返回 ErrActorGone），要么看到 map 里已删除，
// 不会出现「cell 已不在 map、信箱却仍可投递」的窗口。
// Go 知识点：写锁同时覆盖 map 删除与信箱关闭，两个状态的切换对持
// 读锁的 deliver/idle 呈现为原子的一步。
func (sh *shard) remove(id string) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if cell, ok := sh.cells[id]; ok {
		cell.mbox.close()
		delete(sh.cells, id)
		if metrics.Enabled() {
			metrics.ActorCountAdd(-1)
		}
	}
}

// run 分片事件循环：本分片所有 Actor 的消息都在这个 goroutine 上串行处理。
func (sh *shard) run(ctx context.Context, wg *sync.WaitGroup) {
	// defer wg.Done()：即使循环从 return 退出（含未来可能出现的 panic
	// 路径），计数也一定减到 0，Stop 的 Wait 不会被永久挂住。
	defer wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	// defer ticker.Stop()：ticker 底层持有 runtime 定时器资源，不 Stop
	// 会泄漏直到 GC 兜底——time.Ticker 必须显式释放。
	defer ticker.Stop()
	for {
		// 先非阻塞检查退出条件：保证 ctx 取消/Stop 后最多再跑完一轮
		// drain 就退出，不会卡在下面的阻塞 select 上。
		select {
		case <-ctx.Done():
			return
		case <-sh.sys.stopCh:
			return
		default:
		}

		processed := sh.drain(100)
		// 指标：每轮更新一次信箱深度（避免每条消息打点带来的锁竞争）
		sh.reportDepth()
		if processed == 0 {
			// 空闲才阻塞等待：有活干时立即进入下一轮，不浪费一次 select
			// 调度。三个等待源分工明确：wake=新消息（快路径）、
			// ticker=兜底（定时器投递等不经过 notify 的场景）。
			select {
			case <-ctx.Done():
				return
			case <-sh.sys.stopCh:
				return
			case <-sh.wake: // 新消息到达，立即处理
			case <-ticker.C: // 兜底（定时器到期等不经过 deliver 的场景）
			}
		}
	}
}

// drain 处理一轮消息；budget 是**每个 Actor** 的上限，不是整分片的共享额度。
//
// 共享额度会让第一个繁忙 Actor 吃光 100 条，同分片其他 Actor 本轮一条都处理不到，
// 在高负载下表现为长期饥饿（P99 抖动）。按 Actor 分配预算后，每个 Actor
// 每轮至少有机会前进 budget 条，轮转顺序由 allCells 决定。
// Go 知识点：allCells 从 map range 收集而来，Go 的 map 遍历顺序是**随机**
// 的——每轮的 Actor 处理顺序不固定；这不影响公平性（每 Actor 独立
// budget），但意味着不能依赖「先到先处理」的顺序假设。
func (sh *shard) drain(budget int) int {
	processed := 0
	for _, cell := range sh.allCells() {
		for n := 0; n < budget; n++ {
			env, ok := cell.mbox.pop()
			if !ok {
				break
			}
			// inflight 先加后处理：保证 idle() 在看到「信箱空」之前，
			// 先看到「有消息在飞」，杜绝 Drain 提前返回的窗口。
			sh.inflight.Add(1)
			sh.process(cell, env)
			sh.inflight.Add(-1)
			processed++
		}
	}
	return processed
}

// idle 判断分片是否真的空闲：信箱无积压，且没有已取出但未处理完的消息。
//
// inflight 先于 cells 检查：处理中的消息此刻可能已不在任何信箱里，
// 只看信箱会漏判；inflight > 0 直接短路，省掉遍历 map 的开销。
func (sh *shard) idle() bool {
	if sh.inflight.Load() > 0 {
		return false
	}
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	for _, c := range sh.cells {
		if c.mbox.len() > 0 {
			return false
		}
	}
	return true
}

// reportDepth 上报本分片信箱积压（20.3 告警项：Actor 队列积压）。
//
// 快照式遍历：持读锁把总量累加完就释放，不在锁内调用指标库——指标
// 实现可能加自己的锁，嵌套持锁有死锁风险。
// 增量上报 rejected：记录上次累计值取差值，避免同一批拒绝被重复
// 计数；lastRejected 只被本 goroutine 读写，无需同步。
func (sh *shard) reportDepth() {
	if !metrics.Enabled() {
		return
	}
	sh.mu.RLock()
	cells := sh.cells
	total := 0
	var rejected int64
	for _, c := range cells {
		total += c.mbox.len()
		rejected += c.mbox.rejectedCount()
	}
	sh.mu.RUnlock()
	metrics.ActorMailbox(sh.index, total)
	// 只上报增量：记录上次累计值，本次差值即为新增拒绝
	if delta := rejected - sh.lastRejected; delta > 0 {
		metrics.ActorRejectedAdd(delta)
		sh.lastRejected = rejected
	}
}

// allCells 返回分片内全部 cell 的快照。
//
// 为什么必须快照而不能「持锁遍历 + 顺带处理」：process 内的业务可能
// 调用 Terminate → remove（要写锁），若 drain 全程持读锁，就会变成
// 同一 goroutine 内「已持 RLock 再请求 Lock」——RWMutex 不可重入，
// 与自身形成循环等待，直接死锁。快照把临界区压缩成一次拷贝，处理
// 期间被摘除的 cell 只是本轮多处理一个，无害。
// Go 知识点：锁不可重入——已持有 RLock 的 goroutine 再调 Lock 会
// 死等自己释放；快照模式（锁内拷贝、锁外处理）是标准解法。
func (sh *shard) allCells() []*actorCell {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	out := make([]*actorCell, 0, len(sh.cells))
	for _, c := range sh.cells {
		out = append(out, c)
	}
	return out
}

// process 处理一条消息：新建 Context 并调用 Actor.Receive。
//
// panic 边界：defer + recover 只能恢复**当前 goroutine**（分片循环）
// 的 panic——恢复后 defer 链表继续执行、循环接着处理下一条，一个
// 玩家的脏消息不会拖垮整个分片；但注意 recover 救不了其他 goroutine
// 里逃逸的 panic（Actor 内自起 goroutine 炸了仍会挂进程）。
func (sh *shard) process(cell *actorCell, env envelope) {
	start := time.Now()
	ok := true
	defer func() {
		if r := recover(); r != nil {
			ok = false
			metrics.ActorPanicInc()
			// panic 不拖垮分片：记录堆栈后继续处理下一条
			logger := sh.sys.logger
			if logger != nil {
				logger.Error("actor panic recovered",
					"actor", cell.pid.ID, "shard", sh.index, "panic", fmt.Sprint(r),
					"stack", string(debug.Stack()))
			} else {
				debug.PrintStack()
			}
		}
		if metrics.Enabled() {
			metrics.ActorProcessed(ok)
			metrics.ActorObserve(time.Since(start))
		}
	}()
	// Context 每条消息新建：sender/message 只对当次处理有效，天然
	// 杜绝跨消息状态残留（无隐式共享）。
	actx := &Context{
		ctx:     context.Background(),
		system:  sh.sys,
		self:    cell.pid,
		sender:  env.sender,
		message: env.message,
	}
	cell.actor.Receive(actx)
}

// timerEntry 一条待触发的定时消息。
type timerEntry struct {
	fireAt   time.Time
	target   *PID
	msg      any
	attempts int // 已因信箱满重试的次数（上限 timerRetryLimit）
}

// timerRetryLimit 信箱满时定时消息的最大重试次数。
//
// 3 次 × 100ms ≈ 300ms 重试窗口：覆盖「Actor 信箱短暂积压」的抖动，
// 又不至于在目标长期过载时无限占用 timers 切片。
const timerRetryLimit = 3

// timerLoop 定时器 goroutine：每 100ms 扫一遍到期 entry 并投递。
//
// 线性扫描而非时间轮：定时消息量小（千级），O(n) 扫描一次的成本
// 远低于时间轮的结构维护；量级涨上来再换实现。
func (s *System) timerLoop(ctx context.Context) {
	defer s.timerWg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		// 与分片循环同样：ctx/stop 优先退出，ticker 驱动扫描。
		// select 多个 case 同时就绪时随机选取——退出与 tick 同时
		// 就绪时先退出也可接受（剩一轮 tick 不做无损正确性）。
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.fireTimers()
		}
	}
}

// fireTimers 投递到期消息；信箱满的任务按策略重试或记日志丢弃。
func (s *System) fireTimers() {
	// 锁内只做「分区」：到期进 fired，未到期留下。剩余项用
	// s.timers[:0] 原地过滤——复用同一底层数组 append，零分配；
	// fired 里存的是指针副本，覆盖原切片元素不影响已收集的指针。
	s.timerMu.Lock()
	now := time.Now()
	var fired []*timerEntry
	remaining := s.timers[:0]
	for _, t := range s.timers {
		if t.fireAt.Before(now) || t.fireAt.Equal(now) {
			fired = append(fired, t)
		} else {
			remaining = append(remaining, t)
		}
	}
	s.timers = remaining
	s.timerMu.Unlock()

	// 投递在锁外进行：Send 可能慢（Redis 异常时），占着 timerMu 会
	// 阻塞所有 ScheduleOnce——锁粒度原则：临界区只放必要的状态操作。
	for _, t := range fired {
		if err := s.Send(t.target, t.msg); err != nil {
			// 信箱满：保留任务稍后重试，避免超时/结算类定时消息被静默丢弃。
			if errors.Is(err, ErrMailboxFull) && t.attempts < timerRetryLimit {
				t.attempts++
				t.fireAt = now.Add(100 * time.Millisecond)
				s.timerMu.Lock()
				s.timers = append(s.timers, t)
				s.timerMu.Unlock()
				continue
			}
			// 目标已终止或重试耗尽：明确记录，不再静默。
			if s.logger != nil {
				s.logger.Error("actor timer delivery failed",
					"target", t.target.ID, "attempts", t.attempts, "err", err)
			}
		}
	}
}
