// pkg/actor/system.go —— ActorSystem：分片事件循环
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
type Config struct {
	ShardCount  int
	MailboxSize int
	Logger      Logger // 可选：panic 恢复时记录堆栈
}

// NewSystem 创建 ActorSystem。
// NewSystem 创建 ActorSystem（不启动任何 goroutine，需显式 Start）。
//
// 分片在创建时全部就绪，Spawn 之前即可投递——消息先入信箱排队，
// 等 Start 后由分片循环消费。
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
func (s *System) Send(target *PID, msg any) error {
	return s.SendWithSender(target, msg, nil)
}

// SendWithSender 带发送者发送消息。
//
// sender 为 nil 表示匿名发送（定时器/系统消息），接收方 Respond 会跳过；
// target 为 nil 属于调用方 bug，返回错误而不 panic。
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
	wake chan struct{}

	// inflight 已从信箱取出、尚未处理完的消息数。
	//
	// 关闭时需要区分「信箱空」和「真的处理完」：一条消息被 pop 之后信箱计数已归零，
	// 但 handler 可能还在写 Redis。只看 len(mbox) 会让 Drain 提前返回，丢掉最后一条业务。
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

func (sh *shard) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
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
func (sh *shard) drain(budget int) int {
	processed := 0
	for _, cell := range sh.allCells() {
		for n := 0; n < budget; n++ {
			env, ok := cell.mbox.pop()
			if !ok {
				break
			}
			sh.inflight.Add(1)
			sh.process(cell, env)
			sh.inflight.Add(-1)
			processed++
		}
	}
	return processed
}

// idle 判断分片是否真的空闲：信箱无积压，且没有已取出但未处理完的消息。
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

func (sh *shard) allCells() []*actorCell {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	out := make([]*actorCell, 0, len(sh.cells))
	for _, c := range sh.cells {
		out = append(out, c)
	}
	return out
}

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
	actx := &Context{
		ctx:     context.Background(),
		system:  sh.sys,
		self:    cell.pid,
		sender:  env.sender,
		message: env.message,
	}
	cell.actor.Receive(actx)
}

type timerEntry struct {
	fireAt   time.Time
	target   *PID
	msg      any
	attempts int // 已因信箱满重试的次数（上限 timerRetryLimit）
}

// timerRetryLimit 信箱满时定时消息的最大重试次数。
const timerRetryLimit = 3

func (s *System) timerLoop(ctx context.Context) {
	defer s.timerWg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
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

func (s *System) fireTimers() {
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
