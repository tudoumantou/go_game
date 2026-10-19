// pkg/actor/system.go —— ActorSystem：分片事件循环
package actor

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime/debug"
	"sync"
	"time"

	"game_study/pkg/metrics"
)

// Logger Actor 系统日志接口（避免 pkg/actor 依赖具体日志实现）。
type Logger interface {
	Error(msg string, keysAndValues ...any)
}

// System Actor 系统。
type System struct {
	shardCount  int
	mailboxSize int
	shards      []*shard
	logger      Logger
	wg          sync.WaitGroup
	stopOnce    sync.Once
	stopCh      chan struct{}

	// 定时器
	timerMu sync.Mutex
	timers  []*timerEntry
	timerWg sync.WaitGroup
}

// Config ActorSystem 配置。
type Config struct {
	ShardCount  int
	MailboxSize int
	Logger      Logger // 可选：panic 恢复时记录堆栈
}

// NewSystem 创建 ActorSystem。
func NewSystem(cfg Config) *System {
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
func (s *System) Start(ctx context.Context) {
	for _, sh := range s.shards {
		s.wg.Add(1)
		go sh.run(ctx, &s.wg)
	}
	s.timerWg.Add(1)
	go s.timerLoop(ctx)
}

// Stop 停止所有分片。
func (s *System) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
	s.wg.Wait()
	s.timerWg.Wait()
}

// Spawn 创建 Actor。
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
func (s *System) SendWithSender(target *PID, msg any, sender *PID) error {
	if target == nil {
		return fmt.Errorf("target pid is nil")
	}
	sh := s.shardFor(target.ID)
	return sh.deliver(target.ID, envelope{sender: sender, message: msg})
}

// ScheduleOnce 调度一次性延迟消息。
func (s *System) ScheduleOnce(d time.Duration, target *PID, msg any) {
	if target == nil {
		return
	}
	s.timerMu.Lock()
	s.timers = append(s.timers, &timerEntry{fireAt: time.Now().Add(d), target: target, msg: msg})
	s.timerMu.Unlock()
}

func (s *System) shardFor(id string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return s.shards[int(h.Sum32())%s.shardCount]
}

// Terminate 停止并移除 Actor。
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

	// lastRejected 上次上报时的累计拒绝数（只由分片循环 goroutine 读写，无需加锁）
	lastRejected int64
}

func newShard(index int, sys *System) *shard {
	return &shard{
		wake:  make(chan struct{}, 1),
		index: index,
		cells: make(map[string]*actorCell),
		sys:   sys,
	}
}

func (sh *shard) add(cell *actorCell) error {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.cells[cell.pid.ID]; ok {
		return fmt.Errorf("actor %s already exists", cell.pid.ID)
	}
	sh.cells[cell.pid.ID] = cell
	return nil
}

func (sh *shard) deliver(id string, env envelope) error {
	sh.mu.RLock()
	cell, ok := sh.cells[id]
	sh.mu.RUnlock()
	if !ok {
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
			sh.process(cell, env)
			processed++
		}
	}
	return processed
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
