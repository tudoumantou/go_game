// pkg/actor/system.go —— ActorSystem：分片事件循环
package actor

import (
	"context"
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

	// lastDropped 上次上报时的累计丢弃数（只由分片循环 goroutine 读写，无需加锁）
	lastDropped int64
}

func newShard(index int, sys *System) *shard {
	return &shard{index: index, cells: make(map[string]*actorCell), sys: sys}
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
		return fmt.Errorf("actor %s not found", id)
	}
	return cell.mbox.push(env)
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

		processed := 0
		for _, cell := range sh.allCells() {
			for processed < 100 { // 每轮单 Actor 最多处理 100 条，防饿死
				env, ok := cell.mbox.pop()
				if !ok {
					break
				}
				sh.process(cell, env)
				processed++
			}
		}
		// 指标：每轮更新一次信箱深度（避免每条消息打点带来的锁竞争）
		sh.reportDepth()
		if processed == 0 {
			select {
			case <-ctx.Done():
				return
			case <-sh.sys.stopCh:
				return
			case <-ticker.C:
			}
		}
	}
}

// reportDepth 上报本分片信箱积压（20.3 告警项：Actor 队列积压）。
func (sh *shard) reportDepth() {
	if !metrics.Enabled() {
		return
	}
	sh.mu.RLock()
	cells := sh.cells
	total := 0
	var dropped int64
	for _, c := range cells {
		total += c.mbox.len()
		dropped += c.mbox.droppedCount()
	}
	sh.mu.RUnlock()
	metrics.ActorMailbox(sh.index, total)
	// 只上报增量：记录上次累计值，本次差值即为新增丢弃
	if delta := dropped - sh.lastDropped; delta > 0 {
		metrics.ActorDroppedAdd(delta)
		sh.lastDropped = dropped
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
	fireAt time.Time
	target *PID
	msg    any
}

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
		_ = s.Send(t.target, t.msg)
	}
}
