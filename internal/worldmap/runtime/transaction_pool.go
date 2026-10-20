package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"game_study/internal/worldmap/domain"
)

// ErrPoolClosed 表示事务池已关闭。
var ErrPoolClosed = errors.New("worldmap/runtime: 事务池已关闭")

// CompletionToken 是「回执名额」（文档 4.2）。
//
// 为什么提交前要先占名额：提交是异步的，结果只能通过场景的控制通道
// 送回。如果控制通道满了才去送结果，这条结果就可能被丢弃，
// 场景永远停在 pending。名额机制保证"能被接受的事务，一定有地方
// 接收它的结果"——容量在提交前就预留好了。
//
// 名额在事务完成（无论成功/失败/panic）后归还，因此长事务会占用名额，
// 这正是背压的来源：池子打满时新命令被明确拒收，而不是无限堆积。
type CompletionToken struct {
	release func()
	once    sync.Once
}

// Release 归还名额，幂等。重复调用不会把名额放大。
func (t *CompletionToken) Release() {
	if t == nil {
		return
	}
	t.once.Do(func() {
		if t.release != nil {
			t.release()
		}
	})
}

// TxResult 是一个事务的完成结果。
type TxResult struct {
	CommandID string
	Key       domain.CommandKey
	SceneID   domain.SceneID
	Plan      *domain.MutationPlan
	Result    domain.CommitResult
}

// TxJob 是提交给 worker 的不可变任务。
//
// Deliver 是结果投递回调，由 SceneLoop 提供：它把结果塞回该场景的
// 控制通道。用回调而不是共享 channel，是为了让多个场景共用一个池时
// 结果不会串场景——每个 job 自己知道该回到哪。
type TxJob struct {
	Plan    *domain.MutationPlan
	Token   *CompletionToken
	Deliver func(TxResult)
}

// TxWorkerPool 是进程级事务工作池（文档 18.1 transaction_pool.go）。
//
// 并发模型：
//   - jobs 由 N 个 worker 消费，worker 之间并行；
//   - slots 限流在途事务总数（= 队列容量），TryReserveCompletion 非阻塞取；
//   - 每个 job 完成后调用 Deliver 把结果送回场景，再归还名额。
//
// 为什么不在这里直接改场景状态：worker 不能访问 SceneLoop 的 map
// （文档 4.2）。它只负责"提交 + 把结果送回去"，状态推进由场景自己
// 在收到结果时做，这样单写者模型不被破坏。
type TxWorkerPool struct {
	committer Committer
	logger    Logger
	jobs      chan TxJob
	slots     chan struct{}
	wg        sync.WaitGroup
	closed    atomic.Bool
	closeOnce sync.Once
	stop      chan struct{}
	// committed/failed 只用于监控与测试断言。
	committed atomic.Int64
	failed    atomic.Int64
}

// NewTxWorkerPool 创建工作池并启动 worker。
//
// workers 建议 = min(4, CPU)（文档 17.1）：SQL 提交是 IO 密集型，
// 开太多只会增加数据库连接压力，不会提高吞吐。
func NewTxWorkerPool(workers, queueCapacity int, committer Committer, logger Logger) (*TxWorkerPool, error) {
	if workers <= 0 {
		return nil, fmt.Errorf("%w: tx workers 必须为正, got %d", domain.ErrInvalidArgument, workers)
	}
	if queueCapacity < workers {
		queueCapacity = workers
	}
	if committer == nil {
		return nil, fmt.Errorf("%w: committer 不能为空", domain.ErrInvalidArgument)
	}
	if logger == nil {
		logger = NopLogger{}
	}
	p := &TxWorkerPool{
		committer: committer,
		logger:    logger,
		jobs:      make(chan TxJob, queueCapacity),
		slots:     make(chan struct{}, queueCapacity),
		stop:      make(chan struct{}),
	}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker(i)
	}
	return p, nil
}

// TryReserveCompletion 非阻塞占用一个回执名额。
// 池满或已关闭返回 ErrOverloaded / ErrPoolClosed，调用方据此
// 「拒收命令但不写回执」。
func (p *TxWorkerPool) TryReserveCompletion() (*CompletionToken, error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}
	select {
	case p.slots <- struct{}{}:
		return &CompletionToken{release: func() { <-p.slots }}, nil
	default:
		return nil, ErrOverloaded
	}
}

// SubmitImmutable 提交计划。调用方必须保证 plan 与场景内存已隔离
// （规划器负责深拷贝），这里只做最基本的空值防御。
func (p *TxWorkerPool) SubmitImmutable(job TxJob) error {
	if job.Plan == nil {
		return fmt.Errorf("%w: plan 不能为空", domain.ErrInvalidArgument)
	}
	if job.Plan.CommandID == "" {
		return fmt.Errorf("%w: plan.CommandID 不能为空", domain.ErrInvalidArgument)
	}
	if p.closed.Load() {
		job.Token.Release()
		return ErrPoolClosed
	}
	select {
	case p.jobs <- job:
		return nil
	default:
		// 名额已经预留但 jobs 满了：归还名额，避免泄漏。
		// 理论上 slots 容量 == jobs 容量，不会走到这里；
		// 防御性处理，防止将来两者容量被改成不一致。
		job.Token.Release()
		return ErrOverloaded
	}
}

func (p *TxWorkerPool) worker(id int) {
	defer p.wg.Done()
	for job := range p.jobs {
		p.runOne(id, job)
	}
}

// runOne 执行一次提交，并保证「所有出口都归还名额、都投递结果」。
// panic 也必须转成失败结果：worker 静默死掉会让场景永远 pending，
// 比明确的失败更难排查。
func (p *TxWorkerPool) runOne(id int, job TxJob) {
	var res domain.CommitResult
	func() {
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("tx worker panic", "worker", id, "command_id", job.Plan.CommandID, "panic", r)
				res = domain.CommitResult{Err: fmt.Errorf("worldmap: 提交 panic: %v", r)}
			}
		}()
		res = p.committer.Commit(context.Background(), job.Plan)
	}()
	if res.Committed {
		p.committed.Add(1)
	} else {
		p.failed.Add(1)
	}
	if job.Deliver != nil {
		job.Deliver(TxResult{
			CommandID: job.Plan.CommandID,
			Key:       job.Plan.Key,
			SceneID:   planScene(job.Plan),
			Plan:      job.Plan,
			Result:    res,
		})
	}
	job.Token.Release()
}

// planScene 从计划中取受影响场景（回执用）。
func planScene(plan *domain.MutationPlan) domain.SceneID {
	if plan == nil || len(plan.AffectedSceneRevisions) == 0 {
		return 0
	}
	return plan.AffectedSceneRevisions[0].SceneID
}

// Shutdown 停止接收新任务，等待在途事务完成，再关闭队列。
//
// 顺序不能颠倒：先 close(jobs) 再置 closed 标志，会有新任务在
// close 之后写入导致 panic；先置标志再 close，Submit 侧已看到标志
// 会自行拒绝，两者之间没有窗口。
func (p *TxWorkerPool) Shutdown(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		close(p.stop)
		close(p.jobs)
	})
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// 超时不清空队列：已提交的事务可能已经落库，
		// 强行放弃会让上层误判"未执行"。交给调用方决定是否 fail-stop。
		return fmt.Errorf("%w: 等待在途事务超时: %v", ErrSceneClosed, ctx.Err())
	}
}

// Stats 返回已提交/失败计数（监控用）。
func (p *TxWorkerPool) Stats() (committed, failed int64) {
	return p.committed.Load(), p.failed.Load()
}

// InFlight 返回当前占用的回执名额数。
func (p *TxWorkerPool) InFlight() int { return len(p.slots) }
