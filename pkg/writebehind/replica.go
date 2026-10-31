// pkg/writebehind/replica.go —— Actor 内存 → Redis 副本的**异步**回写通道
//
// 职责：把 Actor 线程内已经序列化好的脏字段，按玩家合并后周期性批量写进
// Redis 副本（Store.syncScript）。调用方（player.Actor）入队即返回，不再
// 在消息处理函数里等一次 Redis RTT。
//
// 为什么要有这一层：内存权威（v3.5）之后，Redis 只服务「别人读我」（跨节点
// 只读、顶号判定、崩溃恢复），它的新鲜度要求是「百毫秒级可见」而不是
// 「提交级」。既然没人要求提交级，就没必要让每条业务消息付一次网络往返——
// 那正是内存权威本该省掉的最大成本。
//
// 三个必须守住的不变量（改动本文件前请先读）：
//
//  1. **同玩家按入队序合并，后写覆盖先写**。Actor 单线程 + 消息 FIFO，
//     所以「同 token 的合并」保序即可；不同 token（旧会话残留）直接丢弃新
//     记录之外的那条——旧会话已被接管，重放它的字段会覆盖新会话的值。
//  2. **租约栅栏不因异步而失效**。syncScript 仍校验 token==epoch，被顶号后
//     的旧 Actor 入队的记录会在落库时被整条拒绝（返回 -1），这里按丢弃处理。
//  3. **队列有界**。Redis 挂掉时队列会涨；超过 MaxPending 就丢最旧的并告警
//     ——副本不是权威，丢一条的后果是「副本落后」，读侧会用库版本发现并重建；
//     不丢的后果是进程 OOM，那是不可恢复的。
//
// 失败语义：单条失败留在队列里下轮重试（attempts 计数，超过上限丢弃并告警），
// 不做「失败即回滚内存」——内存是权威，副本落后不影响本次业务正确性。
package writebehind

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"game_study/pkg/log"
	"game_study/pkg/metrics"
	"github.com/redis/go-redis/v9"
)

// 哨兵错误：调用方用 errors.Is 判定，而不是匹配文案。
var (
	// errReplicaDisabled 未装配异步通道（无 Redis，或单测环境）。
	errReplicaDisabled = errors.New("writebehind: replica disabled")
	// errReplicaFlush 强制刷写未全部成功（下线/关服路径据此告警）。
	errReplicaFlush = errors.New("writebehind: replica flush incomplete")
	// errInvalidPlayerID 玩家 id 非法（<=0）。
	errInvalidPlayerID = errors.New("writebehind: invalid player id")
)

// ErrReplicaDisabled 暴露「未装配异步通道」给包外判断。
//
// 包外（player.Actor）需要据此回退到同步 Sync——单测与降级路径都走这条路。
func ErrReplicaDisabled() error { return errReplicaDisabled }

// ReplicaRecord 一条待回写的副本记录。
//
// Args 是**已编码**的 field/value 参数对（field,value 交替；删除写成
// "-field",""）。为什么不传 FieldSource：入队发生在 Actor 线程，出队刷写
// 发生在后台 goroutine——把 FieldSource 传出去等于让后台 goroutine 读一份
// 正在被下一条消息修改的状态（数据竞争）。序列化必须在 Actor 线程完成。
type ReplicaRecord struct {
	// PlayerID 玩家 id（正数）。
	PlayerID int64
	// Token 会话租约令牌；刷写时由 Lua 校验，被顶号的旧记录会被拒绝。
	Token int64
	// Args field/value 交替的参数对（已编码、已校验）。
	Args []string
}

// ReplicaConfig 异步回写配置；零值全部有默认值。
type ReplicaConfig struct {
	// Interval 批量刷写周期，默认 200ms。
	//
	// 200ms 是「跨节点读到的新鲜度」与「Redis 写压力」的折中：再短对玩家
	// 观感无差别（人眼分辨不了 200ms 的排行榜），却让 QPS 成倍上升。
	Interval time.Duration
	// MaxBatch 单轮最多刷多少个玩家，默认 256。
	// 一轮 = 一次 pipeline RTT，太大则单次 RTT 变长、失败重放成本变高。
	MaxBatch int
	// MaxPending 待刷玩家数上限，默认 100000。
	// 超限丢最旧的一条：Redis 长时间不可用时，保队列不保新鲜度。
	MaxPending int
	// MaxAttempts 单条记录的最大重试次数，默认 300（200ms × 300 ≈ 60s）。
	// 超过即丢弃并告警：持续失败通常是格式/权限类硬错误，重试不会自愈。
	MaxAttempts int
}

func (c ReplicaConfig) withDefaults() ReplicaConfig {
	if c.Interval <= 0 {
		c.Interval = 200 * time.Millisecond
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 256
	}
	if c.MaxPending <= 0 {
		c.MaxPending = 100000
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 300
	}
	return c
}

// replicaEntry 队列内的合并体：同一玩家的多条记录合成一条。
//
// values/removed 分开存而不是保留 Args 切片：同一字段在一轮内被改多次时，
// map 后写覆盖先写，天然只留最后一次的值；用切片合并要 O(n²) 扫描。
type replicaEntry struct {
	playerID int64
	token    int64
	values   map[string]string
	removed  map[string]struct{}
	enqueued time.Time
	attempts int
}

// args 把合并体还原成 syncScript 的参数对（field,value 交替）。
func (e *replicaEntry) args() []string {
	out := make([]string, 0, 2*(len(e.values)+len(e.removed)))
	for f, v := range e.values {
		out = append(out, f, v)
	}
	for f := range e.removed {
		out = append(out, "-"+f, "")
	}
	return out
}

// merge 把一条新记录合并进合并体（后写覆盖先写）。
//
// 跨 token 的处理是本文件最容易写错的一处：新 token 意味着**新会话已经接管**，
// 旧会话残留的字段不再可信（它可能是在新会话加载之前的状态），因此整条重置
// 而不是字段级合并——字段级合并会把旧会话的旧值盖到新会话的新值上。
func (e *replicaEntry) merge(rec ReplicaRecord) {
	if rec.Token != e.token {
		if rec.Token < e.token {
			return // 旧会话的迟到记录：丢弃（新会话的值更可信）
		}
		e.token = rec.Token
		e.values = make(map[string]string, len(rec.Args)/2)
		e.removed = make(map[string]struct{})
	}
	for i := 0; i+1 < len(rec.Args); i += 2 {
		f, v := rec.Args[i], rec.Args[i+1]
		if name, ok := strings.CutPrefix(f, "-"); ok {
			delete(e.values, name)
			e.removed[name] = struct{}{}
			continue
		}
		delete(e.removed, f)
		e.values[f] = v
	}
}

// Replica 异步副本回写器。并发安全，通常一个进程一个实例。
type Replica struct {
	store  *Store
	logger *log.Logger
	cfg    ReplicaConfig

	// mu 保护 pending/order；只在入队、取批、统计时短暂持有，绝不跨网络 IO。
	mu      sync.Mutex
	pending map[int64]*replicaEntry
	order   []int64 // 入队顺序（FIFO）；出队时惰性清理已合并/已删除的 id

	// flushMu 串行化「后台定时刷」与「下线/关服强制刷」，
	// 避免同一玩家被两个 goroutine 同时写（Lua 幂等，但版本号会多涨）。
	flushMu sync.Mutex

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewReplica 创建回写器（不启动；由 Start 起后台 goroutine）。
func NewReplica(store *Store, cfg ReplicaConfig, logger *log.Logger) *Replica {
	return &Replica{
		store:   store,
		logger:  logger,
		cfg:     cfg.withDefaults(),
		pending: make(map[int64]*replicaEntry),
		stopCh:  make(chan struct{}),
	}
}

// Start 起后台刷写循环。
//
// ctx 与 stopCh 双路监听：ctx 用于随进程生命周期收尾，stopCh 用于 Shutdown
// 里「先 Flush 再 Stop」的显式顺序（Stop 之后不再有新的刷写，避免与
// close-redis 抢时序）。
func (r *Replica) Start(ctx context.Context) {
	if r == nil || r.store == nil {
		return
	}
	// 预加载脚本：pipeline 里的 EVALSHA 不会像单条命令那样在 NOSCRIPT 时
	// 自动回退到 EVAL（那时命令还没执行、错误还没发生）。加载一次能省掉
	// 首轮的整批失败；Redis 重启导致脚本丢失时由 flush 内的 NOSCRIPT 重试兜底。
	if err := syncScript.Load(ctx, r.store.rdb).Err(); err != nil {
		r.logger.Warn("replica: preload lua script failed", "err", err)
	}
	r.wg.Add(1)
	go r.run(ctx)
}

func (r *Replica) run(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.flushOnce(ctx)
		}
	}
}

// Enqueue 入队一条副本记录（非阻塞，Actor 热路径调用）。
//
// 返回错误只有两种情况：回写器未装配、玩家 id 非法。**Redis 不可用不是
// 错误**——它只会让记录留在队列里等下一轮，业务继续。
func (r *Replica) Enqueue(rec ReplicaRecord) error {
	if r == nil || r.store == nil {
		return errReplicaDisabled
	}
	if rec.PlayerID <= 0 {
		return errInvalidPlayerID
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.pending[rec.PlayerID]; ok {
		e.merge(rec)
		return nil
	}
	// 队列上限：丢最旧的。副本是「尽力而为的视图」，保内存不保完备。
	if len(r.pending) >= r.cfg.MaxPending {
		if id, ok := r.popOldestLocked(); ok {
			if metrics.Enabled() {
				metrics.ReplicaDropped("queue_full")
			}
			r.logger.Warn("replica queue full, drop oldest", "player_id", id, "pending", len(r.pending))
		}
	}
	e := &replicaEntry{
		playerID: rec.PlayerID,
		token:    rec.Token,
		values:   make(map[string]string, len(rec.Args)/2),
		removed:  make(map[string]struct{}),
		enqueued: time.Now(),
	}
	e.merge(rec)
	r.pending[rec.PlayerID] = e
	r.order = append(r.order, rec.PlayerID)
	return nil
}

// popOldestLocked 摘掉队列里最旧的一条（持 mu 调用）。
//
// order 是惰性队列：里面可能有已合并（pending 里仍是同一条，不能删）或
// 已刷出（pending 里没有）的 id。这里只处理「pending 里存在」的第一个 id，
// 并把扫过的无效项从 order 头部截掉，保证 order 不会无限增长。
func (r *Replica) popOldestLocked() (int64, bool) {
	for len(r.order) > 0 {
		id := r.order[0]
		r.order = r.order[1:]
		if e, ok := r.pending[id]; ok {
			delete(r.pending, id)
			_ = e
			return id, true
		}
	}
	return 0, false
}

// take 取一批待刷记录（按入队顺序，最多 n 条）。
func (r *Replica) take(n int) []*replicaEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 {
		return nil
	}
	out := make([]*replicaEntry, 0, n)
	for len(r.order) > 0 && len(out) < n {
		id := r.order[0]
		r.order = r.order[1:]
		e, ok := r.pending[id]
		if !ok {
			continue // 已被 FlushPlayer/淘汰摘走
		}
		delete(r.pending, id)
		out = append(out, e)
	}
	return out
}

// requeue 放回失败的记录（在队尾，下轮再试）。
func (r *Replica) requeue(entries []*replicaEntry) {
	if len(entries) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range entries {
		// 期间若同一玩家有新记录入队，新记录更新（token 更大或字段更新），
		// 失败批次的旧值不能再盖回去——直接丢失败批次，保留队列里的新记录。
		if _, ok := r.pending[e.playerID]; ok {
			continue
		}
		r.pending[e.playerID] = e
		r.order = append(r.order, e.playerID)
	}
}

// flushOnce 一轮批量刷写（后台定时调用）。
func (r *Replica) flushOnce(ctx context.Context) {
	entries := r.take(r.cfg.MaxBatch)
	if len(entries) == 0 {
		return
	}
	r.flushMu.Lock()
	failed := r.write(ctx, entries)
	r.flushMu.Unlock()
	r.requeue(failed)
	if metrics.Enabled() {
		metrics.ReplicaPending(r.pendingCount())
	}
}

// Flush 同步排空队列（下线/关服用）。
//
// 循环取批直到队列空或 ctx 到期：单轮之后队列仍非空说明积压超过 MaxBatch，
// 关服路径必须把它刷平——否则「最后 200ms 的改动」会留在内存里随进程消失。
func (r *Replica) Flush(ctx context.Context) error {
	if r == nil || r.store == nil {
		return nil
	}
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	var firstErr error
	for {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		entries := r.take(r.cfg.MaxBatch)
		if len(entries) == 0 {
			break
		}
		failed := r.write(ctx, entries)
		if len(failed) > 0 {
			if firstErr == nil {
				firstErr = errReplicaFlush
			}
			// 失败项不再回队：调用方（关服）拿到错误即可，
			// 继续重试只会拖长停机时间（与 flusher.FlushAll 同一策略）。
		}
	}
	return firstErr
}

// FlushPlayer 立刻刷某个玩家（不等定时器）。
//
// 使用场景：同步落库后要 Drop + Seed 重建副本（A 类数据），必须先把这个
// 玩家积压的旧记录刷掉，否则它们在 Seed 之后才落地，会把重建好的 hash
// 又改回旧值、还把 version 抬高（旧副本被误判为「不落后于库」）。
func (r *Replica) FlushPlayer(ctx context.Context, playerID int64) error {
	if r == nil || r.store == nil {
		return nil
	}
	r.mu.Lock()
	e, ok := r.pending[playerID]
	if ok {
		delete(r.pending, playerID)
	}
	r.mu.Unlock()
	if !ok {
		return nil
	}
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	failed := r.write(ctx, []*replicaEntry{e})
	if len(failed) > 0 {
		return errReplicaFlush
	}
	return nil
}

// write 执行一批写副本，返回需要重试的记录。
//
// 逐条判定而不是「Exec 有错就整批失败」：一批 256 条里坏一条不该连累其余
// 255 条重写（Redis pipeline 的单条错误与连接错误在 Exec 上都表现为 error，
// 只有 cmd.Err() 能区分）。
func (r *Replica) write(ctx context.Context, entries []*replicaEntry) []*replicaEntry {
	pipe, cmds := r.dispatch(ctx, entries)
	if _, err := pipe.Exec(ctx); err != nil && isNoScript(err) {
		// Redis 重启/SCRIPT FLUSH 之后脚本缓存丢了：重新加载再发一次。
		if lerr := syncScript.Load(ctx, r.store.rdb).Err(); lerr != nil {
			r.logger.Warn("replica: reload lua script failed", "err", lerr)
		} else {
			pipe, cmds = r.dispatch(ctx, entries)
			_, _ = pipe.Exec(ctx)
		}
	}

	var failed []*replicaEntry
	var okCount int
	var maxLag float64
	now := time.Now()
	for i, e := range entries {
		version, err := cmds[i].Int64()
		switch {
		case err == nil && version >= 0:
			okCount++
			if lag := now.Sub(e.enqueued).Seconds(); lag > maxLag {
				maxLag = lag
			}
		case err == nil && version < 0:
			// 租约被接管：旧会话记录，丢弃即可（新会话会写自己的）
			if metrics.Enabled() {
				metrics.ReplicaDropped("lease_superseded")
			}
		default:
			e.attempts++
			if e.attempts >= r.cfg.MaxAttempts {
				if metrics.Enabled() {
					metrics.ReplicaDropped("max_attempts")
				}
				r.logger.Error("replica: drop after too many attempts",
					"player_id", e.playerID, "attempts", e.attempts, "err", err)
				continue
			}
			if metrics.Enabled() {
				metrics.ReplicaFail()
			}
			failed = append(failed, e)
		}
	}
	if metrics.Enabled() {
		metrics.ReplicaRows(okCount)
		metrics.ReplicaLag(maxLag)
	}
	return failed
}

// dispatch 把一批记录塞进 pipeline（不 Exec）。
func (r *Replica) dispatch(ctx context.Context, entries []*replicaEntry) (redis.Pipeliner, []*redis.Cmd) {
	pipe := r.store.rdb.Pipeline()
	cmds := make([]*redis.Cmd, len(entries))
	for i, e := range entries {
		cmds[i] = r.store.syncCmd(ctx, pipe, e.playerID, e.token, e.args())
	}
	return pipe, cmds
}

// Stop 停后台循环（不排空队列；排空由调用方先 Flush 再 Stop）。
//
// 幂等：sync.Once 保证重复调用不 panic；wg.Wait 保证后台 goroutine 真的退出，
// 否则它可能在 close-redis 之后还去访问连接池。
func (r *Replica) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() { close(r.stopCh) })
	r.wg.Wait()
}

// Pending 待刷玩家数（监控用）。
func (r *Replica) Pending() int {
	if r == nil {
		return 0
	}
	return r.pendingCount()
}

func (r *Replica) pendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// isNoScript 判断 pipeline 错误是否为「脚本未加载」。
//
// 用字符串前缀而不是错误码：go-redis 对 pipeline 的批量错误只透传文本，
// 没有可判定的哨兵错误（NOSCRIPT 前缀是 Redis 协议固定文本）。
func isNoScript(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT")
}
