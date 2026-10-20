package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
)

// SceneConfig 是单个场景循环的配置（文档 17.1 的 scene_* 字段）。
type SceneConfig struct {
	// MailboxSize 业务命令邮箱容量。满即拒收（Overloaded），不阻塞。
	MailboxSize int
	// ControlCapacity 控制通道容量。必须大于「同时在途事务数」，
	// 否则结果无处投递（文档 4.2）。
	ControlCapacity int
	// MaxCommandsPerTurn / MaxDueEventsPerTurn 每轮预算。
	// 有预算才能保证定时器与关闭消息不被热点邮箱饿死。
	MaxCommandsPerTurn  int
	MaxDueEventsPerTurn int
	// CommandDeadlineMS 命令处理超时（从接受到回执）。
	CommandDeadlineMS int64
	// TickInterval 定时唤醒间隔；到期事件的实际精度取决于它。
	TickInterval time.Duration
}

// DefaultSceneConfig 返回文档默认值。
func DefaultSceneConfig() SceneConfig {
	return SceneConfig{
		MailboxSize:         4096,
		ControlCapacity:     64,
		MaxCommandsPerTurn:  64,
		MaxDueEventsPerTurn: 64,
		CommandDeadlineMS:   3000,
		TickInterval:        time.Second,
	}
}

// validate 补齐零值并拒绝非法配置。
func (c SceneConfig) validate() (SceneConfig, error) {
	d := DefaultSceneConfig()
	if c.MailboxSize <= 0 {
		c.MailboxSize = d.MailboxSize
	}
	if c.ControlCapacity <= 0 {
		c.ControlCapacity = d.ControlCapacity
	}
	if c.MaxCommandsPerTurn <= 0 {
		c.MaxCommandsPerTurn = d.MaxCommandsPerTurn
	}
	if c.MaxDueEventsPerTurn <= 0 {
		c.MaxDueEventsPerTurn = d.MaxDueEventsPerTurn
	}
	if c.CommandDeadlineMS <= 0 {
		c.CommandDeadlineMS = d.CommandDeadlineMS
	}
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	if c.ControlCapacity < 2 {
		return c, fmt.Errorf("%w: 控制通道至少 2 个槽位（事务结果 + 关闭）", domain.ErrInvalidArgument)
	}
	return c, nil
}

// routedCommand 是邮箱里的一条业务命令。
//
// Reply 是可选的同步回执通道：MQ/RPC 接入时由调用方创建并等待；
// 内部命令与压测可以不传（best-effort）。容量固定 1，避免调用方
// 忘记消费导致发送方阻塞。
type routedCommand struct {
	Env   domain.CommandEnvelope
	Cmd   application.Command
	Reply chan error
}

func (r *routedCommand) reply(err error) {
	if r == nil || r.Reply == nil {
		return
	}
	select {
	case r.Reply <- err:
	default:
		// 调用方已放弃等待（超时/断线），不能让它阻塞场景循环。
	}
}

// ControlKind 控制消息类型。控制消息走独立通道，与业务邮箱隔离。
type ControlKind int32

const (
	// CtrlTxResult 事务完成。
	CtrlTxResult ControlKind = 1
	// CtrlClose 请求关闭。
	CtrlClose ControlKind = 2
	// CtrlWakeup 唤醒（测试推进假时钟后使用）。
	CtrlWakeup ControlKind = 3
	// CtrlSeed 注入种子数据（GM 建城、测试准备、恢复导入）。
	CtrlSeed ControlKind = 4
)

// ControlMessage 是控制通道消息。
type ControlMessage struct {
	Kind ControlKind
	Tx   *TxResult
	Seed *SeedRequest
}

// SeedRequest 是一次种子注入请求。
//
// 为什么注入也要走控制通道而不是直接改 map：单写者模型下，
// 任何对业务状态的写入都必须发生在循环 goroutine 里。直接改
// 会让「正在规划的命令」看到半新半旧的状态，产生无法复现的
// 业务错误。走控制通道还有一个好处：注入与命令自然串行，
// 不需要额外加锁。
type SeedRequest struct {
	Objects  []domain.ObjectSnapshot
	Sessions []*domain.CollectSession
	Revision uint64
	Ack      chan error
}

// Seed 向运行中的场景注入对象（主堡/资源点等）。阻塞到注入完成。
func (s *SceneLoop) Seed(ctx context.Context, objects []domain.ObjectSnapshot, sessions []*domain.CollectSession, revision uint64) error {
	ack := make(chan error, 1)
	req := &SeedRequest{Objects: objects, Sessions: sessions, Revision: revision, Ack: ack}
	if err := s.ctrlBox.TrySend(ControlMessage{Kind: CtrlSeed, Seed: req}); err != nil {
		return err
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wakeup 唤醒循环（假时钟推进后必须调用，否则 select 还在睡）。
func (s *SceneLoop) Wakeup() {
	_ = s.ctrlBox.TrySend(ControlMessage{Kind: CtrlWakeup})
}

// pendingTx 记录在途事务。一个场景同时最多一个（文档 4.2）：
// 两个并发写同一对象会产生写偏序问题，串行化是最便宜的解法。
type pendingTx struct {
	commandID string
	reply     chan error
}

// SceneLoop 是单场景执行器。
//
// 并发契约（务必按此理解代码）：
//   - 业务状态（objects/troops/...）只由 run 所在 goroutine 写；
//   - 业务命令通过有界邮箱进入，控制消息通过独立有界通道进入；
//   - 外部只读访问必须走 Snapshot()/QueryTroops()，它们持读锁拷贝，
//     不会把内部 map 暴露出去；
//   - mu 只用于「查询 vs 循环」的可见性，不是业务锁——业务串行化
//     靠的是单 goroutine，而不是这把锁。
type SceneLoop struct {
	cfg     SceneConfig
	worldID domain.WorldID
	sceneID domain.SceneID
	epoch   uint64
	topo    domain.Topology

	planner application.Planner
	pool    *TxWorkerPool
	sink    ProjectionSink
	clock   Clock
	logger  Logger

	cmdBox  *Mailbox[routedCommand]
	ctrlBox *Mailbox[ControlMessage]

	mu sync.RWMutex

	// 以下字段受 mu 保护（写只在循环内，读在快照/查询）。
	objects   map[domain.ObjectKey]domain.ObjectSnapshot
	troops    map[domain.ObjectID]*domain.Troop
	cities    map[domain.ObjectID]*domain.City
	resources map[domain.ObjectID]*domain.ResourceNode
	sessions  map[domain.ObjectID]*domain.CollectSession
	receipts  map[domain.CommandKey]domain.CommandReceipt
	battles   map[domain.ObjectID]*domain.BattleRecord
	queues    map[domain.ObjectID][]domain.ObjectID
	revision  uint64

	pending *pendingTx
	heap    *DueQueue
	closing bool
	frozen  bool

	started  atomic.Bool
	stopped  atomic.Bool
	done     chan struct{}
	panicVal atomic.Value
	onPanic  func(error)
}

// NewSceneLoop 构造场景。planner/pool/sink/clock 都必须非空：
// 缺任何一个都会在运行时才炸，不如构造期就失败。
func NewSceneLoop(
	cfg SceneConfig,
	worldID domain.WorldID,
	sceneID domain.SceneID,
	epoch uint64,
	topo domain.Topology,
	planner application.Planner,
	pool *TxWorkerPool,
	sink ProjectionSink,
	clock Clock,
	logger Logger,
) (*SceneLoop, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}
	if planner == nil {
		return nil, fmt.Errorf("%w: planner 不能为空", domain.ErrInvalidArgument)
	}
	if pool == nil {
		return nil, fmt.Errorf("%w: tx pool 不能为空", domain.ErrInvalidArgument)
	}
	if clock == nil {
		clock = RealClock{}
	}
	if sink == nil {
		sink = NoopSink{}
	}
	if logger == nil {
		logger = NopLogger{}
	}
	if sceneID < 1 || sceneID > topo.MaxSceneID() {
		return nil, fmt.Errorf("%w: 场景 %d 超出拓扑范围", domain.ErrInvalidArgument, sceneID)
	}
	return &SceneLoop{
		cfg:       cfg,
		worldID:   worldID,
		sceneID:   sceneID,
		epoch:     epoch,
		topo:      topo,
		planner:   planner,
		pool:      pool,
		sink:      sink,
		clock:     clock,
		logger:    logger,
		cmdBox:    NewMailbox[routedCommand](cfg.MailboxSize),
		ctrlBox:   NewMailbox[ControlMessage](cfg.ControlCapacity),
		objects:   make(map[domain.ObjectKey]domain.ObjectSnapshot),
		troops:    make(map[domain.ObjectID]*domain.Troop),
		cities:    make(map[domain.ObjectID]*domain.City),
		resources: make(map[domain.ObjectID]*domain.ResourceNode),
		sessions:  make(map[domain.ObjectID]*domain.CollectSession),
		receipts:  make(map[domain.CommandKey]domain.CommandReceipt),
		battles:   make(map[domain.ObjectID]*domain.BattleRecord),
		queues:    make(map[domain.ObjectID][]domain.ObjectID),
		heap:      NewDueQueue(),
		done:      make(chan struct{}),
	}, nil
}

// SceneID 返回场景编号。
func (s *SceneLoop) SceneID() domain.SceneID { return s.sceneID }

// SetPanicHandler 注册 fail-stop 回调（文档 4.3：场景 panic 必须让
// 整个世界 NotReady 并走恢复，而不是清空 map 继续跑）。
func (s *SceneLoop) SetPanicHandler(fn func(error)) { s.onPanic = fn }

// Start 启动循环 goroutine。重复调用返回错误而不是静默忽略——
// 两个循环同时跑会破坏单写者模型。
func (s *SceneLoop) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: 场景 %d 已启动", domain.ErrInvalidState, s.sceneID)
	}
	go s.run(ctx)
	return nil
}

// Submit 提交业务命令。非阻塞：邮箱满返回 ErrOverloaded。
func (s *SceneLoop) Submit(env domain.CommandEnvelope, cmd application.Command) error {
	return s.cmdBox.TrySend(routedCommand{Env: env, Cmd: cmd})
}

// SubmitSync 提交并等待结果。返回的是「是否被接受并完成」，
// 业务失败通过 error 表达（含业务码）。
func (s *SceneLoop) SubmitSync(env domain.CommandEnvelope, cmd application.Command) error {
	reply := make(chan error, 1)
	rc := routedCommand{Env: env, Cmd: cmd, Reply: reply}
	if err := s.cmdBox.TrySend(rc); err != nil {
		return err
	}
	select {
	case err := <-reply:
		return err
	case <-time.After(time.Duration(s.cfg.CommandDeadlineMS) * time.Millisecond):
		return fmt.Errorf("%w: 场景 %d 回执超时", ErrSceneClosed, s.sceneID)
	}
}

// Shutdown 停止接收新命令，等待在途事务，然后退出循环。
//
// 顺序（文档 16.3）：置 closing → 拒绝新业务 → 等在途事务 → 退出。
// 不先等在途事务就退出，会让已提交但未回执的命令变成"结果未知"。
func (s *SceneLoop) Shutdown(ctx context.Context) error {
	if s.stopped.Load() {
		<-s.done
		return nil
	}
	if err := s.ctrlBox.TrySend(ControlMessage{Kind: CtrlClose}); err != nil {
		// 控制通道满说明循环可能卡住；用 Close 强制唤醒。
		s.logger.Warn("场景关闭请求投递失败，强制关闭", "scene", s.sceneID, "err", err)
		s.cmdBox.Close()
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: 场景 %d 关闭超时", ErrSceneClosed, s.sceneID)
	}
}

// Done 返回退出信号。
func (s *SceneLoop) Done() <-chan struct{} { return s.done }

// run 是主循环。结构对应文档 18.3 伪代码，但用阻塞 select 代替忙轮询。
func (s *SceneLoop) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("场景 %d panic: %v", s.sceneID, r)
			s.panicVal.Store(err)
			s.logger.Error("场景循环 panic，世界需 fail-stop", "scene", s.sceneID, "panic", r)
			if s.onPanic != nil {
				s.onPanic(err)
			}
		}
		s.stopped.Store(true)
		close(s.done)
	}()

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	for {
		s.stepOnce()

		s.mu.RLock()
		closing := s.closing
		pending := s.pending != nil
		s.mu.RUnlock()
		if closing && !pending && s.cmdBox.Len() == 0 {
			return
		}

		timeout := s.waitTimeout()
		timer := time.NewTimer(timeout)
		select {
		case <-ctx.Done():
			// 上下文取消等价于关闭：走同一套排空路径，保证在途事务完成。
			s.mu.Lock()
			s.closing = true
			s.mu.Unlock()
			timer.Stop()
		case ctrl, ok := <-s.ctrlBox.C():
			timer.Stop()
			if !ok {
				return
			}
			s.handleControl(ctrl)
		case rc, ok := <-s.cmdBox.C():
			timer.Stop()
			if !ok {
				return
			}
			// 取出来的命令必须处理，不能再塞回邮箱（会打乱顺序）。
			s.handleCommand(rc)
		case <-ticker.C:
			timer.Stop()
		}
	}
}

// stepOnce 执行一轮「控制 → 到期 → 业务」，每类都有预算。
// 顺序固定：控制优先保证关闭与事务结果不被业务洪峰饿死。
func (s *SceneLoop) stepOnce() {
	s.drainControl()
	s.processDue()
	s.processCommands()
}

// waitTimeout 计算下一次唤醒间隔：取 tick 与最近到期事件的最小值。
// 没有到期事件时用 tick，保证即使没有任何消息也能定期推进时钟相关逻辑。
func (s *SceneLoop) waitTimeout() time.Duration {
	timeout := s.cfg.TickInterval
	next, ok := s.heap.NextDueAt()
	if !ok {
		return timeout
	}
	now := NowMS(s.clock)
	if next <= now {
		// 已到期：不阻塞，立刻进入下一轮（但仍然经过 select，
		// 这样新到的控制消息仍有机会插队）。
		return time.Millisecond
	}
	d := time.Duration(next-now) * time.Millisecond
	if d < timeout {
		timeout = d
	}
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	return timeout
}

// drainControl 处理控制消息，最多一批（防止控制洪泛）。
func (s *SceneLoop) drainControl() {
	const controlBudget = 32
	for i := 0; i < controlBudget; i++ {
		msg, ok := s.ctrlBox.TryRecv()
		if !ok {
			return
		}
		s.handleControl(msg)
	}
}

// handleControl 处理单条控制消息。
func (s *SceneLoop) handleControl(msg ControlMessage) {
	switch msg.Kind {
	case CtrlClose:
		s.mu.Lock()
		s.closing = true
		s.mu.Unlock()
		s.logger.Info("场景收到关闭请求", "scene", s.sceneID)
	case CtrlTxResult:
		if msg.Tx != nil {
			s.handleTxResult(*msg.Tx)
		}
	case CtrlSeed:
		if msg.Seed != nil {
			s.applySeed(msg.Seed)
		}
	case CtrlWakeup:
		// 空操作：只为唤醒 select。
	}
}

// applySeed 在循环内应用注入数据并回执。
func (s *SceneLoop) applySeed(req *SeedRequest) {
	s.mu.Lock()
	for _, obj := range req.Objects {
		cp := cloneObjectSnapshot(obj)
		s.objects[cp.Key] = cp
		switch {
		case cp.Troop != nil:
			s.troops[cp.Key.ID] = cp.Troop
		case cp.City != nil:
			s.cities[cp.Key.ID] = cp.City
		case cp.Resource != nil:
			s.resources[cp.Key.ID] = cp.Resource
		}
	}
	for _, sess := range req.Sessions {
		cp := *sess
		s.sessions[cp.ResourceID] = &cp
	}
	if req.Revision > s.revision {
		s.revision = req.Revision
	}
	s.mu.Unlock()
	if req.Ack != nil {
		req.Ack <- nil
	}
}

// handleTxResult 是状态推进的唯一入口：只有这里会改 revision。
func (s *SceneLoop) handleTxResult(tx TxResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pending == nil || s.pending.commandID != tx.CommandID {
		// 迟到结果（例如关闭期间旧事务返回）：丢弃，不能覆盖新状态。
		s.logger.Warn("丢弃迟到事务结果", "scene", s.sceneID, "command_id", tx.CommandID)
		return
	}
	reply := s.pending.reply
	s.pending = nil

	switch {
	case tx.Result.Unknown:
		// 结果不可解析：冻结场景，拒绝新写，等待运维/恢复流程解析。
		// 继续用旧镜像写会与数据库产生分叉（INV-10）。
		s.frozen = true
		s.logger.Error("事务结果未知，场景冻结", "scene", s.sceneID, "command_id", tx.CommandID)
		if reply != nil {
			reply <- ErrPendingUnknown
		}
		return
	case tx.Result.Err != nil:
		if reply != nil {
			reply <- tx.Result.Err
		}
		return
	}

	// 确定成功：应用计划、推进 revision、发布投影。
	s.applyPlan(tx.Plan)
	if len(tx.Result.SceneRevisions) > 0 {
		s.revision = tx.Result.SceneRevisions[0].Revision
	}
	s.publish(tx.Plan)
	if reply != nil {
		reply <- nil
	}
}

// processDue 处理到期事件（预算内）。若场景有在途事务，事件必须放回堆：
// 事件已经从堆里弹出，丢掉它就是永久丢失一条到达/结算。
func (s *SceneLoop) processDue() {
	now := NowMS(s.clock)
	events := s.heap.PopDue(now, s.cfg.MaxDueEventsPerTurn)
	for i, ev := range events {
		s.mu.RLock()
		blocked := s.pending != nil || s.frozen || s.closing
		s.mu.RUnlock()
		if blocked {
			// 未处理的（含当前这条）原样放回，保持 due_at 不变。
			for _, rest := range events[i:] {
				s.heap.Schedule(rest)
			}
			return
		}
		rc, ok := s.eventCommand(ev, now)
		if !ok {
			continue
		}
		s.submitRouted(rc, now)
	}
}

// processCommands 处理业务命令（预算内）。pending 或冻结时立即停止，
// 命令留在邮箱里等待——这保证了「一个场景同时只有一个在途事务」。
func (s *SceneLoop) processCommands() {
	for i := 0; i < s.cfg.MaxCommandsPerTurn; i++ {
		s.mu.RLock()
		blocked := s.pending != nil || s.frozen || s.closing
		s.mu.RUnlock()
		if blocked {
			return
		}
		rc, ok := s.cmdBox.TryRecv()
		if !ok {
			return
		}
		s.handleCommand(rc)
	}
}

// handleCommand 规划并提交一条业务命令。
func (s *SceneLoop) handleCommand(rc routedCommand) {
	if err := rc.Env.Validate(); err != nil {
		// 格式非法：未接受，不写回执（文档 15.3）。
		rc.reply(err)
		return
	}
	s.submitRouted(rc, NowMS(s.clock))
}

// submitRouted 是「规划 → 预留名额 → 提交」的统一路径。
func (s *SceneLoop) submitRouted(rc routedCommand, nowMS int64) {
	s.mu.RLock()
	frozen, closing := s.frozen, s.closing
	pending := s.pending != nil
	s.mu.RUnlock()
	if frozen {
		rc.reply(ErrPendingUnknown)
		return
	}
	if closing {
		rc.reply(ErrSceneClosed)
		return
	}
	if pending {
		// processCommands/processDue 已在上层挡住，这里只做兜底：
		// 返回 Overloaded 让调用方重试，而不是静默丢命令。
		rc.reply(fmt.Errorf("%w: 场景 %d 有在途事务", ErrOverloaded, s.sceneID))
		return
	}

	snap := s.snapshotForPlanner(nowMS)
	plan, err := s.planner.Plan(snap, rc.Env, rc.Cmd, nowMS)
	if err != nil {
		// 业务失败也要落回执，否则同一 request_id 稍后重试可能变成功
		// （文档 7.2）。回执计划没有对象变更，只有一行 map_command。
		plan = s.rejectionPlan(rc.Env, err, nowMS)
	}

	token, err := s.pool.TryReserveCompletion()
	if err != nil {
		// 未接受：不写回执，客户端可用同一 request_id 重试。
		rc.reply(err)
		return
	}

	s.mu.Lock()
	s.pending = &pendingTx{commandID: plan.CommandID, reply: rc.Reply}
	s.mu.Unlock()

	job := TxJob{
		Plan:  plan,
		Token: token,
		Deliver: func(tx TxResult) {
			// 结果送回控制通道。控制通道容量 ≥ 在途事务数，
			// 因此这里不会因为业务邮箱满而丢结果。
			if err := s.ctrlBox.TrySend(ControlMessage{Kind: CtrlTxResult, Tx: &tx}); err != nil {
				s.logger.Error("事务结果无法投递，场景将卡在 pending",
					"scene", s.sceneID, "command_id", tx.CommandID, "err", err)
			}
		},
	}
	if err := s.pool.SubmitImmutable(job); err != nil {
		s.mu.Lock()
		s.pending = nil
		s.mu.Unlock()
		rc.reply(err)
	}
}

// rejectionPlan 构造只含回执的拒绝计划。
func (s *SceneLoop) rejectionPlan(env domain.CommandEnvelope, cause error, nowMS int64) *domain.MutationPlan {
	code := domain.CodeOf(cause)
	plan := &domain.MutationPlan{
		CommandID:   fmt.Sprintf("reject-%d-%s", nowMS, env.RequestID),
		Key:         env.Envelope(),
		PayloadHash: env.PayloadHash,
		Epoch:       s.epoch,
		AffectedSceneRevisions: []domain.SceneRevision{
			{SceneID: s.sceneID, Revision: s.revision},
		},
		CommandReceipt: &domain.CommandReceipt{
			CommandID:     fmt.Sprintf("reject-%d-%s", nowMS, env.RequestID),
			Key:           env.Envelope(),
			PayloadHash:   env.PayloadHash,
			ResultCode:    code,
			ResultJSON:    []byte(fmt.Sprintf(`{"code":%d,"message":%q}`, code, cause.Error())),
			CommittedAtMS: nowMS,
		},
	}
	return plan
}

// publish 发布投影增量。失败只记日志：投影可以靠 revision 缺口重建，
// 不回滚已提交业务（文档 7.3）。
func (s *SceneLoop) publish(plan *domain.MutationPlan) {
	if plan == nil || len(plan.ObjectsUpsert) == 0 {
		return
	}
	batch := ProjectionBatch{
		WorldID:  s.worldID,
		SceneID:  s.sceneID,
		Revision: s.revision,
	}
	for _, obj := range plan.ObjectsUpsert {
		batch.Updates = append(batch.Updates, obj.Key)
	}
	batch.Leaves = append(batch.Leaves, plan.ObjectsTombstone...)
	if err := s.sink.TryPublish(batch); err != nil {
		s.logger.Warn("投影发布失败，等待重建", "scene", s.sceneID, "revision", s.revision, "err", err)
	}
}

// eventCommand 把到期事件翻译成内部命令。
func (s *SceneLoop) eventCommand(ev domain.DueEvent, nowMS int64) (routedCommand, bool) {
	switch ev.Kind {
	case domain.DueKindMoveArrive:
		s.mu.RLock()
		troop, ok := s.troops[ev.ObjectID]
		s.mu.RUnlock()
		if !ok {
			return routedCommand{}, false
		}
		env := s.internalEnv(troop.PlayerID, domain.ArriveKey(troop.Header.ID, ev.MoveID), nowMS)
		return routedCommand{
			Env: env,
			Cmd: application.Command{
				Kind:        application.CmdArrive,
				TargetScene: s.sceneID,
				Arrive:      application.ArriveParams{TroopID: troop.Header.ID, MoveID: ev.MoveID},
			},
		}, true
	case domain.DueKindCollectEnd:
		s.mu.RLock()
		var sess *domain.CollectSession
		for _, cand := range s.sessions {
			if cand.ID == ev.SessionID {
				cp := *cand
				sess = &cp
				break
			}
		}
		var playerID int64
		if sess != nil {
			if troop, ok := s.troops[sess.TroopID]; ok {
				playerID = troop.PlayerID
			}
		}
		s.mu.RUnlock()
		if sess == nil {
			return routedCommand{}, false
		}
		env := s.internalEnv(playerID, domain.CollectEndKey(sess.ID), nowMS)
		return routedCommand{
			Env: env,
			Cmd: application.Command{
				Kind:          application.CmdCollectSettle,
				TargetScene:   s.sceneID,
				CollectSettle: application.CollectSettleParams{SessionID: sess.ID, SettleAtMS: ev.DueAtMS},
			},
		}, true
	case domain.DueKindBattleTimeout:
		// M8 之前没有 BattleResolver 接入，超时裁决暂不处理：
		// 保留事件类型与优先级，避免将来加实现时改调度契约。
		return routedCommand{}, false
	default:
		return routedCommand{}, false
	}
}

// internalEnv 构造内部命令信封。系统账号用 1（保留 ID）：
// domain.CommandEnvelope.Validate 要求 PlayerID>0，而内部命令
// 没有真实玩家上下文（刷新/超时），用保留账号并通过 Kind.Internal()
// 在规划器侧区分，比放开校验更安全。
func (s *SceneLoop) internalEnv(playerID int64, requestID string, nowMS int64) domain.CommandEnvelope {
	if playerID <= 0 {
		playerID = 1
	}
	return domain.CommandEnvelope{
		WorldID:    s.worldID,
		PlayerID:   playerID,
		RequestID:  requestID,
		WorldEpoch: s.epoch,
		DeadlineMS: nowMS + s.cfg.CommandDeadlineMS,
	}
}

// Snapshot 返回当前已提交状态的只读快照。
//
// 持读锁：查询与循环并发时，读锁保证不会看到"写了一半"的 map
// （Go 的 map 并发读写会直接 fatal）。业务串行化不依赖这把锁，
// 它只解决可见性与 race detector 的要求。
func (s *SceneLoop) Snapshot() *application.SceneSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotForPlanner(NowMS(s.clock))
}

// QueryTroops 返回某玩家的部队副本（pending 期间也可服务，文档 18.3）。
func (s *SceneLoop) QueryTroops(playerID int64) []*domain.Troop {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*domain.Troop
	for _, t := range s.troops {
		if t.PlayerID == playerID {
			out = append(out, t.Clone())
		}
	}
	return out
}

// Frozen 报告场景是否因结果未知而冻结。
func (s *SceneLoop) Frozen() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.frozen
}

// Err 返回 panic 错误（若发生）。
func (s *SceneLoop) Err() error {
	if v := s.panicVal.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// LoadState 用已有对象初始化场景（恢复/测试注入）。
// 只允许在 Start 之前调用：运行中注入状态等于绕过事务，
// 会与 revision 语义冲突。
func (s *SceneLoop) LoadState(objects []domain.ObjectSnapshot, sessions []*domain.CollectSession, revision uint64) error {
	if s.started.Load() {
		return fmt.Errorf("%w: 场景已启动，不能注入状态", domain.ErrInvalidState)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, obj := range objects {
		cp := cloneObjectSnapshot(obj)
		s.objects[cp.Key] = cp
		switch {
		case cp.Troop != nil:
			s.troops[cp.Key.ID] = cp.Troop
		case cp.City != nil:
			s.cities[cp.Key.ID] = cp.City
		case cp.Resource != nil:
			s.resources[cp.Key.ID] = cp.Resource
		}
	}
	for _, sess := range sessions {
		cp := *sess
		s.sessions[cp.ResourceID] = &cp
	}
	s.revision = revision
	return nil
}
