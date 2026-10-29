package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
	"github.com/stretchr/testify/require"
)

// stubPlanner 是最小规划器：产出「一个回执 + 一个对象」的计划，
// 用来测运行时的提交/应用/回执路径，不掺业务逻辑。
type stubPlanner struct {
	calls atomic.Int64
	// fail 非空时直接返回该错误（模拟规划拒绝）。
	fail error
	// panicOnCall 非 0 时在第 N 次调用 panic。
	panicOnCall int64
	// delay 模拟慢规划，用于验证 pending 期间的串行化。
	delay time.Duration
}

func (p *stubPlanner) Plan(snap *application.SceneSnapshot, env domain.CommandEnvelope, cmd application.Command, nowMS int64) (*domain.MutationPlan, error) {
	n := p.calls.Add(1)
	if p.panicOnCall > 0 && n == p.panicOnCall {
		panic("stub planner panic")
	}
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	if p.fail != nil {
		return nil, p.fail
	}
	troop := &domain.Troop{
		Header: domain.ObjectHeader{
			WorldID: env.WorldID, ID: 7001, Kind: domain.ObjectKindTroop,
			OwnerScene: snap.SceneID(), Version: 1,
		},
		PlayerID: env.PlayerID,
		Phase:    domain.TroopPhaseMarching,
	}
	return &domain.MutationPlan{
		CommandID:   "cmd-" + env.RequestID,
		Key:         env.Envelope(),
		PayloadHash: env.PayloadHash,
		Epoch:       snap.Epoch(),
		AffectedSceneRevisions: []domain.SceneRevision{
			{SceneID: snap.SceneID(), Revision: snap.Revision()},
		},
		ObjectsUpsert: []domain.ObjectSnapshot{
			{Key: troop.Header.Key(), Kind: domain.ObjectKindTroop, OwnerScene: snap.SceneID(), Version: 1, PlayerID: env.PlayerID, Troop: troop},
		},
		CommandReceipt: &domain.CommandReceipt{
			CommandID:   "cmd-" + env.RequestID,
			Key:         env.Envelope(),
			PayloadHash: env.PayloadHash,
			ResultCode:  0,
			ResultJSON:  []byte(`{"ok":true}`),
		},
	}, nil
}

func testTopo() domain.Topology {
	return domain.Topology{
		WorldID: 1, WidthCells: 20, HeightCells: 20,
		SceneCols: 2, SceneRows: 2, PositionUnitsCell: 1000,
	}
}

func newTestScene(t *testing.T, planner application.Planner, committer Committer, clock Clock) (*SceneLoop, *TxWorkerPool) {
	t.Helper()
	pool, err := NewTxWorkerPool(2, 8, committer, NopLogger{})
	require.NoError(t, err, "构造事务池失败")
	scene, err := NewSceneLoop(DefaultSceneConfig(), 1, 1, 0, testTopo(), planner, pool, NoopSink{}, clock, NopLogger{})
	require.NoError(t, err, "构造场景失败")
	return scene, pool
}

func envOf(req string) domain.CommandEnvelope {
	return domain.CommandEnvelope{
		WorldID: 1, PlayerID: 1001, RequestID: req,
		PayloadHash: domain.PayloadHashOf([]byte(req)),
	}
}

// TestSceneAppliesCommittedPlan 验证「提交成功 → 应用 → revision 推进」。
func TestSceneAppliesCommittedPlan(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	store := NewMemoryCommitter()
	planner := &stubPlanner{}
	scene, pool := newTestScene(t, planner, store, clock)
	defer func() { _ = pool.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := scene.Start(ctx)
	require.NoError(t, err, fmt.Sprintf("启动场景失败: %v", err))
	defer func() { _ = scene.Shutdown(context.Background()) }()

	err = scene.SubmitSync(envOf("r1"), application.Command{Kind: application.CmdMarchStart})
	require.NoError(t, err, fmt.Sprintf("提交失败: %v", err))
	troops := scene.QueryTroops(1001)
	require.Len(t, troops, 1)
	got := scene.Snapshot().Revision()
	require.EqualValues(t, 1, got, fmt.Sprintf("revision 应推进到 1，实际 %d", got))
	require.EqualValues(t, 1, store.Commits())
}

// TestSceneFreezesOnUnknownCommit 验证「结果未知 → 冻结 → 拒绝新写」。
// 这是数据一致性最关键的一条：宁可不写，也不能带着分叉继续写。
func TestSceneFreezesOnUnknownCommit(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	store := NewMemoryCommitter()
	store.UnknownNext(1)
	scene, pool := newTestScene(t, &stubPlanner{}, store, clock)
	defer func() { _ = pool.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := scene.Start(ctx)
	require.NoError(t, err, fmt.Sprintf("启动失败: %v", err))
	defer func() { _ = scene.Shutdown(context.Background()) }()

	err = scene.SubmitSync(envOf("r1"), application.Command{Kind: application.CmdMarchStart})
	require.False(t, !errors.Is(err, ErrPendingUnknown), fmt.Sprintf("应返回 ErrPendingUnknown，实际 %v", err))
	require.False(t, !scene.Frozen(), "结果未知后场景应冻结")
	// 冻结后新写被拒。
	err = scene.SubmitSync(envOf("r2"), application.Command{Kind: application.CmdMarchStart})
	require.False(t, !errors.Is(err, ErrPendingUnknown), fmt.Sprintf("冻结后应拒绝新写，实际 %v", err))
	// 查询仍可用。
	got := len(scene.QueryTroops(1001))
	require.EqualValues(t, 0, got, fmt.Sprintf("冻结期间查询应可用（0 支部队），实际 %d", got))
}

// TestScenePlannerRejectionStillWritesReceipt 验证「业务失败也落回执」。
func TestScenePlannerRejectionStillWritesReceipt(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	store := NewMemoryCommitter()
	scene, pool := newTestScene(t, &stubPlanner{fail: domain.ErrSlotBusy}, store, clock)
	defer func() { _ = pool.Shutdown(context.Background()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := scene.Start(ctx)
	require.NoError(t, err, fmt.Sprintf("启动失败: %v", err))
	defer func() { _ = scene.Shutdown(context.Background()) }()

	// 规划拒绝时 SubmitSync 返回 nil（命令已被处理，业务码在回执里），
	// 因为错误已经持久化为回执——重复请求会读到同一个失败。
	err = scene.SubmitSync(envOf("r1"), application.Command{Kind: application.CmdMarchStart})
	require.NoError(t, err, fmt.Sprintf("规划拒绝应落回执而不是返回错误: %v", err))
	_, ok := store.Receipt(envOf("r1").Envelope())
	require.True(t, ok, "规划拒绝也必须写回执")
}

// TestScenePanicTriggersFailStop 验证 panic → onPanic 回调 → 循环退出。
func TestScenePanicTriggersFailStop(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	scene, pool := newTestScene(t, &stubPlanner{panicOnCall: 1}, NewMemoryCommitter(), clock)
	defer func() { _ = pool.Shutdown(context.Background()) }()

	panicCh := make(chan error, 1)
	scene.SetPanicHandler(func(err error) { panicCh <- err })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := scene.Start(ctx)
	require.NoError(t, err, fmt.Sprintf("启动失败: %v", err))

	_ = scene.Submit(envOf("r1"), application.Command{Kind: application.CmdMarchStart})
	select {
	case err := <-panicCh:
		require.Error(t, err, "panic 回调应携带错误")
	case <-time.After(3 * time.Second):
		t.Fatal("panic 后应触发 fail-stop 回调")
	}
	select {
	case <-scene.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("panic 后循环应退出")
	}
}

// TestSceneMailboxOverload 验证邮箱满时明确拒收（不阻塞、不丢已入队）。
func TestSceneMailboxOverload(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	cfg := DefaultSceneConfig()
	cfg.MailboxSize = 2
	pool, err := NewTxWorkerPool(1, 2, NewMemoryCommitter(), NopLogger{})
	require.NoError(t, err, "构造池失败")
	defer func() { _ = pool.Shutdown(context.Background()) }()
	scene, err := NewSceneLoop(cfg, 1, 1, 0, testTopo(), &stubPlanner{}, pool, NoopSink{}, clock, NopLogger{})
	require.NoError(t, err, "构造场景失败")

	var overloaded int
	for i := 0; i < 5; i++ {
		if err := scene.Submit(envOf(string(rune('a'+i))), application.Command{Kind: application.CmdMarchStart}); err != nil {
			overloaded++
		}
	}
	require.EqualValues(t, 3, overloaded)
	require.EqualValues(t, 2, scene.cmdBox.Len())
}

// TestDueQueueOrderAndCancel 验证到期堆的排序与懒删除。
func TestDueQueueOrderAndCancel(t *testing.T) {
	q := NewDueQueue()
	q.Schedule(domain.DueEvent{DueAtMS: 100, Kind: domain.DueKindMoveArrive, ObjectID: 2, MoveID: "m1"})
	q.Schedule(domain.DueEvent{DueAtMS: 100, Kind: domain.DueKindCollectEnd, ObjectID: 1, SessionID: "s1"})
	q.Schedule(domain.DueEvent{DueAtMS: 50, Kind: domain.DueKindRefresh, ObjectID: 3})
	q.Cancel(moveRef("m1"))

	got := q.PopDue(1000, 10)
	require.Len(t, got, 2)
	// REFRESH(50) 先出，COLLECT_END(100, kind=20) 后出。
	require.EqualValues(t, domain.DueKindRefresh, got[0].Kind)
	require.EqualValues(t, domain.DueKindCollectEnd, got[1].Kind)
}

// TestTxPoolReservation 验证回执名额与结果投递。
func TestTxPoolReservation(t *testing.T) {
	pool, err := NewTxWorkerPool(1, 2, NewMemoryCommitter(), NopLogger{})
	require.NoError(t, err, "构造池失败")
	defer func() { _ = pool.Shutdown(context.Background()) }()

	tok1, err := pool.TryReserveCompletion()
	require.NoError(t, err, "首个名额应成功")
	_, err = pool.TryReserveCompletion()
	require.NoError(t, err, fmt.Sprintf("第二个名额应成功: %v", err))
	_, err = pool.TryReserveCompletion()
	require.False(t, !errors.Is(err, ErrOverloaded), fmt.Sprintf("名额耗尽应返回 ErrOverloaded，实际 %v", err))

	var got atomic.Int64
	done := make(chan struct{})
	if err := pool.SubmitImmutable(TxJob{
		Plan:  &domain.MutationPlan{CommandID: "c1", Key: envOf("c1").Envelope()},
		Token: tok1,
		Deliver: func(TxResult) {
			got.Add(1)
			close(done)
		},
	}); err != nil {
		t.Fatalf("提交失败: %v", err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("结果未投递")
	}
	require.EqualValues(t, 1, got.Load())
}

// TestMemoryCommitterIdempotency 验证幂等表与故障注入。
func TestMemoryCommitterIdempotency(t *testing.T) {
	store := NewMemoryCommitter()
	env := envOf("same")
	plan := &domain.MutationPlan{
		CommandID:   "c1",
		Key:         env.Envelope(),
		PayloadHash: env.PayloadHash,
		CommandReceipt: &domain.CommandReceipt{
			CommandID: "c1", Key: env.Envelope(), PayloadHash: env.PayloadHash,
		},
		AffectedSceneRevisions: []domain.SceneRevision{{SceneID: 1, Revision: 0}},
	}
	r := store.Commit(context.Background(), plan)
	require.False(t, !r.Committed, fmt.Sprintf("首次提交应成功: %v", r.Err))
	// 同键同参：幂等成功，不重复应用。
	r = store.Commit(context.Background(), plan)
	require.False(t, !r.Committed, fmt.Sprintf("重放应幂等成功: %v", r.Err))
	require.EqualValues(t, 1, store.Commits())
	// 同键不同参：冲突。
	other := *plan
	other.PayloadHash = domain.PayloadHashOf([]byte("other"))
	other.CommandReceipt = &domain.CommandReceipt{
		CommandID: "c2", Key: env.Envelope(), PayloadHash: other.PayloadHash,
	}
	r = store.Commit(context.Background(), &other)
	require.False(t, !errors.Is(r.Err, domain.ErrIdempotencyConflict), fmt.Sprintf("同键不同参应冲突，实际 %v", r.Err))
}

// TestMailboxCloseSemantics 验证关闭后拒收、Recv 返回 false、Close 幂等。
func TestMailboxCloseSemantics(t *testing.T) {
	box := NewMailbox[int](2)
	err := box.TrySend(1)
	require.NoError(t, err, fmt.Sprintf("发送失败: %v", err))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		box.Close()
		box.Close() // 幂等，不应 panic
	}()
	wg.Wait()
	err = box.TrySend(2)
	require.False(t, !errors.Is(err, ErrMailboxClosed), fmt.Sprintf("关闭后发送应返回 ErrMailboxClosed，实际 %v", err))
	// 已入队的仍可取出，取空后返回 false。
	v, ok := box.Recv()
	require.True(t, ok, fmt.Sprintf("应取出已入队的 1，实际 %v %v", v, ok))
	require.EqualValues(t, 1, v)
	_, ok = box.Recv()
	require.False(t, ok, "取空后应返回 false")
}

// TestEventRefRefreshUsesDomainKey 补点去重键必须与 domain.RefreshKey 同源。
//
// 这两个键本来就是同一个字符串空间：runtime 的 eventRef 用它做取消/去重，
// manager 用它当幂等 RequestID（domain/collect.go 的 RefreshKey 注释写着
// 「键相同即已创建，重试不重复抽奖」）。此前 runtime 自己又拼了一份
// "refresh:%d:%d:%d:%d"，改一处漏一处不会报错，只表现为「定时刷新偶尔不
// 触发」——没有任何日志线索。
func TestEventRefRefreshUsesDomainKey(t *testing.T) {
	ev := domain.DueEvent{
		Kind:              domain.DueKindRefresh,
		WorldID:           7,
		RichBlockID:       12,
		RefreshGeneration: 3,
		RefreshSlot:       5,
	}
	require.Equal(t, domain.RefreshKey(7, 12, 3, 5), eventRef(ev),
		"eventRef 必须直接调 domain.RefreshKey，不能本地再拼一份")
}
