package crossplay

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	internalv2 "game_study/pkg/pb/internalv2"
)

// fakeRepo 内存版 Persister。
type fakeRepo struct {
	enrolls   map[int64]Member
	saved     []Group
	scores    []Member
	cleared   int
	failSave  bool
	failClear bool
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{enrolls: make(map[int64]Member)}
}

func (f *fakeRepo) UpsertEnroll(_ context.Context, m Member, _ []byte, _ int64) error {
	f.enrolls[m.PlayerID] = m
	return nil
}

func (f *fakeRepo) DeleteEnroll(_ context.Context, playerID int64) error {
	delete(f.enrolls, playerID)
	return nil
}

func (f *fakeRepo) LoadEnrolls(context.Context) ([]Member, error) {
	out := make([]Member, 0, len(f.enrolls))
	for _, m := range f.enrolls {
		m.Score = InitialScore
		out = append(out, m)
	}
	return out, nil
}

func (f *fakeRepo) SaveGroups(_ context.Context, groups []Group) error {
	if f.failSave {
		return errors.New("boom")
	}
	f.saved = groups
	return nil
}

func (f *fakeRepo) UpsertScores(_ context.Context, members []Member) error {
	f.scores = append(f.scores, members...)
	return nil
}

func (f *fakeRepo) ClearCurrentPeriod(context.Context) (int64, error) {
	if f.failClear {
		return 0, errors.New("boom")
	}
	f.cleared++
	f.enrolls = make(map[int64]Member)
	return int64(len(f.enrolls)), nil
}

func (f *fakeRepo) LoadGroups(context.Context) ([]Group, error) {
	return f.saved, nil
}

// fakeDispatcher 记录下发的报文。
type fakeDispatcher struct {
	nodes  map[int32]string
	byNode map[string][]*internalv2.CenterCommand
}

func newFakeDispatcher() *fakeDispatcher {
	return &fakeDispatcher{nodes: make(map[int32]string), byNode: make(map[string][]*internalv2.CenterCommand)}
}

func (f *fakeDispatcher) NodeIDForServer(serverID int32) (string, bool) {
	id, ok := f.nodes[serverID]
	return id, ok
}

func (f *fakeDispatcher) SendTo(nodeID string, cmd *internalv2.CenterCommand) error {
	f.byNode[nodeID] = append(f.byNode[nodeID], cmd)
	return nil
}

// pushes 取出所有下发的跨服报文（按节点）。
func (f *fakeDispatcher) pushes() map[string][]*internalv2.CrossGroupDeltaPush {
	out := make(map[string][]*internalv2.CrossGroupDeltaPush)
	for node, cmds := range f.byNode {
		for _, c := range cmds {
			if p := c.GetCrossGroupDelta(); p != nil {
				out[node] = append(out[node], p)
			}
		}
	}
	return out
}

// newTestService 30 个服（game-01..game-30，与 makeMembers 的 ServerID 对齐），
// 窗口 60s，时间可注入。
func newTestService(t *testing.T, phase Phase) (*Service, *fakeRepo, *fakeDispatcher) {
	t.Helper()
	repo := newFakeRepo()
	disp := newFakeDispatcher()
	for sid := int32(1); sid <= 30; sid++ {
		disp.nodes[sid] = fmt.Sprintf("game-%02d", sid)
	}
	svc := New(repo, disp, nil, Config{
		Phase: func(time.Time) Phase { return phase },
	})
	return svc, repo, disp
}

func TestServiceEnrollOnlyInEnrollPhase(t *testing.T) {
	for _, ph := range []Phase{PhaseClosed, PhaseGrouping, PhaseActive, PhaseSettling} {
		svc, repo, _ := newTestService(t, ph)
		code, msg := svc.Enroll(context.Background(), Member{PlayerID: 1, ServerID: 1, Power: 100}, nil)
		if code != 1 {
			t.Errorf("阶段 %s 报名 code = %d, 期望 1（拒绝）", ph, code)
		}
		if msg == "" {
			t.Errorf("阶段 %s 拒绝报名时应给出原因", ph)
		}
		if len(repo.enrolls) != 0 {
			t.Errorf("阶段 %s 拒绝后不应写入报名表", ph)
		}
	}
}

func TestServiceEnrollIdempotent(t *testing.T) {
	svc, repo, _ := newTestService(t, PhaseEnroll)

	m := Member{PlayerID: 7, ServerID: 2, Power: 100}
	if code, _ := svc.Enroll(context.Background(), m, nil); code != 0 {
		t.Fatalf("首次报名 code = %d, 期望 0", code)
	}
	// 重复报名返回成功而不是「已报名」：回包丢了时玩家重试能自愈
	if code, _ := svc.Enroll(context.Background(), m, nil); code != 0 {
		t.Fatalf("重复报名 code = %d, 期望 0（幂等）", code)
	}
	if len(repo.enrolls) != 1 {
		t.Errorf("报名表行数 = %d, 期望 1", len(repo.enrolls))
	}
}

func TestServiceEnrollRejectsBadArgs(t *testing.T) {
	svc, _, _ := newTestService(t, PhaseEnroll)
	for _, m := range []Member{
		{PlayerID: 0, ServerID: 1},
		{PlayerID: 1, ServerID: 0},
	} {
		if code, _ := svc.Enroll(context.Background(), m, nil); code != 2 {
			t.Errorf("%+v 应返回 code=2, 得到 %d", m, code)
		}
	}
}

func TestServiceCancelOnlyInEnrollPhase(t *testing.T) {
	svc, repo, _ := newTestService(t, PhaseEnroll)
	repo.enrolls[5] = Member{PlayerID: 5}

	if code, _ := svc.Cancel(context.Background(), 5); code != 0 {
		t.Fatalf("取消报名 code = %d, 期望 0", code)
	}
	if len(repo.enrolls) != 0 {
		t.Error("取消后报名表应无该行")
	}
	// 幂等：再取消一次仍成功
	if code, _ := svc.Cancel(context.Background(), 5); code != 0 {
		t.Errorf("重复取消 code = %d, 期望 0", code)
	}

	svc2, _, _ := newTestService(t, PhaseActive)
	if code, _ := svc2.Cancel(context.Background(), 5); code != 1 {
		t.Errorf("活动期取消 code = %d, 期望 1（已分组不能退）", code)
	}
}

// TestServiceRunGrouping 分组 → 落库 → 清报名表 → 下发快照。
func TestServiceRunGrouping(t *testing.T) {
	svc, repo, disp := newTestService(t, PhaseGrouping)
	// ServerID 用 7 的倍数打散：直接用 i%3 会和蛇形分配的周期共振
	// （同一组拿到的 i 模 3 只有两种取值），造出「某组只有两个服」的假象。
	for i := int64(1); i <= 250; i++ {
		repo.enrolls[i] = Member{PlayerID: i, ServerID: int32((i*7)%30 + 1), Power: 300 - i}
	}

	n, err := svc.RunGrouping(context.Background())
	if err != nil {
		t.Fatalf("RunGrouping: %v", err)
	}
	if n != 3 {
		t.Errorf("组数 = %d, 期望 3", n)
	}
	if len(repo.saved) != 3 {
		t.Errorf("落库组数 = %d, 期望 3", len(repo.saved))
	}
	if repo.cleared != 1 {
		t.Errorf("清表次数 = %d, 期望 1", repo.cleared)
	}
	if got := svc.Store().Len(); got != 250 {
		t.Errorf("内存玩家数 = %d, 期望 250", got)
	}

	// 每个有玩家入组的服都应收到快照，且报文里**必须包含**自己的玩家——
	// 分组结果只存在于中心服，这是游戏服知道自己分组的唯一通道。
	// 遗漏会导致该玩家在本服永远是「已报名、未分组」。
	pushes := disp.pushes()
	if len(pushes) != 30 {
		t.Fatalf("下发节点数 = %d, 期望 30（250 人分布在 30 个服）", len(pushes))
	}
	nodeServer := make(map[string]int32, len(disp.nodes))
	for sid, node := range disp.nodes {
		nodeServer[node] = sid
	}
	for node, list := range pushes {
		sid := nodeServer[node]
		own := 0
		total := 0
		for _, p := range list {
			for _, g := range p.GetGroups() {
				if !g.GetSnapshot() {
					t.Errorf("%s: 分组完成应发快照", node)
				}
				for _, pl := range g.GetPlayers() {
					total++
					if svc.Store().ServerOf(pl.GetPlayerId()) == sid {
						own++
					}
				}
			}
		}
		if own == 0 {
			t.Errorf("%s: 快照里没有自己的玩家——分组归属传不下去", node)
		}
		if total == 0 {
			t.Errorf("%s: 收到了空报文", node)
		}
	}
}

// TestServiceRunGroupingSaveFails 落库失败时不能清报名表（否则永久丢数据）。
func TestServiceRunGroupingSaveFails(t *testing.T) {
	svc, repo, _ := newTestService(t, PhaseGrouping)
	repo.enrolls[1] = Member{PlayerID: 1, ServerID: 1, Power: 10}
	repo.failSave = true

	if _, err := svc.RunGrouping(context.Background()); err == nil {
		t.Fatal("落库失败应返回错误")
	}
	if repo.cleared != 0 {
		t.Error("落库失败时不能清报名表")
	}
	if len(svc.Store().Groups()) != 0 {
		t.Error("落库失败时不应更新内存")
	}
}

// TestServiceRunGroupingClearFailsKeepsData 清表失败不影响分组生效。
func TestServiceRunGroupingClearFailsKeepsData(t *testing.T) {
	svc, repo, _ := newTestService(t, PhaseGrouping)
	repo.enrolls[1] = Member{PlayerID: 1, ServerID: 1, Power: 10}
	repo.failClear = true

	if _, err := svc.RunGrouping(context.Background()); err != nil {
		t.Fatalf("清表失败不应导致分组失败: %v", err)
	}
	if len(svc.Store().Groups()) != 1 {
		t.Error("分组应已生效")
	}
}

// TestServiceScoreWindow 变化后开窗，窗口未到不下发，到点下发。
func TestServiceScoreWindow(t *testing.T) {
	svc, _, disp := newTestService(t, PhaseActive)
	svc.store.Load(Distribute(makeMembers(100))) // 1 组，玩家 1..100，服 1..3

	base := time.Unix(1770000000, 0)
	svc.now = func() time.Time { return base }

	svc.OnScoreBatch([]*internalv2.CrossScoreEntry{
		{PlayerId: 1, Score: 1005, UpdatedAt: base.Unix()},
	})
	if len(disp.pushes()) != 0 {
		t.Fatal("窗口未到不该下发")
	}

	// 窗口内再来一条：不刷新窗口起点
	svc.now = func() time.Time { return base.Add(50 * time.Second) }
	svc.OnScoreBatch([]*internalv2.CrossScoreEntry{
		{PlayerId: 2, Score: 995, UpdatedAt: base.Unix()},
	})

	// 距第一条已 61s：应当下发，且两条变化都在
	svc.flushWindows(base.Add(61 * time.Second))
	pushes := disp.pushes()
	if len(pushes) == 0 {
		t.Fatal("窗口到点应下发")
	}
	total := 0
	for _, list := range pushes {
		for _, p := range list {
			for _, g := range p.GetGroups() {
				total += len(g.GetPlayers())
			}
		}
	}
	// 这一组涉及 30 个服，玩家 1、2 来自不同服：每条变化发给「除自己服外」的
	// 29 个服，2 人合计 58 条。自己的服不发 —— 那是防「加分被吞掉」的关键。
	if total != 58 {
		t.Errorf("下发条目 = %d, 期望 58（2 人 × 另外 29 个服）", total)
	}
}

// TestServiceScoreWindowNoRefresh 持续上报不会把窗口无限推后（throttle 而非 debounce）。
func TestServiceScoreWindowNoRefresh(t *testing.T) {
	svc, _, disp := newTestService(t, PhaseActive)
	svc.store.Load(Distribute(makeMembers(100)))

	base := time.Unix(1770000000, 0)
	svc.now = func() time.Time { return base }
	svc.OnScoreBatch([]*internalv2.CrossScoreEntry{{PlayerId: 1, Score: 1001, UpdatedAt: base.Unix()}})

	// 每 10s 来一条，一直报到 61s
	for i := 1; i <= 6; i++ {
		svc.now = func() time.Time { return base.Add(time.Duration(i*10) * time.Second) }
		svc.OnScoreBatch([]*internalv2.CrossScoreEntry{
			{PlayerId: 2, Score: int64(1000 - i), UpdatedAt: svc.now().Unix()},
		})
	}
	svc.flushWindows(base.Add(61 * time.Second))
	if len(disp.pushes()) == 0 {
		t.Fatal("持续上报不应推迟窗口 —— 这是 throttle 不是 debounce")
	}
}

// TestServiceScoreUnchangedNotPushed 重复上报同分不触发下发。
func TestServiceScoreUnchangedNotPushed(t *testing.T) {
	svc, _, disp := newTestService(t, PhaseActive)
	svc.store.Load(Distribute(makeMembers(100)))

	base := time.Unix(1770000000, 0)
	svc.now = func() time.Time { return base }
	// 初始就是 1000，报 1000 = 没变化
	svc.OnScoreBatch([]*internalv2.CrossScoreEntry{{PlayerId: 1, Score: InitialScore}})
	svc.flushWindows(base.Add(61 * time.Second))
	if len(disp.pushes()) != 0 {
		t.Error("分数没变不该下发")
	}
}

// TestServiceScoreUnknownPlayerIgnored 不在分组里的玩家上报被忽略。
func TestServiceScoreUnknownPlayerIgnored(t *testing.T) {
	svc, _, disp := newTestService(t, PhaseActive)
	svc.store.Load(Distribute(makeMembers(100)))

	base := time.Unix(1770000000, 0)
	svc.now = func() time.Time { return base }
	svc.OnScoreBatch([]*internalv2.CrossScoreEntry{{PlayerId: 99999, Score: 5000}})
	svc.flushWindows(base.Add(61 * time.Second))
	if len(disp.pushes()) != 0 {
		t.Error("未知玩家的上报应被丢弃")
	}
}

// TestServicePushExcludesOwnServer 下发给某服时不能带它自己的玩家。
func TestServicePushExcludesOwnServer(t *testing.T) {
	svc, _, disp := newTestService(t, PhaseActive)
	svc.store.Load(Distribute(makeMembers(100)))

	base := time.Unix(1770000000, 0)
	svc.now = func() time.Time { return base }

	own := svc.Store().ServerOf(1)
	svc.OnScoreBatch([]*internalv2.CrossScoreEntry{{PlayerId: 1, Score: 1007, UpdatedAt: base.Unix()}})
	svc.flushWindows(base.Add(61 * time.Second))

	ownNode := fmt.Sprintf("game-%02d", own)
	list := disp.pushes()[ownNode]
	if list == nil {
		return // 该服没收到任何报文也是正确的（只有它自己变了就没有接收方）
	}
	for _, p := range list {
		for _, g := range p.GetGroups() {
			for _, pl := range g.GetPlayers() {
				if pl.GetPlayerId() == 1 {
					t.Errorf("服 %d 收到了自己的玩家 1 的变化（会覆盖本地新值）", own)
				}
			}
		}
	}
}

// TestServiceFlushScores 积分按玩家去重后落库，重复刷盘不重复写。
func TestServiceFlushScores(t *testing.T) {
	svc, repo, _ := newTestService(t, PhaseActive)
	svc.store.Load(Distribute(makeMembers(100)))

	svc.OnScoreBatch([]*internalv2.CrossScoreEntry{
		{PlayerId: 1, Score: 1010},
		{PlayerId: 1, Score: 1020}, // 同一玩家两次：只落库最终值
	})
	svc.flushScores(context.Background())

	if len(repo.scores) != 1 {
		t.Fatalf("落库行数 = %d, 期望 1（按玩家去重）", len(repo.scores))
	}
	if repo.scores[0].Score != 1020 {
		t.Errorf("落库分数 = %d, 期望 1020（最终值）", repo.scores[0].Score)
	}
	// 再刷一次不应重复写
	svc.flushScores(context.Background())
	if len(repo.scores) != 1 {
		t.Errorf("重复刷盘不应重复写, 得到 %d 行", len(repo.scores))
	}
}
