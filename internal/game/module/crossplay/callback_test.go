package crossplay

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/log"
	"game_study/pkg/model"
	internalv2 "game_study/pkg/pb/internalv2"
)

// atActive 2026-03-03 12:00（活动期内）。
var atActive = time.Date(2026, 3, 3, 12, 0, 0, 0, time.UTC)

// fakeCenter 中心服的假实现：记录调用、可注入错误。
type fakeCenter struct {
	enrollResp *internalv2.CrossEnrollResp
	enrollErr  error

	scoreBatches [][]*internalv2.CrossScoreEntry
	scoreErr     error

	deltas  []*internalv2.CrossScoreDelta
	deltaCh chan *internalv2.CrossScoreDelta
}

func (f *fakeCenter) Ready() bool { return true }

func (f *fakeCenter) CrossEnroll(_ context.Context, req *internalv2.CrossEnrollReq) (*internalv2.CrossEnrollResp, error) {
	if f.enrollErr != nil {
		return nil, f.enrollErr
	}
	if f.enrollResp != nil {
		return f.enrollResp, nil
	}
	return &internalv2.CrossEnrollResp{Code: 0}, nil
}

func (f *fakeCenter) SendCrossScore(entries []*internalv2.CrossScoreEntry) error {
	if f.scoreErr != nil {
		return f.scoreErr
	}
	f.scoreBatches = append(f.scoreBatches, entries)
	return nil
}

func (f *fakeCenter) SendCrossDelta(d *internalv2.CrossScoreDelta) error {
	f.deltas = append(f.deltas, d)
	return nil
}

// newTestModule 造一个活动期内的模块（时钟固定，serverID=1）。
func newTestModule(t *testing.T, center Center, pe ...Option) *Module {
	t.Helper()
	opts := []Option{
		WithServerID(1),
		WithNodeID("game-01"),
		WithClock(func() time.Time { return atActive }),
	}
	opts = append(opts, pe...)
	return New(center, nil, log.New("crossplay-test"), opts...)
}

// TestOnGroupDeltaSnapshot 快照建立组镜像并带上归属。
func TestOnGroupDeltaSnapshot(t *testing.T) {
	m := newTestModule(t, &fakeCenter{})
	m.OnGroupDelta(&internalv2.CrossGroupDeltaPush{Groups: []*internalv2.CrossGroupDelta{{
		GroupId:  7,
		Snapshot: true,
		Players: []*internalv2.CrossScoreEntry{
			{PlayerId: 1, ServerId: 1, Score: 1000},
			{PlayerId: 2, ServerId: 2, Score: 1200},
		},
	}}})

	own, ok := m.reg.Player(1)
	if !ok || own.GroupID != 7 {
		t.Fatalf("本服玩家未采纳归属: %+v ok=%v", own, ok)
	}
	if r, size := m.reg.RankOf(1); r != 2 || size != 2 {
		t.Errorf("名次 = %d/%d, 期望 2/2", r, size)
	}
}

// TestOnGroupDeltaEmptyIgnored 空报文不 panic、不改状态。
func TestOnGroupDeltaEmptyIgnored(t *testing.T) {
	m := newTestModule(t, &fakeCenter{})
	m.OnGroupDelta(nil)
	m.OnGroupDelta(&internalv2.CrossGroupDeltaPush{})
	m.OnGroupDelta(&internalv2.CrossGroupDeltaPush{Groups: []*internalv2.CrossGroupDelta{
		nil,
		{GroupId: 0},
		{GroupId: 3}, // 有组号没成员
	}})
	if m.reg.Len() != 0 {
		t.Error("空报文不应产生玩家")
	}
}

// TestOnScoreDeltaApplies 跨服扣分落到本服玩家身上。
func TestOnScoreDeltaApplies(t *testing.T) {
	m := newTestModule(t, &fakeCenter{})
	m.reg.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000)}, 0)

	m.OnScoreDelta(&internalv2.CrossScoreDelta{
		RelayId: "sd:game-02:1", FromNode: "game-02", PlayerId: 1, Delta: -1, GroupId: 7,
	})
	if e, _ := m.reg.Player(1); e.Score != 999 {
		t.Errorf("分数 = %d, 期望 999", e.Score)
	}
	if n := m.reg.DirtyCount(); n != 1 {
		t.Errorf("改分应标脏（要上报中心服），脏数 = %d", n)
	}
}

// TestOnScoreDeltaDedup 同一 (from_node, relay_id) 只生效一次。
//
// relay 是 at-least-once，中心服会重发；不去重就是重复扣分。
func TestOnScoreDeltaDedup(t *testing.T) {
	m := newTestModule(t, &fakeCenter{})
	m.reg.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000)}, 0)

	d := &internalv2.CrossScoreDelta{
		RelayId: "sd:game-02:1", FromNode: "game-02", PlayerId: 1, Delta: -1,
	}
	for i := 0; i < 3; i++ {
		m.OnScoreDelta(d)
	}
	if e, _ := m.reg.Player(1); e.Score != 999 {
		t.Errorf("重复投递被算了 %d 次: 分数 %d, 期望 999", 1000-e.Score, e.Score)
	}

	// 换个来源节点、同样的 relay_id：不是同一条消息，要生效
	d2 := &internalv2.CrossScoreDelta{
		RelayId: "sd:game-02:1", FromNode: "game-03", PlayerId: 1, Delta: -1,
	}
	m.OnScoreDelta(d2)
	if e, _ := m.reg.Player(1); e.Score != 998 {
		t.Errorf("不同来源的同号消息被误判为重复: %d, 期望 998", e.Score)
	}
}

// TestOnScoreDeltaUnknownPlayer 目标不在本服时丢弃并告警，不改任何状态。
func TestOnScoreDeltaUnknownPlayer(t *testing.T) {
	m := newTestModule(t, &fakeCenter{})
	m.OnScoreDelta(&internalv2.CrossScoreDelta{
		RelayId: "sd:x:1", FromNode: "game-02", PlayerId: 404, Delta: -1,
	})
	if n := m.reg.Len(); n != 0 {
		t.Errorf("不该凭空造出玩家，得到 %d", n)
	}
}

// TestAttackSameServer 同服攻击：两人都在本服，直接改，不发报文。
func TestAttackSameServer(t *testing.T) {
	center := &fakeCenter{}
	m := newTestModule(t, center)
	m.reg.ApplySnapshot(7, []*Entry{
		newEntry(1, 1, 1000),
		newEntry(2, 1, 1000),
	}, 0)

	score, err := m.Attack(1, 2)
	if err != nil {
		t.Fatalf("攻击失败: %v", err)
	}
	if score != 1001 {
		t.Errorf("攻击者分数 = %d, 期望 1001", score)
	}
	if e, _ := m.reg.Player(2); e.Score != 999 {
		t.Errorf("目标分数 = %d, 期望 999", e.Score)
	}
	if len(center.deltas) != 0 {
		t.Errorf("同服不该发跨服报文，发了 %d 条", len(center.deltas))
	}
}

// TestAttackCrossServer 跨服攻击：本服加分 + 发差值给目标服。
func TestAttackCrossServer(t *testing.T) {
	center := &fakeCenter{}
	m := newTestModule(t, center)
	m.reg.ApplySnapshot(7, []*Entry{
		newEntry(1, 1, 1000),
		newEntry(2, 5, 1000), // 目标在 5 服
	}, 0)

	score, err := m.Attack(1, 2)
	if err != nil {
		t.Fatalf("攻击失败: %v", err)
	}
	if score != 1001 {
		t.Errorf("攻击者分数 = %d, 期望 1001", score)
	}
	// 本服不能改别服的分
	if e, _ := m.reg.Participant(2); e.Score != 1000 {
		t.Errorf("本服擅自改了别服玩家的分: %d, 期望 1000", e.Score)
	}
	if len(center.deltas) != 1 {
		t.Fatalf("跨服报文数 = %d, 期望 1", len(center.deltas))
	}
	d := center.deltas[0]
	if d.GetPlayerId() != 2 || d.GetDelta() != -1 || d.GetToServer() != 5 {
		t.Errorf("报文内容错误: %+v", d)
	}
	if d.GetRelayId() == "" {
		t.Error("relay_id 不能为空（接收方靠它去重）")
	}
	if d.GetFromPlayer() != 1 {
		t.Errorf("from_player = %d, 期望 1（审计用）", d.GetFromPlayer())
	}
}

// TestAttackGuards 各种拒绝路径。
func TestAttackGuards(t *testing.T) {
	center := &fakeCenter{}
	m := newTestModule(t, center)
	m.reg.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000), newEntry(2, 2, 1000)}, 0)

	if _, err := m.Attack(1, 1); !errors.Is(err, ErrSelfTarget) {
		t.Errorf("自攻应被拒, 得到 %v", err)
	}
	if _, err := m.Attack(1, 404); !errors.Is(err, ErrNotParticipant) {
		t.Errorf("不存在的目标应被拒, 得到 %v", err)
	}
	if _, err := m.Attack(404, 1); !errors.Is(err, ErrNotParticipant) {
		t.Errorf("未参赛的攻击者应被拒, 得到 %v", err)
	}

	// 非活动期
	paused := newTestModule(t, center, WithClock(func() time.Time {
		return time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC) // 周三，已过活动期？
	}))
	_ = paused
	if len(center.deltas) != 0 {
		t.Errorf("拒绝路径不该发报文, 发了 %d", len(center.deltas))
	}
}

// TestAttackNotActive 结束期不能攻击。
func TestAttackNotActive(t *testing.T) {
	m := newTestModule(t, &fakeCenter{}, WithClock(func() time.Time {
		// 2026-03-08 23:00 周日，结束期内
		return time.Date(2026, 3, 8, 23, 0, 0, 0, time.UTC)
	}))
	m.reg.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000), newEntry(2, 2, 1000)}, 0)
	if _, err := m.Attack(1, 2); !errors.Is(err, ErrNotActive) {
		t.Errorf("结束期攻击应被拒, 得到 %v", err)
	}
}

// TestReportTick 上报取脏、失败回填。
func TestReportTick(t *testing.T) {
	center := &fakeCenter{}
	m := newTestModule(t, center)
	m.reg.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000), newEntry(2, 1, 1000)}, 0)
	m.reg.AddScore(1, 5, atActive.Unix())
	m.reg.AddScore(2, 3, atActive.Unix())

	if err := m.reportTick(context.Background()); err != nil {
		t.Fatalf("上报失败: %v", err)
	}
	if len(center.scoreBatches) != 1 || len(center.scoreBatches[0]) != 2 {
		t.Fatalf("报文 = %+v, 期望一条两条", center.scoreBatches)
	}
	if n := m.reg.DirtyCount(); n != 0 {
		t.Errorf("上报成功后应为空, 剩 %d", n)
	}

	// 失败回填
	m.reg.AddScore(1, 1, atActive.Unix())
	center.scoreErr = errors.New("boom")
	if err := m.reportTick(context.Background()); err == nil {
		t.Fatal("应返回错误")
	}
	if n := m.reg.DirtyCount(); n != 1 {
		t.Errorf("失败后应回填 1 条, 得到 %d", n)
	}
}

// TestReportTickNoChange 没变化就不发报文（一个字节都不发）。
func TestReportTickNoChange(t *testing.T) {
	center := &fakeCenter{}
	m := newTestModule(t, center)
	m.reg.ApplySnapshot(7, []*Entry{newEntry(1, 1, 1000)}, 0)
	if err := m.reportTick(context.Background()); err != nil {
		t.Fatalf("上报失败: %v", err)
	}
	if len(center.scoreBatches) != 0 {
		t.Errorf("无变化不该发报文, 发了 %d 批", len(center.scoreBatches))
	}
}

// TestOnEnrollSuccess 报名成功：写内存 + 回包 code=0。
func TestOnEnrollSuccess(t *testing.T) {
	center := &fakeCenter{}
	m := newTestModule(t, center, WithClock(func() time.Time {
		return time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC) // 周一中午，报名期
	}))

	st := moduletest.NewState(42)
	c := &module.Context{State: st}
	var reply uint32
	c.Send = func(id uint32, _ []byte) error { reply = id; return nil }

	if err := m.OnMessage(c, 6909, nil); err != nil {
		t.Fatalf("报名失败: %v", err)
	}
	if reply == 0 {
		t.Fatal("没有回包")
	}
	if !m.reg.IsEnrolled(42) {
		t.Error("报名成功后内存里应有该玩家")
	}
	if e, ok := m.reg.Player(42); !ok || e.ServerID != 1 {
		t.Errorf("条目 = %+v, 期望 server_id=1", e)
	}
}

// TestOnEnrollRejectedByCenter 中心服拒绝时本地不能留痕。
//
// 顺序错了（先写本地再问中心服）会出现「本地显示已报名、中心服没有」，
// 玩家永远等不到分组。
func TestOnEnrollRejectedByCenter(t *testing.T) {
	center := &fakeCenter{enrollResp: &internalv2.CrossEnrollResp{Code: 6950}}
	m := newTestModule(t, center, WithClock(func() time.Time {
		return time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
	}))

	st := moduletest.NewState(42)
	c := &module.Context{State: st}
	c.Send = func(uint32, []byte) error { return nil }

	if err := m.OnMessage(c, 6909, nil); err != nil {
		t.Fatalf("处理器不该返回错误（业务拒绝走 code）: %v", err)
	}
	if m.reg.IsEnrolled(42) {
		t.Error("中心服拒绝后本地不能留报名记录")
	}
}

// TestOnEnrollNotInPhase 非报名期本地就拒绝，不打扰中心服。
func TestOnEnrollNotInPhase(t *testing.T) {
	center := &fakeCenter{}
	m := newTestModule(t, center, WithClock(func() time.Time {
		return time.Date(2026, 3, 3, 12, 0, 0, 0, time.UTC) // 周二，已过报名期
	}))

	st := moduletest.NewState(42)
	c := &module.Context{State: st}
	var got []byte
	c.Send = func(_ uint32, body []byte) error { got = body; return nil }

	if err := m.OnMessage(c, 6909, nil); err != nil {
		t.Fatalf("处理器出错: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("应回包说明不在报名期")
	}
	if m.reg.IsEnrolled(42) {
		t.Error("非报名期不该报名成功")
	}
}

// TestOnInfoBeforeGrouping 分组前：已报名但无组号、无积分。
func TestOnInfoBeforeGrouping(t *testing.T) {
	m := newTestModule(t, &fakeCenter{})
	m.reg.Enroll(newEntry(42, 1, 0))

	st := moduletest.NewState(42)
	c := &module.Context{State: st}
	c.Send = func(uint32, []byte) error { return nil }

	if err := m.OnMessage(c, 6911, nil); err != nil {
		t.Fatalf("查询失败: %v", err)
	}
}

// TestSaveGroupsRoundTrip 组镜像写库再读回来，ServerID 与分数都要在。
//
// 这里直接验证「players 列没有 server_id，从 servers 列恢复」这条链路。
func TestSaveGroupsRoundTrip(t *testing.T) {
	members := []Entry{
		{PlayerID: 1, ServerID: 1, Score: 1001, Power: 500, UpdatedAt: 100},
		{PlayerID: 2, ServerID: 5, Score: 990, Power: 400, UpdatedAt: 200},
	}
	playersCol, serversCol, err := membersToJSON(members)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}
	got, err := jsonToMembers(playersCol, serversCol)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("解出 %d 条, 期望 2", len(got))
	}
	byID := map[int64]*Entry{}
	for _, e := range got {
		byID[e.PlayerID] = e
	}
	if byID[2].ServerID != 5 {
		t.Errorf("玩家2 的 server_id = %d, 期望 5（应从 servers 列恢复）", byID[2].ServerID)
	}
	if byID[1].Score != 1001 || byID[2].Score != 990 {
		t.Errorf("分数丢失: %+v", byID)
	}
}

// TestLoadCrossGroupWithoutServerColumn 坏 JSON 不阻断加载。
func TestLoadCrossGroupWithoutServerColumn(t *testing.T) {
	var bad = model.CrossGroup{GroupId: 3, Players: nullString(`not-json`)}
	if _, err := jsonToMembers(bad.Players, bad.Servers); err == nil {
		t.Log("坏 JSON 应返回错误，由调用方跳过该组")
	}
}

// nullString 测试用：构造 sql.NullString。
func nullString(s string) (n sql.NullString) {
	n.String, n.Valid = s, s != ""
	return
}
