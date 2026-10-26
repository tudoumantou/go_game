// internal/game/module/battle/battle_test.go —— 战斗模块单测
//
// 这个模块是全模块级共享状态：多个玩家的 Actor 并发进 onMatch，
// CrossServer/中心服回调线程还会直接触达 OnBattleSync / OnBattleResult，
// 所以除了业务分支，还要盯住「会话与队列的一致性」和「幂等 / 令牌校验」。
//
// 跨服与中心服都是接口，测试用 fake 记录调用；不触网、不依赖时序。
package battle

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// 测试替身
// ============================================================================

type snapshotCall struct {
	RoomID   uint64
	PlayerID int64
	Token    string
	Snapshot []byte
}

type opCall struct {
	RoomID   uint64
	PlayerID int64
	MsgID    uint32
	Body     []byte
}

type resultCall struct {
	RoomID   uint64
	PlayerID int64
	Token    string
	Result   int32
	Rewards  []byte
}

type fakeCross struct {
	available bool

	createRoomID uint64
	createTokens map[int64]string
	createErr    error

	sendSnapErr error
	sendOpErr   error
	sendResErr  error

	mu      sync.Mutex
	creates []CreateRoomReq
	snaps   []snapshotCall
	ops     []opCall
	results []resultCall
}

func (f *fakeCross) Available() bool { return f.available }

func (f *fakeCross) CreateRoom(_ context.Context, req CreateRoomReq) (uint64, map[int64]string, error) {
	f.mu.Lock()
	f.creates = append(f.creates, req)
	f.mu.Unlock()
	if f.createErr != nil {
		return 0, nil, f.createErr
	}
	tokens := f.createTokens
	if tokens == nil {
		tokens = map[int64]string{}
		for _, p := range req.PlayerIDs {
			tokens[p] = fmt.Sprintf("tok-%d", p)
		}
	}
	return f.createRoomID, tokens, nil
}

func (f *fakeCross) SendSnapshot(roomID uint64, playerID int64, token string, snapshot []byte) error {
	f.mu.Lock()
	f.snaps = append(f.snaps, snapshotCall{roomID, playerID, token, snapshot})
	f.mu.Unlock()
	return f.sendSnapErr
}

func (f *fakeCross) SendOp(roomID uint64, playerID int64, msgID uint32, body []byte) error {
	f.mu.Lock()
	f.ops = append(f.ops, opCall{roomID, playerID, msgID, body})
	f.mu.Unlock()
	return f.sendOpErr
}

func (f *fakeCross) SendResult(roomID uint64, playerID int64, token string, result int32, rewards []byte) error {
	f.mu.Lock()
	f.results = append(f.results, resultCall{roomID, playerID, token, result, rewards})
	f.mu.Unlock()
	return f.sendResErr
}

func (f *fakeCross) opsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ops)
}

type fakeCenter struct {
	available bool
	matchErr  error

	mu      sync.Mutex
	matches []struct {
		PlayerID int64
		Mode     string
		Elo      int32
	}
}

func (f *fakeCenter) Available() bool { return f.available }

func (f *fakeCenter) Match(playerID int64, mode string, elo int32, _ []byte) error {
	f.mu.Lock()
	f.matches = append(f.matches, struct {
		PlayerID int64
		Mode     string
		Elo      int32
	}{playerID, mode, elo})
	f.mu.Unlock()
	return f.matchErr
}

type pushCall struct {
	PlayerID int64
	MsgID    uint32
	Body     []byte
}

type fakePusher struct {
	mu    sync.Mutex
	calls []pushCall
	ok    bool
}

func (f *fakePusher) Push(playerID int64, msgID uint32, body []byte) bool {
	f.mu.Lock()
	f.calls = append(f.calls, pushCall{playerID, msgID, append([]byte(nil), body...)})
	f.mu.Unlock()
	return f.ok
}

func (f *fakePusher) byMsg(msgID uint32) []pushCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []pushCall
	for _, c := range f.calls {
		if c.MsgID == msgID {
			out = append(out, c)
		}
	}
	return out
}

type fakeRanker struct {
	mu      sync.Mutex
	reports []struct {
		PlayerID int64
		Score    int64
		BizID    string
	}
	err error
}

func (f *fakeRanker) ReportArenaScore(playerID int64, score int64, bizID string) error {
	f.mu.Lock()
	f.reports = append(f.reports, struct {
		PlayerID int64
		Score    int64
		BizID    string
	}{playerID, score, bizID})
	f.mu.Unlock()
	return f.err
}

func (f *fakeRanker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reports)
}

// ============================================================================
// 装配
// ============================================================================

type rig struct {
	m      *Module
	cross  *fakeCross
	center *fakeCenter
	push   *fakePusher
	rank   *fakeRanker
}

func newRig() *rig {
	cross := &fakeCross{available: true, createRoomID: 1000}
	center := &fakeCenter{}
	push := &fakePusher{ok: true}
	rank := &fakeRanker{}
	m := New(cross, center, push, log.New("battle-test", log.WithLevel("error")))
	m.SetRanker(rank)
	return &rig{m: m, cross: cross, center: center, push: push, rank: rank}
}

func (r *rig) ctx(playerID int64) *module.Context {
	rec := moduletest.NewRecorder()
	return moduletest.NewContext(moduletest.NewState(playerID), rec)
}

func (r *rig) match(t *testing.T, playerID int64, mode string) *gamev1.MatchResp {
	t.Helper()
	ctx := r.ctx(playerID)
	rec := moduletest.NewRecorder()
	ctx.Send = rec.Send
	raw, err := moduletest.Call(r.m, ctx, rec, codec.MsgMatchReq, codec.MsgMatchResp, &gamev1.MatchReq{Mode: mode})
	require.NoError(t, err)
	require.NotNil(t, raw, "匹配没有回包")
	resp := &gamev1.MatchResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.Equal(t, 1, rec.Count(codec.MsgMatchResp), "一次请求只能回一个包")
	return resp
}

func (r *rig) reportResult(t *testing.T, playerID int64, req *gamev1.BattleResultReport) *gamev1.BattleSettleNotify {
	t.Helper()
	ctx := r.ctx(playerID)
	rec := moduletest.NewRecorder()
	ctx.Send = rec.Send
	raw, err := moduletest.Call(r.m, ctx, rec, codec.MsgBattleResultReport, codec.MsgBattleSettleNotify, req)
	require.NoError(t, err)
	require.NotNil(t, raw, "结算上报没有回包")
	resp := &gamev1.BattleSettleNotify{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.Equal(t, 1, rec.Count(codec.MsgBattleSettleNotify), "一次请求只能回一个包")
	return resp
}

// ============================================================================
// 匹配：入队与凑人
// ============================================================================

// TestMatchRejectsWhenCrossUnavailable 跨服全挂时直接拒绝，
// 不让玩家「进了队列却永远匹配不到」。
func TestMatchRejectsWhenCrossUnavailable(t *testing.T) {
	r := newRig()
	r.cross.available = false

	resp := r.match(t, 1, "arena")
	require.EqualValues(t, int32(errcode.ErrBattleModeClosed), resp.Code)
	require.Zero(t, r.m.QueueLen("arena"), "拒绝时不能入队")
}

// TestMatchDefaultsEmptyModeToArena 空 mode 兜底 arena，
// 否则客户端不传模式会把队列打散到 "" 键下永远凑不到人。
func TestMatchDefaultsEmptyModeToArena(t *testing.T) {
	r := newRig()
	resp := r.match(t, 1, "")
	require.EqualValues(t, "arena", resp.Mode)
	require.EqualValues(t, 1, r.m.QueueLen("arena"))
	require.Zero(t, r.m.QueueLen(""), "不能落到空串队列")
}

// TestMatchQueuesFirstPlayer 第一个人只入队，不建房。
func TestMatchQueuesFirstPlayer(t *testing.T) {
	r := newRig()

	resp := r.match(t, 1, "arena")
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 1, resp.Queued, "要告诉客户端队列里已有人")
	require.Empty(t, r.cross.creates, "人不齐不能建房")
	require.Empty(t, r.push.byMsg(codec.MsgBattleStartNotify))
}

// TestMatchPairsTwoPlayers 第二个人凑满 → 出队 → 建房 → 双方收到开战通知。
func TestMatchPairsTwoPlayers(t *testing.T) {
	r := newRig()

	r.match(t, 1, "arena")
	resp := r.match(t, 2, "arena")
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 0, resp.Queued, "两人都出队后队列为空")

	require.Len(t, r.cross.creates, 1, "凑满必须建房一次")
	require.Equal(t, []int64{1, 2}, r.cross.creates[0].PlayerIDs)
	require.EqualValues(t, defaultBattleTimeout, r.cross.creates[0].Timeout)
	require.Zero(t, r.m.QueueLen("arena"), "出队后队列必须清空")

	// 双方各收到一条开战通知，且对手信息是对方
	notifies := r.push.byMsg(codec.MsgBattleStartNotify)
	require.Len(t, notifies, 2)
	for _, n := range notifies {
		require.EqualValues(t, 1000, mustNotify(t, n.Body).RoomId)
		require.NotEmpty(t, mustNotify(t, n.Body).BattleToken)
	}

	s1 := r.m.SessionOf(1)
	s2 := r.m.SessionOf(2)
	require.NotNil(t, s1)
	require.NotNil(t, s2)
	require.EqualValues(t, 1000, s1.RoomID)
	require.EqualValues(t, 2, s1.PeerID, "1 号玩家的对手是 2")
	require.EqualValues(t, 1, s2.PeerID)
	require.Equal(t, "tok-1", s1.Token)

	require.Len(t, r.cross.snaps, 2, "快照要先于开战通知送达房间")
	require.True(t, r.m.InBattle(1))
	require.True(t, r.m.InBattle(2))
}

func mustNotify(t *testing.T, raw []byte) *gamev1.BattleStartNotify {
	t.Helper()
	n := &gamev1.BattleStartNotify{}
	require.NoError(t, proto.Unmarshal(raw, n))
	return n
}

// TestMatchRejectsDuplicateQueue 已在队列再匹配 → 6006，队列不能重复计数。
func TestMatchRejectsDuplicateQueue(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")

	resp := r.match(t, 1, "arena")
	require.EqualValues(t, int32(errcode.ErrBattleAlreadyIn), resp.Code)
	require.EqualValues(t, 1, r.m.QueueLen("arena"), "重复入队必须被挡住")
}

// TestMatchRejectsWhileInBattle 战斗中再匹配 → 6006。
func TestMatchRejectsWhileInBattle(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	resp := r.match(t, 1, "arena")
	require.EqualValues(t, int32(errcode.ErrBattleAlreadyIn), resp.Code)
}

// TestMatchClearsSettledSession 已结算但未回收的会话要立即清掉，
// 否则玩家打完一场得等 2s 才能再排。
func TestMatchClearsSettledSession(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")
	require.NoError(t, r.m.OnMessage(r.ctx(1), codec.MsgBattleResultReport, mustMarshal(t, &gamev1.BattleResultReport{
		RoomId: 1000, BattleToken: "tok-1", Result: 0,
	})))

	require.False(t, r.m.InBattle(1), "已结算不再算战斗中")
	resp := r.match(t, 1, "arena")
	require.EqualValues(t, int32(errcode.OK), resp.Code, "已结算会话应被清掉，允许重新匹配")
}

// TestMatchUsesCenterWhenAvailable 中心服可用时上报匹配，不进本地队列。
func TestMatchUsesCenterWhenAvailable(t *testing.T) {
	r := newRig()
	r.center.available = true

	st := moduletest.NewState(1)
	st.SetProfile(module.Profile{Level: 12})
	rec := moduletest.NewRecorder()
	ctx := moduletest.NewContext(st, rec)

	raw, err := moduletest.Call(r.m, ctx, rec, codec.MsgMatchReq, codec.MsgMatchResp, &gamev1.MatchReq{Mode: "arena"})
	require.NoError(t, err)
	resp := &gamev1.MatchResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))

	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 1, resp.Queued, "中心服匹配池不回精确人数，按已入队处理")
	require.Len(t, r.center.matches, 1)
	require.EqualValues(t, 1, r.center.matches[0].PlayerID)
	require.EqualValues(t, 1200, r.center.matches[0].Elo, "Elo 用等级 ×100 近似")
	require.Zero(t, r.m.QueueLen("arena"), "走中心服就不该进本地队列")
}

// TestMatchCenterFailure 中心服上报失败 → 6004，且不入本地队列（不能两边都占）。
func TestMatchCenterFailure(t *testing.T) {
	r := newRig()
	r.center.available = true
	r.center.matchErr = context.DeadlineExceeded

	resp := r.match(t, 1, "arena")
	require.EqualValues(t, int32(errcode.ErrBattleModeClosed), resp.Code)
	require.Zero(t, r.m.QueueLen("arena"))
}

// TestMatchReturnsPlayersToQueueOnCreateFailure 建房失败要把玩家放回队尾，
// 否则两人都被吞掉、永远凑不齐（放队尾而非队首：避免故障节点反复排最前无限失败）。
func TestMatchReturnsPlayersToQueueOnCreateFailure(t *testing.T) {
	r := newRig()
	r.cross.createErr = context.DeadlineExceeded

	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	require.EqualValues(t, 2, r.m.QueueLen("arena"), "建房失败必须回队")
	require.Empty(t, r.push.byMsg(codec.MsgBattleStartNotify), "没建成房不能通知开战")
	require.Nil(t, r.m.SessionOf(1))
}

// TestMatchSnapshotSendFailureDoesNotBlock 快照发送失败只告警，仍要通知开战
// （房间侧有兜底，不能因为一次推送失败让整场战斗卡住）。
func TestMatchSnapshotSendFailureDoesNotBlock(t *testing.T) {
	r := newRig()
	r.cross.sendSnapErr = context.DeadlineExceeded

	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	require.Len(t, r.push.byMsg(codec.MsgBattleStartNotify), 2, "快照失败仍要下发开战通知")
}

// ============================================================================
// 操作透传
// ============================================================================

// TestOpReportRelaysRawBody 正常透传：body 原样、不解析。
func TestOpReportRelaysRawBody(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	body := []byte{0x01, 0x02, 0x03}
	raw := mustMarshal(t, &gamev1.BattleOpReport{RoomId: 1000, MsgId: 77, Body: body})
	require.NoError(t, r.m.OnMessage(r.ctx(1), codec.MsgBattleOpReport, raw))

	require.Equal(t, 1, r.cross.opsCount())
	require.EqualValues(t, 1000, r.cross.ops[0].RoomID)
	require.EqualValues(t, 77, r.cross.ops[0].MsgID)
	require.Equal(t, body, r.cross.ops[0].Body, "body 必须原样透传")
}

// TestOpReportDropsForeignMessages 非当前房间/无会话/已结算的操作静默丢弃，
// 不能报错（客户端重发与延迟到达是常态）。
func TestOpReportDropsForeignMessages(t *testing.T) {
	r := newRig()

	// 无会话
	raw := mustMarshal(t, &gamev1.BattleOpReport{RoomId: 1000, MsgId: 1})
	require.NoError(t, r.m.OnMessage(r.ctx(9), codec.MsgBattleOpReport, raw))
	require.Zero(t, r.cross.opsCount())

	// 房间号不匹配
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")
	raw = mustMarshal(t, &gamev1.BattleOpReport{RoomId: 999, MsgId: 1})
	require.NoError(t, r.m.OnMessage(r.ctx(1), codec.MsgBattleOpReport, raw))
	require.Zero(t, r.cross.opsCount(), "别的房间的操作不能透传")

	// 已结算
	require.NoError(t, r.m.OnMessage(r.ctx(1), codec.MsgBattleResultReport, mustMarshal(t, &gamev1.BattleResultReport{
		RoomId: 1000, BattleToken: "tok-1", Result: 0,
	})))
	raw = mustMarshal(t, &gamev1.BattleOpReport{RoomId: 1000, MsgId: 2})
	require.NoError(t, r.m.OnMessage(r.ctx(1), codec.MsgBattleOpReport, raw))
	require.Zero(t, r.cross.opsCount(), "结算后的操作必须丢弃")
}

// TestOpReportDecodeError 解不出协议体是真故障，要上抛（区别于业务丢弃）。
func TestOpReportDecodeError(t *testing.T) {
	r := newRig()
	err := r.m.OnMessage(r.ctx(1), codec.MsgBattleOpReport, []byte{0xFF, 0xFF, 0xFF})
	require.Error(t, err)
}

// TestOpReportRelayFailureSwallowed 透传失败只记日志，不回错误。
func TestOpReportRelayFailureSwallowed(t *testing.T) {
	r := newRig()
	r.cross.sendOpErr = context.DeadlineExceeded
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	raw := mustMarshal(t, &gamev1.BattleOpReport{RoomId: 1000, MsgId: 1})
	require.NoError(t, r.m.OnMessage(r.ctx(1), codec.MsgBattleOpReport, raw))
}

// ============================================================================
// 结算上报：令牌校验 + 幂等
// ============================================================================

// TestResultReportRejectsInvalidToken 房间不匹配 / 令牌不对都回 6002。
func TestResultReportRejectsInvalidToken(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	// 房间不匹配
	resp := r.reportResult(t, 1, &gamev1.BattleResultReport{RoomId: 999, BattleToken: "tok-1"})
	require.EqualValues(t, int32(errcode.ErrBattleTokenInvalid), resp.Code)

	// 令牌不对（拿别房/自造令牌冒领）
	resp = r.reportResult(t, 1, &gamev1.BattleResultReport{RoomId: 1000, BattleToken: "wrong"})
	require.EqualValues(t, int32(errcode.ErrBattleTokenInvalid), resp.Code)
	require.Empty(t, r.cross.results, "校验失败不能转发结算")

	// 无会话
	resp = r.reportResult(t, 9, &gamev1.BattleResultReport{RoomId: 1000, BattleToken: "tok-9"})
	require.EqualValues(t, int32(errcode.ErrBattleTokenInvalid), resp.Code)
}

// TestResultReportSettlesAndIsIdempotent 正常结算：转发一次、回执带奖励；
// 重复上报回 6003 且不再转发（防重复计分）。
func TestResultReportSettlesAndIsIdempotent(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	rewards := []byte("rewards")
	resp := r.reportResult(t, 1, &gamev1.BattleResultReport{
		RoomId: 1000, BattleToken: "tok-1", Result: 0, Rewards: rewards,
	})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Equal(t, rewards, resp.Rewards, "即时回执要带回奖励")
	require.Len(t, r.cross.results, 1)
	require.EqualValues(t, "tok-1", r.cross.results[0].Token)

	// 第二次上报
	resp = r.reportResult(t, 1, &gamev1.BattleResultReport{
		RoomId: 1000, BattleToken: "tok-1", Result: 1,
	})
	require.EqualValues(t, int32(errcode.ErrResultReported), resp.Code)
	require.Len(t, r.cross.results, 1, "重复上报不能再转发一次")
}

// TestResultReportSurvivesCrossFailure 转发失败只告警，仍回 0：
// 客户端结果已显示，权威奖励由 CrossServer 收齐双方后回推。
func TestResultReportSurvivesCrossFailure(t *testing.T) {
	r := newRig()
	r.cross.sendResErr = context.DeadlineExceeded
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	resp := r.reportResult(t, 1, &gamev1.BattleResultReport{RoomId: 1000, BattleToken: "tok-1"})
	require.EqualValues(t, int32(errcode.OK), resp.Code, "转发失败不能让客户端以为上报失败")
	require.False(t, r.m.InBattle(1), "仍要占住幂等位，不接受第二次上报")
}

// ============================================================================
// 中心服匹配结果回调
// ============================================================================

// TestOnMatchResultBuildsSession 中心服结果：建房（带 PresetTokens）+ 送快照 +
// 建会话 + 推开战通知；令牌一律用中心服签发的。
func TestOnMatchResultBuildsSession(t *testing.T) {
	r := newRig()
	r.m.OnMatchResult(&internalv2.MatchResult{
		RoomId:      2000,
		BattleToken: "center-tok",
		Mode:        "arena",
		PeerId:      99,
		PlayerId:    1,
	})

	require.Len(t, r.cross.creates, 1)
	require.EqualValues(t, 2000, r.cross.creates[0].RoomID)
	require.Equal(t, map[int64]string{1: "center-tok"}, r.cross.creates[0].PresetTokens)

	require.Len(t, r.cross.snaps, 1)
	require.Equal(t, "center-tok", r.cross.snaps[0].Token, "令牌权威在中心服")

	s := r.m.SessionOf(1)
	require.NotNil(t, s)
	require.EqualValues(t, 2000, s.RoomID)
	require.Equal(t, "center-tok", s.Token)
	require.EqualValues(t, 99, s.PeerID)

	require.Len(t, r.push.byMsg(codec.MsgBattleStartNotify), 1)
}

// TestOnMatchResultIgnoresInvalid 缺字段的结果直接丢弃（不能建出一个残缺会话）。
func TestOnMatchResultIgnoresInvalid(t *testing.T) {
	cases := []*internalv2.MatchResult{
		nil,
		{RoomId: 0, BattleToken: "t", PlayerId: 1},
		{RoomId: 1, BattleToken: "", PlayerId: 1},
		{RoomId: 1, BattleToken: "t", PlayerId: 0},
	}
	for i, c := range cases {
		r := newRig()
		r.m.OnMatchResult(c)
		require.Empty(t, r.cross.creates, fmt.Sprintf("case %d 不该建房", i))
		require.Nil(t, r.m.SessionOf(1))
	}
}

// TestOnMatchResultCreateFailureKeepsNoSession 建房失败不能留下「有会话没房间」的幽灵。
func TestOnMatchResultCreateFailureKeepsNoSession(t *testing.T) {
	r := newRig()
	r.cross.createErr = context.DeadlineExceeded
	r.m.OnMatchResult(&internalv2.MatchResult{RoomId: 2000, BattleToken: "t", PlayerId: 1})

	require.Nil(t, r.m.SessionOf(1))
	require.Empty(t, r.push.byMsg(codec.MsgBattleStartNotify))
}

// ============================================================================
// 跨服回调：同步与结算
// ============================================================================

// TestOnBattleSyncPushesPeersOnly 同步消息只推给同房间的其他玩家，不推给发起者。
func TestOnBattleSyncPushesPeersOnly(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	r.m.OnBattleSync(&internalv2.BattleMsg{RoomId: 1000, PlayerId: 1, MsgId: 5, Body: []byte("op")})
	syncs := r.push.byMsg(codec.MsgBattleSyncNotify)
	require.Len(t, syncs, 1, "只推给对手")
	require.EqualValues(t, 2, syncs[0].PlayerID)

	// 别的房间不推
	r.m.OnBattleSync(&internalv2.BattleMsg{RoomId: 4242, PlayerId: 1})
	require.Len(t, r.push.byMsg(codec.MsgBattleSyncNotify), 1)

	// nil 不 panic
	r.m.OnBattleSync(nil)
}

// TestOnBattleResultSettlesAndReportsRank 兜底结算：置 Settled、推通知、
// 胜利 +30、失败 -10、平局不上报（零变动不发请求）。
func TestOnBattleResultSettlesAndReportsRank(t *testing.T) {
	cases := []struct {
		name      string
		result    int32
		wantDelta int64
		wantCount int
	}{
		{"win", 0, 30, 1},
		{"lose", 1, -10, 1},
		{"draw", 2, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newRig()
			r.match(t, 1, "arena")
			r.match(t, 2, "arena")

			r.m.OnBattleResult(&internalv2.BattleResult{RoomId: 1000, PlayerId: 1, Result: c.result})

			require.False(t, r.m.InBattle(1), "结算后不再是战斗中")
			require.Len(t, r.push.byMsg(codec.MsgBattleSettleNotify), 1)
			require.Equal(t, c.wantCount, r.rank.count())
			if c.wantCount > 0 {
				require.EqualValues(t, c.wantDelta, r.rank.reports[0].Score)
				require.EqualValues(t, "battle:1000:1", r.rank.reports[0].BizID,
					"幂等键必须是房间+玩家，重投不能重复加分")
			}
		})
	}
}

// TestOnBattleResultIgnoresStaleRoom 旧房间的迟到结算不能把新会话标成已结算。
func TestOnBattleResultIgnoresStaleRoom(t *testing.T) {
	r := newRig()
	// 手工造一个房间 2000 的新会话
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")
	r.m.mu.Lock()
	r.m.sessions[1] = &Session{RoomID: 3000, Token: "new", PeerID: 9}
	r.m.mu.Unlock()

	r.m.OnBattleResult(&internalv2.BattleResult{RoomId: 1000, PlayerId: 1, Result: 0})
	require.True(t, r.m.InBattle(1), "旧房间的结算不能动新会话")

	// nil 不 panic
	r.m.OnBattleResult(nil)
}

// TestOnBattleResultRankFailureSwallowed 排行上报失败是旁路数据，不影响主链路。
func TestOnBattleResultRankFailureSwallowed(t *testing.T) {
	r := newRig()
	r.rank.err = context.DeadlineExceeded
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	require.NotPanics(t, func() {
		r.m.OnBattleResult(&internalv2.BattleResult{RoomId: 1000, PlayerId: 1, Result: 0})
	})
	require.Len(t, r.push.byMsg(codec.MsgBattleSettleNotify), 1, "排行失败也要推结算")
}

// ============================================================================
// 辅助方法
// ============================================================================

// TestInBattleAndQueueLen InBattle 只认「有会话且未结算」。
func TestInBattleAndQueueLen(t *testing.T) {
	r := newRig()
	require.False(t, r.m.InBattle(1))
	require.Zero(t, r.m.QueueLen("arena"))

	r.match(t, 1, "arena")
	require.False(t, r.m.InBattle(1), "只排队不算战斗中")
	require.EqualValues(t, 1, r.m.QueueLen("arena"))
}

// TestSessionOfReturnsCopy 返回的是快照，改它不能影响内部会话。
func TestSessionOfReturnsCopy(t *testing.T) {
	r := newRig()
	r.match(t, 1, "arena")
	r.match(t, 2, "arena")

	s := r.m.SessionOf(1)
	require.NotNil(t, s)
	s.Settled = true
	require.True(t, r.m.InBattle(1), "改副本不能影响内部状态")
}

// TestSetBattleTimeout 正数生效，非正数忽略（防止误配成 0 让房间瞬间超时）。
func TestSetBattleTimeout(t *testing.T) {
	r := newRig()
	require.EqualValues(t, defaultBattleTimeout, r.m.battleTimeout())

	r.m.SetBattleTimeout(5 * time.Second)
	require.EqualValues(t, 5*time.Second, r.m.battleTimeout())

	r.m.SetBattleTimeout(0)
	require.EqualValues(t, 5*time.Second, r.m.battleTimeout(), "0 必须被忽略")
	r.m.SetBattleTimeout(-time.Second)
	require.EqualValues(t, 5*time.Second, r.m.battleTimeout(), "负数必须被忽略")
}

// TestBuildSnapshotCarriesPlayerID 快照里带 player_id（对手据此渲染）。
func TestBuildSnapshotCarriesPlayerID(t *testing.T) {
	r := newRig()
	raw := r.m.buildSnapshot(42)
	require.NotNil(t, raw)
	info := &gamev1.PlayerInfoResp{}
	require.NoError(t, proto.Unmarshal(raw, info))
	require.EqualValues(t, 42, info.RoleId)
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(m)
	require.NoError(t, err)
	return raw
}

// TestOnRoomReadyIsNoop 房间就绪回调目前无动作（快照已在建房后送达），
// 但它是 cross.Handler 的实现，必须存在且不 panic。
func TestOnRoomReadyIsNoop(t *testing.T) {
	r := newRig()
	require.NotPanics(t, func() { r.m.OnRoomReady(1000) })
}
