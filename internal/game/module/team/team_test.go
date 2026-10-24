// internal/game/module/team/team_test.go —— 组队模块单测
//
// 队伍是临时关系：掉线即退队、队长退队即解散。这里钉死模块层的编排规则
// （谁在队里决定能不能创建、仓储错误怎么翻译成业务码、掉线钩子的容错），
// SQL 事务本身（人数上限并发、软删过滤）由集成测试兜底。
package team

import (
	"context"
	"fmt"
	"testing"

	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// 测试替身
// ============================================================================

type fakeRepo struct {
	team      *Team
	members   []Member
	findErr   error
	createID  int64
	createErr error
	joinErr   error
	leaveErr  error
	memErr    error

	createdWith struct {
		leaderID int64
		copyID   int32
	}
	joinedWith struct {
		teamID   int64
		playerID int64
	}
	leaveCalls int
}

func (f *fakeRepo) FindByPlayer(context.Context, int64) (*Team, error) {
	return f.team, f.findErr
}

func (f *fakeRepo) Create(_ context.Context, leaderID int64, copyID int32) (int64, error) {
	f.createdWith.leaderID = leaderID
	f.createdWith.copyID = copyID
	return f.createID, f.createErr
}

func (f *fakeRepo) Join(_ context.Context, teamID, playerID int64) error {
	f.joinedWith.teamID = teamID
	f.joinedWith.playerID = playerID
	return f.joinErr
}

func (f *fakeRepo) Leave(context.Context, int64) error {
	f.leaveCalls++
	return f.leaveErr
}

func (f *fakeRepo) Members(context.Context, int64) ([]Member, error) {
	return f.members, f.memErr
}

// ============================================================================
// 装配
// ============================================================================

func newTestModule(repo teamRepo) (*Module, *moduletest.State, *moduletest.Recorder, *module.Context) {
	st := moduletest.NewState(100)
	rec := moduletest.NewRecorder()
	m := New(repo, log.New("team-test", log.WithLevel("error")))
	return m, st, rec, moduletest.NewContext(st, rec)
}

func call(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, reqID, respID uint32, req, resp proto.Message) {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, reqID, respID, req)
	require.NoError(t, err)
	require.NotNil(t, raw, "没有回包: req=%d", reqID)
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.Equal(t, 1, rec.Count(respID), "一次请求只能回一个包")
}

// ============================================================================
// 查询
// ============================================================================

// TestInfoNotInTeam 不在队回 6201。
func TestInfoNotInTeam(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{team: nil})
	resp := &gamev1.TeamInfoResp{}
	call(t, m, ctx, rec, msgIDInfoReq, msgIDInfoResp, &gamev1.TeamInfoReq{}, resp)
	require.EqualValues(t, codeNotInTeam, resp.Code)
}

// TestInfoNoRepoDegrades 无库环境按「不在队」降级，不能报错。
func TestInfoNoRepoDegrades(t *testing.T) {
	m, _, rec, ctx := newTestModule(nil)
	resp := &gamev1.TeamInfoResp{}
	call(t, m, ctx, rec, msgIDInfoReq, msgIDInfoResp, &gamev1.TeamInfoReq{}, resp)
	require.EqualValues(t, codeNotInTeam, resp.Code)
}

// TestInfoReturnsTeamAndMembers 在队：下发队伍 ID、副本与成员（含队长标记）。
func TestInfoReturnsTeamAndMembers(t *testing.T) {
	repo := &fakeRepo{
		team: &Team{ID: 7, LeaderID: 100, CopyID: 3},
		members: []Member{
			{PlayerID: 100, Name: "阿甲", IsLeader: true},
			{PlayerID: 200, Name: "阿乙", Ready: true},
		},
	}
	m, _, rec, ctx := newTestModule(repo)

	resp := &gamev1.TeamInfoResp{}
	call(t, m, ctx, rec, msgIDInfoReq, msgIDInfoResp, &gamev1.TeamInfoReq{}, resp)
	require.EqualValues(t, codeOK, resp.Code)
	require.EqualValues(t, 7, resp.TeamId)
	require.EqualValues(t, 3, resp.CopyId)
	require.Len(t, resp.Members, 2)
	require.True(t, resp.Members[0].IsLeader)
	require.False(t, resp.Members[1].IsLeader)
	require.EqualValues(t, "阿乙", resp.Members[1].Name)
	require.True(t, resp.Members[1].Ready)
}

// TestInfoPropagatesRepoErrors 查库真故障要上抛（不能让客户端以为「不在队」）。
func TestInfoPropagatesRepoErrors(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{findErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m, ctx, rec, msgIDInfoReq, msgIDInfoResp, &gamev1.TeamInfoReq{})
	require.Error(t, err)

	// 队伍查到了但成员查询失败，同样要上抛
	m2, _, rec2, ctx2 := newTestModule(&fakeRepo{
		team:   &Team{ID: 7},
		memErr: context.DeadlineExceeded,
	})
	_, err = moduletest.Call(m2, ctx2, rec2, msgIDInfoReq, msgIDInfoResp, &gamev1.TeamInfoReq{})
	require.Error(t, err)
}

// ============================================================================
// 创建
// ============================================================================

// TestCreateRejectsWhenAlreadyInTeam 已在队拒绝建队（一人只能在一队）。
func TestCreateRejectsWhenAlreadyInTeam(t *testing.T) {
	repo := &fakeRepo{team: &Team{ID: 7}}
	m, _, rec, ctx := newTestModule(repo)

	resp := &gamev1.TeamCreateResp{}
	call(t, m, ctx, rec, msgIDCreateReq, msgIDCreateResp, &gamev1.TeamCreateReq{CopyId: 3}, resp)
	require.EqualValues(t, codeAlreadyIn, resp.Code)
	require.Zero(t, repo.createdWith.leaderID, "已在队时不能调 Create")
}

// TestCreatePassesLeaderAndCopy 建队：队长取会话身份，副本取协议。
func TestCreatePassesLeaderAndCopy(t *testing.T) {
	repo := &fakeRepo{createID: 88}
	m, _, rec, ctx := newTestModule(repo)

	resp := &gamev1.TeamCreateResp{}
	call(t, m, ctx, rec, msgIDCreateReq, msgIDCreateResp, &gamev1.TeamCreateReq{CopyId: 3}, resp)
	require.EqualValues(t, codeOK, resp.Code)
	require.EqualValues(t, 88, resp.TeamId)
	require.EqualValues(t, 100, repo.createdWith.leaderID, "队长必须是会话里的玩家，不取协议体")
	require.EqualValues(t, 3, repo.createdWith.copyID)
}

// TestCreateNoRepoDegrades 无库环境按成功降级（协议演示不中断）。
func TestCreateNoRepoDegrades(t *testing.T) {
	m, _, rec, ctx := newTestModule(nil)
	resp := &gamev1.TeamCreateResp{}
	call(t, m, ctx, rec, msgIDCreateReq, msgIDCreateResp, &gamev1.TeamCreateReq{}, resp)
	require.EqualValues(t, codeOK, resp.Code)
}

// TestCreatePropagatesErrors 前置查询失败与建队失败都要上抛。
func TestCreatePropagatesErrors(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{findErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m, ctx, rec, msgIDCreateReq, msgIDCreateResp, &gamev1.TeamCreateReq{})
	require.Error(t, err)

	m2, _, rec2, ctx2 := newTestModule(&fakeRepo{createErr: context.DeadlineExceeded})
	_, err = moduletest.Call(m2, ctx2, rec2, msgIDCreateReq, msgIDCreateResp, &gamev1.TeamCreateReq{})
	require.Error(t, err)
}

// ============================================================================
// 加入
// ============================================================================

// TestJoinTranslatesRepoErrors 仓储哨兵错误翻译成业务码。
func TestJoinTranslatesRepoErrors(t *testing.T) {
	cases := []struct {
		err  error
		want int32
	}{
		{ErrTeamNotExist, codeTeamNotExist},
		{ErrTeamFull, codeTeamFull},
		{ErrAlreadyIn, codeAlreadyIn},
	}
	for _, c := range cases {
		repo := &fakeRepo{joinErr: c.err}
		m, _, rec, ctx := newTestModule(repo)
		resp := &gamev1.TeamJoinResp{}
		call(t, m, ctx, rec, msgIDJoinReq, msgIDJoinResp, &gamev1.TeamJoinReq{TeamId: 7}, resp)
		require.EqualValues(t, c.want, resp.Code, fmt.Sprintf("err=%v", c.err))
	}
}

// TestJoinRejectsWhenAlreadyInTeam 已在某队时拒绝加入另一支（不做自动换队）。
func TestJoinRejectsWhenAlreadyInTeam(t *testing.T) {
	repo := &fakeRepo{team: &Team{ID: 7}}
	m, _, rec, ctx := newTestModule(repo)

	resp := &gamev1.TeamJoinResp{}
	call(t, m, ctx, rec, msgIDJoinReq, msgIDJoinResp, &gamev1.TeamJoinReq{TeamId: 9}, resp)
	require.EqualValues(t, codeAlreadyIn, resp.Code)
	require.Zero(t, repo.joinedWith.teamID, "已在队时不能调 Join")
}

// TestJoinSucceeds 正常加入：传对队伍 ID 与会话身份。
func TestJoinSucceeds(t *testing.T) {
	repo := &fakeRepo{}
	m, _, rec, ctx := newTestModule(repo)

	resp := &gamev1.TeamJoinResp{}
	call(t, m, ctx, rec, msgIDJoinReq, msgIDJoinResp, &gamev1.TeamJoinReq{TeamId: 9}, resp)
	require.EqualValues(t, codeOK, resp.Code)
	require.EqualValues(t, 9, repo.joinedWith.teamID)
	require.EqualValues(t, 100, repo.joinedWith.playerID)
}

// TestJoinUnknownErrorPropagates 非哨兵错误（DB 挂了）必须上抛。
func TestJoinUnknownErrorPropagates(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{joinErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m, ctx, rec, msgIDJoinReq, msgIDJoinResp, &gamev1.TeamJoinReq{TeamId: 7})
	require.Error(t, err)
}

// TestJoinNoRepoDegrades 无库环境按成功降级。
func TestJoinNoRepoDegrades(t *testing.T) {
	m, _, rec, ctx := newTestModule(nil)
	resp := &gamev1.TeamJoinResp{}
	call(t, m, ctx, rec, msgIDJoinReq, msgIDJoinResp, &gamev1.TeamJoinReq{TeamId: 7}, resp)
	require.EqualValues(t, codeOK, resp.Code)
}

// ============================================================================
// 退出
// ============================================================================

// TestLeave 正常退队；不在队回 6201；其他错误上抛。
func TestLeave(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{})
	resp := &gamev1.TeamLeaveResp{}
	call(t, m, ctx, rec, msgIDLeaveReq, msgIDLeaveResp, &gamev1.TeamLeaveReq{}, resp)
	require.EqualValues(t, codeOK, resp.Code)

	m2, _, rec2, ctx2 := newTestModule(&fakeRepo{leaveErr: ErrNotInTeam})
	resp2 := &gamev1.TeamLeaveResp{}
	call(t, m2, ctx2, rec2, msgIDLeaveReq, msgIDLeaveResp, &gamev1.TeamLeaveReq{}, resp2)
	require.EqualValues(t, codeNotInTeam, resp2.Code)

	m3, _, rec3, ctx3 := newTestModule(&fakeRepo{leaveErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m3, ctx3, rec3, msgIDLeaveReq, msgIDLeaveResp, &gamev1.TeamLeaveReq{})
	require.Error(t, err)
}

// TestLeaveNoRepoDegrades 无库环境按成功降级。
func TestLeaveNoRepoDegrades(t *testing.T) {
	m, _, rec, ctx := newTestModule(nil)
	resp := &gamev1.TeamLeaveResp{}
	call(t, m, ctx, rec, msgIDLeaveReq, msgIDLeaveResp, &gamev1.TeamLeaveReq{}, resp)
	require.EqualValues(t, codeOK, resp.Code)
}

// ============================================================================
// 掉线
// ============================================================================

// TestLogoutLeavesTeam 掉线自动退队（临时关系，防挂机占位）。
func TestLogoutLeavesTeam(t *testing.T) {
	repo := &fakeRepo{}
	m, _, _, ctx := newTestModule(repo)
	require.NoError(t, m.OnLogout(ctx))
	require.Equal(t, 1, repo.leaveCalls, "掉线必须退队")
}

// TestLogoutSwallowsRepoError 登出流程不能被 DB 抖动卡住：错误只忽略。
func TestLogoutSwallowsRepoError(t *testing.T) {
	m, _, _, ctx := newTestModule(&fakeRepo{leaveErr: context.DeadlineExceeded})
	require.NoError(t, m.OnLogout(ctx), "退队失败不能阻塞登出")
}

// TestLogoutNoRepo 无库环境不 panic。
func TestLogoutNoRepo(t *testing.T) {
	m, _, _, ctx := newTestModule(nil)
	require.NoError(t, m.OnLogout(ctx))
}

// ============================================================================
// 配置
// ============================================================================

// TestMaxMembersFallback 队伍上限必须为正：配置缺失/首行为 0 时回退 5。
func TestMaxMembersFallback(t *testing.T) {
	require.Greater(t, maxMembers(), 0, "上限不能是 0，否则永远建不了队")
	require.LessOrEqual(t, maxMembers(), 100, "上限应是合理小数字（配置写错时能发现）")
}
